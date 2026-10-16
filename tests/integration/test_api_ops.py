import asyncio
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from tests.fakes import HEADERS, FakeClock, make_settings, webhook_body
from webhook_receiver.api.context import AppContext
from webhook_receiver.app import create_app
from webhook_receiver.store.repository import probe


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    with TestClient(
        create_app(make_settings(tmp_path, workers=2), clock=FakeClock())
    ) as test_client:
        yield test_client


def context(client: TestClient) -> AppContext:
    value = client.app.state.context  # type: ignore[attr-defined]
    assert isinstance(value, AppContext)
    return value


def test_health_and_readiness_report_ok(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}
    response = client.get("/readyz")
    assert response.status_code == 200
    assert response.json() == {"status": "ready", "checks": {"database": "ok", "workers": "ok"}}


def test_readyz_fails_when_workers_are_stale(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(context(client).pool, "alive", lambda now_ms, stale_ms: 1)
    response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["checks"]["workers"] == "1 of 2 alive"


def test_readyz_hides_database_errors_and_logs_them(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def broken(*_args: object) -> None:
        raise sqlite3.OperationalError("unable to open /data/webhooks.db")

    monkeypatch.setattr(context(client).db, "transaction", broken)
    response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["checks"]["database"] == "error"
    assert "/data/webhooks.db" not in response.text
    logged = capsys.readouterr().out
    assert "readiness_probe_failed" in logged
    assert "unable to open /data/webhooks.db" in logged


def test_metrics_endpoint_exposes_intake_and_http_metrics(client: TestClient) -> None:
    client.post("/v1/webhooks", content=webhook_body(), headers=HEADERS)
    text = client.get("/metrics/").text
    assert 'webhooks_received_total{outcome="accepted"} 1.0' in text
    assert (
        'http_request_duration_seconds_count{method="POST",route="/v1/webhooks",status="202"} 1.0'
        in text
    )
    assert "queue_depth" in text


def test_readiness_write_probe_runs_at_most_once_per_second(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = context(client)
    real_transaction = ctx.db.transaction
    probes = 0

    async def counting(fn: Any) -> Any:
        nonlocal probes
        probes += 1
        return await real_transaction(fn)

    monkeypatch.setattr(ctx.db, "transaction", counting)
    codes = {client.get("/readyz").status_code for _ in range(20)}
    assert (codes, probes) == ({200}, 1)
    assert isinstance(ctx.clock, FakeClock)
    ctx.clock.advance(1_000)
    client.get("/readyz")
    assert probes == 2


async def test_concurrent_readiness_probes_share_one_write(tmp_path: Path) -> None:
    app = create_app(make_settings(tmp_path), clock=FakeClock())
    ctx = app.state.context
    assert isinstance(ctx, AppContext)
    async with app.router.lifespan_context(app):
        real_transaction = ctx.db.transaction
        probes = 0

        async def counting(fn: Any) -> Any:
            nonlocal probes
            probes += getattr(fn, "func", None) is probe
            return await real_transaction(fn)

        ctx.db.transaction = counting  # type: ignore[method-assign]
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            responses = await asyncio.gather(*(client.get("/readyz") for _ in range(50)))
    assert {response.status_code for response in responses} == {200}
    assert probes == 1
