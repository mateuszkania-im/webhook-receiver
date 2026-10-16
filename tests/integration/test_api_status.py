import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.fakes import HEADERS, FakeClock, make_settings, open_connection, webhook_body
from webhook_receiver.app import create_app
from webhook_receiver.store.repository import NewWebhook, claim_next, intake


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    settings = make_settings(
        tmp_path,
        workers=2,
        poll_interval_s=0.01,
        simulation_faults=True,
        max_attempts=2,
        backoff_base_s=0.001,
        backoff_cap_s=0.001,
    )
    with TestClient(create_app(settings, clock=FakeClock())) as test_client:
        yield test_client


def send(client: TestClient, key: str, payload: dict[str, Any] | None = None) -> None:
    response = client.post(
        "/v1/webhooks", content=webhook_body(key, payload=payload), headers=HEADERS
    )
    assert response.status_code == 202


def wait_for_status(client: TestClient, key: str, status: str) -> dict[str, Any]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        body: dict[str, Any] = client.get(f"/v1/webhooks/{key}", headers=HEADERS).json()
        if body.get("status") == status:
            return body
        time.sleep(0.01)
    raise AssertionError(f"{key} never reached {status}")


def test_accepted_webhook_is_processed_and_reported(client: TestClient) -> None:
    send(client, "evt-1")
    body = wait_for_status(client, "evt-1", "completed")
    assert body["attempts"] == 1
    assert body["result"]["event_type"] == "order.created"
    assert body["finished_at"].endswith("Z")


def test_keys_with_punctuation_work_in_paths(client: TestClient) -> None:
    send(client, "order:2024.12_A-1")
    body = wait_for_status(client, "order:2024.12_A-1", "completed")
    assert body["idempotency_key"] == "order:2024.12_A-1"


def test_failed_events_stay_visible_with_their_last_error(client: TestClient) -> None:
    send(client, "evt-bad", payload={"simulate": {"permanent": True}})
    send(client, "evt-flaky", payload={"simulate": {"fail_attempts": 5}})
    permanent = wait_for_status(client, "evt-bad", "failed")
    exhausted = wait_for_status(client, "evt-flaky", "failed")
    assert (permanent["attempts"], permanent["last_error"]) == (
        1,
        "PermanentError: simulated permanent failure",
    )
    assert exhausted["attempts"] == 2
    assert exhausted["last_error"].startswith("RetryableError")


def test_status_needs_the_api_key_and_404s_unknown_keys(client: TestClient) -> None:
    assert client.get("/v1/webhooks/missing").status_code == 401
    response = client.get("/v1/webhooks/missing", headers=HEADERS)
    assert (response.status_code, response.json()["error"]["code"]) == (404, "not_found")


def test_rows_left_processing_by_a_crash_resume_at_startup(tmp_path: Path) -> None:
    conn = open_connection(tmp_path / "webhooks.db")
    webhook = NewWebhook("evt-crash", "order.created", "{}", "h", "req")
    intake(conn, new=webhook, now_ms=1_760_000_000_000, backlog_limit=10)
    claim_next(conn, now_ms=1_760_000_000_000, lease_ms=3_600_000, token="dead-process")
    conn.close()
    settings = make_settings(tmp_path, workers=1, poll_interval_s=0.01)
    with TestClient(create_app(settings, clock=FakeClock())) as resumed:
        body = wait_for_status(resumed, "evt-crash", "completed")
    assert body["attempts"] == 2
