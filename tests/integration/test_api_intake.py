from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from tests.fakes import HEADERS, FakeClock, make_settings, webhook_body
from webhook_receiver.api.context import AppContext
from webhook_receiver.app import create_app


def open_client(tmp_path: Path, **overrides: Any) -> TestClient:
    return TestClient(create_app(make_settings(tmp_path, **overrides), clock=FakeClock()))


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    with open_client(tmp_path) as test_client:
        yield test_client


def send(
    client: TestClient, body: bytes | None = None, *, key: str = "evt-1", **header_overrides: str
) -> httpx.Response:
    content = body if body is not None else webhook_body(key)
    headers = HEADERS | {name.replace("_", "-"): value for name, value in header_overrides.items()}
    response: httpx.Response = client.post("/v1/webhooks", content=content, headers=headers)
    return response


def test_new_webhook_is_accepted(client: TestClient) -> None:
    response = send(client, X_Request_ID="trace-42")
    assert response.status_code == 202
    assert response.headers["Location"] == "/v1/webhooks/evt-1"
    assert response.headers["X-Request-ID"] == "trace-42"
    body = response.json()
    assert (body["idempotency_key"], body["status"], body["attempts"]) == ("evt-1", "pending", 0)


def test_duplicate_with_same_payload_returns_200(client: TestClient) -> None:
    send(client)
    response = send(client)
    assert response.status_code == 200
    assert response.headers["Idempotent-Replayed"] == "true"
    assert response.headers["Location"] == "/v1/webhooks/evt-1"


def test_reused_key_with_different_payload_is_a_conflict(client: TestClient) -> None:
    send(client)
    response = send(client, webhook_body(payload={"order_id": "ORD-2"}))
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "idempotency_conflict"
    assert response.json()["error"]["details"] == {"idempotency_key": "evt-1"}


def test_missing_or_wrong_api_key_is_401(client: TestClient) -> None:
    for authorization in ("", "Bearer wrong-key-0123456789"):
        response = send(client, Authorization=authorization)
        assert (response.status_code, response.json()["error"]["code"]) == (401, "unauthorized")


def test_oversized_body_is_413_before_authentication(tmp_path: Path) -> None:
    with open_client(tmp_path, max_body_bytes=1_024) as small:
        response = small.post(
            "/v1/webhooks", content=b"x" * 1_025, headers={"Content-Type": "application/json"}
        )
    assert (response.status_code, response.json()["error"]["code"]) == (413, "payload_too_large")


def test_content_type_must_be_json_with_an_optional_charset(client: TestClient) -> None:
    rejected = send(client, Content_Type="text/plain")
    assert (rejected.status_code, rejected.json()["error"]["code"]) == (
        415,
        "unsupported_media_type",
    )
    assert send(client, Content_Type="application/json; charset=utf-8").status_code == 202


@pytest.mark.parametrize(
    "content",
    [
        b"{not json",
        b'{"idempotency_key":"evt-1","event_type":"e","payload":{"x":NaN}}',
        b'{"idempotency_key":"evt-1","event_type":"e","payload":{"x":1e999}}',
        b'{"idempotency_key":"evt-1","event_type":"e","payload":{"x":"\\ud800"}}',
        b'{"idempotency_key":"evt-1","event_type":"e","payload":'
        + b"[" * 20_000
        + b"]" * 20_000
        + b"}",
    ],
)
def test_invalid_bodies_are_422(client: TestClient, content: bytes) -> None:
    response = send(client, content)
    assert (response.status_code, response.json()["error"]["code"]) == (422, "validation_failed")


def test_rate_limit_returns_429_with_retry_after(tmp_path: Path) -> None:
    with open_client(tmp_path, rate_limit_per_s=1.0, rate_limit_burst=2) as limited:
        codes = [send(limited, key=f"evt-{i}").status_code for i in range(3)]
        response = send(limited, key="evt-9")
    assert codes == [202, 202, 429]
    assert response.headers["Retry-After"] == "1"
    assert response.json()["error"] == {
        "code": "rate_limited",
        "message": "too many requests; retry in 1 s",
        "details": {"retry_after_s": 1},
    }


def test_backlog_full_returns_503_but_duplicates_still_200(tmp_path: Path) -> None:
    with open_client(tmp_path, backlog_limit=1) as full:
        assert send(full, key="evt-1").status_code == 202
        rejected = send(full, key="evt-2")
        duplicate = send(full, key="evt-1")
    assert (rejected.status_code, rejected.headers["Retry-After"]) == (503, "5")
    assert rejected.json()["error"] == {
        "code": "backlog_full",
        "message": "the receiver is at capacity; retry in 5 s",
        "details": {"retry_after_s": 5},
    }
    assert duplicate.status_code == 200


def test_intake_outcomes_are_counted(client: TestClient) -> None:
    send(client)
    send(client)
    send(client, webhook_body(payload={"changed": True}))
    send(client, Authorization="")
    context = client.app.state.context  # type: ignore[attr-defined]
    assert isinstance(context, AppContext)
    sample = context.metrics.registry.get_sample_value
    for outcome in ("accepted", "duplicate", "conflict", "rejected_auth"):
        assert sample("webhooks_received_total", {"outcome": outcome}) == 1


def test_api_key_is_optional(tmp_path: Path) -> None:
    with open_client(tmp_path, api_key=None) as open_intake:
        response = send(open_intake, Authorization="")
    assert response.status_code == 202


def test_api_docs_are_not_served(client: TestClient) -> None:
    for path in ("/docs", "/redoc", "/openapi.json"):
        response = client.get(path)
        assert (response.status_code, response.json()["error"]["code"]) == (404, "not_found")
