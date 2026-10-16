from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from tests.e2e.conftest import ServerProcess, WebhookClient


def test_webhook_is_processed_once_and_duplicates_are_answered_from_storage(
    make_server: Callable[..., ServerProcess],
) -> None:
    server = make_server(WEBHOOK_PROCESSING_MIN_S="1.0", WEBHOOK_PROCESSING_MAX_S="1.5")
    client = WebhookClient(server.url)
    accepted = client.send("evt-001")
    assert (accepted.status_code, accepted.headers["Location"]) == (202, "/v1/webhooks/evt-001")
    during = client.send("evt-001")
    assert (during.status_code, during.json()["status"]) in {(200, "pending"), (200, "processing")}
    body = client.wait_for("evt-001")
    assert (body["status"], body["attempts"]) == ("completed", 1)
    assert body["result"]["event_type"] == "order.created"
    after = client.send("evt-001")
    assert (after.status_code, after.json()["status"]) == (200, "completed")
    client.close()
    server.stop()
    assert [row["process_count"] for row in server.rows()] == [1]


def test_parallel_duplicates_are_processed_once(make_server: Callable[..., ServerProcess]) -> None:
    server = make_server(WEBHOOK_RATE_LIMIT_BURST="1000")
    client = WebhookClient(server.url)
    keys = [f"evt-par-{i % 20}" for i in range(200)]
    with ThreadPoolExecutor(max_workers=50) as pool:
        codes = Counter(response.status_code for response in pool.map(client.send, keys))
    assert codes == Counter({202: 20, 200: 180})
    for key in set(keys):
        assert client.wait_for(key)["status"] == "completed"
    client.close()
    server.stop()
    rows = server.rows()
    assert len(rows) == 20
    assert {row["process_count"] for row in rows} == {1}


BRIEF_EXAMPLES: list[dict[str, Any]] = [
    {
        "idempotency_key": "evt-001",
        "event_type": "order.created",
        "payload": {
            "order_id": "ORD-2024-1234",
            "customer": "Jan Kowalski",
            "amount": 299.99,
            "currency": "PLN",
        },
    },
    {
        "idempotency_key": "evt-002",
        "event_type": "order.paid",
        "payload": {
            "order_id": "ORD-2024-1234",
            "payment_method": "card",
            "paid_at": "2024-12-01T14:30:00Z",
        },
    },
    {
        "idempotency_key": "evt-003",
        "event_type": "order.shipped",
        "payload": {
            "order_id": "ORD-2024-1234",
            "tracking_number": "PL1234567890",
            "carrier": "DPD",
        },
    },
    {
        "idempotency_key": "evt-004",
        "event_type": "user.registered",
        "payload": {"user_id": "USR-9876", "email": "jan@example.com"},
    },
    {
        "idempotency_key": "evt-005",
        "event_type": "payment.failed",
        "payload": {"order_id": "ORD-2024-5678", "reason": "insufficient_funds"},
    },
]


def test_brief_examples_are_each_processed_once(
    make_server: Callable[..., ServerProcess],
) -> None:
    server = make_server()
    client = WebhookClient(server.url)
    for document in BRIEF_EXAMPLES:
        assert client.http.post("/v1/webhooks", json=document).status_code == 202
    for document in BRIEF_EXAMPLES:
        body = client.wait_for(document["idempotency_key"])
        assert (body["status"], body["result"]["event_type"]) == (
            "completed",
            document["event_type"],
        )
    resent = [client.http.post("/v1/webhooks", json=document) for document in BRIEF_EXAMPLES]
    assert {(r.status_code, r.json()["status"]) for r in resent} == {(200, "completed")}
    client.close()
    server.stop()
    rows = server.rows()
    assert sorted(row["idempotency_key"] for row in rows) == [f"evt-00{i}" for i in range(1, 6)]
    assert {row["process_count"] for row in rows} == {1}
