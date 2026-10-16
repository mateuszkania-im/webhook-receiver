from typing import Any

import pytest
from pydantic import ValidationError

from webhook_receiver.api.schemas import StatusDocument, WebhookIn
from webhook_receiver.store.repository import WebhookRecord


def test_invalid_webhooks_are_rejected() -> None:
    body = {"idempotency_key": "evt-1", "event_type": "order.created", "payload": {}}
    cases: list[dict[str, Any]] = [
        {"idempotency_key": "has space"},
        {"idempotency_key": "x" * 256},
        {"event_type": "Order.Created"},
        {"payload": []},
        {"extra": 1},
    ]
    for overrides in cases:
        with pytest.raises(ValidationError):
            WebhookIn.model_validate(body | overrides)


def record(**overrides: Any) -> WebhookRecord:
    values: dict[str, Any] = {
        "id": 1,
        "idempotency_key": "evt-1",
        "event_type": "order.created",
        "payload_hash": "h",
        "status": "completed",
        "attempts": 1,
        "next_attempt_at": None,
        "last_error": None,
        "result": '{"handled_at": "2025-10-15T12:00:03.512Z"}',
        "received_at": 1_760_529_600_104,
        "started_at": 1_760_529_600_120,
        "finished_at": 1_760_529_603_512,
    }
    values.update(overrides)
    return WebhookRecord(**values)


def test_status_document_matches_the_spec_example() -> None:
    assert StatusDocument.from_record(record()).model_dump(mode="json") == {
        "idempotency_key": "evt-1",
        "event_type": "order.created",
        "status": "completed",
        "attempts": 1,
        "next_attempt_at": None,
        "last_error": None,
        "result": {"handled_at": "2025-10-15T12:00:03.512Z"},
        "received_at": "2025-10-15T12:00:00.104Z",
        "started_at": "2025-10-15T12:00:00.120Z",
        "finished_at": "2025-10-15T12:00:03.512Z",
    }


def test_next_attempt_is_shown_only_for_pending_rows() -> None:
    pending = StatusDocument.from_record(
        record(status="pending", next_attempt_at=1_760_529_610_000, result=None)
    )
    processing = StatusDocument.from_record(
        record(status="processing", next_attempt_at=1_760_529_610_000)
    )
    assert pending.next_attempt_at == "2025-10-15T12:00:10.000Z"
    assert processing.next_attempt_at is None
