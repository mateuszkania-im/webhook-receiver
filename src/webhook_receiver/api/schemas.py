import json
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field

from webhook_receiver.clock import to_rfc3339
from webhook_receiver.store.repository import WebhookRecord

KEY_PATTERN = r"^[A-Za-z0-9._:-]{1,255}$"
EVENT_TYPE_PATTERN = r"^[a-z0-9_]+(\.[a-z0-9_]+)*$"


class WebhookIn(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    idempotency_key: str = Field(pattern=KEY_PATTERN)
    event_type: str = Field(max_length=100, pattern=EVENT_TYPE_PATTERN)
    payload: dict[str, Any]


class StatusDocument(BaseModel):
    idempotency_key: str
    event_type: str
    status: Literal["pending", "processing", "completed", "failed"]
    attempts: int
    next_attempt_at: str | None
    last_error: str | None
    result: dict[str, Any] | None
    received_at: str
    started_at: str | None
    finished_at: str | None

    @classmethod
    def from_record(cls, record: WebhookRecord) -> Self:
        scheduled = record.next_attempt_at if record.status == "pending" else None
        return cls.model_validate(
            {
                "idempotency_key": record.idempotency_key,
                "event_type": record.event_type,
                "status": record.status,
                "attempts": record.attempts,
                "next_attempt_at": _timestamp(scheduled),
                "last_error": record.last_error,
                "result": json.loads(record.result) if record.result else None,
                "received_at": to_rfc3339(record.received_at),
                "started_at": _timestamp(record.started_at),
                "finished_at": _timestamp(record.finished_at),
            }
        )


def _timestamp(ms: int | None) -> str | None:
    return None if ms is None else to_rfc3339(ms)
