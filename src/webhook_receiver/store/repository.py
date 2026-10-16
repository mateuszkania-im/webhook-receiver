import json
import sqlite3
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Self

_RECORD_COLUMNS = (
    "id, idempotency_key, event_type, payload_hash, status, attempts, next_attempt_at, "
    "last_error, result, received_at, started_at, finished_at"
)

LOOKUP_SQL = f"SELECT {_RECORD_COLUMNS} FROM webhooks WHERE idempotency_key = :key"
COUNT_PENDING_SQL = "SELECT count(*) FROM webhooks WHERE status = 'pending'"
_INSERT_SQL = f"""
INSERT INTO webhooks (idempotency_key, event_type, payload, payload_hash, status,
                      next_attempt_at, request_id, received_at)
VALUES (:key, :event_type, :payload, :hash, 'pending', :now, :request_id, :now)
RETURNING {_RECORD_COLUMNS}
"""
DELETE_SQL = """
DELETE FROM webhooks
WHERE id IN (
    SELECT id FROM webhooks
    WHERE status IN ('completed', 'failed') AND finished_at < :cutoff
    LIMIT :batch
)
"""
COUNT_BY_STATUS_SQL = "SELECT count(*) FROM webhooks WHERE status = :status"
OLDEST_DUE_SQL = "SELECT min(next_attempt_at) FROM webhooks WHERE status = 'pending'"


@dataclass(frozen=True, slots=True)
class WebhookRecord:
    id: int
    idempotency_key: str
    event_type: str
    payload_hash: str
    status: str
    attempts: int
    next_attempt_at: int | None
    last_error: str | None
    result: str | None
    received_at: int
    started_at: int | None
    finished_at: int | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Self:
        return cls(**dict(row))


@dataclass(frozen=True, slots=True)
class NewWebhook:
    idempotency_key: str
    event_type: str
    payload_json: str
    payload_hash: str
    request_id: str


class IntakeOutcome(StrEnum):
    CREATED = "created"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"
    BACKLOG_FULL = "backlog_full"


@dataclass(frozen=True, slots=True)
class IntakeResult:
    outcome: IntakeOutcome
    record: WebhookRecord | None


@dataclass(frozen=True, slots=True)
class QueueStats:
    pending: int
    processing: int
    failed: int
    lag_ms: int


def get_webhook(conn: sqlite3.Connection, *, key: str) -> WebhookRecord | None:
    row = conn.execute(LOOKUP_SQL, {"key": key}).fetchone()
    return None if row is None else WebhookRecord.from_row(row)


def intake(
    conn: sqlite3.Connection, *, new: NewWebhook, now_ms: int, backlog_limit: int
) -> IntakeResult:
    existing = get_webhook(conn, key=new.idempotency_key)
    if existing is not None:
        same = existing.payload_hash == new.payload_hash
        return IntakeResult(IntakeOutcome.DUPLICATE if same else IntakeOutcome.CONFLICT, existing)
    if conn.execute(COUNT_PENDING_SQL).fetchone()[0] >= backlog_limit:
        return IntakeResult(IntakeOutcome.BACKLOG_FULL, None)
    rows = conn.execute(
        _INSERT_SQL,
        {
            "key": new.idempotency_key,
            "event_type": new.event_type,
            "payload": new.payload_json,
            "hash": new.payload_hash,
            "request_id": new.request_id,
            "now": now_ms,
        },
    ).fetchall()
    return IntakeResult(IntakeOutcome.CREATED, WebhookRecord.from_row(rows[0]))


def delete_expired(conn: sqlite3.Connection, *, cutoff_ms: int, batch: int) -> int:
    return conn.execute(DELETE_SQL, {"cutoff": cutoff_ms, "batch": batch}).rowcount


def queue_stats(conn: sqlite3.Connection, *, now_ms: int) -> QueueStats:
    def count(status: str) -> int:
        return int(conn.execute(COUNT_BY_STATUS_SQL, {"status": status}).fetchone()[0])

    oldest_due = conn.execute(OLDEST_DUE_SQL).fetchone()[0]
    lag = 0 if oldest_due is None else max(0, now_ms - oldest_due)
    return QueueStats(
        pending=count("pending"), processing=count("processing"), failed=count("failed"), lag_ms=lag
    )


def probe(conn: sqlite3.Connection, *, now_ms: int) -> None:
    conn.execute("UPDATE meta SET probed_at = :now WHERE id = 1", {"now": now_ms})


# INDEXED BY is deliberate: without ANALYZE statistics SQLite (3.46.1 and 3.53.1) picks
# ix_webhooks_status_finished for both branches and sorts every pending row.
CLAIM_SQL = """
UPDATE webhooks
SET status = 'processing', attempts = attempts + 1, lease_token = :token,
    lease_expires_at = :lease_until, started_at = coalesce(started_at, :now)
WHERE id = (
    SELECT id FROM (
        SELECT * FROM (
            SELECT id, next_attempt_at AS due FROM webhooks INDEXED BY ix_webhooks_pending_due
            WHERE status = 'pending' AND next_attempt_at <= :now
            ORDER BY next_attempt_at LIMIT 1
        )
        UNION ALL
        SELECT * FROM (
            SELECT id, lease_expires_at AS due FROM webhooks INDEXED BY ix_webhooks_processing_lease
            WHERE status = 'processing' AND lease_expires_at <= :now
            ORDER BY lease_expires_at LIMIT 1
        )
    )
    ORDER BY due LIMIT 1
)
RETURNING id, idempotency_key, event_type, payload, attempts, request_id, received_at
"""
_OWNED = "WHERE id = :id AND lease_token = :token AND status = 'processing'"
_CLEAR_LEASE = "lease_token = NULL, lease_expires_at = NULL"


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    id: int
    idempotency_key: str
    event_type: str
    payload: dict[str, Any]
    attempt: int
    lease_token: str
    request_id: str | None
    received_at: int


def claim_next(
    conn: sqlite3.Connection, *, now_ms: int, lease_ms: int, token: str
) -> ClaimedJob | None:
    rows = conn.execute(
        CLAIM_SQL, {"token": token, "lease_until": now_ms + lease_ms, "now": now_ms}
    ).fetchall()
    if not rows:
        return None
    row = rows[0]
    return ClaimedJob(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        event_type=row["event_type"],
        payload=json.loads(row["payload"]),
        attempt=row["attempts"],
        lease_token=token,
        request_id=row["request_id"],
        received_at=row["received_at"],
    )


def renew_lease(
    conn: sqlite3.Connection, *, job_id: int, token: str, lease_until_ms: int, now_ms: int
) -> bool:
    cursor = conn.execute(
        f"UPDATE webhooks SET lease_expires_at = :until {_OWNED}",
        {"id": job_id, "token": token, "until": lease_until_ms, "now": now_ms},
    )
    return cursor.rowcount == 1


def complete(
    conn: sqlite3.Connection, *, job_id: int, token: str, result_json: str, now_ms: int
) -> bool:
    return _finish(
        conn,
        "status = 'completed', result = :result, last_error = NULL, finished_at = :now, "
        "next_attempt_at = NULL, process_count = process_count + 1",
        {"id": job_id, "token": token, "result": result_json, "now": now_ms},
    )


def schedule_retry(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    token: str,
    error: str,
    next_attempt_at_ms: int,
    now_ms: int,
) -> bool:
    return _finish(
        conn,
        "status = 'pending', last_error = :error, next_attempt_at = :next",
        {"id": job_id, "token": token, "error": error, "next": next_attempt_at_ms, "now": now_ms},
    )


def fail(conn: sqlite3.Connection, *, job_id: int, token: str, error: str, now_ms: int) -> bool:
    return _finish(
        conn,
        "status = 'failed', last_error = :error, finished_at = :now, next_attempt_at = NULL",
        {"id": job_id, "token": token, "error": error, "now": now_ms},
    )


def release(conn: sqlite3.Connection, *, job_id: int, token: str, now_ms: int) -> bool:
    return _finish(
        conn,
        "status = 'pending', attempts = attempts - 1, next_attempt_at = :now",
        {"id": job_id, "token": token, "now": now_ms},
    )


def _finish(conn: sqlite3.Connection, assignments: str, params: dict[str, object]) -> bool:
    cursor = conn.execute(f"UPDATE webhooks SET {assignments}, {_CLEAR_LEASE} {_OWNED}", params)
    return cursor.rowcount == 1


def reset_in_flight(conn: sqlite3.Connection, *, now_ms: int) -> int:
    cursor = conn.execute(
        "UPDATE webhooks SET status = 'pending', lease_token = NULL, lease_expires_at = NULL, "
        "next_attempt_at = :now WHERE status = 'processing'",
        {"now": now_ms},
    )
    return cursor.rowcount
