import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.fakes import open_connection
from webhook_receiver.store.repository import (
    CLAIM_SQL,
    NewWebhook,
    claim_next,
    complete,
    fail,
    get_webhook,
    intake,
    release,
    renew_lease,
    reset_in_flight,
    schedule_retry,
)

NOW = 1_760_000_000_000
LEASE = 30_000


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    connection = open_connection(tmp_path / "x.db")
    yield connection
    connection.close()


def add(conn: sqlite3.Connection, key: str, now_ms: int = NOW) -> None:
    webhook = NewWebhook(key, "order.created", '{"n": 1}', f"h-{key}", f"req-{key}")
    intake(conn, new=webhook, now_ms=now_ms, backlog_limit=1_000_000)


def test_claim_takes_the_oldest_due_event(conn: sqlite3.Connection) -> None:
    add(conn, "later", now_ms=NOW + 10_000)
    add(conn, "second", now_ms=NOW - 1_000)
    add(conn, "first", now_ms=NOW - 2_000)
    job = claim_next(conn, now_ms=NOW, lease_ms=LEASE, token="t1")
    assert job is not None
    assert (job.idempotency_key, job.attempt, job.lease_token) == ("first", 1, "t1")
    assert (job.payload, job.request_id) == ({"n": 1}, "req-first")
    row = conn.execute(
        "SELECT status, lease_expires_at, started_at FROM webhooks WHERE id = ?", (job.id,)
    ).fetchone()
    assert tuple(row) == ("processing", NOW + LEASE, NOW)


def test_a_leased_event_is_reclaimed_only_after_the_lease_expires(
    conn: sqlite3.Connection,
) -> None:
    add(conn, "only")
    first = claim_next(conn, now_ms=NOW, lease_ms=LEASE, token="t1")
    assert claim_next(conn, now_ms=NOW + LEASE - 1, lease_ms=LEASE, token="t2") is None
    second = claim_next(conn, now_ms=NOW + LEASE, lease_ms=LEASE, token="t2")
    assert first is not None
    assert second is not None
    assert (second.id, second.attempt, second.lease_token) == (first.id, 2, "t2")


def test_only_the_current_lease_holder_can_complete(conn: sqlite3.Connection) -> None:
    add(conn, "only")
    claim_next(conn, now_ms=NOW, lease_ms=LEASE, token="t1")
    job = claim_next(conn, now_ms=NOW + LEASE, lease_ms=LEASE, token="t2")
    assert job is not None
    assert complete(conn, job_id=job.id, token="t1", result_json="{}", now_ms=NOW + LEASE) is False
    assert complete(conn, job_id=job.id, token="t2", result_json='{"ok": true}', now_ms=NOW + LEASE)
    record = get_webhook(conn, key="only")
    assert record is not None
    assert (record.status, record.result, record.finished_at) == (
        "completed",
        '{"ok": true}',
        NOW + LEASE,
    )
    row = conn.execute("SELECT process_count, lease_token FROM webhooks").fetchone()
    assert tuple(row) == (1, None)
    assert claim_next(conn, now_ms=NOW + 10 * LEASE, lease_ms=LEASE, token="t3") is None


def test_stale_tokens_cannot_change_a_claimed_row(conn: sqlite3.Connection) -> None:
    add(conn, "only")
    job = claim_next(conn, now_ms=NOW, lease_ms=LEASE, token="t1")
    assert job is not None
    stale = "stale"
    assert not renew_lease(
        conn, job_id=job.id, token=stale, lease_until_ms=NOW + 2 * LEASE, now_ms=NOW
    )
    assert not schedule_retry(
        conn, job_id=job.id, token=stale, error="e", next_attempt_at_ms=NOW, now_ms=NOW
    )
    assert not fail(conn, job_id=job.id, token=stale, error="e", now_ms=NOW)
    assert not release(conn, job_id=job.id, token=stale, now_ms=NOW)
    row = conn.execute("SELECT status, lease_token, lease_expires_at FROM webhooks").fetchone()
    assert tuple(row) == ("processing", "t1", NOW + LEASE)


def test_renewal_keeps_the_event_from_being_reclaimed(conn: sqlite3.Connection) -> None:
    add(conn, "only")
    job = claim_next(conn, now_ms=NOW, lease_ms=LEASE, token="t1")
    assert job is not None
    assert renew_lease(conn, job_id=job.id, token="t1", lease_until_ms=NOW + 2 * LEASE, now_ms=NOW)
    assert claim_next(conn, now_ms=NOW + LEASE, lease_ms=LEASE, token="t2") is None


def test_retry_returns_the_event_to_pending_until_its_next_attempt(
    conn: sqlite3.Connection,
) -> None:
    add(conn, "only")
    job = claim_next(conn, now_ms=NOW, lease_ms=LEASE, token="t1")
    assert job is not None
    assert schedule_retry(
        conn,
        job_id=job.id,
        token="t1",
        error="RetryableError: x",
        next_attempt_at_ms=NOW + 4_000,
        now_ms=NOW,
    )
    record = get_webhook(conn, key="only")
    assert record is not None
    assert (record.status, record.last_error) == ("pending", "RetryableError: x")
    assert claim_next(conn, now_ms=NOW + 3_999, lease_ms=LEASE, token="t2") is None
    again = claim_next(conn, now_ms=NOW + 4_000, lease_ms=LEASE, token="t2")
    assert again is not None
    assert again.attempt == 2


def test_claim_uses_both_partial_indexes(conn: sqlite3.Connection) -> None:
    params = {"token": "t", "lease_until": NOW + LEASE, "now": NOW}
    details = [row[3] for row in conn.execute(f"EXPLAIN QUERY PLAN {CLAIM_SQL}", params)]
    assert "SCAN webhooks" not in details
    assert any("ix_webhooks_pending_due" in detail for detail in details)
    assert any("ix_webhooks_processing_lease" in detail for detail in details)


@pytest.mark.slow
def test_claim_stays_fast_with_a_day_of_history(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN")
    conn.executemany(
        "INSERT INTO webhooks (idempotency_key, event_type, payload, payload_hash, status, "
        "process_count, received_at, finished_at) "
        "VALUES (?, 'e', '{}', 'h', 'completed', 1, ?, ?)",
        [(f"old-{i}", NOW, NOW) for i in range(100_000)],
    )
    conn.execute("COMMIT")
    for i in range(100):
        add(conn, f"new-{i}")
    started = time.perf_counter()
    for i in range(100):
        assert claim_next(conn, now_ms=NOW, lease_ms=LEASE, token=f"t{i}") is not None
    average_ms = (time.perf_counter() - started) * 10
    assert average_ms < 5


def test_reset_in_flight_returns_crashed_rows_to_pending_keeping_attempts(
    conn: sqlite3.Connection,
) -> None:
    add(conn, "crashed")
    claim_next(conn, now_ms=NOW, lease_ms=LEASE, token="t1")
    assert reset_in_flight(conn, now_ms=NOW + 5) == 1
    again = claim_next(conn, now_ms=NOW + 5, lease_ms=LEASE, token="t2")
    assert again is not None
    assert (again.idempotency_key, again.attempt) == ("crashed", 2)
