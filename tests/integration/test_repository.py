import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.fakes import open_connection
from webhook_receiver.store.repository import (
    COUNT_BY_STATUS_SQL,
    COUNT_PENDING_SQL,
    DELETE_SQL,
    LOOKUP_SQL,
    OLDEST_DUE_SQL,
    IntakeOutcome,
    NewWebhook,
    QueueStats,
    delete_expired,
    intake,
    probe,
    queue_stats,
)

NOW = 1_760_000_000_000


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    connection = open_connection(tmp_path / "x.db")
    yield connection
    connection.close()


def new(key: str = "evt-1", digest: str = "h1") -> NewWebhook:
    return NewWebhook(
        idempotency_key=key,
        event_type="order.created",
        payload_json='{"order_id": "ORD-1"}',
        payload_hash=digest,
        request_id="req-1",
    )


def add(conn: sqlite3.Connection, key: str, *, at: int, status: str = "pending") -> None:
    intake(conn, new=new(key), now_ms=at, backlog_limit=1_000_000)
    if status != "pending":
        conn.execute(
            "UPDATE webhooks SET status = :status, next_attempt_at = NULL, finished_at = :at "
            "WHERE idempotency_key = :key",
            {"status": status, "at": at, "key": key},
        )


def test_new_webhook_is_created_pending_and_due_now(conn: sqlite3.Connection) -> None:
    result = intake(conn, new=new(), now_ms=NOW, backlog_limit=10)
    assert result.outcome is IntakeOutcome.CREATED
    assert result.record is not None
    assert (result.record.status, result.record.attempts) == ("pending", 0)
    assert result.record.next_attempt_at == result.record.received_at == NOW


def test_same_key_is_a_duplicate_or_a_conflict_by_hash(conn: sqlite3.Connection) -> None:
    first = intake(conn, new=new(), now_ms=NOW, backlog_limit=10)
    duplicate = intake(conn, new=new(), now_ms=NOW + 5, backlog_limit=10)
    conflict = intake(conn, new=new(digest="h2"), now_ms=NOW + 5, backlog_limit=10)
    assert (duplicate.outcome, duplicate.record) == (IntakeOutcome.DUPLICATE, first.record)
    assert (conflict.outcome, conflict.record) == (IntakeOutcome.CONFLICT, first.record)
    assert conn.execute("SELECT count(*) FROM webhooks").fetchone()[0] == 1


def test_backlog_limit_rejects_only_new_events(conn: sqlite3.Connection) -> None:
    intake(conn, new=new("a"), now_ms=NOW, backlog_limit=2)
    intake(conn, new=new("b"), now_ms=NOW, backlog_limit=2)
    rejected = intake(conn, new=new("c"), now_ms=NOW, backlog_limit=2)
    duplicate = intake(conn, new=new("a"), now_ms=NOW, backlog_limit=2)
    assert (rejected.outcome, rejected.record) == (IntakeOutcome.BACKLOG_FULL, None)
    assert duplicate.outcome is IntakeOutcome.DUPLICATE


@pytest.mark.slow
def test_intake_stays_fast_with_a_full_backlog(conn: sqlite3.Connection) -> None:
    for i in range(4_999):
        add(conn, f"seed-{i}", at=NOW)
    started = time.perf_counter()
    for i in range(100):
        intake(conn, new=new(f"probe-{i}"), now_ms=NOW, backlog_limit=5_000)
    average_ms = (time.perf_counter() - started) * 10
    assert average_ms < 5


def test_delete_expired_removes_old_finished_rows_in_batches(conn: sqlite3.Connection) -> None:
    for i in range(3):
        add(conn, f"old-{i}", at=NOW - 10_000, status="completed")
    add(conn, "old-failed", at=NOW - 10_000, status="failed")
    add(conn, "fresh", at=NOW, status="completed")
    add(conn, "waiting", at=NOW - 10_000)
    assert delete_expired(conn, cutoff_ms=NOW - 5_000, batch=3) == 3
    assert delete_expired(conn, cutoff_ms=NOW - 5_000, batch=3) == 1
    keys = {row[0] for row in conn.execute("SELECT idempotency_key FROM webhooks")}
    assert keys == {"fresh", "waiting"}


def test_queue_stats_counts_active_rows_and_lag(conn: sqlite3.Connection) -> None:
    add(conn, "due-early", at=NOW - 3_000)
    add(conn, "due-late", at=NOW - 1_000)
    add(conn, "future", at=NOW + 60_000)
    add(conn, "done", at=NOW - 50_000, status="completed")
    add(conn, "dead", at=NOW - 50_000, status="failed")
    probe(conn, now_ms=NOW)
    assert queue_stats(conn, now_ms=NOW) == QueueStats(
        pending=3, processing=0, failed=1, lag_ms=3_000
    )
    assert conn.execute("SELECT probed_at FROM meta").fetchone()[0] == NOW


def test_queries_use_an_index_instead_of_a_full_scan(conn: sqlite3.Connection) -> None:
    queries: list[tuple[str, dict[str, int | str]]] = [
        (LOOKUP_SQL, {"key": "k"}),
        (COUNT_PENDING_SQL, {}),
        (DELETE_SQL, {"cutoff": NOW, "batch": 10}),
        (COUNT_BY_STATUS_SQL, {"status": "failed"}),
        (OLDEST_DUE_SQL, {}),
    ]
    for sql, params in queries:
        plan = [row[3] for row in conn.execute(f"EXPLAIN QUERY PLAN {sql}", params)]
        assert "SCAN webhooks" not in plan, sql
        assert any("USING" in detail and "INDEX" in detail for detail in plan), sql
