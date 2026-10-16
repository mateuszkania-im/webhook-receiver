import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.fakes import open_connection
from webhook_receiver.store.schema import SCHEMA_VERSION, migrate

INSERT = (
    "INSERT INTO webhooks (idempotency_key, event_type, payload, payload_hash, status, "
    "next_attempt_at, lease_token, lease_expires_at, finished_at, process_count, "
    "received_at) "
    "VALUES (:key, 'e', '{}', 'h', :status, :next, :token, :lease, :finished, :count, 1)"
)


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    connection = open_connection(tmp_path / "x.db")
    yield connection
    connection.close()


def row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "key": "k",
        "status": "pending",
        "next": 1,
        "token": None,
        "lease": None,
        "finished": None,
        "count": 0,
    }
    values.update(overrides)
    return values


def test_migrate_is_idempotent_and_refuses_newer_schemas(conn: sqlite3.Connection) -> None:
    migrate(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION == 1
    conn.execute("PRAGMA user_version = 99")
    with pytest.raises(RuntimeError, match="newer than this build"):
        migrate(conn)


def test_invariants_are_enforced_by_the_schema(conn: sqlite3.Connection) -> None:
    conn.execute(INSERT, row())
    cases: list[tuple[dict[str, object], str]] = [
        ({}, "UNIQUE"),
        ({"key": "a", "status": "processing", "next": None}, "status = 'processing'"),
        ({"key": "b", "next": None}, "next_attempt_at IS NOT NULL"),
        ({"key": "c", "status": "completed", "next": None, "count": 1}, "finished_at IS NOT NULL"),
        ({"key": "d", "status": "completed", "next": None, "finished": 5, "count": 2}, "<= 1"),
    ]
    for overrides, message in cases:
        with pytest.raises(sqlite3.IntegrityError, match=message):
            conn.execute(INSERT, row(**overrides))


def test_failed_migration_leaves_no_partial_schema(tmp_path: Path) -> None:
    connection = sqlite3.connect(tmp_path / "y.db", autocommit=True)
    connection.execute("CREATE TABLE meta (id INTEGER)")
    with pytest.raises(sqlite3.OperationalError, match="already exists"):
        migrate(connection)
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master")}
    assert connection.in_transaction is False
    assert connection.execute("PRAGMA user_version").fetchone()[0] == 0
    assert "webhooks" not in tables
    connection.close()
