import sqlite3
import threading
from pathlib import Path

import pytest

from webhook_receiver.store.db import Database


async def test_open_applies_pragmas(tmp_path: Path) -> None:
    db = Database(tmp_path / "x.db", synchronous="FULL")
    await db.open()
    try:
        pragmas = await db.run(
            lambda conn: {
                name: conn.execute(f"PRAGMA {name}").fetchone()[0]
                for name in ("journal_mode", "synchronous", "busy_timeout", "locking_mode")
            }
        )
    finally:
        await db.close()
    assert pragmas == {
        "journal_mode": "wal",
        "synchronous": 2,
        "busy_timeout": 5000,
        "locking_mode": "exclusive",
    }


async def test_all_calls_run_on_one_thread(tmp_path: Path) -> None:
    db = Database(tmp_path / "x.db")
    await db.open()
    try:
        names = {await db.run(lambda _conn: threading.current_thread().name) for _ in range(20)}
    finally:
        await db.close()
    assert len(names) == 1
    assert names.pop().startswith("sqlite")


async def test_transaction_rolls_back_on_error(tmp_path: Path) -> None:
    db = Database(tmp_path / "x.db")
    await db.open()

    def insert_then_fail(conn: sqlite3.Connection) -> None:
        conn.execute("INSERT INTO t VALUES (1)")
        raise RuntimeError("boom")

    try:
        await db.run(lambda conn: conn.execute("CREATE TABLE t (v INTEGER)"))
        await db.transaction(lambda conn: conn.execute("INSERT INTO t VALUES (0)"))
        with pytest.raises(RuntimeError, match="boom"):
            await db.transaction(insert_then_fail)
        count = await db.run(lambda conn: conn.execute("SELECT count(*) FROM t").fetchone()[0])
        in_transaction = await db.run(lambda conn: conn.in_transaction)
    finally:
        await db.close()
    assert count == 1
    assert in_transaction is False


async def test_second_process_cannot_open_the_same_file(tmp_path: Path) -> None:
    path = tmp_path / "x.db"
    db = Database(path)
    await db.open()
    try:
        other = sqlite3.connect(path, timeout=0)
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            other.execute("SELECT count(*) FROM sqlite_master").fetchone()
        other.close()
    finally:
        await db.close()


async def test_failed_commit_rolls_back_and_the_connection_stays_usable(tmp_path: Path) -> None:
    db = Database(tmp_path / "x.db")
    await db.open()

    def violate_deferred_key(conn: sqlite3.Connection) -> None:
        conn.execute("INSERT INTO child VALUES (1, 999)")

    try:
        await db.run(lambda conn: conn.execute("PRAGMA foreign_keys = ON"))
        await db.run(lambda conn: conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)"))
        await db.run(
            lambda conn: conn.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER "
                "REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED)"
            )
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            await db.transaction(violate_deferred_key)
        await db.transaction(lambda conn: conn.execute("INSERT INTO parent VALUES (1)"))
        rows = await db.run(lambda conn: conn.execute("SELECT count(*) FROM child").fetchone()[0])
    finally:
        await db.close()
    assert rows == 0
