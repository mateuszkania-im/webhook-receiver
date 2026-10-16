import asyncio
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal

# EXCLUSIVE must come before WAL: the WAL index then lives in process memory and
# a second connection fails fast instead of racing (SQLite WAL-reset bug, < 3.51.3).
_PRAGMAS = ("locking_mode=EXCLUSIVE", "journal_mode=WAL")


class Database:
    def __init__(self, path: Path, synchronous: Literal["NORMAL", "FULL"] = "NORMAL") -> None:
        self._path = path
        self._synchronous = synchronous
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sqlite")
        self._conn: sqlite3.Connection | None = None

    async def open(self) -> None:
        await self._submit(self._open)

    async def close(self) -> None:
        if self._conn is not None:
            await self._submit(self._conn.close)
            self._conn = None
        self._executor.shutdown(wait=True)

    async def run[T](self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return await self._submit(lambda: fn(self._connection()))

    async def transaction[T](self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return await self._submit(lambda: self._in_transaction(fn))

    def _open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._path, autocommit=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        for pragma in (*_PRAGMAS, f"synchronous={self._synchronous}"):
            conn.execute(f"PRAGMA {pragma}")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("COMMIT")
        self._conn = conn

    def _in_transaction[T](self, fn: Callable[[sqlite3.Connection], T]) -> T:
        conn = self._connection()
        conn.execute("BEGIN IMMEDIATE")
        try:
            result = fn(conn)
            conn.execute("COMMIT")
        except BaseException:
            # SQLite may already have rolled back (SQLITE_FULL, IOERR); a failed COMMIT
            # (deferred constraint, busy) leaves the transaction open for every later call.
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        return result

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("database is not open")
        return self._conn

    async def _submit[T](self, fn: Callable[[], T]) -> T:
        return await asyncio.get_running_loop().run_in_executor(self._executor, fn)
