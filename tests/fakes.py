import asyncio
import json
import sqlite3
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from webhook_receiver.config import Settings
from webhook_receiver.store.db import Database
from webhook_receiver.store.repository import ClaimedJob
from webhook_receiver.store.schema import migrate


class FakeClock:
    def __init__(self, start_ms: int = 1_760_000_000_000) -> None:
        self.current = start_ms

    def now_ms(self) -> int:
        return self.current

    def advance(self, ms: int) -> None:
        self.current += ms


API_KEY = "test-api-key-0123456789"


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "db_path": tmp_path / "webhooks.db",
        "api_key": API_KEY,
        "workers": 0,
        "processing_min_s": 0.0,
        "processing_max_s": 0.0,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def open_connection(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, autocommit=True)
    conn.row_factory = sqlite3.Row
    migrate(conn)
    return conn


async def open_database(path: Path) -> Database:
    db = Database(path)
    await db.open()
    await db.run(migrate)
    return db


Step = dict[str, Any] | BaseException | None


class ScriptedProcessor:
    """Each call consumes one step: a result, an exception to raise, or None to block."""

    def __init__(self, *steps: Step) -> None:
        self._steps = list(steps)
        self.calls: list[int] = []
        self.started = asyncio.Event()

    async def process(self, job: ClaimedJob) -> dict[str, Any]:
        self.calls.append(job.attempt)
        step = self._steps.pop(0) if self._steps else {"ok": True}
        self.started.set()
        if step is None:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        if isinstance(step, BaseException):
            raise step
        return step


async def wait_until(predicate: Callable[[], Awaitable[bool]], timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.01)


HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}


def webhook_body(
    key: str = "evt-1", event_type: str = "order.created", payload: dict[str, Any] | None = None
) -> bytes:
    document = {
        "idempotency_key": key,
        "event_type": event_type,
        "payload": payload or {"order_id": "ORD-1"},
    }
    return json.dumps(document).encode()
