import asyncio
import random
import sqlite3
import time
from collections.abc import AsyncIterator
from functools import partial
from pathlib import Path

import pytest

from tests.fakes import FakeClock, ScriptedProcessor, make_settings, open_database
from webhook_receiver.maintenance import Retention, StatsRefresher, run_periodically
from webhook_receiver.observability import Metrics
from webhook_receiver.store.db import Database
from webhook_receiver.store.repository import (
    NewWebhook,
    QueueStats,
    claim_next,
    complete,
    fail,
    intake,
)
from webhook_receiver.worker.pool import WorkerPool

NOW = 1_760_000_000_000


@pytest.fixture
async def db(tmp_path: Path) -> AsyncIterator[Database]:
    database = await open_database(tmp_path / "x.db")
    yield database
    await database.close()


def seed(conn: sqlite3.Connection, key: str, *, at: int, outcome: str | None) -> None:
    webhook = NewWebhook(key, "order.created", "{}", f"h-{key}", "req")
    intake(conn, new=webhook, now_ms=at, backlog_limit=1_000)
    if outcome is None:
        return
    job = claim_next(conn, now_ms=at, lease_ms=1_000, token=key)
    assert job is not None
    if outcome == "completed":
        complete(conn, job_id=job.id, token=key, result_json="{}", now_ms=at)
    else:
        fail(conn, job_id=job.id, token=key, error="boom", now_ms=at)


async def test_retention_deletes_expired_rows_in_batches(db: Database, tmp_path: Path) -> None:
    for i in range(4):
        await db.transaction(partial(seed, key=f"old-{i}", at=NOW - 200_000, outcome="completed"))
    await db.transaction(partial(seed, key="old-failed", at=NOW - 200_000, outcome="failed"))
    await db.transaction(partial(seed, key="recent", at=NOW - 20_000, outcome="completed"))
    await db.transaction(partial(seed, key="waiting", at=NOW - 200_000, outcome=None))
    settings = make_settings(tmp_path, retention_s=100)
    metrics = Metrics()
    retention = Retention(db=db, settings=settings, clock=FakeClock(NOW), metrics=metrics, batch=2)
    assert await retention.run_once() == 5
    assert await retention.run_once() == 0
    assert metrics.registry.get_sample_value("retention_deleted_total") == 5
    remaining = await db.run(
        lambda conn: conn.execute("SELECT count(*) FROM webhooks").fetchone()[0]
    )
    assert remaining == 2


async def test_stats_refresher_sets_every_gauge_from_one_refresh(
    db: Database, tmp_path: Path
) -> None:
    await db.transaction(partial(seed, key="busy", at=NOW - 6_000, outcome=None))
    await db.transaction(partial(seed, key="waiting", at=NOW - 4_000, outcome=None))
    await db.transaction(partial(seed, key="dead", at=NOW - 9_000, outcome="failed"))
    settings = make_settings(tmp_path, workers=1, poll_interval_s=0.01)
    metrics = Metrics()
    clock = FakeClock(NOW)
    processor = ScriptedProcessor(None)
    pool = WorkerPool(
        db=db,
        processor=processor,
        settings=settings,
        clock=clock,
        metrics=metrics,
        rng=random.Random(1),
    )
    refresher = StatsRefresher(db=db, pool=pool, settings=settings, clock=clock, metrics=metrics)
    value = metrics.registry.get_sample_value
    pool.start()
    try:
        await processor.started.wait()
        assert value("workers_busy") == 0
        stats = await refresher.run_once()
    finally:
        await pool.stop()
    assert stats == QueueStats(pending=1, processing=1, failed=1, lag_ms=4_000)
    assert value("queue_depth", {"status": "pending"}) == 1
    assert value("queue_depth", {"status": "processing"}) == 1
    assert value("workers_busy") == 1
    assert value("queue_lag_seconds") == 4.0
    assert value("dead_letter_size") == 1
    assert value("workers_alive") == 1


async def test_periodic_runner_survives_errors_and_stops_mid_interval() -> None:
    calls = 0
    stop = asyncio.Event()

    async def action() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("first run fails")

    runner = asyncio.create_task(run_periodically(0.01, action, name="test", stop=stop))
    await asyncio.sleep(0.1)
    assert calls > 1
    slow = asyncio.create_task(run_periodically(60, action, name="slow", stop=stop))
    await asyncio.sleep(0.05)
    started = time.perf_counter()
    stop.set()
    await asyncio.wait_for(asyncio.gather(runner, slow), timeout=1)
    assert time.perf_counter() - started < 0.5


async def test_retention_caps_batches_per_run_so_intake_is_not_starved(
    db: Database, tmp_path: Path
) -> None:
    for i in range(5):
        await db.transaction(partial(seed, key=f"old-{i}", at=NOW - 200_000, outcome="completed"))
    settings = make_settings(tmp_path, retention_s=100)
    retention = Retention(
        db=db, settings=settings, clock=FakeClock(NOW), metrics=Metrics(), batch=2, max_batches=2
    )
    assert await retention.run_once() == 4
    assert await retention.run_once() == 1
