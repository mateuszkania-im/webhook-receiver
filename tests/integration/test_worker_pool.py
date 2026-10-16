import asyncio
import random
import sqlite3
from collections.abc import AsyncIterator, Callable
from functools import partial
from pathlib import Path
from typing import Any

import pytest

from tests.fakes import FakeClock, ScriptedProcessor, make_settings, open_database, wait_until
from webhook_receiver.observability import Metrics
from webhook_receiver.store.db import Database
from webhook_receiver.store.repository import (
    NewWebhook,
    WebhookRecord,
    claim_next,
    get_webhook,
    intake,
)
from webhook_receiver.store.schema import migrate
from webhook_receiver.worker.pool import WorkerPool
from webhook_receiver.worker.processor import RetryableError

NOW = 1_760_000_000_000


@pytest.fixture
async def db(tmp_path: Path) -> AsyncIterator[Database]:
    database = await open_database(tmp_path / "x.db")
    yield database
    await database.close()


def make_pool(
    db: Database, tmp_path: Path, processor: ScriptedProcessor, clock: FakeClock, **overrides: Any
) -> tuple[WorkerPool, Metrics]:
    values: dict[str, Any] = {
        "workers": 1,
        "poll_interval_s": 0.01,
        "heartbeat_s": 0.05,
        "lease_s": 1.0,
        "max_attempts": 3,
        "backoff_base_s": 0.001,
        "backoff_cap_s": 0.001,
    }
    values.update(overrides)
    metrics = Metrics()
    pool = WorkerPool(
        db=db,
        processor=processor,
        settings=make_settings(tmp_path, **values),
        clock=clock,
        metrics=metrics,
        rng=random.Random(1),
    )
    return pool, metrics


async def add(db: Database, key: str = "evt-1") -> None:
    webhook = NewWebhook(key, "order.created", '{"n": 1}', f"h-{key}", "req")
    await db.transaction(partial(intake, new=webhook, now_ms=NOW, backlog_limit=1_000))


async def record(db: Database, key: str = "evt-1") -> WebhookRecord:
    found = await db.run(partial(get_webhook, key=key))
    assert found is not None
    return found


def has_status(db: Database, status: str, key: str = "evt-1") -> Callable[[], Any]:
    async def check() -> bool:
        return (await record(db, key)).status == status

    return check


def sample(metrics: Metrics, name: str, **labels: str) -> float:
    return metrics.registry.get_sample_value(name, labels) or 0.0


async def test_transient_failures_are_retried_until_success(db: Database, tmp_path: Path) -> None:
    processor = ScriptedProcessor(RetryableError("a"), RetryableError("b"), {"ok": True})
    pool, metrics = make_pool(db, tmp_path, processor, FakeClock(NOW))
    await add(db)
    pool.start()
    try:
        await wait_until(has_status(db, "completed"))
    finally:
        await pool.stop()
    assert processor.calls == [1, 2, 3]
    done = await record(db)
    assert (done.attempts, done.last_error, done.result) == (3, None, '{"ok": true}')
    assert sample(metrics, "jobs_processed_total", outcome="retry") == 2
    assert sample(metrics, "jobs_processed_total", outcome="completed") == 1


async def test_exhausted_attempts_fail_the_event(db: Database, tmp_path: Path) -> None:
    processor = ScriptedProcessor(RetryableError("a"), RetryableError("b"), RetryableError("c"))
    pool, metrics = make_pool(db, tmp_path, processor, FakeClock(NOW))
    await add(db)
    pool.start()
    try:
        await wait_until(has_status(db, "failed"))
    finally:
        await pool.stop()
    assert (await record(db)).last_error == "RetryableError: c"
    assert sample(metrics, "jobs_processed_total", outcome="failed") == 1


async def test_processing_timeout_is_retried(db: Database, tmp_path: Path) -> None:
    processor = ScriptedProcessor(None, {"ok": True})
    pool, _ = make_pool(db, tmp_path, processor, FakeClock(NOW), processing_timeout_s=0.05)
    await add(db)
    pool.start()
    try:
        await wait_until(has_status(db, "completed"))
    finally:
        await pool.stop()
    assert processor.calls == [1, 2]


async def test_heartbeat_extends_the_lease(db: Database, tmp_path: Path) -> None:
    clock = FakeClock(NOW)
    processor = ScriptedProcessor(None)
    pool, _ = make_pool(db, tmp_path, processor, clock)
    await add(db)
    pool.start()
    try:
        await processor.started.wait()
        clock.advance(900)

        async def renewed() -> bool:
            row = await db.run(
                lambda conn: conn.execute("SELECT lease_expires_at FROM webhooks").fetchone()
            )
            return bool(row[0] == NOW + 1_900)

        await wait_until(renewed)
    finally:
        await pool.stop()


async def test_lost_lease_drops_the_result(db: Database, tmp_path: Path) -> None:
    processor = ScriptedProcessor(None)
    pool, metrics = make_pool(db, tmp_path, processor, FakeClock(NOW))
    await add(db)
    pool.start()
    try:
        await processor.started.wait()
        await db.transaction(lambda conn: conn.execute("UPDATE webhooks SET lease_token = 'thief'"))

        async def lost() -> bool:
            return sample(metrics, "lease_lost_total") == 1

        await wait_until(lost)
    finally:
        await pool.stop()
    row = await db.run(
        lambda conn: tuple(conn.execute("SELECT status, lease_token FROM webhooks").fetchone())
    )
    assert row == ("processing", "thief")


async def test_stop_releases_in_flight_events(db: Database, tmp_path: Path) -> None:
    processor = ScriptedProcessor(None)
    pool, _ = make_pool(db, tmp_path, processor, FakeClock(NOW))
    await add(db)
    pool.start()
    await processor.started.wait()
    await pool.stop()
    released = await record(db)
    assert (released.status, released.attempts) == ("pending", 0)


async def test_worker_survives_unexpected_errors(db: Database, tmp_path: Path) -> None:
    class FlakyDatabase(Database):
        failures = 0

        async def transaction[T](self, fn: Callable[[sqlite3.Connection], T]) -> T:
            if self.failures:
                self.failures -= 1
                raise sqlite3.OperationalError("disk I/O error")
            return await super().transaction(fn)

    await db.close()
    flaky = FlakyDatabase(tmp_path / "x.db")
    await flaky.open()
    await add(flaky)
    flaky.failures = 1
    pool, metrics = make_pool(flaky, tmp_path, ScriptedProcessor({"ok": True}), FakeClock(NOW))
    pool.start()
    try:
        await wait_until(has_status(flaky, "completed"))
    finally:
        await pool.stop()
        await flaky.close()
    assert sample(metrics, "worker_errors_total") == 1


async def test_notify_wakes_an_idle_worker_before_the_poll_interval(
    db: Database, tmp_path: Path
) -> None:
    pool, _ = make_pool(
        db,
        tmp_path,
        ScriptedProcessor({"ok": True}),
        FakeClock(NOW),
        poll_interval_s=30.0,
        readiness_stale_s=60.0,
    )
    pool.start()
    try:
        await asyncio.sleep(0.05)
        await add(db)
        pool.notify()
        await wait_until(has_status(db, "completed"), timeout=1.0)
    finally:
        await pool.stop()


async def test_reclaimed_event_past_max_attempts_fails_without_processing(
    db: Database, tmp_path: Path
) -> None:
    processor = ScriptedProcessor({"ok": True})
    pool, metrics = make_pool(db, tmp_path, processor, FakeClock(NOW))
    await add(db)
    await db.transaction(
        lambda conn: conn.execute(
            "UPDATE webhooks SET status = 'processing', next_attempt_at = NULL, "
            "lease_token = 'crashed', lease_expires_at = ?, attempts = 3",
            (NOW - 1,),
        )
    )
    pool.start()
    try:
        await wait_until(has_status(db, "failed"))
    finally:
        await pool.stop()
    failed = await record(db)
    assert processor.calls == []
    assert failed.attempts == 4
    assert failed.last_error is not None
    assert "attempts" in failed.last_error
    assert sample(metrics, "jobs_processed_total", outcome="failed") == 1


async def test_cancellation_outside_shutdown_uses_up_an_attempt(
    db: Database, tmp_path: Path
) -> None:
    processor = ScriptedProcessor(asyncio.CancelledError(), {"ok": True})
    pool, _ = make_pool(db, tmp_path, processor, FakeClock(NOW))
    await add(db)
    pool.start()
    try:
        await wait_until(has_status(db, "completed"))
    finally:
        await pool.stop()
    assert processor.calls == [1, 2]
    assert (await record(db)).attempts == 2


async def test_renewal_error_cancels_the_unsupervised_job(db: Database, tmp_path: Path) -> None:
    class RenewFailsDatabase(Database):
        calls = 0

        async def transaction[T](self, fn: Callable[[sqlite3.Connection], T]) -> T:
            self.calls += 1
            if self.calls == 2:
                raise sqlite3.OperationalError("disk I/O error")
            return await super().transaction(fn)

    await db.close()
    flaky = RenewFailsDatabase(tmp_path / "x.db")
    await flaky.open()
    await add(flaky)
    flaky.calls = 0
    processor = ScriptedProcessor(None)
    pool, metrics = make_pool(flaky, tmp_path, processor, FakeClock(NOW))
    pool.start()
    try:

        async def errored() -> bool:
            return sample(metrics, "worker_errors_total") == 1

        await wait_until(errored)
        await asyncio.sleep(0.05)
        running = [
            task
            for task in asyncio.all_tasks()
            if task.get_name().startswith("job-") and not task.done()
        ]
    finally:
        await pool.stop()
        await flaky.close()
    assert running == []


async def test_a_burst_of_notifies_does_not_cause_empty_claims(tmp_path: Path) -> None:
    class CountingDatabase(Database):
        claims = 0

        async def transaction[T](self, fn: Callable[[sqlite3.Connection], T]) -> T:
            self.claims += getattr(fn, "func", None) is claim_next
            return await super().transaction(fn)

    counting = CountingDatabase(tmp_path / "y.db")
    await counting.open()
    await counting.run(migrate)
    pool, _ = make_pool(
        counting,
        tmp_path,
        ScriptedProcessor(),
        FakeClock(NOW),
        poll_interval_s=30.0,
        readiness_stale_s=60.0,
    )
    pool.start()
    try:
        await asyncio.sleep(0.05)
        before = counting.claims
        for _ in range(100):
            pool.notify()
        await asyncio.sleep(0.1)
        extra = counting.claims - before
    finally:
        await pool.stop()
        await counting.close()
    assert extra <= 1
