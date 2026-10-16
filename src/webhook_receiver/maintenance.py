import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from functools import partial

import structlog

from webhook_receiver.clock import Clock
from webhook_receiver.config import Settings
from webhook_receiver.observability import Metrics
from webhook_receiver.store.db import Database
from webhook_receiver.store.repository import QueueStats, delete_expired, queue_stats
from webhook_receiver.worker.pool import WorkerPool

_log = structlog.get_logger("maintenance")
# After a long outage the backlog of expired rows is cleared over several runs, so one
# sweep never holds the single database thread for thousands of transactions in a row.
MAX_RETENTION_BATCHES = 100
RETENTION_BATCH = 1_000


class Retention:
    def __init__(
        self,
        *,
        db: Database,
        settings: Settings,
        clock: Clock,
        metrics: Metrics,
        batch: int = RETENTION_BATCH,
        max_batches: int = MAX_RETENTION_BATCHES,
    ) -> None:
        self._db = db
        self._settings = settings
        self._clock = clock
        self._metrics = metrics
        self._batch = batch
        self._max_batches = max_batches

    async def run_once(self) -> int:
        cutoff = self._clock.now_ms() - self._settings.retention_s * 1000
        batch = self._batch
        total = 0
        for _ in range(self._max_batches):
            count = await self._db.transaction(
                partial(delete_expired, cutoff_ms=cutoff, batch=batch)
            )
            total += count
            if count < batch:
                break
        self._metrics.retention_deleted.inc(total)
        if total:
            _log.info("retention_deleted", deleted=total)
        return total


class StatsRefresher:
    def __init__(
        self, *, db: Database, pool: WorkerPool, settings: Settings, clock: Clock, metrics: Metrics
    ) -> None:
        self._db = db
        self._pool = pool
        self._settings = settings
        self._clock = clock
        self._metrics = metrics

    async def run_once(self) -> QueueStats:
        now = self._clock.now_ms()
        stats = await self._db.run(partial(queue_stats, now_ms=now))
        self._metrics.queue_depth.labels(status="pending").set(stats.pending)
        self._metrics.queue_depth.labels(status="processing").set(stats.processing)
        self._metrics.queue_lag.set(stats.lag_ms / 1000)
        self._metrics.dead_letter_size.set(stats.failed)
        self._metrics.workers_busy.set(self._pool.busy)
        self._metrics.workers_alive.set(
            self._pool.alive(now, int(self._settings.readiness_stale_s * 1000))
        )
        return stats


async def run_periodically(
    interval_s: float, action: Callable[[], Awaitable[object]], *, name: str, stop: asyncio.Event
) -> None:
    while not stop.is_set():
        try:
            await action()
        except Exception:
            _log.exception("periodic_task_failed", task=name)
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(interval_s):
                await stop.wait()
