import asyncio
import contextlib
import json
import random
import secrets
import time
from functools import partial
from typing import Any

import structlog
from structlog.typing import FilteringBoundLogger

from webhook_receiver.clock import Clock
from webhook_receiver.config import Settings
from webhook_receiver.observability import Metrics
from webhook_receiver.store.db import Database
from webhook_receiver.store.repository import (
    ClaimedJob,
    claim_next,
    complete,
    fail,
    release,
    renew_lease,
    schedule_retry,
)
from webhook_receiver.worker.backoff import backoff_delay_ms
from webhook_receiver.worker.processor import PermanentError, Processor

_MAX_ERROR_LENGTH = 500
BACKOFF_FACTOR = 2.0


class WorkerPool:
    def __init__(
        self,
        *,
        db: Database,
        processor: Processor,
        settings: Settings,
        clock: Clock,
        metrics: Metrics,
        rng: random.Random,
    ) -> None:
        self._db = db
        self._processor = processor
        self._settings = settings
        self._clock = clock
        self._metrics = metrics
        self._rng = rng
        self._lease_ms = int(settings.lease_s * 1000)
        self._wakeups = asyncio.Semaphore(0)
        self._stopping = asyncio.Event()
        self._idle = 0
        self._pending_wakeups = 0
        self._tasks: list[asyncio.Task[None]] = []
        self._current: dict[int, asyncio.Task[dict[str, Any]]] = {}
        self._beats: list[int] = []
        self._log: FilteringBoundLogger = structlog.get_logger("worker")

    @property
    def size(self) -> int:
        return self._settings.workers

    @property
    def busy(self) -> int:
        return len(self._current)

    def alive(self, now_ms: int, stale_ms: int) -> int:
        return sum(
            1
            for index, beat in enumerate(self._beats)
            if index in self._current or now_ms - beat <= stale_ms
        )

    def start(self) -> None:
        self._beats = [self._clock.now_ms()] * self.size
        self._tasks = [
            asyncio.create_task(self._run(i), name=f"worker-{i}") for i in range(self.size)
        ]

    async def stop(self) -> None:
        self._stopping.set()
        for task in self._current.values():
            task.cancel()
        for _ in range(self.size):
            self._wakeups.release()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    def notify(self) -> None:
        # One permit per idle worker at most: extra permits would wake workers later for
        # empty claims on the single database thread.
        if self._idle > self._pending_wakeups:
            self._pending_wakeups += 1
            self._wakeups.release()

    async def _run(self, index: int) -> None:
        while not self._stopping.is_set():
            self._beats[index] = self._clock.now_ms()
            try:
                job = await self._claim()
                if job is None:
                    await self._wait_for_work()
                else:
                    await self._handle(index, job)
            except Exception:
                self._metrics.worker_errors.inc()
                self._log.exception("worker_error", worker=index)
                await self._pause(self._settings.poll_interval_s)

    async def _claim(self) -> ClaimedJob | None:
        now = self._clock.now_ms()
        token = secrets.token_hex(16)
        return await self._db.transaction(
            partial(claim_next, now_ms=now, lease_ms=self._lease_ms, token=token)
        )

    async def _wait_for_work(self) -> None:
        self._idle += 1
        try:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(self._settings.poll_interval_s):
                    await self._wakeups.acquire()
                    self._pending_wakeups -= 1
        finally:
            self._idle -= 1

    async def _pause(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(seconds):
                await self._stopping.wait()

    async def _handle(self, index: int, job: ClaimedJob) -> None:
        log = self._log.bind(
            webhook_id=job.id,
            idempotency_key=job.idempotency_key,
            attempt=job.attempt,
            request_id=job.request_id,
            lease_token=job.lease_token,
        )
        log.info("job_claimed")
        if job.attempt > self._settings.max_attempts:
            # Only an expired lease gets past max_attempts: the process died or hung while
            # holding it, so this payload may crash it again and must not loop forever.
            limit = self._settings.max_attempts
            message = f"exceeded {limit} attempts; the last lease expired without a result"
            await self._handle_failure(job, RuntimeError(message), log)
            return
        task = asyncio.create_task(self._process(job), name=f"job-{job.id}")
        if self._stopping.is_set():
            task.cancel()
        self._current[index] = task
        started = time.perf_counter()
        try:
            lease_held = await self._supervise(job, task)
        finally:
            del self._current[index]
            self._beats[index] = self._clock.now_ms()
        if not lease_held:
            self._lease_lost(log)
            return
        if task.cancelled() and self._stopping.is_set():
            await self._release(job, log)
            return
        self._metrics.job_processing.observe(time.perf_counter() - started)
        cancelled = asyncio.CancelledError("processing was cancelled")
        error = cancelled if task.cancelled() else task.exception()
        if error is None:
            await self._complete(job, task.result(), log)
        else:
            await self._handle_failure(job, error, log)

    async def _process(self, job: ClaimedJob) -> dict[str, Any]:
        async with asyncio.timeout(self._settings.processing_timeout_s):
            return await self._processor.process(job)

    async def _supervise(self, job: ClaimedJob, task: asyncio.Task[dict[str, Any]]) -> bool:
        while True:
            done, _ = await asyncio.wait({task}, timeout=self._settings.heartbeat_s)
            if done:
                return True
            now = self._clock.now_ms()
            renew = partial(
                renew_lease,
                job_id=job.id,
                token=job.lease_token,
                lease_until_ms=now + self._lease_ms,
                now_ms=now,
            )
            try:
                renewed = await self._db.transaction(renew)
            except BaseException:
                task.cancel()
                await asyncio.wait({task})
                raise
            if not renewed:
                task.cancel()
                await asyncio.wait({task})
                return False

    async def _complete(
        self, job: ClaimedJob, result: dict[str, Any], log: FilteringBoundLogger
    ) -> None:
        now = self._clock.now_ms()
        action = partial(
            complete,
            job_id=job.id,
            token=job.lease_token,
            result_json=json.dumps(result),
            now_ms=now,
        )
        if not await self._db.transaction(action):
            self._lease_lost(log)
            return
        self._metrics.jobs_processed.labels(outcome="completed").inc()
        self._metrics.job_end_to_end.observe((now - job.received_at) / 1000)
        log.info("job_completed")

    async def _handle_failure(
        self, job: ClaimedJob, error: BaseException, log: FilteringBoundLogger
    ) -> None:
        now = self._clock.now_ms()
        message = f"{type(error).__name__}: {error}"[:_MAX_ERROR_LENGTH]
        if isinstance(error, PermanentError) or job.attempt >= self._settings.max_attempts:
            action = partial(fail, job_id=job.id, token=job.lease_token, error=message, now_ms=now)
            outcome, event, delay = "failed", "job_failed", None
        else:
            delay = backoff_delay_ms(
                job.attempt,
                base_ms=int(self._settings.backoff_base_s * 1000),
                factor=BACKOFF_FACTOR,
                cap_ms=int(self._settings.backoff_cap_s * 1000),
                rng=self._rng,
            )
            action = partial(
                schedule_retry,
                job_id=job.id,
                token=job.lease_token,
                error=message,
                next_attempt_at_ms=now + delay,
                now_ms=now,
            )
            outcome, event = "retry", "job_retry_scheduled"
        if not await self._db.transaction(action):
            self._lease_lost(log)
            return
        self._metrics.jobs_processed.labels(outcome=outcome).inc()
        log.warning(event, error=message, retry_in_ms=delay)

    async def _release(self, job: ClaimedJob, log: FilteringBoundLogger) -> None:
        action = partial(release, job_id=job.id, token=job.lease_token, now_ms=self._clock.now_ms())
        if await self._db.transaction(action):
            log.info("lease_released_on_shutdown")

    def _lease_lost(self, log: FilteringBoundLogger) -> None:
        self._metrics.lease_lost.inc()
        log.warning("lease_lost")
