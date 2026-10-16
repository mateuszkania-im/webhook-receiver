import asyncio
import random
from typing import Any, Protocol

from webhook_receiver.clock import Clock, to_rfc3339
from webhook_receiver.store.repository import ClaimedJob


class RetryableError(Exception):
    pass


class PermanentError(Exception):
    pass


class Processor(Protocol):
    async def process(self, job: ClaimedJob) -> dict[str, Any]: ...


class SimulatedProcessor:
    def __init__(
        self, *, min_s: float, max_s: float, faults_enabled: bool, clock: Clock, rng: random.Random
    ) -> None:
        self._min_s = min_s
        self._max_s = max_s
        self._faults_enabled = faults_enabled
        self._clock = clock
        self._rng = rng

    async def process(self, job: ClaimedJob) -> dict[str, Any]:
        await asyncio.sleep(self._rng.uniform(self._min_s, self._max_s))
        if self._faults_enabled:
            self._inject_fault(job)
        return {"event_type": job.event_type, "handled_at": to_rfc3339(self._clock.now_ms())}

    def _inject_fault(self, job: ClaimedJob) -> None:
        simulate = job.payload.get("simulate")
        if not isinstance(simulate, dict):
            return
        if simulate.get("permanent") is True:
            raise PermanentError("simulated permanent failure")
        fail_attempts = simulate.get("fail_attempts")
        if isinstance(fail_attempts, int) and job.attempt <= fail_attempts:
            raise RetryableError(f"simulated transient failure on attempt {job.attempt}")
