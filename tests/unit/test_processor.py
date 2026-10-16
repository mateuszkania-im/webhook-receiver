import asyncio
import random
from typing import Any

import pytest

from tests.fakes import FakeClock
from webhook_receiver.store.repository import ClaimedJob
from webhook_receiver.worker.processor import PermanentError, RetryableError, SimulatedProcessor


def job(payload: dict[str, Any], attempt: int = 1) -> ClaimedJob:
    return ClaimedJob(
        id=1,
        idempotency_key="evt-1",
        event_type="order.created",
        payload=payload,
        attempt=attempt,
        lease_token="t",
        request_id="req",
        received_at=0,
    )


def processor(faults: bool = True) -> SimulatedProcessor:
    return SimulatedProcessor(
        min_s=0, max_s=0, faults_enabled=faults, clock=FakeClock(0), rng=random.Random(1)
    )


async def test_fail_attempts_fails_the_first_n_attempts_then_succeeds() -> None:
    payload = {"simulate": {"fail_attempts": 2}}
    for attempt in (1, 2):
        with pytest.raises(RetryableError, match=f"attempt {attempt}"):
            await processor().process(job(payload, attempt=attempt))
    assert await processor().process(job(payload, attempt=3)) == {
        "event_type": "order.created",
        "handled_at": "1970-01-01T00:00:00.000Z",
    }


async def test_permanent_flag_raises_a_permanent_error() -> None:
    with pytest.raises(PermanentError):
        await processor().process(job({"simulate": {"permanent": True}}))


async def test_faults_are_ignored_when_disabled() -> None:
    result = await processor(faults=False).process(job({"simulate": {"permanent": True}}))
    assert result["event_type"] == "order.created"


async def test_simulated_work_sleeps_between_min_and_max(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []

    async def record(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", record)
    simulated = SimulatedProcessor(
        min_s=2.0, max_s=5.0, faults_enabled=False, clock=FakeClock(0), rng=random.Random(7)
    )
    for _ in range(200):
        await simulated.process(job({}))
    assert all(2.0 <= seconds <= 5.0 for seconds in slept)
    assert max(slept) - min(slept) > 2.5
