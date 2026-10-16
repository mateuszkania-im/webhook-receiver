import pytest

from tests.fakes import FakeClock
from webhook_receiver.rate_limit import TokenBucketLimiter


def test_burst_then_limit_then_refill_up_to_the_burst() -> None:
    clock = FakeClock()
    limiter = TokenBucketLimiter(rate_per_s=2.0, burst=3, clock=clock)
    assert [limiter.acquire() for _ in range(3)] == [None, None, None]
    assert limiter.acquire() == pytest.approx(0.5)
    clock.advance(60_000)
    assert [limiter.acquire() for _ in range(4)][-1] is not None


def test_clock_going_backwards_does_not_add_tokens() -> None:
    clock = FakeClock()
    limiter = TokenBucketLimiter(rate_per_s=1.0, burst=1, clock=clock)
    assert limiter.acquire() is None
    clock.advance(-10_000)
    assert limiter.acquire() is not None
