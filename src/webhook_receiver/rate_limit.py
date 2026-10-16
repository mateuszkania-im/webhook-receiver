from webhook_receiver.clock import Clock


class TokenBucketLimiter:
    def __init__(self, rate_per_s: float, burst: int, clock: Clock) -> None:
        self._rate = rate_per_s
        self._burst = float(burst)
        self._clock = clock
        self._tokens = self._burst
        self._updated_ms = clock.now_ms()

    def acquire(self) -> float | None:
        now = self._clock.now_ms()
        elapsed_s = max(0, now - self._updated_ms) / 1000
        self._tokens = min(self._burst, self._tokens + elapsed_s * self._rate)
        self._updated_ms = max(self._updated_ms, now)
        if self._tokens >= 1:
            self._tokens -= 1
            return None
        return (1 - self._tokens) / self._rate
