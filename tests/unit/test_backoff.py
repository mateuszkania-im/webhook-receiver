import random

from webhook_receiver.worker.backoff import backoff_delay_ms


def test_delay_is_full_jitter_below_the_capped_ceiling() -> None:
    rng = random.Random(7)
    for attempt, ceiling in [(1, 2_000), (3, 8_000), (10_000, 300_000)]:
        delays = [
            backoff_delay_ms(attempt, base_ms=2_000, factor=2.0, cap_ms=300_000, rng=rng)
            for _ in range(500)
        ]
        assert all(0 <= delay <= ceiling for delay in delays)
        assert min(delays) < ceiling * 0.1
        assert max(delays) > ceiling * 0.9
