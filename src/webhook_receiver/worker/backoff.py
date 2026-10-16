import random

MAX_EXPONENT = 64


def backoff_delay_ms(
    attempt: int, *, base_ms: int, factor: float, cap_ms: int, rng: random.Random
) -> int:
    ceiling = min(float(cap_ms), base_ms * factor ** min(attempt - 1, MAX_EXPONENT))
    return int(rng.uniform(0, ceiling))
