"""Open-loop load generator: sends at a fixed rate whatever the response time."""

import argparse
import asyncio
import json
import random
import statistics
import time
import uuid
from collections import Counter
from typing import Any

import aiohttp


async def send(
    session: aiohttp.ClientSession, args: argparse.Namespace, key: str
) -> tuple[int, float]:
    document = {"idempotency_key": key, "event_type": "order.created", "payload": {"key": key}}
    started = time.perf_counter()
    try:
        async with session.post(f"{args.url}/v1/webhooks", json=document) as response:
            await response.read()
            return response.status, time.perf_counter() - started
    except (aiohttp.ClientError, TimeoutError):
        return 0, time.perf_counter() - started


async def final_status(session: aiohttp.ClientSession, args: argparse.Namespace, key: str) -> str:
    deadline = time.monotonic() + args.wait_timeout
    while time.monotonic() < deadline:
        try:
            async with session.get(f"{args.url}/v1/webhooks/{key}") as response:
                if response.status != 200:
                    return f"http_{response.status}"
                status = str((await response.json()).get("status"))
        except (aiohttp.ClientError, TimeoutError):
            status = "unreachable"
        if status in ("completed", "failed"):
            return status
        await asyncio.sleep(1)
    return "unfinished"


async def run(args: argparse.Namespace) -> dict[str, Any]:
    rng = random.Random(args.seed)
    keys: list[str] = []
    sent_keys: list[str] = []
    tasks: list[asyncio.Task[tuple[int, float]]] = []
    timeout = aiohttp.ClientTimeout(total=10)
    headers = {"Authorization": f"Bearer {args.api_key}"} if args.api_key else {}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        started = time.perf_counter()
        for i in range(int(args.rate * args.duration)):
            delay = started + i / args.rate - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            duplicate = keys and rng.random() < args.duplicates
            key = rng.choice(keys) if duplicate else f"load-{uuid.uuid4().hex}"
            if not duplicate:
                keys.append(key)
            sent_keys.append(key)
            tasks.append(asyncio.create_task(send(session, args, key)))
        results = await asyncio.gather(*tasks)
        # Only keys the receiver accepted can finish; polling rejected ones wastes the wait.
        accepted = {
            key for key, (code, _) in zip(sent_keys, results, strict=True) if 200 <= code < 300
        }
        keys = [key for key in keys if key in accepted]
        latencies_ms = sorted(latency * 1000 for code, latency in results if 200 <= code < 300)
        gate = asyncio.Semaphore(50)

        async def bounded(key: str) -> str:
            async with gate:
                return await final_status(session, args, key)

        finals = Counter(await asyncio.gather(*(bounded(key) for key in keys)))
    percentiles = statistics.quantiles(latencies_ms, n=100) if len(latencies_ms) > 1 else [0.0] * 99
    return {
        "sent": len(results),
        "unique_keys": len(keys),
        "status_codes": dict(Counter(code for code, _ in results)),
        "intake_ms": {"p50": percentiles[49], "p95": percentiles[94], "p99": percentiles[98]},
        "final_statuses": dict(finals),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--api-key", default="")
    parser.add_argument("--rate", type=float, default=1000 / 60)
    parser.add_argument("--duration", type=float, default=300)
    parser.add_argument("--duplicates", type=float, default=0.0)
    parser.add_argument("--wait-timeout", type=float, default=120)
    parser.add_argument("--seed", type=int, default=1)
    summary = asyncio.run(run(parser.parse_args()))
    print(json.dumps(summary, indent=2))
    non_2xx = sum(count for code, count in summary["status_codes"].items() if not 200 <= code < 300)
    unfinished = sum(
        count
        for status, count in summary["final_statuses"].items()
        if status not in ("completed", "failed")
    )
    return 1 if non_2xx or unfinished else 0


if __name__ == "__main__":
    raise SystemExit(main())
