import asyncio
from dataclasses import dataclass, field
from typing import Annotated

from fastapi import Depends, Request

from webhook_receiver.api.auth import check_api_key
from webhook_receiver.clock import Clock
from webhook_receiver.config import Settings
from webhook_receiver.observability import Metrics
from webhook_receiver.rate_limit import TokenBucketLimiter
from webhook_receiver.store.db import Database
from webhook_receiver.worker.pool import WorkerPool


# /readyz is unauthenticated, so its write probe is cached: a flood of probes must not
# queue writes ahead of intake and claims on the single database thread.
@dataclass
class DatabaseProbe:
    interval_ms: int = 1_000
    checked_at: int | None = None
    healthy: bool = False
    in_flight: asyncio.Task[bool] | None = None


@dataclass(frozen=True)
class AppContext:
    settings: Settings
    clock: Clock
    db: Database
    metrics: Metrics
    pool: WorkerPool
    limiter: TokenBucketLimiter
    probe: DatabaseProbe = field(default_factory=DatabaseProbe)


def get_context(request: Request) -> AppContext:
    context = request.app.state.context
    if not isinstance(context, AppContext):
        raise RuntimeError("application context is not configured")
    return context


def require_api_key(request: Request) -> None:
    check_api_key(get_context(request).settings.api_key, request.headers.get("authorization"))


Context = Annotated[AppContext, Depends(get_context)]
