import asyncio
import sqlite3
from functools import partial

import structlog
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from webhook_receiver.api.context import AppContext, Context
from webhook_receiver.store.repository import probe

router = APIRouter(tags=["operations"])
_log = structlog.get_logger("ops")


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(ctx: Context) -> JSONResponse:
    now = ctx.clock.now_ms()
    checks: dict[str, str] = {}
    last = ctx.probe
    stale = last.checked_at is None or not 0 <= now - last.checked_at < last.interval_ms
    if stale and last.in_flight is None:
        last.in_flight = asyncio.create_task(_write_probe(ctx, now))
    if last.in_flight is not None:
        task = last.in_flight
        healthy = await asyncio.shield(task)
        if last.in_flight is task:
            last.healthy, last.checked_at, last.in_flight = healthy, now, None
    checks["database"] = "ok" if last.healthy else "error"
    alive = ctx.pool.alive(now, int(ctx.settings.readiness_stale_s * 1000))
    checks["workers"] = "ok" if alive == ctx.pool.size else f"{alive} of {ctx.pool.size} alive"
    ready = all(value == "ok" for value in checks.values())
    body = {"status": "ready" if ready else "not_ready", "checks": checks}
    return JSONResponse(body, status_code=200 if ready else 503)


async def _write_probe(ctx: AppContext, now_ms: int) -> bool:
    try:
        await ctx.db.transaction(partial(probe, now_ms=now_ms))
    except (sqlite3.Error, RuntimeError):
        _log.exception("readiness_probe_failed")
        return False
    return True
