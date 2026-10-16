import asyncio
import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial

import structlog
from fastapi import FastAPI
from prometheus_client import make_asgi_app

from webhook_receiver import __version__
from webhook_receiver.api import ops, webhooks
from webhook_receiver.api.context import AppContext
from webhook_receiver.api.errors import install_error_handlers
from webhook_receiver.api.middleware import install_request_context
from webhook_receiver.clock import Clock, SystemClock
from webhook_receiver.config import Settings
from webhook_receiver.maintenance import Retention, StatsRefresher, run_periodically
from webhook_receiver.observability import Metrics, configure_logging
from webhook_receiver.rate_limit import TokenBucketLimiter
from webhook_receiver.store.db import Database
from webhook_receiver.store.repository import reset_in_flight
from webhook_receiver.store.schema import migrate
from webhook_receiver.worker.pool import WorkerPool
from webhook_receiver.worker.processor import Processor, SimulatedProcessor

_log = structlog.get_logger("app")
STATS_INTERVAL_S = 5.0


def create_app(
    settings: Settings | None = None,
    *,
    clock: Clock | None = None,
    processor: Processor | None = None,
    rng: random.Random | None = None,
) -> FastAPI:
    settings = settings if settings is not None else Settings()
    clock = clock or SystemClock()
    rng = rng or random.Random()
    configure_logging(settings.log_level)
    metrics = Metrics()
    db = Database(settings.db_path)
    processor = processor or SimulatedProcessor(
        min_s=settings.processing_min_s,
        max_s=settings.processing_max_s,
        faults_enabled=settings.simulation_faults,
        clock=clock,
        rng=rng,
    )
    pool = WorkerPool(
        db=db, processor=processor, settings=settings, clock=clock, metrics=metrics, rng=rng
    )
    context = AppContext(
        settings=settings,
        clock=clock,
        db=db,
        metrics=metrics,
        pool=pool,
        limiter=TokenBucketLimiter(settings.rate_limit_per_s, settings.rate_limit_burst, clock),
    )
    retention = Retention(db=db, settings=settings, clock=clock, metrics=metrics)
    stats = StatsRefresher(db=db, pool=pool, settings=settings, clock=clock, metrics=metrics)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await db.open()
        await db.run(migrate)
        # The exclusive lock proves no other process holds a lease, so rows a crash left in
        # processing can run now instead of after their lease expires. Attempts are kept.
        resumed = await db.transaction(partial(reset_in_flight, now_ms=clock.now_ms()))
        stop = asyncio.Event()
        pool.start()
        background = [
            asyncio.create_task(
                run_periodically(
                    settings.retention_interval_s, retention.run_once, name="retention", stop=stop
                )
            ),
            asyncio.create_task(
                run_periodically(STATS_INTERVAL_S, stats.run_once, name="stats", stop=stop)
            ),
        ]
        _log.info(
            "startup",
            version=__version__,
            workers=settings.workers,
            db_path=str(settings.db_path),
            auth=settings.api_key is not None,
            resumed=resumed,
        )
        try:
            yield
        finally:
            stop.set()
            await pool.stop()
            await asyncio.gather(*background)
            await db.close()
            _log.info("shutdown_complete")

    app = FastAPI(
        title="Webhook receiver",
        version=__version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.context = context
    install_error_handlers(app)
    install_request_context(app, metrics)
    app.include_router(webhooks.router)
    app.include_router(ops.router)
    app.mount("/metrics", make_asgi_app(registry=metrics.registry))
    return app
