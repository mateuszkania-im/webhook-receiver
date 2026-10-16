import logging

import structlog
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from structlog.tracebacks import ExceptionDictTransformer

_HTTP_BUCKETS = (0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5)
_JOB_BUCKETS = (0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 7.5, 10.0, 30.0, 60.0)
_END_TO_END_BUCKETS = (1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0)


def configure_logging(level: str) -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            # Frame locals can hold API keys, signing secrets or payloads.
            structlog.processors.ExceptionRenderer(ExceptionDictTransformer(show_locals=False)),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelNamesMapping()[level]),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        r = self.registry
        self.webhooks_received = Counter(
            "webhooks_received", "Webhook intake outcomes", ["outcome"], registry=r
        )
        self.http_duration = Histogram(
            "http_request_duration_seconds",
            "HTTP request duration",
            ["route", "method", "status"],
            buckets=_HTTP_BUCKETS,
            registry=r,
        )
        self.jobs_processed = Counter(
            "jobs_processed", "Processing outcomes", ["outcome"], registry=r
        )
        self.job_processing = Histogram(
            "job_processing_seconds", "Processor run time", buckets=_JOB_BUCKETS, registry=r
        )
        self.job_end_to_end = Histogram(
            "job_end_to_end_seconds",
            "Intake to completion",
            buckets=_END_TO_END_BUCKETS,
            registry=r,
        )
        self.lease_lost = Counter("lease_lost", "Results dropped after losing a lease", registry=r)
        self.worker_errors = Counter("worker_errors", "Unexpected worker loop errors", registry=r)
        self.queue_depth = Gauge("queue_depth", "Active rows by status", ["status"], registry=r)
        self.queue_lag = Gauge(
            "queue_lag_seconds", "How long the most overdue pending event has been due", registry=r
        )
        self.dead_letter_size = Gauge("dead_letter_size", "Rows in failed status", registry=r)
        self.workers_busy = Gauge("workers_busy", "Workers currently processing", registry=r)
        self.workers_alive = Gauge("workers_alive", "Workers with a fresh heartbeat", registry=r)
        self.retention_deleted = Counter(
            "retention_deleted", "Finished rows deleted by retention", registry=r
        )
