import json

import pytest
import structlog
from prometheus_client import generate_latest

from webhook_receiver.observability import Metrics, configure_logging

SPEC_METRICS = {
    "webhooks_received_total",
    "http_request_duration_seconds",
    "jobs_processed_total",
    "job_processing_seconds",
    "job_end_to_end_seconds",
    "lease_lost_total",
    "worker_errors_total",
    "queue_depth",
    "queue_lag_seconds",
    "dead_letter_size",
    "workers_busy",
    "workers_alive",
    "retention_deleted_total",
}


def test_every_metric_in_the_spec_is_exposed() -> None:
    metrics = Metrics()
    metrics.webhooks_received.labels(outcome="accepted").inc()
    metrics.http_duration.labels(route="/v1/webhooks", method="POST", status="202").observe(0.01)
    metrics.jobs_processed.labels(outcome="completed").inc()
    metrics.queue_depth.labels(status="pending").set(3)
    exposition = generate_latest(metrics.registry).decode()
    names = {line.split(" ")[2] for line in exposition.splitlines() if line.startswith("# TYPE")}
    exposed = {name if name in SPEC_METRICS else f"{name}_total" for name in names}
    assert exposed >= SPEC_METRICS


def test_logs_are_json_lines_with_bound_context(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    structlog.contextvars.bind_contextvars(request_id="req-1")
    try:
        structlog.get_logger().info("webhook_received", idempotency_key="evt-1")
        structlog.get_logger().debug("noise")
    finally:
        structlog.contextvars.clear_contextvars()
    lines = capsys.readouterr().out.strip().splitlines()
    line = json.loads(lines[-1])
    assert (line["event"], line["request_id"], line["level"]) == (
        "webhook_received",
        "req-1",
        "info",
    )
    assert line["timestamp"].endswith("Z")
    assert not any("noise" in text for text in lines)


def test_tracebacks_never_include_local_variables(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")

    def check(api_key: str) -> None:
        raise RuntimeError("authentication crashed")

    try:
        check("never-in-logs-0123456789")
    except RuntimeError:
        structlog.get_logger().exception("unhandled_error")
    out = capsys.readouterr().out
    assert "authentication crashed" in out
    assert "never-in-logs-0123456789" not in out
