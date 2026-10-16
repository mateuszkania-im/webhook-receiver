import time
from collections.abc import Callable

import httpx

from tests.e2e.conftest import ServerProcess, WebhookClient


def test_transient_failures_retry_and_exhausted_events_end_failed(client: WebhookClient) -> None:
    client.send("evt-retry", {"simulate": {"fail_attempts": 2}})
    client.send("evt-exhaust", {"simulate": {"fail_attempts": 9}})
    retried = client.wait_for("evt-retry")
    exhausted = client.wait_for("evt-exhaust")
    assert (retried["status"], retried["attempts"]) == ("completed", 3)
    assert (exhausted["status"], exhausted["attempts"]) == ("failed", 3)
    assert exhausted["last_error"].startswith("RetryableError")


def test_backpressure_rejects_new_events_but_answers_duplicates(
    make_server: Callable[..., ServerProcess],
) -> None:
    server = make_server(WEBHOOK_WORKERS="0", WEBHOOK_BACKLOG_LIMIT="2")
    client = WebhookClient(server.url)
    assert [client.send(f"evt-bp-{i}").status_code for i in range(2)] == [202, 202]
    rejected = client.send("evt-bp-2")
    assert (rejected.status_code, rejected.headers["Retry-After"]) == (503, "5")
    assert client.send("evt-bp-0").status_code == 200
    client.close()


def test_retention_deletes_finished_rows_after_the_ttl(
    make_server: Callable[..., ServerProcess],
) -> None:
    server = make_server(WEBHOOK_RETENTION_S="1", WEBHOOK_RETENTION_INTERVAL_S="0.2")
    client = WebhookClient(server.url)
    client.send("evt-ttl")
    client.wait_for("evt-ttl")
    deadline = time.monotonic() + 5
    while client.status("evt-ttl").status_code != 404:
        assert time.monotonic() < deadline, "row was never deleted"
        time.sleep(0.1)
    assert client.send("evt-ttl").status_code == 202
    client.close()


def test_readiness_and_metrics_are_exposed(server: ServerProcess, client: WebhookClient) -> None:
    client.send("evt-metrics")
    client.wait_for("evt-metrics")
    assert httpx.get(f"{server.url}/readyz").json()["status"] == "ready"
    text = httpx.get(f"{server.url}/metrics/").text
    assert 'jobs_processed_total{outcome="completed"}' in text
    assert "queue_lag_seconds" in text
    assert "workers_alive 20.0" in text
