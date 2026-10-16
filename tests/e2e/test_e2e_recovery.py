import time
from collections.abc import Callable

from tests.e2e.conftest import ServerProcess, WebhookClient


def wait_until_processing(
    client: WebhookClient, keys: list[str], minimum: int, timeout: float = 10.0
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        busy = sum(client.status(key).json()["status"] == "processing" for key in keys)
        if busy >= minimum:
            return
        time.sleep(0.05)
    raise AssertionError(f"fewer than {minimum} events reached processing")


def test_sigkill_during_processing_loses_nothing_and_processes_once(
    make_server: Callable[..., ServerProcess],
) -> None:
    server = make_server(WEBHOOK_PROCESSING_MIN_S="1.0", WEBHOOK_PROCESSING_MAX_S="1.5")
    client = WebhookClient(server.url)
    keys = [f"evt-crash-{i}" for i in range(30)]
    assert {client.send(key).status_code for key in keys} == {202}
    wait_until_processing(client, keys, minimum=5)
    server.kill()
    server.start()
    for key in keys:
        assert client.wait_for(key)["status"] == "completed"
    client.close()
    server.stop()
    rows = server.rows()
    assert len(rows) == 30
    assert {row["status"] for row in rows} == {"completed"}
    assert {row["process_count"] for row in rows} == {1}
    assert max(row["attempts"] for row in rows) >= 2


def test_sigterm_releases_leases_and_exits_cleanly(
    make_server: Callable[..., ServerProcess],
) -> None:
    server = make_server(WEBHOOK_PROCESSING_MIN_S="2.0", WEBHOOK_PROCESSING_MAX_S="3.0")
    client = WebhookClient(server.url)
    keys = [f"evt-stop-{i}" for i in range(10)]
    for key in keys:
        client.send(key)
    wait_until_processing(client, keys, minimum=5)
    assert server.terminate() == 0
    rows = server.rows()
    released = [row for row in rows if row["status"] == "pending"]
    assert "processing" not in {row["status"] for row in rows}
    assert released
    assert {row["attempts"] for row in released} == {0}
    server.start()
    for key in keys:
        assert client.wait_for(key)["status"] == "completed"
    client.close()
    server.stop()
    assert {row["process_count"] for row in server.rows()} == {1}
