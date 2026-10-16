import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.fakes import API_KEY

BASE_ENV = {
    "WEBHOOK_API_KEY": API_KEY,
    "WEBHOOK_WORKERS": "20",
    "WEBHOOK_POLL_INTERVAL_S": "0.05",
    "WEBHOOK_PROCESSING_MIN_S": "0.1",
    "WEBHOOK_PROCESSING_MAX_S": "0.3",
    "WEBHOOK_LEASE_S": "2",
    "WEBHOOK_HEARTBEAT_S": "0.5",
    "WEBHOOK_SIMULATION_FAULTS": "true",
    "WEBHOOK_MAX_ATTEMPTS": "3",
    "WEBHOOK_BACKOFF_BASE_S": "0.05",
    "WEBHOOK_BACKOFF_CAP_S": "0.2",
    "WEBHOOK_READINESS_STALE_S": "2",
}
FINAL = ("completed", "failed")


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
        return port


class ServerProcess:
    def __init__(self, workdir: Path, env: dict[str, str]) -> None:
        self.workdir = workdir
        self.db_path = workdir / "webhooks.db"
        self.log_path = workdir / "server.log"
        self.port = free_port()
        self.env = {**BASE_ENV, **env}
        self.process: subprocess.Popen[bytes] | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self, timeout: float = 20.0) -> None:
        env = {
            **os.environ,
            **self.env,
            "WEBHOOK_HOST": "127.0.0.1",
            "WEBHOOK_PORT": str(self.port),
            "WEBHOOK_DB_PATH": str(self.db_path),
        }
        with self.log_path.open("ab") as log:
            self.process = subprocess.Popen(
                [sys.executable, "-m", "webhook_receiver"],
                cwd=self.workdir,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"server exited early:\n{self.log_path.read_text()[-2000:]}")
            try:
                if httpx.get(f"{self.url}/readyz", timeout=0.5).status_code == 200:
                    return
            except httpx.TransportError:
                pass
            time.sleep(0.05)
        raise RuntimeError("server did not become ready")

    def kill(self) -> None:
        assert self.process is not None
        self.process.send_signal(signal.SIGKILL)
        self.process.wait()

    def terminate(self, timeout: float = 15.0) -> int:
        assert self.process is not None
        self.process.send_signal(signal.SIGTERM)
        return self.process.wait(timeout=timeout)

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.terminate()

    def rows(self) -> list[dict[str, Any]]:
        assert self.process is None or self.process.poll() is not None, (
            "stop the server before reading the file"
        )
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(row) for row in conn.execute("SELECT * FROM webhooks ORDER BY id")]
        finally:
            conn.close()


class WebhookClient:
    def __init__(self, base_url: str) -> None:
        self.http = httpx.Client(
            base_url=base_url, timeout=10, headers={"Authorization": f"Bearer {API_KEY}"}
        )

    def send(self, key: str, payload: dict[str, Any] | None = None) -> httpx.Response:
        document = {
            "idempotency_key": key,
            "event_type": "order.created",
            "payload": payload or {"order_id": key},
        }
        return self.http.post("/v1/webhooks", json=document)

    def status(self, key: str) -> httpx.Response:
        return self.http.get(f"/v1/webhooks/{key}")

    def wait_for(
        self, key: str, statuses: tuple[str, ...] = FINAL, timeout: float = 30.0
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = self.status(key)
            if response.status_code == 200 and response.json()["status"] in statuses:
                body: dict[str, Any] = response.json()
                return body
            time.sleep(0.05)
        raise AssertionError(f"{key} did not reach {statuses} within {timeout}s")

    def close(self) -> None:
        self.http.close()


@pytest.fixture
def make_server(tmp_path: Path) -> Iterator[Callable[..., ServerProcess]]:
    started: list[ServerProcess] = []

    def factory(**env: str) -> ServerProcess:
        server = ServerProcess(tmp_path, env)
        server.start()
        started.append(server)
        return server

    yield factory
    for server in started:
        server.stop()


@pytest.fixture
def server(make_server: Callable[..., ServerProcess]) -> ServerProcess:
    return make_server()


@pytest.fixture
def client(server: ServerProcess) -> Iterator[WebhookClient]:
    webhook_client = WebhookClient(server.url)
    yield webhook_client
    webhook_client.close()
