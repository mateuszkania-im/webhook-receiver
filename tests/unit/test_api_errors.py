import random
import string
from typing import Annotated

import pytest
from fastapi import FastAPI, Query
from fastapi.testclient import TestClient

from webhook_receiver.api.errors import ApiError, install_error_handlers
from webhook_receiver.api.middleware import install_request_context
from webhook_receiver.observability import Metrics


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    metrics = Metrics()
    install_error_handlers(app)
    install_request_context(app, metrics)
    app.state.metrics = metrics

    @app.get("/teapot")
    async def teapot() -> None:
        raise ApiError(
            418, "teapot", "short and stout", details={"spout": True}, headers={"Retry-After": "3"}
        )

    @app.get("/typed")
    async def typed(limit: Annotated[int, Query(ge=1)]) -> dict[str, int]:
        return {"limit": limit}

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("secret detail")

    return TestClient(app, raise_server_exceptions=False)


def test_api_error_uses_the_envelope_and_headers(client: TestClient) -> None:
    response = client.get("/teapot")
    assert response.status_code == 418
    assert response.json() == {
        "error": {"code": "teapot", "message": "short and stout", "details": {"spout": True}}
    }
    assert response.headers["Retry-After"] == "3"


def test_validation_errors_use_the_envelope(client: TestClient) -> None:
    response = client.get("/typed", params={"limit": 0})
    body = response.json()
    assert response.status_code == 422
    assert body["error"]["code"] == "validation_failed"
    assert body["error"]["details"]["errors"][0]["loc"] == "query.limit"


def test_valid_request_ids_are_echoed_and_invalid_ones_replaced(client: TestClient) -> None:
    echoed = client.get("/typed", params={"limit": 1}, headers={"X-Request-ID": "abc-123"})
    replaced = client.get("/typed", params={"limit": 1}, headers={"X-Request-ID": "a b\tc"})
    assert echoed.headers["X-Request-ID"] == "abc-123"
    assert len(replaced.headers["X-Request-ID"]) == 32


def test_arbitrary_methods_share_one_metric_series(client: TestClient) -> None:
    rng = random.Random(7)
    for _ in range(50):
        client.request("".join(rng.choices(string.ascii_uppercase, k=12)), "/typed")
    metrics: Metrics = client.app.state.metrics  # type: ignore[attr-defined]
    counts = [
        sample
        for family in metrics.registry.collect()
        for sample in family.samples
        if sample.name == "http_request_duration_seconds_count"
    ]
    assert {sample.labels["method"] for sample in counts} == {"OTHER"}
    assert sum(sample.value for sample in counts) == 50


def test_unexpected_errors_are_hidden_traced_and_measured(
    client: TestClient,
) -> None:
    response = client.get("/boom", headers={"X-Request-ID": "trace-500"})
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal_error"
    assert "secret detail" not in response.text
    assert response.headers["X-Request-ID"] == "trace-500"
    assert response.json()["error"]["details"] == {"request_id": "trace-500"}
    metrics: Metrics = client.app.state.metrics  # type: ignore[attr-defined]
    count = metrics.registry.get_sample_value(
        "http_request_duration_seconds_count", {"route": "/boom", "method": "GET", "status": "500"}
    )
    assert count == 1
