import re
import time
import uuid
from collections.abc import Awaitable, Callable

import structlog
from fastapi import FastAPI, Request, Response

from webhook_receiver.api.errors import unexpected_error_response
from webhook_receiver.observability import Metrics

_REQUEST_ID = re.compile(r"[A-Za-z0-9._-]{1,128}")
# The client picks the method, so only known methods become label values.
_METRIC_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})


def install_request_context(app: FastAPI, metrics: Metrics) -> None:
    @app.middleware("http")
    async def _request_context(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        incoming = request.headers.get("x-request-id", "")
        request_id = incoming if _REQUEST_ID.fullmatch(incoming) else uuid.uuid4().hex
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)
        request.state.request_id = request_id
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            # Answer here so the 500 still gets the header and the metric, and uvicorn
            # never prints its own plain-text traceback.
            response = unexpected_error_response(exc)
        route = getattr(request.scope.get("route"), "path", "unmatched")
        method = request.method if request.method in _METRIC_METHODS else "OTHER"
        metrics.http_duration.labels(
            route=route, method=method, status=str(response.status_code)
        ).observe(time.perf_counter() - started)
        response.headers["X-Request-ID"] = request_id
        return response
