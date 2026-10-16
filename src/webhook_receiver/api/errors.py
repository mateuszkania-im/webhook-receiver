from collections.abc import Mapping, Sequence
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

_log = structlog.get_logger("api")
_CODES_BY_STATUS = {404: "not_found", 405: "method_not_allowed"}


class ApiError(Exception):
    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}
        self.headers = headers or {}


def error_response(
    status: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    content = {"error": {"code": code, "message": message, "details": details or {}}}
    return JSONResponse(content, status_code=status, headers=headers)


def validation_details(errors: Sequence[Any]) -> dict[str, Any]:
    return {
        "errors": [
            {"loc": ".".join(str(part) for part in error["loc"]), "message": error["msg"]}
            for error in errors
        ]
    }


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(_request: Request, exc: ApiError) -> JSONResponse:
        return error_response(exc.status, exc.code, exc.message, exc.details, exc.headers)

    @app.exception_handler(RequestValidationError)
    async def _validation(_request: Request, exc: RequestValidationError) -> JSONResponse:
        message = "the request does not match the schema; see details.errors"
        return error_response(422, "validation_failed", message, validation_details(exc.errors()))

    @app.exception_handler(HTTPException)
    async def _http(_request: Request, exc: HTTPException) -> JSONResponse:
        code = _CODES_BY_STATUS.get(exc.status_code, "http_error")
        return error_response(exc.status_code, code, str(exc.detail), headers=exc.headers)


def unexpected_error_response(exc: Exception) -> JSONResponse:
    _log.error("unhandled_error", exc_info=exc)
    request_id = structlog.contextvars.get_contextvars().get("request_id")
    message = "unexpected error; retry later and report the request_id if it persists"
    return error_response(500, "internal_error", message, {"request_id": request_id})
