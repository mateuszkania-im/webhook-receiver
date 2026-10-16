import json
import math
from functools import partial
from typing import Any, NoReturn

import structlog
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from webhook_receiver.api.auth import check_api_key
from webhook_receiver.api.context import AppContext, Context, get_context, require_api_key
from webhook_receiver.api.errors import ApiError, validation_details
from webhook_receiver.api.schemas import StatusDocument, WebhookIn
from webhook_receiver.idempotency import payload_hash
from webhook_receiver.store.repository import (
    IntakeOutcome,
    IntakeResult,
    NewWebhook,
    get_webhook,
    intake,
)

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])
_log = structlog.get_logger("api")
RETRY_AFTER_S = 5
_OUTCOME_BY_CODE = {
    "unauthorized": "rejected_auth",
    "rate_limited": "rate_limited",
    "backlog_full": "backpressure",
    "idempotency_conflict": "conflict",
}


def status_url(key: str) -> str:
    return f"{router.prefix}/{key}"


@router.post("", status_code=202)
async def receive_webhook(request: Request) -> JSONResponse:
    ctx = get_context(request)
    try:
        body = await _read_limited_body(request, ctx.settings.max_body_bytes)
        _require_json(request.headers.get("content-type"))
        check_api_key(ctx.settings.api_key, request.headers.get("authorization"))
        webhook = parse_webhook(body)
        _check_rate_limit(ctx)
        new = NewWebhook(
            idempotency_key=webhook.idempotency_key,
            event_type=webhook.event_type,
            payload_json=json.dumps(webhook.payload, ensure_ascii=False),
            payload_hash=payload_hash(webhook.event_type, webhook.payload),
            request_id=request.state.request_id,
        )
        result = await ctx.db.transaction(
            partial(
                intake, new=new, now_ms=ctx.clock.now_ms(), backlog_limit=ctx.settings.backlog_limit
            )
        )
        return _intake_response(ctx, result)
    except ApiError as error:
        outcome = _OUTCOME_BY_CODE.get(error.code, "invalid")
        ctx.metrics.webhooks_received.labels(outcome=outcome).inc()
        event = "webhook_conflict" if outcome == "conflict" else "webhook_rejected"
        _log.info(event, code=error.code, status=error.status)
        raise


@router.get("/{key}", dependencies=[Depends(require_api_key)])
async def get_status(key: str, ctx: Context) -> JSONResponse:
    record = await ctx.db.run(partial(get_webhook, key=key))
    if record is None:
        raise ApiError(404, "not_found", f"no webhook with idempotency_key {key}")
    return JSONResponse(StatusDocument.from_record(record).model_dump(mode="json"))


def parse_webhook(body: bytes) -> WebhookIn:
    try:
        document = json.loads(body, parse_constant=_reject_constant, parse_float=_finite_float)
        # A lone surrogate escape ("\ud800") parses but cannot be encoded for hashing or storage.
        json.dumps(document, ensure_ascii=False).encode()
    except (ValueError, RecursionError) as exc:
        details = {"errors": [{"loc": "body", "message": str(exc) or type(exc).__name__}]}
        raise ApiError(
            422, "validation_failed", "the body is not valid JSON", details=details
        ) from exc
    try:
        return WebhookIn.model_validate(document)
    except ValidationError as exc:
        message = "the webhook does not match the schema; see details.errors"
        raise ApiError(
            422, "validation_failed", message, details=validation_details(exc.errors())
        ) from exc


async def _read_limited_body(request: Request, limit: int) -> bytes:
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > limit:
        _too_large(limit)
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            _too_large(limit)
    return bytes(body)


def _too_large(limit: int) -> NoReturn:
    raise ApiError(
        413, "payload_too_large", f"the body exceeds {limit} bytes", details={"limit": limit}
    )


def _require_json(content_type: str | None) -> None:
    if (content_type or "").split(";")[0].strip().lower() != "application/json":
        raise ApiError(
            415, "unsupported_media_type", "send the webhook with Content-Type: application/json"
        )


def _check_rate_limit(ctx: AppContext) -> None:
    wait_s = ctx.limiter.acquire()
    if wait_s is not None:
        raise _retry_later(429, "rate_limited", "too many requests", max(1, math.ceil(wait_s)))


def _retry_later(status: int, code: str, reason: str, retry_after_s: int) -> ApiError:
    return ApiError(
        status,
        code,
        f"{reason}; retry in {retry_after_s} s",
        details={"retry_after_s": retry_after_s},
        headers={"Retry-After": str(retry_after_s)},
    )


def _intake_response(ctx: AppContext, result: IntakeResult) -> JSONResponse:
    if result.outcome is IntakeOutcome.BACKLOG_FULL:
        raise _retry_later(503, "backlog_full", "the receiver is at capacity", RETRY_AFTER_S)
    record = result.record
    if record is None:
        raise RuntimeError(f"intake returned no record for outcome {result.outcome}")
    if result.outcome is IntakeOutcome.CONFLICT:
        message = (
            f"key {record.idempotency_key} was already used with a different payload; "
            "send a new idempotency_key"
        )
        raise ApiError(
            409,
            "idempotency_conflict",
            message,
            details={"idempotency_key": record.idempotency_key},
        )
    document = StatusDocument.from_record(record).model_dump(mode="json")
    headers = {"Location": status_url(record.idempotency_key)}
    if result.outcome is IntakeOutcome.DUPLICATE:
        ctx.metrics.webhooks_received.labels(outcome="duplicate").inc()
        _log.info("webhook_duplicate", idempotency_key=record.idempotency_key)
        return JSONResponse(
            document, status_code=200, headers={**headers, "Idempotent-Replayed": "true"}
        )
    ctx.pool.notify()
    ctx.metrics.webhooks_received.labels(outcome="accepted").inc()
    _log.info("webhook_received", idempotency_key=record.idempotency_key)
    return JSONResponse(document, status_code=202, headers=headers)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")


def _finite_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        raise ValueError(f"{text} is out of range")
    return value
