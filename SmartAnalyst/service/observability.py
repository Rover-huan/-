"""Request-scoped observability helpers for API and worker logs."""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

from fastapi import Request
from starlette.responses import Response


LOGGER = logging.getLogger("smartanalyst")


def emit_structured_log(
    logger: logging.Logger,
    *,
    level: int = logging.INFO,
    event: str,
    **fields: Any,
) -> None:
    """Write one structured JSON log line with consistent field names."""
    payload: dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "event": event,
    }
    for key, value in fields.items():
        if value is None:
            continue
        payload[key] = value
    logger.log(level, json.dumps(payload, ensure_ascii=False, default=str))


def get_client_ip(request: Request) -> str:
    """Resolve the best-effort client IP, honoring reverse-proxy headers."""
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        first_hop = forwarded_for.split(",", 1)[0].strip()
        if first_hop:
            return first_hop
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def get_request_id(request: Request) -> str:
    """Return the request id assigned by middleware, if present."""
    request_id = getattr(request.state, "request_id", None)
    if isinstance(request_id, str) and request_id.strip():
        return request_id
    return "unknown"


def install_request_context_middleware(app) -> None:
    """Attach request-id propagation and structured request logging."""

    @app.middleware("http")
    async def request_context_middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        request.state.client_ip = get_client_ip(request)
        started_at = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception as exc:
            emit_structured_log(
                LOGGER,
                level=logging.ERROR,
                event="http.request.failed",
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                client_ip=request.state.client_ip,
                error=str(exc),
            )
            raise

        response.headers["X-Request-ID"] = request_id
        emit_structured_log(
            LOGGER,
            event="http.request.completed",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            client_ip=request.state.client_ip,
            duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        )
        return response
