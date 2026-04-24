"""Redis-backed request rate limiting helpers."""

from __future__ import annotations

import logging
from functools import lru_cache

from fastapi import HTTPException, status
from redis import Redis

from service.config import get_settings
from service.observability import emit_structured_log


LOGGER = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_redis_client() -> Redis:
    """Return a shared Redis client for quotas and rate limits."""
    return Redis.from_url(get_settings().redis_url, decode_responses=True)


def enforce_rate_limit(
    *,
    namespace: str,
    subject: str,
    limit: int,
    window_seconds: int,
    error_message: str,
) -> None:
    """Raise HTTP 429 when the subject exceeds the configured request budget."""
    if limit <= 0 or window_seconds <= 0 or not subject.strip():
        return

    key = f"ratelimit:{namespace}:{subject.strip()}"
    try:
        client = get_redis_client()
        current = int(client.incr(key))
        if current == 1:
            client.expire(key, window_seconds)
        if current <= limit:
            return

        retry_after = max(int(client.ttl(key)), 1)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=error_message,
            headers={"Retry-After": str(retry_after)},
        )
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - fail-open on transient redis errors
        emit_structured_log(
            LOGGER,
            level=logging.WARNING,
            event="ratelimit.backend_unavailable",
            namespace=namespace,
            subject=subject,
            error=str(exc),
        )
