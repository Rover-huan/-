"""Redis-backed model usage accounting and budget guardrails."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from service.config import get_settings
from service.rate_limit import get_redis_client


LOGGER = logging.getLogger(__name__)


def _llm_usage_key(day: datetime | None = None) -> str:
    target_day = day or datetime.utcnow()
    return f"usage:llm_calls:{target_day:%Y%m%d}"


def get_llm_usage_snapshot() -> dict[str, int]:
    """Return the current daily model-call usage snapshot."""
    settings = get_settings()
    try:
        client = get_redis_client()
        calls_today = int(client.get(_llm_usage_key()) or 0)
    except Exception as exc:  # pragma: no cover - fail-open on transient redis errors
        LOGGER.warning("Could not read LLM usage from Redis: %s", exc)
        calls_today = 0
    return {
        "calls_today": calls_today,
        "daily_budget_limit": settings.llm_daily_budget_limit,
        "remaining": max(settings.llm_daily_budget_limit - calls_today, 0)
        if settings.llm_daily_budget_limit > 0
        else 0,
    }


def enforce_llm_budget() -> None:
    """Raise when the global daily LLM call budget has been exhausted."""
    settings = get_settings()
    if settings.llm_daily_budget_limit <= 0:
        return

    snapshot = get_llm_usage_snapshot()
    if snapshot["calls_today"] >= settings.llm_daily_budget_limit:
        raise RuntimeError("Daily LLM budget limit reached. Please try again after the budget resets.")


def record_llm_call() -> None:
    """Record one successful upstream model call."""
    try:
        client = get_redis_client()
        key = _llm_usage_key()
        current = int(client.incr(key))
        if current == 1:
            client.expire(key, int(timedelta(days=2).total_seconds()))
    except Exception as exc:  # pragma: no cover - fail-open on transient redis errors
        LOGGER.warning("Could not record LLM usage in Redis: %s", exc)
