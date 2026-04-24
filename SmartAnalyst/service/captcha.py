"""Captcha verification helpers for public registration."""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import HTTPException, Request, status

from service.config import get_settings


def _captcha_success(payload: Any) -> bool:
    """Accept common success shapes from managed captcha providers."""
    if not isinstance(payload, dict):
        return False

    if payload.get("success") is True:
        return True
    if payload.get("passed") is True:
        return True

    code = payload.get("code")
    if code in {0, "0", "Success", "success", "OK", "ok"}:
        return True

    result = payload.get("result")
    if isinstance(result, dict):
        return _captcha_success(result)

    return False


def verify_captcha(captcha_verify_param: str | None, request: Request) -> None:
    """Raise an HTTP error when captcha verification fails."""
    settings = get_settings()
    if not settings.captcha_required:
        return

    token = (captcha_verify_param or "").strip()
    if settings.captcha_bypass_token and token == settings.captcha_bypass_token:
        return
    if not token:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Captcha verification is required.")
    if not settings.captcha_verify_url or not settings.captcha_secret_key:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Captcha is not configured.")

    client_ip = getattr(request.state, "client_ip", "")
    if not client_ip and request.client is not None:
        client_ip = request.client.host
    payload = {
        "provider": settings.captcha_provider,
        "secret": settings.captcha_secret_key,
        "response": token,
        "captcha_verify_param": token,
        "remote_ip": client_ip,
    }
    try:
        response = httpx.post(settings.captcha_verify_url, json=payload, timeout=5.0)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Captcha provider is temporarily unavailable.",
        ) from exc

    try:
        provider_payload = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Captcha provider returned an invalid response.",
        ) from exc

    if not _captcha_success(provider_payload):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Captcha verification failed.")
