"""Email verification helpers for public registration."""

from __future__ import annotations

import hashlib
import hmac
import secrets
import smtplib
from datetime import datetime, timedelta
from email.message import EmailMessage
from urllib.parse import urlencode

from sqlalchemy import select
from sqlalchemy.orm import Session

from service.config import get_settings
from service.models import User


def email_is_verified(user: User) -> bool:
    """Return whether a user can submit report jobs."""
    settings = get_settings()
    return (not settings.email_verification_required) or user.email_verified_at is not None


def hash_email_token(token: str) -> str:
    """Hash verification tokens before storing them."""
    settings = get_settings()
    digest = hmac.new(settings.secret_key.encode("utf-8"), token.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest


def issue_email_verification(user: User) -> str:
    """Create and attach a new verification token for a user."""
    token = secrets.token_urlsafe(32)
    user.email_verification_token_hash = hash_email_token(token)
    user.email_verification_sent_at = datetime.utcnow()
    if not get_settings().email_verification_required:
        user.email_verified_at = datetime.utcnow()
    return token


def build_verification_url(email: str, token: str) -> str:
    """Build the public browser URL used by email verification messages."""
    settings = get_settings()
    query = urlencode({"email": email, "token": token})
    return f"{settings.public_base_url.rstrip('/')}/verify-email?{query}"


def ensure_email_transport_configured() -> None:
    """Fail fast in production when email verification is enabled but SMTP is missing."""
    settings = get_settings()
    if not settings.email_verification_required:
        return
    missing = [
        name
        for name, value in {
            "SMTP_HOST": settings.smtp_host,
            "SMTP_FROM_EMAIL": settings.smtp_from_email,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Email verification is enabled but {', '.join(missing)} is not configured.")


def send_verification_email(user: User, token: str) -> None:
    """Send the verification email when SMTP is configured."""
    settings = get_settings()
    if not settings.email_verification_required:
        return

    ensure_email_transport_configured()
    verification_url = build_verification_url(user.email, token)

    message = EmailMessage()
    message["Subject"] = "Verify your SmartAnalyst account"
    message["From"] = settings.smtp_from_email or ""
    message["To"] = user.email
    message.set_content(
        "Welcome to SmartAnalyst.\n\n"
        "Please verify your email address before submitting report-generation jobs:\n"
        f"{verification_url}\n\n"
        "If you did not create this account, you can ignore this email.\n"
    )

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=15) as smtp:
        if settings.smtp_use_tls:
            smtp.starttls()
        if settings.smtp_username and settings.smtp_password:
            smtp.login(settings.smtp_username, settings.smtp_password)
        smtp.send_message(message)


def verify_email_token(db: Session, email: str, token: str) -> User | None:
    """Mark one user as verified when the token is valid and not expired."""
    settings = get_settings()
    normalized_email = email.strip().lower()
    normalized_token = token.strip()
    if not normalized_email or not normalized_token:
        return None

    user = db.scalar(select(User).where(User.email == normalized_email))
    if user is None or user.email_verification_token_hash is None or user.email_verification_sent_at is None:
        return None

    sent_at = user.email_verification_sent_at
    expires_at = sent_at + timedelta(minutes=settings.email_verification_token_ttl_minutes)
    if datetime.utcnow() > expires_at:
        return None

    if not hmac.compare_digest(user.email_verification_token_hash, hash_email_token(normalized_token)):
        return None

    user.email_verified_at = datetime.utcnow()
    user.email_verification_token_hash = None
    user.email_verification_sent_at = None
    db.add(user)
    db.commit()
    db.refresh(user)
    return user
