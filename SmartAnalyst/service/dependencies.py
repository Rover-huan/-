"""FastAPI dependencies shared across route handlers."""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from service.config import get_settings
from service.db import get_db_session
from service.models import Job, User, UserStatus
from service.security import decode_access_token


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)


def get_access_token(
    request: Request,
    bearer_token: str | None = Depends(oauth2_scheme),
) -> str:
    """Resolve the access token from the session cookie or Authorization header."""
    settings = get_settings()
    cookie_token = request.cookies.get(settings.session_cookie_name)
    token = cookie_token or bearer_token
    if isinstance(token, str) and token.strip():
        return token.strip()
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials.",
        headers={"WWW-Authenticate": "Bearer"},
    )


def get_current_user(
    token: str = Depends(get_access_token),
    db: Session = Depends(get_db_session),
) -> User:
    """Resolve the authenticated user from the bearer token."""
    credentials_error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = decode_access_token(token)
    except Exception as exc:  # pragma: no cover - token library exceptions vary
        raise credentials_error from exc

    subject = payload.get("sub")
    if not isinstance(subject, str) or not subject.strip():
        raise credentials_error

    user = db.scalar(select(User).where(User.id == subject.strip()))
    if user is None or user.status != UserStatus.ACTIVE.value:
        raise credentials_error
    return user


def get_owned_job(job_id: str, current_user: User, db: Session) -> Job:
    """Load a job and ensure it belongs to the current user."""
    job = db.scalar(select(Job).where(Job.id == job_id, Job.user_id == current_user.id))
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job
