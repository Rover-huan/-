"""Administrator account bootstrap and auth dependencies."""

from __future__ import annotations

from datetime import datetime

from fastapi import Depends, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from service.config import get_settings
from service.db import SessionLocal, get_db_session
from service.models import AdminAccount, AdminRole, AdminStatus
from service.security import create_access_token, decode_access_token, hash_password


def ensure_owner_admin() -> None:
    """Create the configured owner admin account if it does not already exist."""
    settings = get_settings()
    owner_email = (settings.admin_owner_email or "").strip().lower()
    owner_password = settings.admin_owner_initial_password or ""
    if not owner_email or not owner_password:
        return

    db = SessionLocal()
    try:
        existing = db.scalar(select(AdminAccount).where(AdminAccount.email == owner_email))
        if existing is not None:
            if existing.role != AdminRole.OWNER.value or existing.status != AdminStatus.ACTIVE.value:
                existing.role = AdminRole.OWNER.value
                existing.status = AdminStatus.ACTIVE.value
                db.add(existing)
                db.commit()
            return

        owner = AdminAccount(
            email=owner_email,
            password_hash=hash_password(owner_password),
            role=AdminRole.OWNER.value,
            status=AdminStatus.ACTIVE.value,
        )
        db.add(owner)
        db.commit()
    finally:
        db.close()


def set_admin_session_cookie(response: Response, admin_id: str) -> None:
    """Set the HttpOnly admin session cookie."""
    settings = get_settings()
    token = create_access_token(
        admin_id,
        expires_minutes=settings.admin_access_token_expire_minutes,
        token_type="admin",
    )
    response.set_cookie(
        key=settings.admin_session_cookie_name,
        value=token,
        max_age=settings.admin_access_token_expire_minutes * 60,
        httponly=True,
        secure=settings.auth_cookie_secure,
        samesite=settings.auth_cookie_samesite,
        domain=settings.auth_cookie_domain,
        path="/",
    )


def clear_admin_session_cookie(response: Response) -> None:
    """Clear the admin session cookie."""
    settings = get_settings()
    response.delete_cookie(
        key=settings.admin_session_cookie_name,
        domain=settings.auth_cookie_domain,
        path="/",
    )


def get_current_admin(
    request: Request,
    db: Session = Depends(get_db_session),
) -> AdminAccount:
    """Resolve the authenticated administrator from the admin session cookie."""
    credentials_error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="管理员登录状态无效，请重新登录。",
    )
    token = request.cookies.get(get_settings().admin_session_cookie_name)
    if not token:
        raise credentials_error

    try:
        payload = decode_access_token(token)
    except Exception as exc:  # pragma: no cover - token library exceptions vary
        raise credentials_error from exc

    if payload.get("type") != "admin":
        raise credentials_error
    subject = payload.get("sub")
    if not isinstance(subject, str) or not subject.strip():
        raise credentials_error

    admin = db.scalar(select(AdminAccount).where(AdminAccount.id == subject.strip()))
    if admin is None or admin.status != AdminStatus.ACTIVE.value:
        raise credentials_error
    return admin


def require_owner_admin(admin: AdminAccount = Depends(get_current_admin)) -> AdminAccount:
    """Require the current administrator to be the owner."""
    if admin.role != AdminRole.OWNER.value:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="需要所有者权限。")
    return admin


def mark_admin_login(db: Session, admin: AdminAccount) -> None:
    """Update last-login timestamp."""
    admin.last_login_at = datetime.utcnow()
    db.add(admin)
    db.commit()
    db.refresh(admin)
