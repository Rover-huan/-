from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from service import admin_auth
from service.admin_auth import require_owner_admin
from service.config import get_settings
from service.db import Base
from service.models import AdminAccount, AdminRole, User, UserPresence, UserQuotaOverride
from service.quota import get_user_quota_snapshot
from service.security import hash_password, verify_password


def test_owner_admin_bootstrap_preserves_existing_password(monkeypatch):
    monkeypatch.setenv("ADMIN_OWNER_EMAIL", "owner@example.com")
    monkeypatch.setenv("ADMIN_OWNER_INITIAL_PASSWORD", "initial-pass")
    get_settings.cache_clear()

    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    monkeypatch.setattr(admin_auth, "SessionLocal", SessionLocal)

    admin_auth.ensure_owner_admin()
    with SessionLocal() as db:
        owner = db.query(AdminAccount).filter_by(email="owner@example.com").one()
        original_hash = owner.password_hash
        assert owner.role == AdminRole.OWNER.value
        assert verify_password("initial-pass", owner.password_hash)

    monkeypatch.setenv("ADMIN_OWNER_INITIAL_PASSWORD", "changed-pass")
    get_settings.cache_clear()
    admin_auth.ensure_owner_admin()
    with SessionLocal() as db:
        owner = db.query(AdminAccount).filter_by(email="owner@example.com").one()
        assert owner.password_hash == original_hash
        assert verify_password("initial-pass", owner.password_hash)

    get_settings.cache_clear()


def test_viewer_admin_cannot_use_owner_dependency():
    viewer = AdminAccount(
        email="viewer@example.com",
        password_hash=hash_password("viewer-pass"),
        role=AdminRole.VIEWER.value,
    )
    with pytest.raises(HTTPException) as exc_info:
        require_owner_admin(viewer)
    assert exc_info.value.status_code == 403


def test_quota_override_changes_effective_limits():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    with SessionLocal() as db:
        user = User(email="user@example.com", password_hash="hash")
        db.add(user)
        db.flush()
        db.add(
            UserQuotaOverride(
                user_id=user.id,
                daily_job_limit=3,
                daily_upload_bytes_limit=1024,
                active_job_limit=1,
            )
        )
        db.commit()
        snapshot = get_user_quota_snapshot(db, user.id)
        assert snapshot.daily_job_limit == 3
        assert snapshot.daily_upload_bytes_limit == 1024
        assert snapshot.active_job_limit == 1


def test_presence_online_window_logic():
    now = datetime.utcnow()
    active = UserPresence(
        user_id="u1",
        email="active@example.com",
        last_seen_at=now - timedelta(minutes=4),
    )
    stale = UserPresence(
        user_id="u2",
        email="stale@example.com",
        last_seen_at=now - timedelta(minutes=6),
    )
    cutoff = now - timedelta(minutes=5)
    assert active.last_seen_at >= cutoff
    assert stale.last_seen_at < cutoff
