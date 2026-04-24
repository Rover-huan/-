from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from service.config import get_settings
from service.db import Base
from service.email_verification import email_is_verified, issue_email_verification, verify_email_token
from service.models import User


def test_email_verification_token_roundtrip(monkeypatch):
    monkeypatch.setenv("EMAIL_VERIFICATION_REQUIRED", "true")
    monkeypatch.setenv("SECRET_KEY", "test-secret")
    get_settings.cache_clear()

    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, future=True)

    with SessionLocal() as db:
        user = User(email="person@example.com", password_hash="hash")
        token = issue_email_verification(user)
        db.add(user)
        db.commit()

        assert not email_is_verified(user)

        verified_user = verify_email_token(db, "person@example.com", token)
        assert verified_user is not None
        assert verified_user.email_verified_at is not None
        assert verified_user.email_verification_token_hash is None

    get_settings.cache_clear()
