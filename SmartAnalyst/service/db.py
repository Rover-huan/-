"""Database setup for the SmartAnalyst service."""

from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker

from service.config import get_settings


settings = get_settings()
connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(settings.database_url, future=True, pool_pre_ping=True, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


def get_db_session() -> Generator:
    """FastAPI dependency that yields a managed SQLAlchemy session."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _upgrade_local_sqlite_schema() -> None:
    """Apply tiny local-only SQLite upgrades for developer databases."""
    if not settings.database_url.startswith("sqlite"):
        return

    inspector = inspect(engine)
    if "users" not in inspector.get_table_names():
        return

    existing_columns = {column["name"] for column in inspector.get_columns("users")}
    sqlite_columns = {
        "email_verified_at": "DATETIME",
        "email_verification_token_hash": "VARCHAR(128)",
        "email_verification_sent_at": "DATETIME",
    }
    with engine.begin() as connection:
        for column_name, column_type in sqlite_columns.items():
            if column_name not in existing_columns:
                connection.execute(text(f"ALTER TABLE users ADD COLUMN {column_name} {column_type}"))


def init_db() -> None:
    """Create all ORM-backed tables for local development."""
    from service import models  # noqa: F401

    if not settings.auto_create_tables:
        return

    Base.metadata.create_all(bind=engine)
    _upgrade_local_sqlite_schema()
