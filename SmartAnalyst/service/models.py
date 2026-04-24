"""ORM models for the SmartAnalyst service."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from service.db import Base


def _uuid() -> str:
    return uuid.uuid4().hex


class UserStatus(str, enum.Enum):
    ACTIVE = "active"
    DISABLED = "disabled"


class AdminRole(str, enum.Enum):
    OWNER = "owner"
    VIEWER = "viewer"


class AdminStatus(str, enum.Enum):
    ACTIVE = "active"
    DISABLED = "disabled"


class JobStatus(str, enum.Enum):
    UPLOADED = "uploaded"
    QUEUED_ANALYSIS = "queued_analysis"
    RUNNING_ANALYSIS = "running_analysis"
    AWAITING_SELECTION = "awaiting_selection"
    QUEUED_RENDER = "queued_render"
    RENDERING = "rendering"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


class JobPhase(str, enum.Enum):
    UPLOAD = "upload"
    ANALYSIS = "analysis"
    SELECTION = "selection"
    RENDER = "render"
    COMPLETE = "complete"
    FAILED = "failed"
    EXPIRED = "expired"


class ArtifactType(str, enum.Enum):
    DOCX = "docx"
    PDF = "pdf"
    NOTEBOOK = "ipynb"
    CLEANING_SUMMARY = "txt"
    ZIP = "zip"


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uuid)
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default=UserStatus.ACTIVE.value, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    email_verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    email_verification_token_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    email_verification_sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    jobs: Mapped[list["Job"]] = relationship(back_populates="user", cascade="all, delete-orphan")


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uuid)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), default=JobStatus.UPLOADED.value, nullable=False, index=True)
    phase: Mapped[str] = mapped_column(String(32), default=JobPhase.UPLOAD.value, nullable=False)
    progress_percent: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    report_title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    selected_task_ids: Mapped[list[int] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    queue_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    dataset_meta_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    data_summary_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    report_payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    user: Mapped["User"] = relationship(back_populates="jobs")
    inputs: Mapped[list["JobInput"]] = relationship(back_populates="job", cascade="all, delete-orphan")
    tasks: Mapped[list["JobTask"]] = relationship(back_populates="job", cascade="all, delete-orphan")
    artifacts: Mapped[list["JobArtifact"]] = relationship(back_populates="job", cascade="all, delete-orphan")
    events: Mapped[list["JobEvent"]] = relationship(back_populates="job", cascade="all, delete-orphan")


class UserPresence(Base):
    __tablename__ = "user_presence"

    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    email: Mapped[str] = mapped_column(String(320), nullable=False, index=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    current_job_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    current_path: Mapped[str | None] = mapped_column(String(512), nullable=True)


class UserQuotaOverride(Base):
    __tablename__ = "user_quota_overrides"

    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    daily_job_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    daily_upload_bytes_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    active_job_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_by_admin_id: Mapped[str | None] = mapped_column(String(32), nullable=True)


class AdminAccount(Base):
    __tablename__ = "admin_accounts"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uuid)
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(512), nullable=False)
    role: Mapped[str] = mapped_column(String(32), default=AdminRole.VIEWER.value, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), default=AdminStatus.ACTIVE.value, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class JobInput(Base):
    __tablename__ = "job_inputs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    original_name: Mapped[str] = mapped_column(String(512), nullable=False)
    storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)

    job: Mapped["Job"] = relationship(back_populates="inputs")


class JobTask(Base):
    __tablename__ = "job_tasks"
    __table_args__ = (UniqueConstraint("job_id", "task_index", name="uq_job_task_index"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    task_index: Mapped[int] = mapped_column(Integer, nullable=False)
    question_zh: Mapped[str] = mapped_column(String(1024), nullable=False)
    analysis_type: Mapped[str] = mapped_column(String(32), nullable=False)
    required_datasets: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    image_storage_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    analysis_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    selected: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    task_plan_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    result_payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    job: Mapped["Job"] = relationship(back_populates="tasks")


class JobArtifact(Base):
    __tablename__ = "job_artifacts"
    __table_args__ = (UniqueConstraint("job_id", "artifact_type", name="uq_job_artifact_type"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    artifact_type: Mapped[str] = mapped_column(String(32), nullable=False)
    storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    file_size: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    job: Mapped["Job"] = relationship(back_populates="artifacts")


class JobEvent(Base):
    __tablename__ = "job_events"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    job: Mapped["Job"] = relationship(back_populates="events")
