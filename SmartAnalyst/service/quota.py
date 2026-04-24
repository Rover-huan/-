"""User quota calculations for public-beta guardrails."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from service.config import get_settings
from service.models import Job, JobInput, JobStatus, UserQuotaOverride


ACTIVE_JOB_STATUSES = {
    JobStatus.QUEUED_ANALYSIS.value,
    JobStatus.RUNNING_ANALYSIS.value,
    JobStatus.AWAITING_SELECTION.value,
    JobStatus.QUEUED_RENDER.value,
    JobStatus.RENDERING.value,
}


@dataclass(frozen=True)
class QuotaSnapshot:
    jobs_used_today: int
    jobs_remaining: int
    daily_job_limit: int
    upload_bytes_used_today: int
    upload_bytes_remaining: int
    daily_upload_bytes_limit: int
    active_jobs: int
    active_jobs_remaining: int
    active_job_limit: int

    def to_payload(self) -> dict[str, int]:
        return {
            "jobs_used_today": self.jobs_used_today,
            "jobs_remaining": self.jobs_remaining,
            "daily_job_limit": self.daily_job_limit,
            "upload_bytes_used_today": self.upload_bytes_used_today,
            "upload_bytes_remaining": self.upload_bytes_remaining,
            "daily_upload_bytes_limit": self.daily_upload_bytes_limit,
            "active_jobs": self.active_jobs,
            "active_jobs_remaining": self.active_jobs_remaining,
            "active_job_limit": self.active_job_limit,
        }


def _utc_day_start() -> datetime:
    now = datetime.utcnow()
    return datetime(now.year, now.month, now.day)


def get_user_quota_limits(db: Session, user_id: str) -> dict[str, int]:
    """Return effective quota limits with optional per-user overrides."""
    settings = get_settings()
    override = db.scalar(select(UserQuotaOverride).where(UserQuotaOverride.user_id == user_id))
    return {
        "daily_job_limit": (
            int(override.daily_job_limit)
            if override is not None and override.daily_job_limit is not None
            else settings.max_daily_jobs_per_user
        ),
        "daily_upload_bytes_limit": (
            int(override.daily_upload_bytes_limit)
            if override is not None and override.daily_upload_bytes_limit is not None
            else settings.max_daily_upload_bytes_per_user
        ),
        "active_job_limit": (
            int(override.active_job_limit)
            if override is not None and override.active_job_limit is not None
            else settings.max_user_active_jobs
        ),
    }


def get_user_quota_snapshot(db: Session, user_id: str) -> QuotaSnapshot:
    """Calculate the user's current active and daily usage budget."""
    day_start = _utc_day_start()
    limits = get_user_quota_limits(db, user_id)

    jobs_used_today = int(
        db.scalar(
            select(func.count(Job.id)).where(
                Job.user_id == user_id,
                Job.created_at >= day_start,
            )
        )
        or 0
    )
    upload_bytes_used_today = int(
        db.scalar(
            select(func.coalesce(func.sum(JobInput.size_bytes), 0))
            .select_from(JobInput)
            .join(Job, JobInput.job_id == Job.id)
            .where(
                Job.user_id == user_id,
                Job.created_at >= day_start,
            )
        )
        or 0
    )
    active_jobs = int(
        db.scalar(
            select(func.count(Job.id)).where(
                Job.user_id == user_id,
                Job.status.in_(ACTIVE_JOB_STATUSES),
            )
        )
        or 0
    )

    return QuotaSnapshot(
        jobs_used_today=jobs_used_today,
        jobs_remaining=max(limits["daily_job_limit"] - jobs_used_today, 0),
        daily_job_limit=limits["daily_job_limit"],
        upload_bytes_used_today=upload_bytes_used_today,
        upload_bytes_remaining=max(limits["daily_upload_bytes_limit"] - upload_bytes_used_today, 0),
        daily_upload_bytes_limit=limits["daily_upload_bytes_limit"],
        active_jobs=active_jobs,
        active_jobs_remaining=max(limits["active_job_limit"] - active_jobs, 0),
        active_job_limit=limits["active_job_limit"],
    )


def enforce_submission_quota(db: Session, user_id: str, incoming_bytes: int) -> QuotaSnapshot:
    """Validate quota limits before accepting a new report-generation job."""
    snapshot = get_user_quota_snapshot(db, user_id)
    if snapshot.jobs_remaining <= 0:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Daily job limit reached for this account.",
        )
    if snapshot.upload_bytes_remaining < incoming_bytes:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Daily upload quota exceeded for this account.",
        )
    return snapshot
