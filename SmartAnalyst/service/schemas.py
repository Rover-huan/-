"""API schemas for the SmartAnalyst service."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class RegisterRequest(BaseModel):
    email: str
    password: str = Field(min_length=8)
    captcha_verify_param: str | None = None


class LoginRequest(BaseModel):
    email: str
    password: str


class AdminLoginRequest(BaseModel):
    email: str
    password: str


class PresenceHeartbeatRequest(BaseModel):
    current_job_id: str | None = None
    current_path: str | None = None


class AdminGrantRequest(BaseModel):
    email: str
    role: str = "viewer"


class UserStatusUpdateRequest(BaseModel):
    status: str


class UserQuotaOverrideRequest(BaseModel):
    daily_job_limit: int | None = Field(default=None, ge=0)
    daily_upload_bytes_limit: int | None = Field(default=None, ge=0)
    active_job_limit: int | None = Field(default=None, ge=0)


class UserResponse(BaseModel):
    id: str
    email: str
    status: str
    created_at: datetime
    email_verified: bool
    email_verified_at: datetime | None = None


class EmailVerificationRequest(BaseModel):
    email: str
    token: str


class ResendEmailVerificationRequest(BaseModel):
    email: str


class MessageResponse(BaseModel):
    status: str
    message: str


class SessionResponse(BaseModel):
    user: UserResponse


class AdminAccountResponse(BaseModel):
    id: str
    email: str
    role: str
    status: str
    created_at: datetime
    last_login_at: datetime | None = None


class AdminSessionResponse(BaseModel):
    admin: AdminAccountResponse


class JobSelectionRequest(BaseModel):
    selected_task_ids: list[int]


class QuotaRemainingResponse(BaseModel):
    jobs_used_today: int
    jobs_remaining: int
    daily_job_limit: int
    upload_bytes_used_today: int
    upload_bytes_remaining: int
    daily_upload_bytes_limit: int
    active_jobs: int
    active_jobs_remaining: int
    active_job_limit: int


class JobTaskResponse(BaseModel):
    task_index: int
    question_zh: str
    analysis_type: str
    required_datasets: list[str]
    selected: bool
    analysis_text: str | None = None
    chart_url: str | None = None


class JobArtifactResponse(BaseModel):
    artifact_type: str
    download_url: str
    created_at: datetime


class JobEventResponse(BaseModel):
    id: str
    level: str
    event_type: str
    message: str
    payload_json: dict[str, Any] | None = None
    created_at: datetime


class JobSummaryResponse(BaseModel):
    id: str
    status: str
    phase: str
    progress_percent: int
    report_title: str | None = None
    selected_task_ids: list[int] | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    retention_expires_at: datetime | None = None
    error_summary: str | None = None
    error_title: str | None = None
    user_message: str | None = None
    error_category: str | None = None
    error_code: str | None = None
    raw_detail: str | None = None
    suggested_actions: list[str] = Field(default_factory=list)
    queue_position: int | None = None
    retry_count: int = 0
    failure_code: str | None = None
    failure_stage: str | None = None
    latest_event_id: str | None = None
    quota_remaining: QuotaRemainingResponse
    tasks_url: str
    events_url: str
    artifacts_url: str
    stream_url: str


class JobListResponse(BaseModel):
    jobs: list[JobSummaryResponse]


class JobDeleteResponse(BaseModel):
    status: str
    job_id: str
    queue_task_revoked: bool = False
    quota_remaining: QuotaRemainingResponse


class JobTaskListResponse(BaseModel):
    tasks: list[JobTaskResponse] = Field(default_factory=list)


class JobArtifactListResponse(BaseModel):
    artifacts: list[JobArtifactResponse] = Field(default_factory=list)


class JobEventListResponse(BaseModel):
    events: list[JobEventResponse] = Field(default_factory=list)
    cursor: str | None = None


class AdminJobDebugResponse(BaseModel):
    user: UserResponse
    job: JobSummaryResponse
    inputs: list[dict[str, Any]] = Field(default_factory=list)
    tasks: list[JobTaskResponse] = Field(default_factory=list)
    artifacts: list[JobArtifactResponse] = Field(default_factory=list)
    events: list[JobEventResponse] = Field(default_factory=list)


class AdminOverviewResponse(BaseModel):
    total_users: int
    verified_users: int
    active_jobs_total: int
    queued_analysis: int
    running_analysis: int
    awaiting_selection: int
    queued_render: int
    rendering: int
    jobs_created_today: int
    completed_today: int
    failed_today: int
    uploads_bytes_today: int
    llm_calls_today: int
    llm_daily_budget_limit: int
