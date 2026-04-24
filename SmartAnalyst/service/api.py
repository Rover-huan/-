"""FastAPI application for SmartAnalyst public-beta orchestration."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import Depends, FastAPI, File, Header, HTTPException, Query, Request, Response, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy import func, or_, select, text
from sqlalchemy.orm import Session, selectinload

from service.captcha import verify_captcha
from service.celery_app import celery_app
from service.admin_auth import (
    clear_admin_session_cookie,
    ensure_owner_admin,
    get_current_admin,
    mark_admin_login,
    require_owner_admin,
    set_admin_session_cookie,
)
from service.config import get_settings
from service.db import SessionLocal, get_db_session, init_db
from service.dependencies import get_current_user
from service.email_verification import (
    email_is_verified,
    ensure_email_transport_configured,
    issue_email_verification,
    send_verification_email,
    verify_email_token,
)
from service.error_mapper import map_failure_details
from service.job_service import (
    create_job_with_uploads,
    delete_job_resources,
    ensure_job_can_be_deleted,
    log_job_event,
    stream_storage_file,
    update_job_state,
)
from service.models import (
    AdminAccount,
    AdminRole,
    AdminStatus,
    Job,
    JobArtifact,
    JobEvent,
    JobInput,
    JobPhase,
    JobStatus,
    JobTask,
    User,
    UserPresence,
    UserQuotaOverride,
    UserStatus,
)
from service.observability import emit_structured_log, get_request_id, install_request_context_middleware
from service.quota import ACTIVE_JOB_STATUSES, QuotaSnapshot, get_user_quota_limits, get_user_quota_snapshot
from service.rate_limit import enforce_rate_limit, get_redis_client
from service.schemas import (
    AdminJobDebugResponse,
    AdminAccountResponse,
    AdminGrantRequest,
    AdminLoginRequest,
    AdminOverviewResponse,
    AdminSessionResponse,
    EmailVerificationRequest,
    JobArtifactListResponse,
    JobArtifactResponse,
    JobDeleteResponse,
    JobEventListResponse,
    JobEventResponse,
    JobListResponse,
    JobSelectionRequest,
    JobSummaryResponse,
    JobTaskListResponse,
    JobTaskResponse,
    LoginRequest,
    MessageResponse,
    QuotaRemainingResponse,
    RegisterRequest,
    ResendEmailVerificationRequest,
    SessionResponse,
    PresenceHeartbeatRequest,
    UserQuotaOverrideRequest,
    UserResponse,
    UserStatusUpdateRequest,
)
from service.security import create_access_token, hash_password, verify_password
from service.storage import check_storage_ready, get_storage_backend, guess_content_type
from service.tasks import run_analysis_job, run_render_job
from service.usage import get_llm_usage_snapshot


settings = get_settings()
LOGGER = logging.getLogger(__name__)
STOP_STREAM_STATUSES = {
    JobStatus.AWAITING_SELECTION.value,
    JobStatus.COMPLETED.value,
    JobStatus.FAILED.value,
    JobStatus.EXPIRED.value,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    del app
    init_db()
    ensure_owner_admin()
    settings.local_storage_root.mkdir(parents=True, exist_ok=True)
    settings.job_workspace_root.mkdir(parents=True, exist_ok=True)
    yield


app = FastAPI(title="SmartAnalyst API", version="2.0.0", lifespan=lifespan, root_path=settings.api_root_path)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
install_request_context_middleware(app)


def _serialize_user(user: User) -> UserResponse:
    return UserResponse(
        id=user.id,
        email=user.email,
        status=user.status,
        created_at=user.created_at,
        email_verified=email_is_verified(user),
        email_verified_at=user.email_verified_at,
    )


def _serialize_admin(admin: AdminAccount) -> AdminAccountResponse:
    return AdminAccountResponse(
        id=admin.id,
        email=admin.email,
        role=admin.role,
        status=admin.status,
        created_at=admin.created_at,
        last_login_at=admin.last_login_at,
    )


def _derive_failure_code(error_summary: str | None, stage: str | None = None) -> str | None:
    details = map_failure_details(raw_message=error_summary, stage=stage)
    return details.error_code if details is not None else None


def _derive_failure_stage(job: Job, latest_failed_event: JobEvent | None) -> str | None:
    if latest_failed_event is None and not job.error_summary:
        return None
    if latest_failed_event is not None:
        if latest_failed_event.event_type.startswith("job.analysis"):
            return JobPhase.ANALYSIS.value
        if latest_failed_event.event_type.startswith("job.render"):
            return JobPhase.RENDER.value
    if job.phase in {
        JobPhase.UPLOAD.value,
        JobPhase.ANALYSIS.value,
        JobPhase.SELECTION.value,
        JobPhase.RENDER.value,
        JobPhase.FAILED.value,
        JobPhase.EXPIRED.value,
    }:
        return job.phase
    return None


def _compute_queue_position(db: Session, job: Job) -> int | None:
    if job.status not in {JobStatus.QUEUED_ANALYSIS.value, JobStatus.QUEUED_RENDER.value}:
        return None

    return int(
        db.scalar(
            select(func.count(Job.id)).where(
                Job.status == job.status,
                Job.created_at <= job.created_at,
            )
        )
        or 0
    )


def _get_retry_count(db: Session, job_id: str) -> int:
    return int(
        db.scalar(
            select(func.count(JobEvent.id)).where(
                JobEvent.job_id == job_id,
                JobEvent.event_type.in_(("job.analysis_retrying", "job.render_retrying")),
            )
        )
        or 0
    )


def _get_latest_event(db: Session, job_id: str) -> JobEvent | None:
    return db.scalar(
        select(JobEvent)
        .where(JobEvent.job_id == job_id)
        .order_by(JobEvent.created_at.desc())
        .limit(1)
    )


def _get_latest_failed_event(db: Session, job_id: str) -> JobEvent | None:
    return db.scalar(
        select(JobEvent)
        .where(
            JobEvent.job_id == job_id,
            JobEvent.level == "error",
        )
        .order_by(JobEvent.created_at.desc())
        .limit(1)
    )


def _serialize_quota(snapshot: QuotaSnapshot) -> QuotaRemainingResponse:
    return QuotaRemainingResponse(**snapshot.to_payload())


def _serialize_job_summary(
    job: Job,
    request: Request,
    quota_snapshot: QuotaSnapshot,
    db: Session,
    *,
    include_raw_detail: bool = False,
) -> JobSummaryResponse:
    latest_event = _get_latest_event(db, job.id)
    latest_failed_event = _get_latest_failed_event(db, job.id)
    failure_stage = _derive_failure_stage(job, latest_failed_event)
    raw_detail = job.error_summary
    if latest_failed_event is not None and latest_failed_event.payload_json:
        traceback_text = latest_failed_event.payload_json.get("traceback")
        if isinstance(traceback_text, str) and traceback_text.strip():
            raw_detail = f"{job.error_summary or latest_failed_event.message}\n\n{traceback_text.strip()}"
    failure_details = map_failure_details(
        raw_message=job.error_summary or (latest_failed_event.message if latest_failed_event else None),
        stage=failure_stage,
        include_raw_detail=include_raw_detail,
        raw_detail=raw_detail,
    )
    return JobSummaryResponse(
        id=job.id,
        status=job.status,
        phase=job.phase,
        progress_percent=job.progress_percent,
        report_title=job.report_title,
        selected_task_ids=job.selected_task_ids,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        retention_expires_at=job.expires_at,
        error_summary=failure_details.user_message if failure_details is not None else job.error_summary,
        error_title=failure_details.error_title if failure_details is not None else None,
        user_message=failure_details.user_message if failure_details is not None else None,
        error_category=failure_details.error_category if failure_details is not None else None,
        error_code=failure_details.error_code if failure_details is not None else None,
        raw_detail=failure_details.raw_detail if failure_details is not None else None,
        suggested_actions=failure_details.suggested_actions if failure_details is not None else [],
        queue_position=_compute_queue_position(db, job),
        retry_count=_get_retry_count(db, job.id),
        failure_code=failure_details.error_code if failure_details is not None else None,
        failure_stage=failure_stage,
        latest_event_id=latest_event.id if latest_event is not None else None,
        quota_remaining=_serialize_quota(quota_snapshot),
        tasks_url=str(request.url_for("get_job_tasks", job_id=job.id)),
        events_url=str(request.url_for("get_job_events", job_id=job.id)),
        artifacts_url=str(request.url_for("list_job_artifacts", job_id=job.id)),
        stream_url=str(request.url_for("stream_job", job_id=job.id)),
    )


def _serialize_job_task(job: Job, request: Request, task: JobTask) -> JobTaskResponse:
    return JobTaskResponse(
        task_index=task.task_index,
        question_zh=task.question_zh,
        analysis_type=task.analysis_type,
        required_datasets=task.required_datasets,
        selected=task.selected,
        analysis_text=task.analysis_text,
        chart_url=(
            str(request.url_for("download_job_chart", job_id=job.id, task_index=task.task_index))
            if task.image_storage_key
            else None
        ),
    )


def _serialize_job_artifact(request: Request, job: Job, artifact: JobArtifact) -> JobArtifactResponse:
    return JobArtifactResponse(
        artifact_type=artifact.artifact_type,
        download_url=str(
            request.url_for("download_job_artifact", job_id=job.id, artifact_type=artifact.artifact_type)
        ),
        created_at=artifact.created_at,
    )


def _derive_event_stage(event: JobEvent) -> str | None:
    if event.event_type.startswith("job.analysis"):
        return JobPhase.ANALYSIS.value
    if event.event_type.startswith("job.render"):
        return JobPhase.RENDER.value
    return None


def _serialize_job_event(event: JobEvent, *, include_raw_detail: bool = False) -> JobEventResponse:
    message = event.message
    payload_json = event.payload_json
    if event.level == "error" and not include_raw_detail:
        details = map_failure_details(raw_message=event.message, stage=_derive_event_stage(event))
        if details is not None:
            message = details.user_message
            payload_json = {
                "error_title": details.error_title,
                "error_category": details.error_category,
                "error_code": details.error_code,
                "suggested_actions": details.suggested_actions,
            }
    return JobEventResponse(
        id=event.id,
        level=event.level,
        event_type=event.event_type,
        message=message,
        payload_json=payload_json,
        created_at=event.created_at,
    )


def _load_job(job_id: str, user: User, db: Session) -> Job:
    job = db.scalar(select(Job).where(Job.id == job_id, Job.user_id == user.id))
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job


def _load_job_with_tasks(job_id: str, user: User, db: Session) -> Job:
    job = db.scalar(select(Job).options(selectinload(Job.tasks)).where(Job.id == job_id, Job.user_id == user.id))
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job


def _load_job_with_artifacts(job_id: str, user: User, db: Session) -> Job:
    job = db.scalar(
        select(Job).options(selectinload(Job.artifacts)).where(Job.id == job_id, Job.user_id == user.id)
    )
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job


def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=settings.session_cookie_name,
        value=token,
        max_age=settings.access_token_expire_minutes * 60,
        httponly=True,
        secure=settings.auth_cookie_secure,
        samesite=settings.auth_cookie_samesite,
        domain=settings.auth_cookie_domain,
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(
        key=settings.session_cookie_name,
        domain=settings.auth_cookie_domain,
        path="/",
    )


def _get_events_payload(db: Session, job_id: str, after: str | None = None) -> JobEventListResponse:
    query = select(JobEvent).where(JobEvent.job_id == job_id).order_by(JobEvent.created_at.asc())
    if after:
        anchor_event = db.scalar(select(JobEvent).where(JobEvent.job_id == job_id, JobEvent.id == after))
        if anchor_event is not None:
            query = query.where(JobEvent.created_at > anchor_event.created_at)

    events = list(db.scalars(query))
    cursor = events[-1].id if events else after
    return JobEventListResponse(events=[_serialize_job_event(event) for event in events], cursor=cursor)


def _format_sse(event: str, data: dict, event_id: str | None = None) -> str:
    lines: list[str] = []
    if event_id:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    payload = json_dumps(data)
    for line in payload.splitlines():
        lines.append(f"data: {line}")
    return "\n".join(lines) + "\n\n"


def json_dumps(payload: dict) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False, default=str)


def _require_admin(x_admin_token: str | None = Header(default=None)) -> None:
    if not settings.enable_admin_debug:
        raise HTTPException(status_code=404, detail="Admin debug endpoints are disabled.")
    if not settings.admin_api_token:
        raise HTTPException(status_code=503, detail="Admin API token is not configured.")
    if x_admin_token != settings.admin_api_token:
        raise HTTPException(status_code=401, detail="Invalid admin token.")


@app.post("/auth/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def register(
    payload: RegisterRequest,
    request: Request,
    db: Session = Depends(get_db_session),
) -> UserResponse:
    """Create a new account for the public beta."""
    enforce_rate_limit(
        namespace="auth.register",
        subject=getattr(request.state, "client_ip", "unknown"),
        limit=settings.register_rate_limit_per_hour_ip,
        window_seconds=3600,
        error_message="Too many registration attempts from this IP. Please try again later.",
    )
    verify_captcha(payload.captcha_verify_param, request)
    try:
        ensure_email_transport_configured()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    normalized_email = payload.email.strip().lower()
    if not normalized_email:
        raise HTTPException(status_code=400, detail="Email is required.")
    existing_user = db.scalar(select(User).where(User.email == normalized_email))
    if existing_user is not None:
        raise HTTPException(status_code=409, detail="Email already registered.")

    user = User(email=normalized_email, password_hash=hash_password(payload.password))
    verification_token = issue_email_verification(user)
    db.add(user)
    db.commit()
    db.refresh(user)
    try:
        send_verification_email(user, verification_token)
    except Exception as exc:
        db.delete(user)
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Verification email could not be sent. Please try again later.",
        ) from exc
    emit_structured_log(
        LOGGER,
        event="auth.registered",
        request_id=get_request_id(request),
        user_id=user.id,
        email=user.email,
    )
    return _serialize_user(user)


@app.post("/auth/verify-email", response_model=MessageResponse)
def verify_email(
    payload: EmailVerificationRequest,
    db: Session = Depends(get_db_session),
) -> MessageResponse:
    """Verify a registered user's email address."""
    user = verify_email_token(db, payload.email, payload.token)
    if user is None:
        raise HTTPException(status_code=400, detail="Invalid or expired email verification token.")
    return MessageResponse(status="ok", message="Email verified.")


@app.post("/auth/resend-verification", response_model=MessageResponse)
def resend_email_verification(
    payload: ResendEmailVerificationRequest,
    request: Request,
    db: Session = Depends(get_db_session),
) -> MessageResponse:
    """Send a fresh email verification link without revealing account existence."""
    enforce_rate_limit(
        namespace="auth.resend_verification",
        subject=getattr(request.state, "client_ip", "unknown"),
        limit=settings.register_rate_limit_per_hour_ip,
        window_seconds=3600,
        error_message="Too many verification email requests from this IP. Please try again later.",
    )
    normalized_email = payload.email.strip().lower()
    user = db.scalar(select(User).where(User.email == normalized_email))
    if user is not None and user.email_verified_at is None:
        try:
            ensure_email_transport_configured()
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
        verification_token = issue_email_verification(user)
        db.add(user)
        db.commit()
        db.refresh(user)
        try:
            send_verification_email(user, verification_token)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Verification email could not be sent. Please try again later.",
            ) from exc
    return MessageResponse(status="ok", message="If the account exists, a verification email has been sent.")


@app.post("/auth/login", response_model=SessionResponse)
def login(
    payload: LoginRequest,
    request: Request,
    response: Response,
    db: Session = Depends(get_db_session),
) -> SessionResponse:
    """Authenticate a user and set the browser session cookie."""
    enforce_rate_limit(
        namespace="auth.login",
        subject=getattr(request.state, "client_ip", "unknown"),
        limit=settings.login_rate_limit_per_15_min_ip,
        window_seconds=15 * 60,
        error_message="Too many login attempts from this IP. Please try again later.",
    )
    normalized_email = payload.email.strip().lower()
    user = db.scalar(select(User).where(User.email == normalized_email))
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    token = create_access_token(user.id)
    _set_session_cookie(response, token)
    emit_structured_log(
        LOGGER,
        event="auth.logged_in",
        request_id=get_request_id(request),
        user_id=user.id,
        email=user.email,
    )
    return SessionResponse(user=_serialize_user(user))


@app.get("/me", response_model=SessionResponse)
def get_me(current_user: User = Depends(get_current_user)) -> SessionResponse:
    """Return the currently authenticated browser session."""
    return SessionResponse(user=_serialize_user(current_user))


@app.post("/auth/logout")
def logout(response: Response) -> dict[str, str]:
    """Clear the browser session cookie."""
    _clear_session_cookie(response)
    return {"status": "ok"}


@app.post("/presence/heartbeat", response_model=MessageResponse)
def presence_heartbeat(
    payload: PresenceHeartbeatRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> MessageResponse:
    """Record that the current user is actively using the app."""
    current_path = (payload.current_path or "").strip()[:512] or None
    current_job_id = (payload.current_job_id or "").strip()[:32] or None
    presence = db.scalar(select(UserPresence).where(UserPresence.user_id == current_user.id))
    if presence is None:
        presence = UserPresence(user_id=current_user.id, email=current_user.email)
    presence.email = current_user.email
    presence.last_seen_at = datetime.utcnow()
    presence.current_job_id = current_job_id
    presence.current_path = current_path
    db.add(presence)
    db.commit()
    return MessageResponse(status="ok", message="Presence recorded.")


@app.post("/jobs", response_model=JobSummaryResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_job(
    request: Request,
    files: list[UploadFile] = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> JobSummaryResponse:
    """Create a new report-generation job and enqueue the analysis phase."""
    if not email_is_verified(current_user):
        raise HTTPException(status_code=403, detail="Please verify your email before submitting report jobs.")

    job = await create_job_with_uploads(db, current_user, files, storage=get_storage_backend())
    update_job_state(
        db,
        job,
        status_value=JobStatus.QUEUED_ANALYSIS.value,
        phase_value=JobPhase.ANALYSIS.value,
        progress_percent=1,
        clear_error_summary=True,
    )
    db.commit()

    async_result = run_analysis_job.apply_async(args=[job.id], queue=settings.analysis_queue_name)
    job.queue_task_id = async_result.id
    db.add(job)
    log_job_event(
        db,
        job.id,
        event_type="job.analysis_queued",
        message="Analysis task queued.",
        payload={"celery_task_id": async_result.id},
        user_id=current_user.id,
        phase=JobPhase.ANALYSIS.value,
        celery_task_id=async_result.id,
        request_id=get_request_id(request),
    )
    db.commit()
    db.refresh(job)
    return _serialize_job_summary(job, request, get_user_quota_snapshot(db, current_user.id), db)


@app.get("/jobs", response_model=JobListResponse)
def list_jobs(
    request: Request,
    limit: int = Query(default=12, ge=1, le=50),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> JobListResponse:
    """List the current user's recent jobs."""
    jobs = list(
        db.scalars(
            select(Job)
            .where(Job.user_id == current_user.id)
            .order_by(Job.created_at.desc())
            .limit(limit)
        )
    )
    quota_snapshot = get_user_quota_snapshot(db, current_user.id)
    return JobListResponse(
        jobs=[_serialize_job_summary(job, request, quota_snapshot, db) for job in jobs]
    )


@app.delete("/jobs/{job_id}", response_model=JobDeleteResponse)
def delete_job(
    job_id: str,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> JobDeleteResponse:
    """Delete a user-owned job and release its occupied resources."""
    job = _load_job(job_id, current_user, db)
    ensure_job_can_be_deleted(job)

    queue_task_revoked = False
    if job.queue_task_id and job.status in {JobStatus.QUEUED_ANALYSIS.value, JobStatus.QUEUED_RENDER.value}:
        celery_app.control.revoke(job.queue_task_id, terminate=False)
        queue_task_revoked = True

    delete_job_resources(db, job, storage=get_storage_backend())
    db.commit()

    emit_structured_log(
        LOGGER,
        event="job.deleted",
        request_id=get_request_id(request),
        job_id=job_id,
        user_id=current_user.id,
        phase=job.phase,
        event_type="job.deleted",
        queue_task_revoked=queue_task_revoked,
    )
    return JobDeleteResponse(
        status="deleted",
        job_id=job_id,
        queue_task_revoked=queue_task_revoked,
        quota_remaining=_serialize_quota(get_user_quota_snapshot(db, current_user.id)),
    )


@app.get("/jobs/{job_id}", response_model=JobSummaryResponse)
def get_job_summary(
    job_id: str,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> JobSummaryResponse:
    """Return the lightweight status summary for one job."""
    job = _load_job(job_id, current_user, db)
    return _serialize_job_summary(job, request, get_user_quota_snapshot(db, current_user.id), db)


@app.get("/jobs/{job_id}/tasks", response_model=JobTaskListResponse, name="get_job_tasks")
def get_job_tasks(
    job_id: str,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> JobTaskListResponse:
    """Return candidate tasks for one job."""
    job = _load_job_with_tasks(job_id, current_user, db)
    tasks = sorted(job.tasks, key=lambda item: int(item.task_index))
    return JobTaskListResponse(tasks=[_serialize_job_task(job, request, task) for task in tasks])


@app.get("/jobs/{job_id}/events", response_model=JobEventListResponse, name="get_job_events")
def get_job_events(
    job_id: str,
    after: str | None = Query(default=None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> JobEventListResponse:
    """Return incremental lifecycle events for one job."""
    _load_job(job_id, current_user, db)
    return _get_events_payload(db, job_id, after=after)


@app.get("/jobs/{job_id}/artifacts", response_model=JobArtifactListResponse, name="list_job_artifacts")
def list_job_artifacts(
    job_id: str,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> JobArtifactListResponse:
    """List rendered artifacts for one job."""
    job = _load_job_with_artifacts(job_id, current_user, db)
    artifacts = sorted(job.artifacts, key=lambda item: item.created_at)
    return JobArtifactListResponse(
        artifacts=[_serialize_job_artifact(request, job, artifact) for artifact in artifacts]
    )


@app.post("/jobs/{job_id}/selection", response_model=JobSummaryResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_selection(
    job_id: str,
    payload: JobSelectionRequest,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> JobSummaryResponse:
    """Persist selected charts and enqueue the render phase."""
    job = _load_job_with_tasks(job_id, current_user, db)
    if job.status != JobStatus.AWAITING_SELECTION.value:
        raise HTTPException(status_code=409, detail="This job is not waiting for chart selection.")

    available_task_ids = {int(item.task_index) for item in job.tasks}
    selected_task_ids = sorted({int(item) for item in payload.selected_task_ids})
    if not selected_task_ids:
        raise HTTPException(status_code=400, detail="At least one task must be selected.")
    if any(item not in available_task_ids for item in selected_task_ids):
        raise HTTPException(status_code=400, detail="Selection contains unknown task ids.")

    selected_set = set(selected_task_ids)
    for task in job.tasks:
        task.selected = int(task.task_index) in selected_set
        db.add(task)

    update_job_state(
        db,
        job,
        status_value=JobStatus.QUEUED_RENDER.value,
        phase_value=JobPhase.RENDER.value,
        progress_percent=82,
        selected_task_ids=selected_task_ids,
        clear_error_summary=True,
    )
    db.commit()

    async_result = run_render_job.apply_async(args=[job.id], queue=settings.render_queue_name)
    job.queue_task_id = async_result.id
    db.add(job)
    log_job_event(
        db,
        job.id,
        event_type="job.render_queued",
        message="Render task queued after user chart selection.",
        payload={"selected_task_ids": selected_task_ids, "celery_task_id": async_result.id},
        user_id=current_user.id,
        phase=JobPhase.RENDER.value,
        celery_task_id=async_result.id,
        request_id=get_request_id(request),
    )
    db.commit()
    db.refresh(job)
    return _serialize_job_summary(job, request, get_user_quota_snapshot(db, current_user.id), db)


@app.get("/jobs/{job_id}/stream", name="stream_job")
async def stream_job(
    job_id: str,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
):
    """Stream job summary and incremental events using Server-Sent Events."""
    _load_job(job_id, current_user, db)

    async def event_stream():
        last_event_id: str | None = None
        while True:
            if await request.is_disconnected():
                break

            stream_db = SessionLocal()
            try:
                job = stream_db.scalar(select(Job).where(Job.id == job_id, Job.user_id == current_user.id))
                if job is None:
                    yield _format_sse("job.deleted", {"job_id": job_id})
                    break

                quota_snapshot = get_user_quota_snapshot(stream_db, current_user.id)
                summary = _serialize_job_summary(job, request, quota_snapshot, stream_db)
                yield _format_sse(
                    "job.summary",
                    summary.model_dump(mode="json"),
                    event_id=summary.latest_event_id or job.id,
                )

                event_bundle = _get_events_payload(stream_db, job.id, after=last_event_id)
                if event_bundle.events:
                    yield _format_sse(
                        "job.events",
                        event_bundle.model_dump(mode="json"),
                        event_id=event_bundle.cursor,
                    )
                    last_event_id = event_bundle.cursor
                else:
                    yield ": heartbeat\n\n"

                if job.status in STOP_STREAM_STATUSES:
                    break
            finally:
                stream_db.close()

            await asyncio.sleep(max(settings.sse_poll_interval_seconds, 1))

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/jobs/{job_id}/download/{artifact_type}", name="download_job_artifact")
def download_job_artifact(
    job_id: str,
    artifact_type: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
):
    """Stream a controlled artifact download for one job."""
    job = _load_job_with_artifacts(job_id, current_user, db)
    artifact = next((item for item in job.artifacts if item.artifact_type == artifact_type), None)
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found.")

    stream = stream_storage_file(get_storage_backend(), artifact.storage_key)
    filename = Path(artifact.storage_key).name
    return StreamingResponse(
        stream,
        media_type=guess_content_type(Path(filename)),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/jobs/{job_id}/tasks/{task_index}/chart", name="download_job_chart")
def download_job_chart(
    job_id: str,
    task_index: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
):
    """Stream one candidate chart image."""
    job = _load_job_with_tasks(job_id, current_user, db)
    task = next((item for item in job.tasks if int(item.task_index) == int(task_index)), None)
    if task is None or not task.image_storage_key:
        raise HTTPException(status_code=404, detail="Chart not found.")

    stream = stream_storage_file(get_storage_backend(), task.image_storage_key)
    filename = Path(task.image_storage_key).name
    return StreamingResponse(
        stream,
        media_type="image/png",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@app.post("/admin/auth/login", response_model=AdminSessionResponse)
def admin_login(
    payload: AdminLoginRequest,
    response: Response,
    db: Session = Depends(get_db_session),
) -> AdminSessionResponse:
    """Authenticate an administrator and set the admin session cookie."""
    normalized_email = payload.email.strip().lower()
    admin = db.scalar(select(AdminAccount).where(AdminAccount.email == normalized_email))
    if admin is None or admin.status != AdminStatus.ACTIVE.value or not verify_password(
        payload.password, admin.password_hash
    ):
        raise HTTPException(status_code=401, detail="管理员邮箱或密码不正确。")

    mark_admin_login(db, admin)
    set_admin_session_cookie(response, admin.id)
    return AdminSessionResponse(admin=_serialize_admin(admin))


@app.post("/admin/auth/logout", response_model=MessageResponse)
def admin_logout(response: Response) -> MessageResponse:
    """Clear the admin session cookie."""
    clear_admin_session_cookie(response)
    return MessageResponse(status="ok", message="管理员登录状态已清除。")


@app.get("/admin/me", response_model=AdminSessionResponse)
def admin_me(admin: AdminAccount = Depends(get_current_admin)) -> AdminSessionResponse:
    """Return the currently authenticated administrator."""
    return AdminSessionResponse(admin=_serialize_admin(admin))


def _admin_overview_payload(db: Session) -> AdminOverviewResponse:
    """Return high-level queue health for operators."""
    day_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    llm_usage = get_llm_usage_snapshot()
    return AdminOverviewResponse(
        total_users=int(db.scalar(select(func.count(User.id))) or 0),
        verified_users=int(db.scalar(select(func.count(User.id)).where(User.email_verified_at.is_not(None))) or 0),
        active_jobs_total=int(db.scalar(select(func.count(Job.id)).where(Job.status.in_(ACTIVE_JOB_STATUSES))) or 0),
        queued_analysis=int(db.scalar(select(func.count(Job.id)).where(Job.status == JobStatus.QUEUED_ANALYSIS.value)) or 0),
        running_analysis=int(db.scalar(select(func.count(Job.id)).where(Job.status == JobStatus.RUNNING_ANALYSIS.value)) or 0),
        awaiting_selection=int(
            db.scalar(select(func.count(Job.id)).where(Job.status == JobStatus.AWAITING_SELECTION.value)) or 0
        ),
        queued_render=int(db.scalar(select(func.count(Job.id)).where(Job.status == JobStatus.QUEUED_RENDER.value)) or 0),
        rendering=int(db.scalar(select(func.count(Job.id)).where(Job.status == JobStatus.RENDERING.value)) or 0),
        jobs_created_today=int(db.scalar(select(func.count(Job.id)).where(Job.created_at >= day_start)) or 0),
        completed_today=int(
            db.scalar(
                select(func.count(Job.id)).where(
                    Job.status == JobStatus.COMPLETED.value,
                    Job.finished_at.is_not(None),
                    Job.finished_at >= day_start,
                )
            )
            or 0
        ),
        failed_today=int(
            db.scalar(
                select(func.count(Job.id)).where(
                    Job.status == JobStatus.FAILED.value,
                    Job.finished_at.is_not(None),
                    Job.finished_at >= day_start,
                )
            )
            or 0
        ),
        uploads_bytes_today=int(
            db.scalar(
                select(func.coalesce(func.sum(JobInput.size_bytes), 0))
                .select_from(JobInput)
                .join(Job, JobInput.job_id == Job.id)
                .where(Job.created_at >= day_start)
            )
            or 0
        ),
        llm_calls_today=int(llm_usage["calls_today"]),
        llm_daily_budget_limit=int(llm_usage["daily_budget_limit"]),
    )


@app.get("/admin/overview", response_model=AdminOverviewResponse)
def admin_overview(
    admin: AdminAccount = Depends(get_current_admin),
    db: Session = Depends(get_db_session),
) -> AdminOverviewResponse:
    """Return high-level queue health for operators."""
    del admin
    return _admin_overview_payload(db)


def _readiness_payload() -> dict[str, object]:
    checks: dict[str, dict[str, str | bool]] = {}
    overall_ok = True
    db = SessionLocal()
    try:
        db.execute(text("SELECT 1"))
        checks["database"] = {"ok": True, "detail": "ok"}
    except Exception as exc:
        overall_ok = False
        checks["database"] = {"ok": False, "detail": str(exc)}
    finally:
        db.close()

    try:
        get_redis_client().ping()
        checks["redis"] = {"ok": True, "detail": "ok"}
    except Exception as exc:
        overall_ok = False
        checks["redis"] = {"ok": False, "detail": str(exc)}

    storage_ok, storage_detail = check_storage_ready()
    checks["storage"] = {"ok": storage_ok, "detail": storage_detail}
    overall_ok = overall_ok and storage_ok
    return {"status": "ok" if overall_ok else "degraded", "checks": checks}


@app.get("/admin/dashboard")
def admin_dashboard(
    admin: AdminAccount = Depends(get_current_admin),
    db: Session = Depends(get_db_session),
) -> dict[str, object]:
    """Return the full admin landing-page snapshot."""
    del admin
    online_cutoff = datetime.utcnow() - timedelta(seconds=settings.presence_online_window_seconds)
    overview = _admin_overview_payload(db)
    online_users = list(
        db.scalars(
            select(UserPresence)
            .where(UserPresence.last_seen_at >= online_cutoff)
            .order_by(UserPresence.last_seen_at.desc())
            .limit(50)
        )
    )
    failed_today = overview.failed_today
    completed_today = overview.completed_today
    total_done_today = failed_today + completed_today
    llm_usage = get_llm_usage_snapshot()
    return {
        "overview": overview.model_dump(mode="json"),
        "online_window_seconds": settings.presence_online_window_seconds,
        "online_users": [
            {
                "user_id": item.user_id,
                "email": item.email,
                "last_seen_at": item.last_seen_at,
                "current_job_id": item.current_job_id,
                "current_path": item.current_path,
            }
            for item in online_users
        ],
        "failure_rate_today": (failed_today / total_done_today) if total_done_today else 0,
        "llm_usage": llm_usage,
        "readiness": _readiness_payload(),
    }


@app.get("/admin/jobs")
def admin_list_jobs(
    status_filter: str | None = Query(default=None, alias="status"),
    email: str | None = Query(default=None),
    failed_only: bool = Query(default=False),
    created_from: datetime | None = Query(default=None),
    created_to: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    admin: AdminAccount = Depends(get_current_admin),
    db: Session = Depends(get_db_session),
) -> dict[str, object]:
    """Return queue and failure details for operators."""
    del admin
    query = select(Job, User.email).join(User, User.id == Job.user_id).order_by(Job.created_at.desc()).limit(limit)
    if status_filter:
        query = query.where(Job.status == status_filter)
    if failed_only:
        query = query.where(Job.status == JobStatus.FAILED.value)
    if email:
        query = query.where(User.email.ilike(f"%{email.strip().lower()}%"))
    if created_from:
        query = query.where(Job.created_at >= created_from)
    if created_to:
        query = query.where(Job.created_at <= created_to)
    rows = list(db.execute(query))
    serialized_jobs = []
    for job, user_email in rows:
        latest_failed_event = _get_latest_failed_event(db, job.id)
        failure_stage = _derive_failure_stage(job, latest_failed_event)
        failure_details = map_failure_details(raw_message=job.error_summary, stage=failure_stage)
        serialized_jobs.append(
            {
                "id": job.id,
                "user_id": job.user_id,
                "user_email": user_email,
                "status": job.status,
                "phase": job.phase,
                "progress_percent": job.progress_percent,
                "queue_task_id": job.queue_task_id,
                "report_title": job.report_title,
                "created_at": job.created_at,
                "started_at": job.started_at,
                "finished_at": job.finished_at,
                "queue_position": _compute_queue_position(db, job),
                "failure_code": failure_details.error_code if failure_details is not None else None,
                "failure_stage": failure_stage,
                "error_summary": failure_details.user_message if failure_details is not None else job.error_summary,
            }
        )
    return {
        "jobs": serialized_jobs
    }


@app.get("/admin/users")
def admin_list_users(
    email: str | None = Query(default=None),
    online_only: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
    admin: AdminAccount = Depends(get_current_admin),
    db: Session = Depends(get_db_session),
) -> dict[str, object]:
    """Return users with quota and online status summaries."""
    del admin
    online_cutoff = datetime.utcnow() - timedelta(seconds=settings.presence_online_window_seconds)
    query = select(User, UserPresence).outerjoin(UserPresence, UserPresence.user_id == User.id).order_by(
        User.created_at.desc()
    )
    if email:
        query = query.where(User.email.ilike(f"%{email.strip().lower()}%"))
    if online_only:
        query = query.where(UserPresence.last_seen_at >= online_cutoff)
    rows = list(db.execute(query.limit(limit)))
    return {
        "users": [
            {
                "id": user.id,
                "email": user.email,
                "status": user.status,
                "created_at": user.created_at,
                "email_verified": email_is_verified(user),
                "email_verified_at": user.email_verified_at,
                "online": bool(presence and presence.last_seen_at >= online_cutoff),
                "last_seen_at": presence.last_seen_at if presence else None,
                "current_job_id": presence.current_job_id if presence else None,
                "quota": get_user_quota_snapshot(db, user.id).to_payload(),
            }
            for user, presence in rows
        ]
    }


@app.get("/admin/users/{user_lookup}/usage")
def admin_user_usage(
    user_lookup: str,
    admin: AdminAccount = Depends(get_current_admin),
    db: Session = Depends(get_db_session),
) -> dict[str, object]:
    """Return quota and recent usage for one user by id or email."""
    del admin
    lookup = user_lookup.strip().lower()
    user = db.scalar(select(User).where(User.id == user_lookup))
    if user is None:
        user = db.scalar(select(User).where(User.email == lookup))
    if user is None:
        raise HTTPException(status_code=404, detail="未找到用户。")

    day_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    recent_jobs = list(
        db.scalars(
            select(Job)
            .where(Job.user_id == user.id)
            .order_by(Job.created_at.desc())
            .limit(20)
        )
    )
    quota = get_user_quota_snapshot(db, user.id).to_payload()
    override = db.scalar(select(UserQuotaOverride).where(UserQuotaOverride.user_id == user.id))
    limits = get_user_quota_limits(db, user.id)
    presence = db.scalar(select(UserPresence).where(UserPresence.user_id == user.id))
    online_cutoff = datetime.utcnow() - timedelta(seconds=settings.presence_online_window_seconds)
    return {
        "user": _serialize_user(user).model_dump(mode="json"),
        "quota": quota,
        "quota_limits": limits,
        "quota_override": {
            "daily_job_limit": override.daily_job_limit if override else None,
            "daily_upload_bytes_limit": override.daily_upload_bytes_limit if override else None,
            "active_job_limit": override.active_job_limit if override else None,
            "updated_at": override.updated_at if override else None,
        },
        "presence": {
            "online": bool(presence and presence.last_seen_at >= online_cutoff),
            "last_seen_at": presence.last_seen_at if presence else None,
            "current_job_id": presence.current_job_id if presence else None,
            "current_path": presence.current_path if presence else None,
        },
        "jobs_created_today": int(
            db.scalar(select(func.count(Job.id)).where(Job.user_id == user.id, Job.created_at >= day_start)) or 0
        ),
        "uploads_bytes_today": int(
            db.scalar(
                select(func.coalesce(func.sum(JobInput.size_bytes), 0))
                .select_from(JobInput)
                .join(Job, JobInput.job_id == Job.id)
                .where(Job.user_id == user.id, Job.created_at >= day_start)
            )
            or 0
        ),
        "recent_jobs": [
            {
                "id": job.id,
                "status": job.status,
                "phase": job.phase,
                "created_at": job.created_at,
                "finished_at": job.finished_at,
                "error_summary": (
                    map_failure_details(raw_message=job.error_summary, stage=job.phase).user_message
                    if map_failure_details(raw_message=job.error_summary, stage=job.phase) is not None
                    else job.error_summary
                ),
            }
            for job in recent_jobs
        ],
    }


@app.post("/admin/users/{user_lookup}/grant", response_model=AdminAccountResponse)
def admin_grant_viewer(
    user_lookup: str,
    payload: AdminGrantRequest,
    owner: AdminAccount = Depends(require_owner_admin),
    db: Session = Depends(get_db_session),
) -> AdminAccountResponse:
    """Grant backend access to an existing user as a viewer."""
    del user_lookup
    if payload.role != AdminRole.VIEWER.value:
        raise HTTPException(status_code=400, detail="后台当前只支持授予观察员权限。")
    normalized_email = payload.email.strip().lower()
    user = db.scalar(select(User).where(User.email == normalized_email))
    if user is None:
        raise HTTPException(status_code=404, detail="该用户需要先注册普通账号，才能授予后台权限。")
    admin = db.scalar(select(AdminAccount).where(AdminAccount.email == normalized_email))
    if admin is None:
        admin = AdminAccount(
            email=normalized_email,
            password_hash=user.password_hash,
            role=AdminRole.VIEWER.value,
            status=AdminStatus.ACTIVE.value,
        )
    elif admin.role == AdminRole.OWNER.value and owner.id != admin.id:
        raise HTTPException(status_code=403, detail="不能降低所有者账号的权限。")
    else:
        admin.role = AdminRole.VIEWER.value
        admin.status = AdminStatus.ACTIVE.value
        admin.password_hash = user.password_hash
    db.add(admin)
    db.commit()
    db.refresh(admin)
    return _serialize_admin(admin)


@app.post("/admin/admins/grant", response_model=AdminAccountResponse)
def admin_grant_viewer_by_email(
    payload: AdminGrantRequest,
    owner: AdminAccount = Depends(require_owner_admin),
    db: Session = Depends(get_db_session),
) -> AdminAccountResponse:
    """Grant viewer admin access to an existing user by email."""
    return admin_grant_viewer(payload.email, payload=payload, owner=owner, db=db)


@app.patch("/admin/users/{user_lookup}/status", response_model=UserResponse)
def admin_update_user_status(
    user_lookup: str,
    payload: UserStatusUpdateRequest,
    owner: AdminAccount = Depends(require_owner_admin),
    db: Session = Depends(get_db_session),
) -> UserResponse:
    """Enable or disable a public user account."""
    del owner
    if payload.status not in {UserStatus.ACTIVE.value, UserStatus.DISABLED.value}:
        raise HTTPException(status_code=400, detail="不支持的用户状态。")
    lookup = user_lookup.strip().lower()
    user = db.scalar(select(User).where(or_(User.id == user_lookup, User.email == lookup)))
    if user is None:
        raise HTTPException(status_code=404, detail="未找到用户。")
    user.status = payload.status
    db.add(user)
    db.commit()
    db.refresh(user)
    return _serialize_user(user)


@app.put("/admin/users/{user_lookup}/quota")
def admin_update_user_quota(
    user_lookup: str,
    payload: UserQuotaOverrideRequest,
    owner: AdminAccount = Depends(require_owner_admin),
    db: Session = Depends(get_db_session),
) -> dict[str, object]:
    """Set per-user quota overrides."""
    lookup = user_lookup.strip().lower()
    user = db.scalar(select(User).where(or_(User.id == user_lookup, User.email == lookup)))
    if user is None:
        raise HTTPException(status_code=404, detail="未找到用户。")

    override = db.scalar(select(UserQuotaOverride).where(UserQuotaOverride.user_id == user.id))
    if override is None:
        override = UserQuotaOverride(user_id=user.id)
    override.daily_job_limit = payload.daily_job_limit
    override.daily_upload_bytes_limit = payload.daily_upload_bytes_limit
    override.active_job_limit = payload.active_job_limit
    override.updated_at = datetime.utcnow()
    override.updated_by_admin_id = owner.id
    db.add(override)
    db.commit()
    return admin_user_usage(user.id, admin=owner, db=db)


@app.post("/admin/users/{user_lookup}/resend-verification", response_model=MessageResponse)
def admin_resend_user_verification(
    user_lookup: str,
    owner: AdminAccount = Depends(require_owner_admin),
    db: Session = Depends(get_db_session),
) -> MessageResponse:
    """Resend a user's email verification message."""
    del owner
    lookup = user_lookup.strip().lower()
    user = db.scalar(select(User).where(or_(User.id == user_lookup, User.email == lookup)))
    if user is None:
        raise HTTPException(status_code=404, detail="未找到用户。")
    if user.email_verified_at is not None:
        return MessageResponse(status="ok", message="该用户邮箱已验证。")
    try:
        ensure_email_transport_configured()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    verification_token = issue_email_verification(user)
    db.add(user)
    db.commit()
    db.refresh(user)
    send_verification_email(user, verification_token)
    return MessageResponse(status="ok", message="验证邮件已发送。")


@app.post("/admin/jobs/{job_id}/retry", response_model=MessageResponse)
def admin_retry_job(
    job_id: str,
    owner: AdminAccount = Depends(require_owner_admin),
    db: Session = Depends(get_db_session),
) -> MessageResponse:
    """Retry a failed job from the safest known phase."""
    job = db.scalar(select(Job).where(Job.id == job_id))
    if job is None:
        raise HTTPException(status_code=404, detail="未找到任务。")
    if job.status != JobStatus.FAILED.value:
        raise HTTPException(status_code=409, detail="只有失败的任务可以重试。")

    retry_render = bool(job.selected_task_ids and job.data_summary_json and job.report_title)
    if retry_render:
        update_job_state(
            db,
            job,
            status_value=JobStatus.QUEUED_RENDER.value,
            phase_value=JobPhase.RENDER.value,
            progress_percent=82,
            clear_error_summary=True,
        )
        db.commit()
        async_result = run_render_job.apply_async(args=[job.id], queue=settings.render_queue_name)
        event_type = "job.render_retry_queued_by_admin"
        message = "管理员已将报告生成任务重新加入队列。"
    else:
        update_job_state(
            db,
            job,
            status_value=JobStatus.QUEUED_ANALYSIS.value,
            phase_value=JobPhase.ANALYSIS.value,
            progress_percent=1,
            clear_error_summary=True,
        )
        db.commit()
        async_result = run_analysis_job.apply_async(args=[job.id], queue=settings.analysis_queue_name)
        event_type = "job.analysis_retry_queued_by_admin"
        message = "管理员已将分析任务重新加入队列。"

    job.queue_task_id = async_result.id
    db.add(job)
    log_job_event(
        db,
        job.id,
        event_type=event_type,
        message=message,
        payload={"admin_id": owner.id, "celery_task_id": async_result.id},
        user_id=job.user_id,
        phase=job.phase,
        celery_task_id=async_result.id,
    )
    db.commit()
    return MessageResponse(status="ok", message=message)


@app.post("/admin/jobs/{job_id}/cancel", response_model=MessageResponse)
def admin_cancel_queued_job(
    job_id: str,
    owner: AdminAccount = Depends(require_owner_admin),
    db: Session = Depends(get_db_session),
) -> MessageResponse:
    """Cancel a queued job and mark it failed."""
    job = db.scalar(select(Job).where(Job.id == job_id))
    if job is None:
        raise HTTPException(status_code=404, detail="未找到任务。")
    if job.status not in {JobStatus.QUEUED_ANALYSIS.value, JobStatus.QUEUED_RENDER.value}:
        raise HTTPException(status_code=409, detail="只有排队中的任务可以取消。")
    if job.queue_task_id:
        celery_app.control.revoke(job.queue_task_id, terminate=False)
    update_job_state(
        db,
        job,
        status_value=JobStatus.FAILED.value,
        phase_value=JobPhase.FAILED.value,
        progress_percent=100,
        error_summary="管理员已取消该任务。",
        finished=True,
    )
    log_job_event(
        db,
        job.id,
        event_type="job.cancelled_by_admin",
        level="error",
        message="管理员已取消排队任务。",
        payload={"admin_id": owner.id},
        user_id=job.user_id,
        phase=JobPhase.FAILED.value,
    )
    db.commit()
    return MessageResponse(status="ok", message="排队任务已取消。")


@app.get("/admin/jobs/{job_id}", response_model=AdminJobDebugResponse)
def admin_job_debug(
    job_id: str,
    request: Request,
    admin: AdminAccount = Depends(get_current_admin),
    db: Session = Depends(get_db_session),
) -> AdminJobDebugResponse:
    """Return a protected, operator-facing full lifecycle view for one job."""
    job = db.scalar(
        select(Job)
        .options(selectinload(Job.inputs), selectinload(Job.tasks), selectinload(Job.artifacts), selectinload(Job.events))
        .where(Job.id == job_id)
    )
    if job is None:
        raise HTTPException(status_code=404, detail="未找到任务。")
    user = db.scalar(select(User).where(User.id == job.user_id))
    if user is None:
        raise HTTPException(status_code=404, detail="未找到用户。")

    tasks = sorted(job.tasks, key=lambda item: int(item.task_index))
    artifacts = sorted(job.artifacts, key=lambda item: item.created_at)
    events = sorted(job.events, key=lambda item: item.created_at)
    return AdminJobDebugResponse(
        user=_serialize_user(user),
        job=_serialize_job_summary(
            job,
            request,
            get_user_quota_snapshot(db, job.user_id),
            db,
            include_raw_detail=admin.role == AdminRole.OWNER.value,
        ),
        inputs=[
            {
                "id": item.id,
                "original_name": item.original_name,
                "size_bytes": item.size_bytes,
                "mime_type": item.mime_type,
                "storage_key": item.storage_key,
                "sha256": item.sha256,
            }
            for item in sorted(job.inputs, key=lambda item: item.original_name.lower())
        ],
        tasks=[_serialize_job_task(job, request, task) for task in tasks],
        artifacts=[_serialize_job_artifact(request, job, artifact) for artifact in artifacts],
        events=[_serialize_job_event(event, include_raw_detail=admin.role == AdminRole.OWNER.value) for event in events],
    )


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    """Lightweight liveness signal for orchestration and load balancers."""
    return {"status": "ok"}


@app.get("/readyz")
def readiness_check() -> dict[str, object]:
    """Readiness signal that checks backing services used by production traffic."""
    payload = _readiness_payload()
    if payload["status"] != "ok":
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=payload)
    return payload
