"""Database and storage helpers for job lifecycle management."""

from __future__ import annotations

import hashlib
import io
import logging
import re
import shutil
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import delete, func, or_, select
from sqlalchemy.orm import Session

from service.config import get_settings
from service.models import ArtifactType, Job, JobArtifact, JobEvent, JobInput, JobPhase, JobStatus, JobTask, User
from service.observability import emit_structured_log
from service.quota import enforce_submission_quota, get_user_quota_limits
from service.storage import StorageBackend, get_storage_backend, guess_content_type


SAFE_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._\-\u4e00-\u9fff]+")
ALLOWED_UPLOAD_SUFFIXES = {".csv", ".xlsx", ".xls"}
ACTIVE_JOB_STATUSES = {
    JobStatus.QUEUED_ANALYSIS.value,
    JobStatus.RUNNING_ANALYSIS.value,
    JobStatus.AWAITING_SELECTION.value,
    JobStatus.QUEUED_RENDER.value,
    JobStatus.RENDERING.value,
}
DELETABLE_JOB_STATUSES = {
    JobStatus.UPLOADED.value,
    JobStatus.QUEUED_ANALYSIS.value,
    JobStatus.AWAITING_SELECTION.value,
    JobStatus.QUEUED_RENDER.value,
    JobStatus.COMPLETED.value,
    JobStatus.FAILED.value,
    JobStatus.EXPIRED.value,
}
NON_DELETABLE_JOB_STATUSES = {
    JobStatus.RUNNING_ANALYSIS.value,
    JobStatus.RENDERING.value,
}
LOGGER = logging.getLogger(__name__)


def sanitize_filename(name: str) -> str:
    """Normalize filenames before saving them to storage."""
    stripped = Path(name).name.strip()
    if not stripped:
        stripped = "upload.dat"
    cleaned = SAFE_FILENAME_PATTERN.sub("_", stripped)
    return cleaned or "upload.dat"


def compute_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def log_job_event(
    db: Session,
    job_id: str,
    *,
    event_type: str,
    message: str,
    level: str = "info",
    payload: dict | None = None,
    user_id: str | None = None,
    phase: str | None = None,
    celery_task_id: str | None = None,
    request_id: str | None = None,
) -> None:
    """Append an event row for a job."""
    db.add(
        JobEvent(
            job_id=job_id,
            level=level,
            event_type=event_type,
            message=message,
            payload_json=payload,
        )
    )
    emit_structured_log(
        LOGGER,
        level=getattr(logging, level.upper(), logging.INFO),
        event="job.event",
        request_id=request_id,
        job_id=job_id,
        user_id=user_id,
        phase=phase,
        event_type=event_type,
        level_name=level,
        celery_task_id=celery_task_id or (payload or {}).get("celery_task_id"),
        message=message,
    )


def update_job_state(
    db: Session,
    job: Job,
    *,
    status_value: str | None = None,
    phase_value: str | None = None,
    progress_percent: int | None = None,
    error_summary: str | None = None,
    clear_error_summary: bool = False,
    report_title: str | None = None,
    selected_task_ids: list[int] | None = None,
    expires_at: datetime | None = None,
    started: bool = False,
    finished: bool = False,
) -> Job:
    """Update persisted job state in one place."""
    if status_value is not None:
        job.status = status_value
    if phase_value is not None:
        job.phase = phase_value
    if progress_percent is not None:
        job.progress_percent = max(0, min(progress_percent, 100))
    if clear_error_summary:
        job.error_summary = None
    elif error_summary is not None:
        job.error_summary = error_summary
    if report_title is not None:
        job.report_title = report_title
    if selected_task_ids is not None:
        job.selected_task_ids = sorted(set(int(item) for item in selected_task_ids))
    if expires_at is not None:
        job.expires_at = expires_at
    if started and job.started_at is None:
        job.started_at = datetime.utcnow()
    if finished:
        job.finished_at = datetime.utcnow()
    db.add(job)
    return job


async def create_job_with_uploads(
    db: Session,
    user: User,
    files: list[UploadFile],
    *,
    storage: StorageBackend | None = None,
) -> Job:
    """Persist a new uploaded job and its input files."""
    settings = get_settings()
    storage = storage or get_storage_backend()

    if not files:
        raise HTTPException(status_code=400, detail="At least one data file is required.")
    if len(files) > settings.max_upload_files:
        raise HTTPException(
            status_code=400,
            detail=f"Too many files. Maximum allowed is {settings.max_upload_files}.",
        )

    active_user_jobs = db.scalar(
        select(func.count(Job.id)).where(
            Job.user_id == user.id,
            Job.status.in_(ACTIVE_JOB_STATUSES),
        )
    )
    quota_limits = get_user_quota_limits(db, user.id)
    if int(active_user_jobs or 0) >= quota_limits["active_job_limit"]:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many active jobs for this user.",
        )

    active_global_jobs = db.scalar(select(func.count(Job.id)).where(Job.status.in_(ACTIVE_JOB_STATUSES)))
    if int(active_global_jobs or 0) >= settings.max_global_active_jobs:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="The system queue is full. Please try again later.",
        )

    total_size = 0
    payloads: list[tuple[UploadFile, bytes]] = []
    for upload in files:
        suffix = Path(upload.filename or "").suffix.lower()
        if suffix not in ALLOWED_UPLOAD_SUFFIXES:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {upload.filename}. Allowed: {', '.join(sorted(ALLOWED_UPLOAD_SUFFIXES))}.",
            )
        payload = await upload.read()
        payload_size = len(payload)
        total_size += payload_size
        if payload_size > settings.max_file_size_bytes:
            raise HTTPException(
                status_code=400,
                detail=f"File {upload.filename} exceeds the per-file limit.",
            )
        payloads.append((upload, payload))

    if total_size > settings.max_total_upload_bytes:
        raise HTTPException(status_code=400, detail="Total upload size exceeds the configured limit.")

    enforce_submission_quota(db, user.id, total_size)

    job = Job(
        user_id=user.id,
        status=JobStatus.UPLOADED.value,
        phase=JobPhase.UPLOAD.value,
        progress_percent=0,
    )
    db.add(job)
    db.flush()

    log_job_event(
        db,
        job.id,
        event_type="job.created",
        message="Job created and uploads received.",
        user_id=user.id,
        phase=JobPhase.UPLOAD.value,
    )

    for index, (upload, payload) in enumerate(payloads, start=1):
        original_name = Path(upload.filename or "upload.dat").name.strip() or "upload.dat"
        safe_filename = sanitize_filename(original_name)
        storage_key = f"jobs/{job.id}/inputs/{index:02d}_{safe_filename}"
        storage.upload_bytes(storage_key, payload, content_type=upload.content_type)
        db.add(
            JobInput(
                job_id=job.id,
                original_name=original_name,
                storage_key=storage_key,
                size_bytes=len(payload),
                mime_type=upload.content_type,
                sha256=compute_sha256(payload),
            )
        )
        log_job_event(
            db,
            job.id,
            event_type="job.input_uploaded",
            message=f"Uploaded input file {original_name}.",
            payload={"storage_key": storage_key, "size_bytes": len(payload)},
            user_id=user.id,
            phase=JobPhase.UPLOAD.value,
        )

    db.commit()
    db.refresh(job)
    return job


def build_job_workspace(job_id: str) -> Path:
    """Return the dedicated local workspace directory for a job."""
    workspace = get_settings().job_workspace_root / job_id
    workspace.mkdir(parents=True, exist_ok=True)
    return workspace


def prepare_job_directories(job_id: str) -> dict[str, Path]:
    """Create all local directories used during worker execution."""
    workspace = build_job_workspace(job_id)
    paths = {
        "workspace": workspace,
        "inputs": workspace / "inputs",
        "charts": workspace / "charts",
        "artifacts": workspace / "artifacts",
        "logs": workspace / "logs",
    }
    for item in paths.values():
        item.mkdir(parents=True, exist_ok=True)
    return paths


def download_job_inputs(job: Job, *, storage: StorageBackend | None = None) -> list[Path]:
    """Materialize job inputs from object storage into the worker workspace."""
    storage = storage or get_storage_backend()
    directories = prepare_job_directories(job.id)
    local_paths: list[Path] = []
    for item in sorted(job.inputs, key=lambda row: row.original_name.lower()):
        local_path = directories["inputs"] / Path(item.storage_key).name
        storage.download_to_path(item.storage_key, local_path)
        local_paths.append(local_path)
    return local_paths


def replace_job_tasks(db: Session, job: Job, task_rows: Iterable[JobTask]) -> None:
    """Replace persisted task rows after the analysis phase."""
    db.execute(delete(JobTask).where(JobTask.job_id == job.id))
    for task_row in task_rows:
        db.add(task_row)


def replace_job_artifacts(db: Session, job: Job, artifact_rows: Iterable[JobArtifact]) -> None:
    """Replace persisted render artifacts after the render phase."""
    db.execute(delete(JobArtifact).where(JobArtifact.job_id == job.id))
    for artifact_row in artifact_rows:
        db.add(artifact_row)


def package_directory_to_zip(source_dir: Path, destination_zip: Path) -> None:
    """Zip a directory tree for convenient final download."""
    destination_zip.parent.mkdir(parents=True, exist_ok=True)
    resolved_destination = destination_zip.resolve()
    with zipfile.ZipFile(destination_zip, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        for file_path in sorted(source_dir.rglob("*")):
            if file_path.is_file() and file_path.resolve() != resolved_destination:
                archive.write(file_path, arcname=file_path.relative_to(source_dir))


def upload_artifact_bundle(
    storage: StorageBackend,
    *,
    job_id: str,
    local_path: Path,
    artifact_type: str,
) -> JobArtifact:
    """Upload one artifact file and return the ORM row."""
    key = f"jobs/{job_id}/artifacts/{local_path.name}"
    storage.upload_file(local_path, key, content_type=guess_content_type(local_path))
    return JobArtifact(
        job_id=job_id,
        artifact_type=artifact_type,
        storage_key=key,
        file_size=local_path.stat().st_size,
    )


def expire_job(job: Job, db: Session) -> None:
    """Mark a job as expired."""
    update_job_state(
        db,
        job,
        status_value=JobStatus.EXPIRED.value,
        phase_value=JobPhase.EXPIRED.value,
        progress_percent=100,
        error_summary="Expired and cleaned up.",
    )


def cleanup_job_workspace(job_id: str) -> None:
    """Remove the local worker workspace for a job."""
    workspace = get_settings().job_workspace_root / job_id
    if workspace.exists():
        shutil.rmtree(workspace, ignore_errors=True)


def ensure_job_can_be_deleted(job: Job) -> None:
    """Reject unsafe job-deletion attempts with user-facing API errors."""
    if job.status in NON_DELETABLE_JOB_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="This job is currently running and cannot be cancelled yet. Please wait for this phase to finish.",
        )
    if job.status not in DELETABLE_JOB_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Job status {job.status} cannot be deleted.",
        )


def delete_job_resources(
    db: Session,
    job: Job,
    *,
    storage: StorageBackend | None = None,
) -> None:
    """Remove persisted storage, workspace files, and database rows for one job."""
    storage = storage or get_storage_backend()
    storage.delete_prefix(f"jobs/{job.id}")
    cleanup_job_workspace(job.id)
    db.execute(delete(JobInput).where(JobInput.job_id == job.id))
    db.execute(delete(JobTask).where(JobTask.job_id == job.id))
    db.execute(delete(JobArtifact).where(JobArtifact.job_id == job.id))
    db.execute(delete(JobEvent).where(JobEvent.job_id == job.id))
    db.execute(delete(Job).where(Job.id == job.id))


def stream_storage_file(storage: StorageBackend, key: str) -> io.BufferedReader | io.BytesIO:
    """Open a stored object for controlled API downloads."""
    return storage.open_stream(key)


def list_expired_jobs(db: Session) -> list[Job]:
    """Return jobs that have crossed their expiration time and still hold resources."""
    now = datetime.utcnow()
    return list(
        db.scalars(
            select(Job).where(
                Job.expires_at.is_not(None),
                Job.expires_at <= now,
                Job.status.in_(
                    [
                        JobStatus.AWAITING_SELECTION.value,
                        JobStatus.COMPLETED.value,
                        JobStatus.FAILED.value,
                    ]
                ),
            )
        )
    )


def set_job_ttl(job: Job) -> datetime:
    """Return the computed expiration timestamp for a finished job."""
    return datetime.utcnow() + timedelta(minutes=get_settings().artifact_ttl_minutes)
