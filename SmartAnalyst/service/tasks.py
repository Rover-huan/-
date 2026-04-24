"""Celery tasks that execute SmartAnalyst analysis and rendering jobs."""

from __future__ import annotations

import sys
import time
import traceback
from pathlib import Path

from celery.utils.log import get_task_logger
from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main import _build_load_code, run_analysis_phase, run_render_phase
from service.celery_app import celery_app
from service.db import SessionLocal
from service.job_service import (
    cleanup_job_workspace,
    download_job_inputs,
    expire_job,
    list_expired_jobs,
    log_job_event,
    package_directory_to_zip,
    prepare_job_directories,
    replace_job_artifacts,
    replace_job_tasks,
    set_job_ttl,
    update_job_state,
    upload_artifact_bundle,
)
from service.models import ArtifactType, Job, JobArtifact, JobInput, JobPhase, JobStatus, JobTask
from service.storage import get_storage_backend, guess_content_type


LOGGER = get_task_logger(__name__)


def _elapsed_ms(started_at: float) -> int:
    return int((time.perf_counter() - started_at) * 1000)


def _build_job_event_callback(
    db,
    *,
    job_id: str,
    user_id: str,
    phase: str,
    celery_task_id: str | None,
):
    """Create a best-effort progress event writer for long-running pipeline steps."""

    def callback(event_type: str, message: str, payload: dict | None = None) -> None:
        try:
            log_job_event(
                db,
                job_id,
                event_type=event_type,
                message=message,
                payload=payload or {},
                user_id=user_id,
                phase=phase,
                celery_task_id=celery_task_id,
            )
            db.commit()
        except Exception:
            db.rollback()
            LOGGER.warning("Failed to persist progress event %s for job %s", event_type, job_id, exc_info=True)

    return callback


def _summarize_exception_chain(exc: BaseException) -> str:
    """Flatten nested exceptions into one concise operator-facing error summary."""
    parts: list[str] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        text = str(current).strip()
        if text and text not in parts:
            parts.append(text)
        current = current.__cause__ or current.__context__

    if not parts:
        return type(exc).__name__
    return " | ".join(parts[:5])


def _is_transient_llm_failure(exc: BaseException) -> bool:
    """Return True for retryable upstream model/network failures."""
    summary = _summarize_exception_chain(exc).lower()
    transient_markers = (
        "connection error",
        "api connection error",
        "unexpected_eof_while_reading",
        "eof occurred in violation of protocol",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "rate limit",
        "server disconnected",
    )
    return any(marker in summary for marker in transient_markers)


def _refresh_render_data_summary(
    data_summary: dict,
    local_input_paths: list[Path],
) -> dict:
    """Repoint renderer summary paths to the current job workspace inputs."""
    refreshed = dict(data_summary)
    resolved_paths = [Path(path).resolve() for path in local_input_paths]
    if not resolved_paths:
        return refreshed

    refreshed["dataset_path"] = resolved_paths[0].as_posix()
    refreshed["load_code"] = _build_load_code(resolved_paths)

    raw_summaries = refreshed.get("dataset_summaries")
    if isinstance(raw_summaries, list):
        updated_summaries: list[dict] = []
        for summary_item, dataset_path in zip(raw_summaries, resolved_paths):
            if not isinstance(summary_item, dict):
                continue
            summary_copy = dict(summary_item)
            summary_copy["dataset_path"] = dataset_path.as_posix()
            updated_summaries.append(summary_copy)
        if updated_summaries:
            refreshed["dataset_summaries"] = updated_summaries

    return refreshed


@celery_app.task(name="service.run_analysis_job", bind=True)
def run_analysis_job(self, job_id: str) -> None:
    """Run the analysis phase and persist candidate charts plus task metadata."""
    job_started_at = time.perf_counter()
    db = SessionLocal()
    storage = get_storage_backend()
    try:
        job = db.scalar(select(Job).options(selectinload(Job.inputs)).where(Job.id == job_id))
        if job is None:
            raise ValueError(f"Job not found: {job_id}")

        update_job_state(
            db,
            job,
            status_value=JobStatus.RUNNING_ANALYSIS.value,
            phase_value=JobPhase.ANALYSIS.value,
            progress_percent=5,
            started=True,
            clear_error_summary=True,
        )
        log_job_event(
            db,
            job.id,
            event_type="job.analysis_started",
            message="Analysis phase started.",
            user_id=job.user_id,
            phase=JobPhase.ANALYSIS.value,
            celery_task_id=self.request.id,
        )
        db.commit()

        directories = prepare_job_directories(job.id)
        local_input_paths = download_job_inputs(job, storage=storage)

        analysis_bundle = run_analysis_phase(
            local_input_paths,
            directories["charts"],
            progress_callback=_build_job_event_callback(
                db,
                job_id=job.id,
                user_id=job.user_id,
                phase=JobPhase.ANALYSIS.value,
                celery_task_id=self.request.id,
            ),
        )
        task_rows: list[JobTask] = []
        total_tasks = max(len(analysis_bundle["task_plans"]), 1)
        for index, (task_plan, result_payload) in enumerate(
            zip(analysis_bundle["task_plans"], analysis_bundle["results"]),
            start=1,
        ):
            local_image_path = Path(result_payload["image_path"]).resolve()
            chart_key = f"jobs/{job.id}/charts/{local_image_path.name}"
            storage.upload_file(local_image_path, chart_key, content_type=guess_content_type(local_image_path))
            persisted_result = dict(result_payload)
            persisted_result["image_storage_key"] = chart_key
            task_rows.append(
                JobTask(
                    job_id=job.id,
                    task_index=int(task_plan["task_id"]),
                    question_zh=str(task_plan["question_zh"]).strip(),
                    analysis_type=str(task_plan["analysis_type"]).strip(),
                    required_datasets=list(task_plan.get("required_datasets", [])),
                    image_storage_key=chart_key,
                    analysis_text=str(result_payload.get("analysis_text", "")).strip() or None,
                    selected=False,
                    task_plan_json=dict(task_plan),
                    result_payload_json=persisted_result,
                )
            )
            log_job_event(
                db,
                job.id,
                event_type="job.task_ready",
                message=f"Prepared candidate chart for task {task_plan['task_id']}.",
                payload={"task_id": int(task_plan["task_id"]), "chart_key": chart_key},
                user_id=job.user_id,
                phase=JobPhase.ANALYSIS.value,
                celery_task_id=self.request.id,
            )
            update_job_state(
                db,
                job,
                progress_percent=5 + int((index / total_tasks) * 70),
            )
            db.commit()

        replace_job_tasks(db, job, task_rows)
        job.dataset_meta_json = dict(analysis_bundle["dataset_meta"])
        job.data_summary_json = dict(analysis_bundle["data_summary"])
        job.report_title = str(analysis_bundle["report_title"]).strip()
        update_job_state(
            db,
            job,
            status_value=JobStatus.AWAITING_SELECTION.value,
            phase_value=JobPhase.SELECTION.value,
            progress_percent=80,
            expires_at=set_job_ttl(job),
        )
        log_job_event(
            db,
            job.id,
            event_type="job.awaiting_selection",
            message="Analysis complete; awaiting user chart selection.",
            payload={"task_count": len(task_rows), "duration_ms": _elapsed_ms(job_started_at)},
            user_id=job.user_id,
            phase=JobPhase.SELECTION.value,
            celery_task_id=self.request.id,
        )
        db.commit()
        LOGGER.info("Analysis phase completed for job %s in %sms", job.id, _elapsed_ms(job_started_at))
    except Exception as exc:
        db.rollback()
        job = db.scalar(select(Job).where(Job.id == job_id))
        if job is not None and _is_transient_llm_failure(exc) and self.request.retries < 2:
            countdown = 30 * (self.request.retries + 1)
            update_job_state(
                db,
                job,
                status_value=JobStatus.QUEUED_ANALYSIS.value,
                phase_value=JobPhase.ANALYSIS.value,
                progress_percent=1,
                clear_error_summary=True,
            )
            log_job_event(
                db,
                job.id,
                event_type="job.analysis_retrying",
                message=f"Transient upstream model/network error detected; retrying analysis in {countdown} seconds.",
                payload={
                    "retry_index": self.request.retries + 1,
                    "error_summary": _summarize_exception_chain(exc),
                    "duration_ms": _elapsed_ms(job_started_at),
                },
                user_id=job.user_id,
                phase=JobPhase.ANALYSIS.value,
                celery_task_id=self.request.id,
            )
            db.commit()
            raise self.retry(exc=exc, countdown=countdown, max_retries=2)
        if job is not None:
            error_summary = _summarize_exception_chain(exc)
            update_job_state(
                db,
                job,
                status_value=JobStatus.FAILED.value,
                phase_value=JobPhase.FAILED.value,
                progress_percent=100,
                error_summary=error_summary,
                finished=True,
                expires_at=set_job_ttl(job),
            )
            log_job_event(
                db,
                job.id,
                event_type="job.analysis_failed",
                level="error",
                message=error_summary,
                payload={"traceback": traceback.format_exc(), "duration_ms": _elapsed_ms(job_started_at)},
                user_id=job.user_id,
                phase=JobPhase.FAILED.value,
                celery_task_id=self.request.id,
            )
            db.commit()
        raise
    finally:
        cleanup_job_workspace(job_id)
        db.close()


@celery_app.task(name="service.run_render_job", bind=True)
def run_render_job(self, job_id: str) -> None:
    """Run the final synthesis/render phase for the user's selected charts."""
    job_started_at = time.perf_counter()
    db = SessionLocal()
    storage = get_storage_backend()
    try:
        job = db.scalar(
            select(Job).options(selectinload(Job.tasks), selectinload(Job.inputs)).where(Job.id == job_id)
        )
        if job is None:
            raise ValueError(f"Job not found: {job_id}")
        if not job.selected_task_ids:
            raise ValueError("No selected task ids were provided for rendering.")
        if not job.data_summary_json or not job.report_title:
            raise ValueError("Job is missing the persisted analysis context required for rendering.")

        update_job_state(
            db,
            job,
            status_value=JobStatus.RENDERING.value,
            phase_value=JobPhase.RENDER.value,
            progress_percent=85,
            clear_error_summary=True,
        )
        log_job_event(
            db,
            job.id,
            event_type="job.render_started",
            message="Render phase started.",
            user_id=job.user_id,
            phase=JobPhase.RENDER.value,
            celery_task_id=self.request.id,
        )
        db.commit()

        directories = prepare_job_directories(job.id)
        local_input_paths = download_job_inputs(job, storage=storage)
        render_data_summary = _refresh_render_data_summary(dict(job.data_summary_json), local_input_paths)
        selected_set = {int(item) for item in job.selected_task_ids}
        selected_rows = sorted(
            [row for row in job.tasks if int(row.task_index) in selected_set],
            key=lambda row: int(row.task_index),
        )
        if not selected_rows:
            raise ValueError("Selected tasks are missing from the analysis output.")

        selected_plans: list[dict] = []
        selected_results: list[dict] = []
        for row in selected_rows:
            if not row.task_plan_json or not row.result_payload_json or not row.image_storage_key:
                raise ValueError(f"Task {row.task_index} is missing persisted render context.")
            local_chart_path = directories["charts"] / Path(row.image_storage_key).name
            storage.download_to_path(row.image_storage_key, local_chart_path)
            task_result = dict(row.result_payload_json)
            task_result["image_path"] = str(local_chart_path.resolve())
            selected_plans.append(dict(row.task_plan_json))
            selected_results.append(task_result)

        render_bundle = run_render_phase(
            selected_plans,
            selected_results,
            render_data_summary,
            job.report_title,
            directories["artifacts"],
        )

        artifact_rows: list[JobArtifact] = []
        artifact_map = {
            ArtifactType.DOCX.value: render_bundle["artifacts"]["docx_path"],
            ArtifactType.NOTEBOOK.value: render_bundle["artifacts"]["notebook_path"],
            ArtifactType.CLEANING_SUMMARY.value: render_bundle["artifacts"]["cleaning_summary_path"],
        }
        pdf_path = render_bundle["artifacts"].get("pdf_path")
        if pdf_path:
            artifact_map[ArtifactType.PDF.value] = pdf_path

        for artifact_type, artifact_path in artifact_map.items():
            local_path = Path(str(artifact_path)).resolve()
            artifact_rows.append(
                upload_artifact_bundle(storage, job_id=job.id, local_path=local_path, artifact_type=artifact_type)
            )

        zip_path = directories["workspace"] / "SmartAnalyst_Result_Bundle.zip"
        package_directory_to_zip(directories["artifacts"], zip_path)
        artifact_rows.append(
            upload_artifact_bundle(storage, job_id=job.id, local_path=zip_path, artifact_type=ArtifactType.ZIP.value)
        )

        replace_job_artifacts(db, job, artifact_rows)
        job.report_payload_json = dict(render_bundle["report_text"])
        update_job_state(
            db,
            job,
            status_value=JobStatus.COMPLETED.value,
            phase_value=JobPhase.COMPLETE.value,
            progress_percent=100,
            finished=True,
            expires_at=set_job_ttl(job),
        )
        log_job_event(
            db,
            job.id,
            event_type="job.render_completed",
            message="Render phase completed and artifacts were uploaded.",
            payload={
                "artifact_types": [item.artifact_type for item in artifact_rows],
                "duration_ms": _elapsed_ms(job_started_at),
            },
            user_id=job.user_id,
            phase=JobPhase.COMPLETE.value,
            celery_task_id=self.request.id,
        )
        db.commit()
        LOGGER.info("Render phase completed for job %s in %sms", job.id, _elapsed_ms(job_started_at))
    except Exception as exc:
        db.rollback()
        job = db.scalar(select(Job).where(Job.id == job_id))
        if job is not None and _is_transient_llm_failure(exc) and self.request.retries < 2:
            countdown = 30 * (self.request.retries + 1)
            update_job_state(
                db,
                job,
                status_value=JobStatus.QUEUED_RENDER.value,
                phase_value=JobPhase.RENDER.value,
                progress_percent=82,
                clear_error_summary=True,
            )
            log_job_event(
                db,
                job.id,
                event_type="job.render_retrying",
                message=f"Transient upstream model/network error detected; retrying render in {countdown} seconds.",
                payload={
                    "retry_index": self.request.retries + 1,
                    "error_summary": _summarize_exception_chain(exc),
                    "duration_ms": _elapsed_ms(job_started_at),
                },
                user_id=job.user_id,
                phase=JobPhase.RENDER.value,
                celery_task_id=self.request.id,
            )
            db.commit()
            raise self.retry(exc=exc, countdown=countdown, max_retries=2)
        if job is not None:
            error_summary = _summarize_exception_chain(exc)
            update_job_state(
                db,
                job,
                status_value=JobStatus.FAILED.value,
                phase_value=JobPhase.FAILED.value,
                progress_percent=100,
                error_summary=error_summary,
                finished=True,
                expires_at=set_job_ttl(job),
            )
            log_job_event(
                db,
                job.id,
                event_type="job.render_failed",
                level="error",
                message=error_summary,
                payload={"traceback": traceback.format_exc(), "duration_ms": _elapsed_ms(job_started_at)},
                user_id=job.user_id,
                phase=JobPhase.FAILED.value,
                celery_task_id=self.request.id,
            )
            db.commit()
        raise
    finally:
        cleanup_job_workspace(job_id)
        db.close()


@celery_app.task(name="service.cleanup_expired_jobs")
def cleanup_expired_jobs() -> int:
    """Delete storage resources for expired jobs and mark them as expired."""
    db = SessionLocal()
    storage = get_storage_backend()
    expired_count = 0
    try:
        for job in list_expired_jobs(db):
            storage.delete_prefix(f"jobs/{job.id}")
            db.execute(delete(JobInput).where(JobInput.job_id == job.id))
            db.execute(delete(JobTask).where(JobTask.job_id == job.id))
            db.execute(delete(JobArtifact).where(JobArtifact.job_id == job.id))
            job.dataset_meta_json = None
            job.data_summary_json = None
            job.report_payload_json = None
            expire_job(job, db)
            log_job_event(
                db,
                job.id,
                event_type="job.expired",
                message="Expired job resources were deleted.",
                user_id=job.user_id,
                phase=JobPhase.EXPIRED.value,
            )
            cleanup_job_workspace(job.id)
            expired_count += 1
        db.commit()
        return expired_count
    finally:
        db.close()
