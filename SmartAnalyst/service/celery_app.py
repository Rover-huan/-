"""Celery application bootstrap for SmartAnalyst."""

from __future__ import annotations

from celery import Celery

from service.config import get_settings


settings = get_settings()
celery_app = Celery(
    "smartanalyst",
    broker=settings.redis_url,
    backend=settings.redis_url,
)
celery_app.conf.update(
    task_default_queue=settings.queue_name,
    task_routes={
        "service.run_analysis_job": {"queue": settings.analysis_queue_name},
        "service.run_render_job": {"queue": settings.render_queue_name},
        "service.cleanup_expired_jobs": {"queue": settings.queue_name},
    },
    task_track_started=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    broker_connection_retry_on_startup=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=settings.celery_soft_time_limit,
    task_time_limit=settings.celery_hard_time_limit,
    timezone="UTC",
    beat_schedule={
        "cleanup-expired-jobs-every-10-minutes": {
            "task": "service.cleanup_expired_jobs",
            "schedule": 600.0,
        }
    },
)
celery_app.autodiscover_tasks(["service"])
