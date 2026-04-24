"""Configuration helpers for the SmartAnalyst service runtime."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency guard
    def load_dotenv(*args: object, **kwargs: object) -> bool:
        return False


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)


def _read_str(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    cleaned = value.strip()
    return cleaned or default


def _read_int(name: str, default: int) -> int:
    value = _read_str(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _clamp_int(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(value, maximum))


def _read_bool(name: str, default: bool) -> bool:
    value = _read_str(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    """All environment-backed runtime settings."""

    project_root: Path
    app_env: str
    public_base_url: str
    api_root_path: str
    auto_create_tables: bool
    secret_key: str
    access_token_expire_minutes: int
    session_cookie_name: str
    auth_cookie_secure: bool
    auth_cookie_samesite: str
    auth_cookie_domain: str | None
    database_url: str
    redis_url: str
    storage_backend: str
    local_storage_root: Path
    job_workspace_root: Path
    s3_bucket: str
    s3_region: str
    s3_endpoint_url: str | None
    s3_access_key_id: str | None
    s3_secret_access_key: str | None
    s3_secure: bool
    s3_addressing_style: str
    artifact_ttl_minutes: int
    max_upload_files: int
    max_file_size_bytes: int
    max_total_upload_bytes: int
    max_user_active_jobs: int
    max_global_active_jobs: int
    max_job_runtime_seconds: int
    cors_origins: list[str]
    queue_name: str
    analysis_queue_name: str
    render_queue_name: str
    celery_soft_time_limit: int
    celery_hard_time_limit: int
    download_url_expire_seconds: int
    runner_mode: str
    runner_timeout_seconds: int
    chart_generation_concurrency: int
    llm_daily_budget_limit: int
    max_daily_jobs_per_user: int
    max_daily_upload_bytes_per_user: int
    register_rate_limit_per_hour_ip: int
    login_rate_limit_per_15_min_ip: int
    admin_api_token: str | None
    enable_admin_debug: bool
    admin_owner_email: str | None
    admin_owner_initial_password: str | None
    admin_session_cookie_name: str
    admin_access_token_expire_minutes: int
    presence_online_window_seconds: int
    sse_poll_interval_seconds: int
    captcha_required: bool
    captcha_provider: str
    captcha_verify_url: str | None
    captcha_secret_key: str | None
    captcha_bypass_token: str | None
    email_verification_required: bool
    email_verification_token_ttl_minutes: int
    smtp_host: str | None
    smtp_port: int
    smtp_username: str | None
    smtp_password: str | None
    smtp_from_email: str | None
    smtp_use_tls: bool
    enable_auto_toc: bool
    auto_toc_backend: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Build and cache service settings."""
    project_root = PROJECT_ROOT
    app_env = (_read_str("APP_ENV", "local") or "local").strip().lower()
    is_production = app_env in {"prod", "production"}
    database_url = _read_str("DATABASE_URL", "sqlite:///./smartanalyst.db")
    redis_url = _read_str("REDIS_URL", "redis://localhost:6379/0") or "redis://localhost:6379/0"
    auth_cookie_samesite = (_read_str("AUTH_COOKIE_SAMESITE", "lax") or "lax").strip().lower()
    if auth_cookie_samesite not in {"lax", "strict", "none"}:
        auth_cookie_samesite = "lax"
    local_storage_root = Path(
        _read_str("LOCAL_STORAGE_ROOT", str(project_root / "storage")) or (project_root / "storage").as_posix()
    ).resolve()
    job_workspace_root = Path(
        _read_str("JOB_WORKSPACE_ROOT", str(project_root / "runs")) or (project_root / "runs").as_posix()
    ).resolve()
    cors_raw = _read_str("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000") or ""
    cors_origins = [item.strip() for item in cors_raw.split(",") if item.strip()]
    secret_key = _read_str("SECRET_KEY", "change-this-in-production") or "change-this-in-production"
    if is_production:
        if database_url.startswith("sqlite"):
            raise RuntimeError("DATABASE_URL must point to PostgreSQL in production.")
        if secret_key == "change-this-in-production":
            raise RuntimeError("SECRET_KEY must be changed in production.")

    runner_mode = (_read_str("RUNNER_MODE", "subprocess") or "subprocess").lower()
    chart_generation_concurrency = _clamp_int(_read_int("CHART_GENERATION_CONCURRENCY", 2), 1, 2)
    if runner_mode == "inprocess":
        chart_generation_concurrency = 1
    auto_toc_backend = (_read_str("AUTO_TOC_BACKEND", "word_com") or "word_com").lower()
    if auto_toc_backend not in {"word_com", "libreoffice", "aspose", "none"}:
        auto_toc_backend = "word_com"

    return Settings(
        project_root=project_root,
        app_env=app_env,
        public_base_url=_read_str("PUBLIC_BASE_URL", "http://127.0.0.1:8000") or "http://127.0.0.1:8000",
        api_root_path=_read_str("API_ROOT_PATH", "") or "",
        auto_create_tables=_read_bool("AUTO_CREATE_TABLES", not is_production),
        secret_key=secret_key,
        access_token_expire_minutes=_read_int("ACCESS_TOKEN_EXPIRE_MINUTES", 60 * 12),
        session_cookie_name=_read_str("SESSION_COOKIE_NAME", "smartanalyst_session") or "smartanalyst_session",
        auth_cookie_secure=_read_bool("AUTH_COOKIE_SECURE", is_production),
        auth_cookie_samesite=auth_cookie_samesite,
        auth_cookie_domain=_read_str("AUTH_COOKIE_DOMAIN"),
        database_url=database_url,
        redis_url=redis_url,
        storage_backend=(_read_str("STORAGE_BACKEND", "local") or "local").lower(),
        local_storage_root=local_storage_root,
        job_workspace_root=job_workspace_root,
        s3_bucket=_read_str("S3_BUCKET", "smartanalyst") or "smartanalyst",
        s3_region=_read_str("S3_REGION", "auto") or "auto",
        s3_endpoint_url=_read_str("S3_ENDPOINT_URL"),
        s3_access_key_id=_read_str("S3_ACCESS_KEY_ID"),
        s3_secret_access_key=_read_str("S3_SECRET_ACCESS_KEY"),
        s3_secure=_read_bool("S3_SECURE", True),
        s3_addressing_style=(_read_str("S3_ADDRESSING_STYLE", "virtual") or "virtual").lower(),
        artifact_ttl_minutes=_read_int("ARTIFACT_TTL_MINUTES", 120),
        max_upload_files=_read_int("MAX_UPLOAD_FILES", 5),
        max_file_size_bytes=_read_int("MAX_FILE_SIZE_BYTES", 20 * 1024 * 1024),
        max_total_upload_bytes=_read_int("MAX_TOTAL_UPLOAD_BYTES", 100 * 1024 * 1024),
        max_user_active_jobs=_read_int("MAX_USER_ACTIVE_JOBS", 2),
        max_global_active_jobs=_read_int("MAX_GLOBAL_ACTIVE_JOBS", 20),
        max_job_runtime_seconds=_read_int("MAX_JOB_RUNTIME_SECONDS", 1800),
        cors_origins=cors_origins,
        queue_name=_read_str("QUEUE_NAME", "smartanalyst") or "smartanalyst",
        analysis_queue_name=_read_str("ANALYSIS_QUEUE_NAME", _read_str("QUEUE_NAME", "smartanalyst")) or "smartanalyst",
        render_queue_name=_read_str("RENDER_QUEUE_NAME", _read_str("QUEUE_NAME", "smartanalyst")) or "smartanalyst",
        celery_soft_time_limit=_read_int("CELERY_SOFT_TIME_LIMIT", 1800),
        celery_hard_time_limit=_read_int("CELERY_HARD_TIME_LIMIT", 2100),
        download_url_expire_seconds=_read_int("DOWNLOAD_URL_EXPIRE_SECONDS", 300),
        runner_mode=runner_mode,
        runner_timeout_seconds=_read_int("RUNNER_TIMEOUT_SECONDS", 240),
        chart_generation_concurrency=chart_generation_concurrency,
        llm_daily_budget_limit=_read_int("LLM_DAILY_BUDGET_LIMIT", 0),
        max_daily_jobs_per_user=_read_int("MAX_DAILY_JOBS_PER_USER", 10),
        max_daily_upload_bytes_per_user=_read_int("MAX_DAILY_UPLOAD_BYTES_PER_USER", 250 * 1024 * 1024),
        register_rate_limit_per_hour_ip=_read_int("REGISTER_RATE_LIMIT_PER_HOUR_IP", 10),
        login_rate_limit_per_15_min_ip=_read_int("LOGIN_RATE_LIMIT_PER_15_MIN_IP", 30),
        admin_api_token=_read_str("ADMIN_API_TOKEN"),
        enable_admin_debug=_read_bool("ENABLE_ADMIN_DEBUG", False),
        admin_owner_email=_read_str("ADMIN_OWNER_EMAIL"),
        admin_owner_initial_password=_read_str("ADMIN_OWNER_INITIAL_PASSWORD"),
        admin_session_cookie_name=_read_str("ADMIN_SESSION_COOKIE_NAME", "smartanalyst_admin_session")
        or "smartanalyst_admin_session",
        admin_access_token_expire_minutes=_read_int("ADMIN_ACCESS_TOKEN_EXPIRE_MINUTES", 60 * 12),
        presence_online_window_seconds=_read_int("PRESENCE_ONLINE_WINDOW_SECONDS", 5 * 60),
        sse_poll_interval_seconds=_read_int("SSE_POLL_INTERVAL_SECONDS", 2),
        captcha_required=_read_bool("CAPTCHA_REQUIRED", is_production),
        captcha_provider=(_read_str("CAPTCHA_PROVIDER", "generic") or "generic").lower(),
        captcha_verify_url=_read_str("CAPTCHA_VERIFY_URL"),
        captcha_secret_key=_read_str("CAPTCHA_SECRET_KEY"),
        captcha_bypass_token=_read_str("CAPTCHA_BYPASS_TOKEN"),
        email_verification_required=_read_bool("EMAIL_VERIFICATION_REQUIRED", is_production),
        email_verification_token_ttl_minutes=_read_int("EMAIL_VERIFICATION_TOKEN_TTL_MINUTES", 60 * 24),
        smtp_host=_read_str("SMTP_HOST"),
        smtp_port=_read_int("SMTP_PORT", 587),
        smtp_username=_read_str("SMTP_USERNAME"),
        smtp_password=_read_str("SMTP_PASSWORD"),
        smtp_from_email=_read_str("SMTP_FROM_EMAIL"),
        smtp_use_tls=_read_bool("SMTP_USE_TLS", True),
        enable_auto_toc=_read_bool("ENABLE_AUTO_TOC", True),
        auto_toc_backend=auto_toc_backend,
    )
