"""Initial SmartAnalyst schema."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(length=32), primary_key=True),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("password_hash", sa.String(length=512), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("email_verified_at", sa.DateTime(), nullable=True),
        sa.Column("email_verification_token_hash", sa.String(length=128), nullable=True),
        sa.Column("email_verification_sent_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("email"),
    )
    op.create_index("ix_users_email", "users", ["email"])

    op.create_table(
        "jobs",
        sa.Column("id", sa.String(length=32), primary_key=True),
        sa.Column("user_id", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("phase", sa.String(length=32), nullable=False),
        sa.Column("progress_percent", sa.Integer(), nullable=False),
        sa.Column("report_title", sa.String(length=512), nullable=True),
        sa.Column("selected_task_ids", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("queue_task_id", sa.String(length=64), nullable=True),
        sa.Column("dataset_meta_json", sa.JSON(), nullable=True),
        sa.Column("data_summary_json", sa.JSON(), nullable=True),
        sa.Column("report_payload_json", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_jobs_user_id", "jobs", ["user_id"])
    op.create_index("ix_jobs_status", "jobs", ["status"])
    op.create_index("ix_jobs_expires_at", "jobs", ["expires_at"])

    op.create_table(
        "job_inputs",
        sa.Column("id", sa.String(length=32), primary_key=True),
        sa.Column("job_id", sa.String(length=32), nullable=False),
        sa.Column("original_name", sa.String(length=512), nullable=False),
        sa.Column("storage_key", sa.String(length=1024), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("mime_type", sa.String(length=255), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_job_inputs_job_id", "job_inputs", ["job_id"])

    op.create_table(
        "job_tasks",
        sa.Column("id", sa.String(length=32), primary_key=True),
        sa.Column("job_id", sa.String(length=32), nullable=False),
        sa.Column("task_index", sa.Integer(), nullable=False),
        sa.Column("question_zh", sa.String(length=1024), nullable=False),
        sa.Column("analysis_type", sa.String(length=32), nullable=False),
        sa.Column("required_datasets", sa.JSON(), nullable=False),
        sa.Column("image_storage_key", sa.String(length=1024), nullable=True),
        sa.Column("analysis_text", sa.Text(), nullable=True),
        sa.Column("selected", sa.Boolean(), nullable=False),
        sa.Column("task_plan_json", sa.JSON(), nullable=True),
        sa.Column("result_payload_json", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("job_id", "task_index", name="uq_job_task_index"),
    )
    op.create_index("ix_job_tasks_job_id", "job_tasks", ["job_id"])

    op.create_table(
        "job_artifacts",
        sa.Column("id", sa.String(length=32), primary_key=True),
        sa.Column("job_id", sa.String(length=32), nullable=False),
        sa.Column("artifact_type", sa.String(length=32), nullable=False),
        sa.Column("storage_key", sa.String(length=1024), nullable=False),
        sa.Column("file_size", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("job_id", "artifact_type", name="uq_job_artifact_type"),
    )
    op.create_index("ix_job_artifacts_job_id", "job_artifacts", ["job_id"])

    op.create_table(
        "job_events",
        sa.Column("id", sa.String(length=32), primary_key=True),
        sa.Column("job_id", sa.String(length=32), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_job_events_job_id", "job_events", ["job_id"])


def downgrade() -> None:
    op.drop_table("job_events")
    op.drop_table("job_artifacts")
    op.drop_table("job_tasks")
    op.drop_table("job_inputs")
    op.drop_table("jobs")
    op.drop_table("users")
