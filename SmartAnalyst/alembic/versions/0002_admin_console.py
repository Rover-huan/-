"""Add administrator console tables."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_admin_console"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "admin_accounts",
        sa.Column("id", sa.String(length=32), primary_key=True),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("password_hash", sa.String(length=512), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("last_login_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("email"),
    )
    op.create_index("ix_admin_accounts_email", "admin_accounts", ["email"])
    op.create_index("ix_admin_accounts_role", "admin_accounts", ["role"])

    op.create_table(
        "user_presence",
        sa.Column("user_id", sa.String(length=32), nullable=False),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(), nullable=False),
        sa.Column("current_job_id", sa.String(length=32), nullable=True),
        sa.Column("current_path", sa.String(length=512), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id"),
    )
    op.create_index("ix_user_presence_email", "user_presence", ["email"])
    op.create_index("ix_user_presence_last_seen_at", "user_presence", ["last_seen_at"])
    op.create_index("ix_user_presence_current_job_id", "user_presence", ["current_job_id"])

    op.create_table(
        "user_quota_overrides",
        sa.Column("user_id", sa.String(length=32), nullable=False),
        sa.Column("daily_job_limit", sa.Integer(), nullable=True),
        sa.Column("daily_upload_bytes_limit", sa.Integer(), nullable=True),
        sa.Column("active_job_limit", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("updated_by_admin_id", sa.String(length=32), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id"),
    )


def downgrade() -> None:
    op.drop_table("user_quota_overrides")
    op.drop_table("user_presence")
    op.drop_table("admin_accounts")
