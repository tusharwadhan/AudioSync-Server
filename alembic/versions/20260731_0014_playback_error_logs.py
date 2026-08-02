"""Durable playback/download error reports

Revision ID: 0014_playback_error_logs
Revises: 0013_dm_disappeared
Create Date: 2026-07-31

Device error reports previously lived only in a process-local ring buffer
plus a JSONL file on the instance disk. Render's filesystem is ephemeral,
so every redeploy wiped the history — losing the evidence precisely when a
fresh release is most worth watching.

This table is the durable copy. No FK to `users`: reports are anonymous
device diagnostics and must keep working for signed-out users.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0014_playback_error_logs"
down_revision: Union[str, None] = "0013_dm_disappeared"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "playback_error_logs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.String(length=128), nullable=True),
        sa.Column("device_model", sa.String(length=128), nullable=True),
        sa.Column("android_version", sa.String(length=32), nullable=True),
        sa.Column("app_version", sa.String(length=32), nullable=True),
        sa.Column("app_version_code", sa.Integer(), nullable=True),
        sa.Column("network", sa.String(length=32), nullable=True),
        sa.Column("error_type", sa.String(length=64), nullable=True),
        sa.Column("song_id", sa.String(length=64), nullable=True),
        sa.Column("song_title", sa.String(length=512), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("recent_logs", sa.Text(), nullable=True),
        sa.Column("client_ip", sa.String(length=64), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_playback_error_logs_received", "playback_error_logs", ["received_at"]
    )
    op.create_index(
        "ix_playback_error_logs_device_received",
        "playback_error_logs",
        ["device_id", "received_at"],
    )
    op.create_index(
        "ix_playback_error_logs_type_received",
        "playback_error_logs",
        ["error_type", "received_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_playback_error_logs_type_received", table_name="playback_error_logs")
    op.drop_index("ix_playback_error_logs_device_received", table_name="playback_error_logs")
    op.drop_index("ix_playback_error_logs_received", table_name="playback_error_logs")
    op.drop_table("playback_error_logs")
