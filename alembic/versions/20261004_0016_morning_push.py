"""morning push: app_config KV, per-account FCM tokens, send log, drafts

Revision ID: 0016_morning_push
Revises: 0015_lyrics_offsets
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0016_morning_push"
down_revision: Union[str, None] = "0015_lyrics_offsets"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "app_config",
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("key"),
    )

    op.create_table(
        "user_fcm_tokens",
        sa.Column("token", sa.String(length=512), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("device", sa.String(length=128), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False,
                  server_default=sa.text("true")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("token"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_user_fcm_tokens_user", "user_fcm_tokens", ["user_id"])

    op.create_table(
        "morning_push_log",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("sent_date", sa.String(length=10), nullable=False),
        sa.Column("video_id", sa.String(length=32), nullable=True),
        sa.Column("title", sa.String(length=256), nullable=True),
        sa.Column("body", sa.String(length=512), nullable=True),
        sa.Column("status", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_morning_push_log_user_date", "morning_push_log",
                    ["user_id", "sent_date"])
    op.create_index("ix_morning_push_log_date", "morning_push_log", ["sent_date"])

    op.create_table(
        "morning_push_drafts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False,
                  server_default=sa.text("'pending'")),
        sa.Column("payload", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("morning_push_drafts")
    op.drop_index("ix_morning_push_log_date", table_name="morning_push_log")
    op.drop_index("ix_morning_push_log_user_date", table_name="morning_push_log")
    op.drop_table("morning_push_log")
    op.drop_index("ix_user_fcm_tokens_user", table_name="user_fcm_tokens")
    op.drop_table("user_fcm_tokens")
    op.drop_table("app_config")
