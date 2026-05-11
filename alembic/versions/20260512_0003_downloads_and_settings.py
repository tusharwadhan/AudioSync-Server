"""downloads sync table + per-user settings blob

Revision ID: 0003_downloads_and_settings
Revises: 0002_sync_tables
Create Date: 2026-05-12

Adds:
  * users.settings_json — JSONB blob of the client's app settings
    (auto-download toggle/threshold, wifi-only, client-extraction
    toggle, hum on/off + consent, onboarding/prompt flags, display
    name). Client owns the schema; last-write-wins on an embedded
    "_updated_at" key.
  * user_downloads — bookkeeping for offline-downloaded songs (metadata
    + flags only; the audio blob is never uploaded). Same tombstone /
    last-write-wins conventions as user_favorites.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0003_downloads_and_settings"
down_revision: Union[str, None] = "0002_sync_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── users.settings_json ───────────────────────────────────────────────
    op.add_column(
        "users",
        sa.Column("settings_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    # ── user_downloads ────────────────────────────────────────────────────
    op.create_table(
        "user_downloads",
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("video_id", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=True),
        sa.Column("uploader", sa.String(length=255), nullable=True),
        sa.Column("duration", sa.Integer(), nullable=True),
        sa.Column("thumbnail", sa.String(length=1024), nullable=True),
        sa.Column("file_size", sa.BigInteger(), nullable=True),
        sa.Column("is_auto_downloaded", sa.Boolean(), nullable=False),
        sa.Column("downloaded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("user_id", "video_id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_user_downloads_user_updated",
        "user_downloads",
        ["user_id", "updated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_user_downloads_user_updated", table_name="user_downloads")
    op.drop_table("user_downloads")
    op.drop_column("users", "settings_json")
