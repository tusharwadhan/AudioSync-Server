"""sync tables: favorites, playlists, playlist_songs, listen_events

Revision ID: 0002_sync_tables
Revises: 0001_users
Create Date: 2026-05-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0002_sync_tables"
down_revision: Union[str, None] = "0001_users"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── user_favorites ────────────────────────────────────────────────────
    op.create_table(
        "user_favorites",
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("video_id", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=True),
        sa.Column("uploader", sa.String(length=255), nullable=True),
        sa.Column("duration", sa.Integer(), nullable=True),
        sa.Column("thumbnail", sa.String(length=1024), nullable=True),
        sa.Column("favorited_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("user_id", "video_id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_user_favorites_user_updated",
        "user_favorites",
        ["user_id", "updated_at"],
        unique=False,
    )

    # ── user_playlists ────────────────────────────────────────────────────
    op.create_table(
        "user_playlists",
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("sync_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("auto_backup_enabled", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("user_id", "sync_id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_user_playlists_user_updated",
        "user_playlists",
        ["user_id", "updated_at"],
        unique=False,
    )

    # ── user_playlist_songs ───────────────────────────────────────────────
    op.create_table(
        "user_playlist_songs",
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("playlist_sync_id", sa.String(length=64), nullable=False),
        sa.Column("video_id", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=True),
        sa.Column("uploader", sa.String(length=255), nullable=True),
        sa.Column("duration", sa.Integer(), nullable=True),
        sa.Column("thumbnail", sa.String(length=1024), nullable=True),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("user_id", "playlist_sync_id", "video_id"),
        sa.ForeignKeyConstraint(
            ["user_id", "playlist_sync_id"],
            ["user_playlists.user_id", "user_playlists.sync_id"],
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_user_playlist_songs_user_updated",
        "user_playlist_songs",
        ["user_id", "updated_at"],
        unique=False,
    )

    # ── user_listen_events ────────────────────────────────────────────────
    op.create_table(
        "user_listen_events",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("video_id", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=True),
        sa.Column("uploader", sa.String(length=255), nullable=True),
        sa.Column("duration", sa.Integer(), nullable=True),
        sa.Column("thumbnail", sa.String(length=1024), nullable=True),
        sa.Column("played_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("duration_listened", sa.Integer(), nullable=True),
        sa.Column("completion_pct", sa.Integer(), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_user_listen_events_user_received",
        "user_listen_events",
        ["user_id", "received_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_user_listen_events_user_received", table_name="user_listen_events")
    op.drop_table("user_listen_events")
    op.drop_index("ix_user_playlist_songs_user_updated", table_name="user_playlist_songs")
    op.drop_table("user_playlist_songs")
    op.drop_index("ix_user_playlists_user_updated", table_name="user_playlists")
    op.drop_table("user_playlists")
    op.drop_index("ix_user_favorites_user_updated", table_name="user_favorites")
    op.drop_table("user_favorites")
