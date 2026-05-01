"""
SQLAlchemy ORM models for the SyncAura Postgres data layer.

All models share the declarative Base from db.py so Alembic's
autogenerate picks up new tables and column changes consistently.

Phase 1: User.
Phase 2-4: UserFavorite, UserPlaylist, UserPlaylistSong, UserListenEvent.

Sync model conventions used across the Phase 2-4 tables:
  * `updated_at` is server-stamped on every upsert. Clients pull rows where
    `updated_at > last_sync` to incrementally hydrate.
  * `deleted_at` (nullable) is the tombstone — a row stays in the table
    after a delete so the deletion can replicate to other devices on
    their next pull. Clients prune locally when they see a tombstone.
  * Last-write-wins by `updated_at`. Server is authority on this clock.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from db import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class User(Base):
    """A signed-in user identified by Firebase UID.

    Created/updated on every successful /auth/sync call. The Firebase token
    is the source of truth for identity — this row just lets us join the
    user against their synced data (favorites, playlists, history) and
    track when we last saw them.
    """
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)  # Firebase UID
    email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    photo_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email}>"


class UserFavorite(Base):
    """A song the user has marked favorite, replicated from any device.

    Song metadata (title/uploader/etc.) is denormalized so a fresh install
    can rebuild the favorites list without first having to look up each
    videoId against the catalog. Tombstoned via `deleted_at`.
    """
    __tablename__ = "user_favorites"

    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    video_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    uploader: Mapped[str | None] = mapped_column(String(255), nullable=True)
    duration: Mapped[int | None] = mapped_column(Integer, nullable=True)
    thumbnail: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    favorited_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("ix_user_favorites_user_updated", "user_id", "updated_at"),
    )


class UserPlaylist(Base):
    """A user-owned playlist, identified by a client-generated UUID.

    Local Room IDs are autoincrement Longs which aren't portable across
    devices, so each playlist also carries a `sync_id` (UUID generated on
    the client at creation time). The pair `(user_id, sync_id)` is the
    cross-device identity.

    `auto_backup_enabled` mirrors the per-playlist offline-backup toggle
    so the setting follows the user across devices.
    """
    __tablename__ = "user_playlists"

    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    sync_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    auto_backup_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("ix_user_playlists_user_updated", "user_id", "updated_at"),
    )


class UserPlaylistSong(Base):
    """A song inside a user playlist, with cross-device position ordering.

    Composite PK is `(user_id, playlist_sync_id, video_id)`. Song metadata
    is denormalized for the same reason as UserFavorite. `position` is a
    sparse integer; reorders rewrite individual rows rather than the whole
    list.
    """
    __tablename__ = "user_playlist_songs"

    user_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    playlist_sync_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    video_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    uploader: Mapped[str | None] = mapped_column(String(255), nullable=True)
    duration: Mapped[int | None] = mapped_column(Integer, nullable=True)
    thumbnail: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        # FK to (user_id, playlist_sync_id) on user_playlists. Cascade so
        # deleting a playlist also tombstones its songs server-side.
        ForeignKeyConstraint(
            ["user_id", "playlist_sync_id"],
            ["user_playlists.user_id", "user_playlists.sync_id"],
            ondelete="CASCADE",
        ),
        Index(
            "ix_user_playlist_songs_user_updated", "user_id", "updated_at"
        ),
    )


class UserListenEvent(Base):
    """Append-only log of every play event on every signed-in device.

    Unlike favorites / playlists, this table has no tombstones — events
    are immutable history. Clients write events in batches; server stamps
    `received_at` so out-of-order arrivals from offline-then-online clients
    are still queryable in a stable order.

    `played_at` is the client-reported wall clock (when the user actually
    pressed play). `received_at` is when the server saw the row.
    """
    __tablename__ = "user_listen_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    video_id: Mapped[str] = mapped_column(String(32), nullable=False)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    uploader: Mapped[str | None] = mapped_column(String(255), nullable=True)
    duration: Mapped[int | None] = mapped_column(Integer, nullable=True)
    thumbnail: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    played_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    duration_listened: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completion_pct: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source: Mapped[str | None] = mapped_column(String(32), nullable=True)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (
        Index("ix_user_listen_events_user_received", "user_id", "received_at"),
    )
