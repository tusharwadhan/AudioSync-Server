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
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
)
# Aliased to avoid shadowing inside class bodies — DmMessage and
# LoungeMessage both declare a `text` column, so referring to the SQL
# expression helper as `text(...)` inside their __table_args__ would
# resolve to the column's MappedColumn (Python class-body scoping) and
# blow up at import time.
from sqlalchemy import text as sql_text
from sqlalchemy.dialects.postgresql import JSONB
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
    # Flat key→value blob of the user's app settings (auto-download
    # toggle/threshold, wifi-only, client-extraction toggle, hum
    # on/off + consent, onboarding/prompt flags, display name). The
    # client owns the schema; the server just stores it. Conflict
    # resolution is last-write-wins on the embedded "_updated_at" key
    # (unix ms) — a push with an older "_updated_at" than what's stored
    # is ignored. Defaults to {} for users created before this column.
    settings_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

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


class UserDownload(Base):
    """Bookkeeping for a song the user has downloaded for offline play.

    The audio blob itself is NOT uploaded — only the metadata + flags, so
    a fresh install can show "you have N downloads in your account; tap to
    re-download" (the re-download flow is a later client feature). Same
    tombstone / last-write-wins conventions as user_favorites.

    Deliberately omitted vs. the client's `downloaded_songs` table:
      * `localPath` — device-specific, meaningless on another device
      * `downloadStatus` / `fileSize` are advisory; the client re-derives
        status locally (it's "remote / not downloaded here" after a pull)
    """
    __tablename__ = "user_downloads"

    user_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    video_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    uploader: Mapped[str | None] = mapped_column(String(255), nullable=True)
    duration: Mapped[int | None] = mapped_column(Integer, nullable=True)
    thumbnail: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    file_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    is_auto_downloaded: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    downloaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("ix_user_downloads_user_updated", "user_id", "updated_at"),
    )


# ─────────────────────────────────────────────────────────────────────
# Social v1 — lounge, DMs, friendships
# ─────────────────────────────────────────────────────────────────────


def _ordered_pair(uid1: str, uid2: str) -> tuple[str, str]:
    """Return (uid_a, uid_b) sorted lex-ascending — the canonical pair
    ordering used by friendships and dm_thread_state."""
    return (uid1, uid2) if uid1 < uid2 else (uid2, uid1)


class Friendship(Base):
    """Two users who have crossed the DM stranger-gate.

    Created the moment either side taps Accept on a pending request —
    a single Accept is enough; mutual acceptance is not required. Once
    a row exists, subsequent dm_sends in either direction bypass the
    gate and land directly in the recipient's thread.

    `uid_a` is always lexicographically smaller than `uid_b` (CHECK
    constraint at the DB level) so we never get two rows for the same
    pair.
    """
    __tablename__ = "friendships"

    uid_a: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    uid_b: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    formed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (
        CheckConstraint("uid_a < uid_b", name="ck_friendships_uid_order"),
        Index("ix_friendships_uid_a", "uid_a"),
        Index("ix_friendships_uid_b", "uid_b"),
    )


class DmThreadState(Base):
    """Per-pair stranger-gate state for DMs.

    States:
      * 'pending_from_a' — uid_a sent first; uid_b has not yet acted.
      * 'pending_from_b' — uid_b sent first; uid_a has not yet acted.
      * 'accepted'       — either side accepted; a Friendship row also
                            exists. New messages flow as normal DMs.
      * 'declined_by_a'  — uid_a tapped Decline. Sender (uid_b) never
                            sees a read/delivered signal; their messages
                            sit in unread state forever from their side,
                            while uid_a's view only ever shows the
                            latest unread declined message.
      * 'declined_by_b'  — mirror of declined_by_a.

    The pair (uid_a, uid_b) is always stored with uid_a < uid_b.
    """
    __tablename__ = "dm_thread_state"

    uid_a: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    uid_b: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (
        CheckConstraint("uid_a < uid_b", name="ck_dm_thread_uid_order"),
    )


class DmMessage(Base):
    """A single direct message between two users.

    Lifecycle:
      1. Insert with `read_at = NULL`. Server pushes to recipient over
         WS if connected, FCM otherwise.
      2. Recipient opens the thread → client sends `dm_read` → server
         sets `read_at = now()`.
      3. Periodic prune deletes rows where read_at is set + at least
         one hour has passed.

    Either `text`, `np_video_id`, or `share_moment` must be set
    (CHECK constraint) — a DM is text, a now-playing share card,
    a share-moment card, or any combination.

    Chat-parity fields (added in migration 0005):
      * reactions {emoji -> [uid, ...]} — matches room chat shape
      * reply_to_message_id — soft reference (no FK; replied-to may
        have been pruned, client renders a placeholder in that case)
      * edited_at — set when the sender edits their text
      * deleted — tombstone; row still rendered as "Message deleted"
      * share_moment — JSONB payload for share-moment cards
    """
    __tablename__ = "dm_messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    from_uid: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    to_uid: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    np_video_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    np_title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    np_artist: Mapped[str | None] = mapped_column(String(255), nullable=True)
    np_thumbnail: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    read_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    reactions: Mapped[dict] = mapped_column(
        JSONB, default=dict, server_default=sql_text("'{}'::jsonb"), nullable=False
    )
    reply_to_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    edited_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=sql_text("false"), nullable=False
    )
    share_moment: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "text IS NOT NULL OR np_video_id IS NOT NULL OR share_moment IS NOT NULL",
            name="ck_dm_messages_has_content",
        ),
        # Partial indexes on unread rows — hot path for snapshot / WS
        # reconnect catch-up.
        Index(
            "ix_dm_messages_to_unread",
            "to_uid",
            "sent_at",
            postgresql_where=sql_text("read_at IS NULL"),
        ),
        Index(
            "ix_dm_messages_from_unread",
            "from_uid",
            "sent_at",
            postgresql_where=sql_text("read_at IS NULL"),
        ),
    )


class LoungeMessage(Base):
    """A message posted in the global lounge.

    `from_name` / `from_avatar_url` are snapshots at send-time so old
    messages keep their original identity even if the user later
    renames or signs out of Google. Retention: a periodic prune task
    keeps only the most recent 200 rows.

    Either `text` or `np_video_id` must be set — a lounge post is
    either chat text, a now-playing share card, or both.
    """
    __tablename__ = "lounge_messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    from_uid: Mapped[str] = mapped_column(
        String(128), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    from_name: Mapped[str] = mapped_column(String(255), nullable=False)
    from_avatar_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    np_video_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    np_title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    np_artist: Mapped[str | None] = mapped_column(String(255), nullable=True)
    np_thumbnail: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "text IS NOT NULL OR np_video_id IS NOT NULL",
            name="ck_lounge_messages_has_content",
        ),
        # Newest-first read pattern for the snapshot's "last 200".
        Index("ix_lounge_messages_sent_at_desc", sql_text("sent_at DESC")),
    )
