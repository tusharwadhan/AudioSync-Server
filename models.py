"""
SQLAlchemy ORM models for the SyncAura Postgres data layer.

All models share the declarative Base from db.py so Alembic's
autogenerate picks up new tables and column changes consistently.

Phase 1: User. Later phases add user_favorites, user_playlists,
user_playlist_songs, user_listen_history, room_sessions.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import BigInteger, Boolean, Column, DateTime, String
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
