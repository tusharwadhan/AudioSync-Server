"""
Postgres data layer for SyncAura.

Exposes an async SQLAlchemy engine + session factory backed by Neon (or any
Postgres-compatible host). Reads connection details from the DATABASE_URL
environment variable. The same engine is reused across the FastAPI process;
sessions are short-lived and scoped per-request via the get_session()
dependency.

Tables (and migrations) are managed by Alembic — see alembic/ in this dir.
This file owns only the connection plumbing and SQLAlchemy declarative base.
"""

from __future__ import annotations

import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


# Connection string. Format expected:
#   postgresql+asyncpg://user:pass@host/dbname?ssl=require
# Neon's default UI shows a `postgresql://` URL — we rewrite it on read so the
# operator can paste either form into their env without thinking about it.
_DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def _normalize_url(url: str) -> str:
    """Convert a libpq-style URL to the SQLAlchemy + asyncpg flavor."""
    if not url:
        return url
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and "+asyncpg" not in url:
        url = "postgresql+asyncpg://" + url[len("postgresql://"):]
    # asyncpg's TLS arg is `ssl`, not `sslmode`. Translate if present.
    if "sslmode=require" in url:
        url = url.replace("sslmode=require", "ssl=require")
    # `channel_binding` is a libpq-only parameter; asyncpg rejects unknown DSN
    # keys at parse time. Strip it — channel binding is still negotiated at the
    # SCRAM-SHA-256-PLUS layer when both ends support it (Neon does), so the
    # security property is preserved.
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

    parts = urlsplit(url)
    if parts.query:
        kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
                if k != "channel_binding"]
        url = urlunsplit(parts._replace(query=urlencode(kept)))
    return url


_NORMALIZED_URL = _normalize_url(_DATABASE_URL)


class Base(DeclarativeBase):
    """Common declarative base for all ORM models in the project."""
    pass


# Lazy singletons. We don't construct the engine at import time so the
# server can still boot in environments where DATABASE_URL isn't set yet
# (e.g. local dev without Postgres) — endpoints that touch the DB will
# raise a clear error instead.
_engine = None
_SessionLocal: async_sessionmaker[AsyncSession] | None = None


def _ensure_engine():
    global _engine, _SessionLocal
    if _engine is not None:
        return
    if not _NORMALIZED_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. Configure it (e.g. Neon connection string) "
            "before calling any endpoint that touches Postgres."
        )
    _engine = create_async_engine(
        _NORMALIZED_URL,
        # Neon suspends idle connections; pool_pre_ping verifies a connection
        # is alive before handing it out, transparently reconnecting if not.
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        echo=False,
    )
    _SessionLocal = async_sessionmaker(_engine, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency. Yields a per-request DB session."""
    _ensure_engine()
    assert _SessionLocal is not None
    async with _SessionLocal() as session:
        yield session


def is_configured() -> bool:
    """True if DATABASE_URL is set. Useful for /health-style checks."""
    return bool(_NORMALIZED_URL)
