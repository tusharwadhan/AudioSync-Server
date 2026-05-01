"""
Alembic environment for SyncAura's Postgres schema.

Runs in async mode against the same DATABASE_URL the FastAPI app uses.
Imports the project's Base so `alembic revision --autogenerate` can detect
model changes.
"""

from __future__ import annotations

import asyncio
import os
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

# Make project modules importable when alembic is invoked from server/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import after sys.path setup so we can pick up Base + models
from db import Base, _normalize_url  # noqa: E402

# Models must be imported (even unused) for autogenerate to see them.
# Add new model imports here as they're added in later phases.
import models  # noqa: F401, E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _resolve_database_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL must be set to run migrations. Export it (e.g. Neon "
            "connection string) before calling alembic upgrade."
        )
    return _normalize_url(url)


def run_migrations_offline() -> None:
    """Generate SQL without a live DB connection. Useful for review."""
    context.configure(
        url=_resolve_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Apply migrations against the live DB."""
    cfg_section = config.get_section(config.config_ini_section, {}) or {}
    cfg_section["sqlalchemy.url"] = _resolve_database_url()

    connectable = async_engine_from_config(
        cfg_section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
