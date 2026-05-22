"""Alembic environment for DeerFlow application tables.

ONLY manages DeerFlow's tables (runs, threads_meta, cron_jobs, users).
LangGraph's checkpointer tables are managed by LangGraph itself -- they
have their own schema lifecycle and must not be touched by Alembic.
"""

from __future__ import annotations

import asyncio
import logging
from logging.config import fileConfig

from alembic import context
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.base import Base

# Import all models so metadata is populated.
try:
    import deerflow.persistence.models as models  # register ORM models with Base.metadata

    _ = models
except ImportError:
    # Models not available — migration will work with existing metadata only.
    logging.getLogger(__name__).warning("Could not import deerflow.persistence.models; Alembic may not detect all tables")

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _resolve_url() -> str:
    """Resolve the SQLAlchemy URL, preferring DatabaseConfig over alembic.ini.

    Falls back to alembic.ini's ``sqlalchemy.url`` if AppConfig can't be loaded
    (e.g., ``alembic`` invoked from a CLI without the project rooted on
    PYTHONPATH). This keeps both ``make dev`` and direct ``alembic upgrade``
    callable.
    """
    try:
        from deerflow.config import get_app_config

        return get_app_config().database.app_sqlalchemy_url
    except Exception:
        return config.get_main_option("sqlalchemy.url") or ""


def _is_sqlite(url: str) -> bool:
    if not url:
        return False
    try:
        return make_url(url).get_backend_name() == "sqlite"
    except Exception:
        return url.startswith("sqlite")


def run_migrations_offline() -> None:
    url = _resolve_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        render_as_batch=_is_sqlite(url),
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        # Batch mode is only required for SQLite's limited ALTER TABLE — let
        # PostgreSQL / MySQL / OceanBase use native ALTERs which are richer.
        render_as_batch=(connection.dialect.name == "sqlite"),
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    connectable = create_async_engine(_resolve_url())
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
