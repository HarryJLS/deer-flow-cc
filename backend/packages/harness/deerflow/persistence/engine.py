"""Async SQLAlchemy engine lifecycle management.

Initializes at Gateway startup, provides session factory for
repositories, disposes at shutdown.

When database.backend="memory", init_engine is a no-op and
get_session_factory() returns None. Repositories must check for
None and fall back to in-memory implementations.
"""

from __future__ import annotations

import json
import logging

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def _json_serializer(obj: object) -> str:
    """JSON serializer with ensure_ascii=False for Chinese character support."""
    return json.dumps(obj, ensure_ascii=False)


logger = logging.getLogger(__name__)

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


async def _auto_create_postgres_db(url: str) -> None:
    """Connect to the ``postgres`` maintenance DB and CREATE DATABASE.

    The target database name is extracted from *url*.  The connection is
    made to the default ``postgres`` database on the same server using
    ``AUTOCOMMIT`` isolation (CREATE DATABASE cannot run inside a
    transaction).
    """
    from sqlalchemy import text
    from sqlalchemy.engine.url import make_url

    parsed = make_url(url)
    db_name = parsed.database
    if not db_name:
        raise ValueError("Cannot auto-create database: no database name in URL")

    # Connect to the default 'postgres' database to issue CREATE DATABASE
    maint_url = parsed.set(database="postgres")
    maint_engine = create_async_engine(maint_url, isolation_level="AUTOCOMMIT")
    try:
        async with maint_engine.connect() as conn:
            await conn.execute(text(f'CREATE DATABASE "{db_name}"'))
        logger.info("Auto-created PostgreSQL database: %s", db_name)
    finally:
        await maint_engine.dispose()


async def _auto_create_mysql_db(url: str) -> None:
    """Connect without a target database and CREATE DATABASE IF NOT EXISTS.

    Mirrors :func:`_auto_create_postgres_db` for OceanBase / MySQL: drops the
    database segment from the URL so the server accepts the connection, then
    issues ``CREATE DATABASE IF NOT EXISTS`` with the project's standard
    charset/collation.
    """
    from sqlalchemy import text
    from sqlalchemy.engine.url import make_url

    parsed = make_url(url)
    db_name = parsed.database
    if not db_name:
        raise ValueError("Cannot auto-create OceanBase database: no database name in URL")

    # SQLAlchemy URL.set() treats None as "don't change" (no-op). Use the
    # underlying _replace() to actually strip the database segment so asyncmy
    # connects to the server without selecting a schema at auth time.
    maint_url = parsed._replace(database=None)
    maint_engine = create_async_engine(maint_url)
    try:
        async with maint_engine.connect() as conn:
            await conn.execute(
                text(
                    f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            )
            await conn.commit()
        logger.info("Auto-created OceanBase database: %s", db_name)
    finally:
        await maint_engine.dispose()


async def init_engine(
    backend: str,
    *,
    url: str = "",
    echo: bool = False,
    pool_size: int = 5,
    sqlite_dir: str = "",
    pool_recycle: int = 3600,
) -> None:
    """Create the async engine and session factory, then auto-create tables.

    Args:
        backend: "memory", "sqlite", "postgres", or "oceanbase".
        url: SQLAlchemy async URL (for sqlite/postgres/oceanbase).
        echo: Echo SQL to log.
        pool_size: Postgres/OceanBase connection pool size.
        sqlite_dir: Directory to create for SQLite (ensured to exist).
        pool_recycle: Seconds before idle connections are recycled
            (oceanbase only; OBProxy cuts idle connections at 7200s).
    """
    global _engine, _session_factory

    if backend == "memory":
        logger.info("Persistence backend=memory -- ORM engine not initialized")
        return

    if backend == "postgres":
        try:
            import asyncpg  # noqa: F401
        except ImportError:
            raise ImportError(
                "database.backend is set to 'postgres' but asyncpg is not installed.\n"
                "Install it with:\n"
                "    cd backend && uv sync --all-packages --extra postgres\n"
                "On the next `make dev` the postgres extra is auto-detected from\n"
                "config.yaml (database.backend: postgres) and reinstalled, so it\n"
                "will not be wiped again. Set UV_EXTRAS=postgres in .env to opt in\n"
                "explicitly. Or switch to backend: sqlite in config.yaml for\n"
                "single-node deployment."
            ) from None

    if backend == "oceanbase":
        try:
            import asyncmy  # noqa: F401
        except ImportError:
            raise ImportError(
                "database.backend is set to 'oceanbase' but asyncmy is not installed.\n"
                "Install it with:\n"
                "    cd backend && uv sync --all-packages --extra oceanbase\n"
                "On the next `make dev` the oceanbase extra is auto-detected from\n"
                "config.yaml (database.backend: oceanbase) and reinstalled, so it\n"
                "will not be wiped again. Set UV_EXTRAS=oceanbase in .env to opt\n"
                "in explicitly. Or switch to backend: sqlite in config.yaml for\n"
                "single-node deployment."
            ) from None

    if backend == "sqlite":
        import os

        from sqlalchemy import event

        os.makedirs(sqlite_dir or ".", exist_ok=True)
        _engine = create_async_engine(url, echo=echo, json_serializer=_json_serializer)

        # Enable WAL on every new connection. SQLite PRAGMA settings are
        # per-connection, so we wire the listener instead of running PRAGMA
        # once at startup. WAL gives concurrent reads + writers without
        # blocking and is the standard recommendation for any production
        # SQLite deployment (TC-UPG-06 in AUTH_TEST_PLAN.md). The companion
        # ``synchronous=NORMAL`` is the safe-and-fast pairing — fsync only
        # at WAL checkpoint boundaries instead of every commit.
        # Note: we do not set PRAGMA busy_timeout here — Python's sqlite3
        # driver already defaults to a 5-second busy timeout (see the
        # ``timeout`` kwarg of ``sqlite3.connect``), and aiosqlite /
        # SQLAlchemy's aiosqlite dialect inherit that default.  Setting
        # it again would be a no-op.
        @event.listens_for(_engine.sync_engine, "connect")
        def _enable_sqlite_wal(dbapi_conn, _record):  # noqa: ARG001 — SQLAlchemy contract
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("PRAGMA journal_mode=WAL;")
                cursor.execute("PRAGMA synchronous=NORMAL;")
                cursor.execute("PRAGMA foreign_keys=ON;")
            finally:
                cursor.close()
    elif backend == "postgres":
        _engine = create_async_engine(
            url,
            echo=echo,
            pool_size=pool_size,
            pool_pre_ping=True,
            json_serializer=_json_serializer,
        )
    elif backend == "oceanbase":
        from sqlalchemy import event

        _engine = create_async_engine(
            url,
            echo=echo,
            pool_size=pool_size,
            pool_pre_ping=True,
            pool_recycle=pool_recycle,
            json_serializer=_json_serializer,
        )

        # OceanBase / MySQL stores DATETIME without timezone info. Pin every
        # session to UTC so naive timestamps round-trip correctly with the
        # application layer which stores datetime.now(UTC). STRICT_TRANS_TABLES
        # turns silent truncation into hard errors, matching the behaviour
        # SQLite (busy_timeout) and Postgres already guarantee.
        @event.listens_for(_engine.sync_engine, "connect")
        def _set_oceanbase_session(dbapi_conn, _record):  # noqa: ARG001 — SQLAlchemy contract
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("SET SESSION time_zone='+00:00'")
                cursor.execute("SET SESSION sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
            finally:
                cursor.close()
    else:
        raise ValueError(f"Unknown persistence backend: {backend!r}")

    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)

    # Auto-create tables (dev convenience). Production should use Alembic.
    from deerflow.persistence.base import Base

    # Import all models so Base.metadata discovers them.
    # When no models exist yet (scaffolding phase), this is a no-op.
    try:
        import deerflow.persistence.models  # noqa: F401
    except ImportError:
        # Models package not yet available — tables won't be auto-created.
        # This is expected during initial scaffolding or minimal installs.
        logger.debug("deerflow.persistence.models not found; skipping auto-create tables")

    try:
        async with _engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except Exception as exc:
        msg = str(exc)
        if backend == "postgres" and "does not exist" in msg:
            # Database not yet created — attempt to auto-create it, then retry.
            await _auto_create_postgres_db(url)
            # Rebuild engine against the now-existing database
            await _engine.dispose()
            _engine = create_async_engine(url, echo=echo, pool_size=pool_size, pool_pre_ping=True, json_serializer=_json_serializer)
            _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
            async with _engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        elif backend == "oceanbase" and ("Unknown database" in msg or "1049" in msg):
            # OceanBase reports missing schema as `Unknown database 'X' (1049)`.
            await _auto_create_mysql_db(url)
            from sqlalchemy import event

            await _engine.dispose()
            _engine = create_async_engine(
                url,
                echo=echo,
                pool_size=pool_size,
                pool_pre_ping=True,
                pool_recycle=pool_recycle,
                json_serializer=_json_serializer,
            )

            @event.listens_for(_engine.sync_engine, "connect")
            def _set_oceanbase_session_retry(dbapi_conn, _record):  # noqa: ARG001
                cursor = dbapi_conn.cursor()
                try:
                    cursor.execute("SET SESSION time_zone='+00:00'")
                    cursor.execute("SET SESSION sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
                finally:
                    cursor.close()

            _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
            async with _engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        else:
            raise

    logger.info("Persistence engine initialized: backend=%s", backend)


async def init_engine_from_config(config) -> None:
    """Convenience: init engine from a DatabaseConfig object."""
    if config.backend == "memory":
        await init_engine("memory")
        return
    await init_engine(
        backend=config.backend,
        url=config.app_sqlalchemy_url,
        echo=config.echo_sql,
        pool_size=config.pool_size,
        sqlite_dir=config.sqlite_dir if config.backend == "sqlite" else "",
        pool_recycle=config.oceanbase_pool_recycle if config.backend == "oceanbase" else 3600,
    )


def get_session_factory() -> async_sessionmaker[AsyncSession] | None:
    """Return the async session factory, or None if backend=memory."""
    return _session_factory


def get_engine() -> AsyncEngine | None:
    """Return the async engine, or None if not initialized."""
    return _engine


async def close_engine() -> None:
    """Dispose the engine, release all connections."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        logger.info("Persistence engine closed")
    _engine = None
    _session_factory = None
