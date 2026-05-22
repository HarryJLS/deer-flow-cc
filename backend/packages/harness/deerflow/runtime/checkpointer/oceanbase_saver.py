"""Async LangGraph checkpoint saver backed by OceanBase / MySQL.

OceanBase implements the MySQL wire protocol, so ``langgraph-checkpoint-postgres``
cannot connect (it speaks the Postgres wire). This module mirrors the schema and
semantics of the reference ``langgraph.checkpoint.sqlite.aio.AsyncSqliteSaver``
but adapts the SQL to MySQL syntax:

* ``BLOB`` -> ``LONGBLOB`` (DDL in ``_oceanbase_schema.sql``)
* ``INSERT OR REPLACE INTO`` -> ``INSERT ... ON DUPLICATE KEY UPDATE``
* ``INSERT OR IGNORE INTO`` -> ``INSERT IGNORE INTO``
* ``json_extract(CAST(metadata AS TEXT), '$.k')`` ->
  ``JSON_UNQUOTE(JSON_EXTRACT(CONVERT(metadata USING utf8mb4), '$."k"'))``

A single ``asyncmy.Pool`` backs all queries; the lifecycle is managed by
:meth:`AsyncOceanBaseSaver.from_conn_string` so callers can use the standard
``async with`` pattern. There is no global mutex like the SQLite saver --
MySQL supports concurrent writes natively, and our SQL uses primary-key UPSERT
semantics so concurrent ``aput`` calls for the same checkpoint converge.
"""

from __future__ import annotations

import json
import logging
import random
import re
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    SerializerProtocol,
    get_checkpoint_id,
    get_checkpoint_metadata,
)
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

logger = logging.getLogger(__name__)

_FILTER_KEY_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")


def _validate_filter_key(key: str) -> None:
    if not _FILTER_KEY_RE.match(key):
        raise ValueError(
            f"Invalid filter key: {key!r}. Only [A-Za-z0-9_.-] allowed (used "
            "literally inside the compiled SQL path expression)."
        )


def _parse_mysql_url(url: str) -> dict[str, Any]:
    """Extract asyncmy.connect() kwargs from a SQLAlchemy-style URL.

    Accepts ``mysql://`` and ``mysql+asyncmy://`` (the latter is what
    SQLAlchemy renders for us, but we own the saver connection separately
    from the ORM engine).
    """
    from sqlalchemy.engine.url import make_url

    parsed = make_url(url)
    if parsed.get_backend_name() != "mysql":
        raise ValueError(
            f"AsyncOceanBaseSaver requires a mysql:// URL; got driver={parsed.drivername!r}"
        )
    kwargs: dict[str, Any] = {
        "host": parsed.host or "127.0.0.1",
        "port": parsed.port or 2881,
        "user": parsed.username or "root",
        "password": parsed.password or "",
        "db": parsed.database or None,
    }
    return kwargs


class AsyncOceanBaseSaver(BaseCheckpointSaver[str]):
    """Async checkpoint saver backed by an OceanBase (MySQL-protocol) database.

    Schema lives in ``_oceanbase_schema.sql`` and is created lazily on the first
    call to :meth:`setup` (also called automatically by every public method).

    See module docstring for the PG/SQLite -> MySQL adaptation notes.
    """

    def __init__(
        self,
        pool,
        *,
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.jsonplus_serde = JsonPlusSerializer()
        self.pool = pool
        self._is_setup = False

    @classmethod
    @asynccontextmanager
    async def from_conn_string(cls, conn_string: str) -> AsyncIterator[AsyncOceanBaseSaver]:
        """Open an asyncmy pool and yield a configured saver.

        ``conn_string`` may be either ``mysql://...`` or
        ``mysql+asyncmy://...``; the driver suffix is stripped.
        """
        try:
            import asyncmy
        except ImportError as exc:
            raise ImportError(
                "AsyncOceanBaseSaver requires the asyncmy driver. Install with:\n"
                "    cd backend && uv sync --all-packages --extra oceanbase"
            ) from exc

        kwargs = _parse_mysql_url(conn_string)
        pool = await asyncmy.create_pool(
            minsize=1,
            maxsize=5,
            autocommit=True,
            charset="utf8mb4",
            **kwargs,
        )
        try:
            saver = cls(pool)
            yield saver
        finally:
            pool.close()
            await pool.wait_closed()

    async def setup(self) -> None:
        """Create checkpoint tables if they do not exist.

        Reads ``_oceanbase_schema.sql`` and executes each statement. Safe to
        call repeatedly -- the DDL uses ``CREATE TABLE IF NOT EXISTS``.
        """
        if self._is_setup:
            return
        from pathlib import Path

        schema_path = Path(__file__).with_name("_oceanbase_schema.sql")
        ddl_script = schema_path.read_text(encoding="utf-8")

        # Strip ``-- ...`` line comments before splitting on ``;`` -- asyncmy
        # sends each statement verbatim and chokes on leading comment text.
        clean = "\n".join(
            line for line in ddl_script.splitlines() if not line.lstrip().startswith("--")
        )
        statements = [s.strip() for s in clean.split(";") if s.strip()]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for stmt in statements:
                    await cur.execute(stmt)

        self._is_setup = True

    # ------------------------------------------------------------------
    # Read APIs
    # ------------------------------------------------------------------

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        await self.setup()
        thread_id = str(config["configurable"]["thread_id"])
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        ckpt_id = get_checkpoint_id(config)

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                if ckpt_id:
                    await cur.execute(
                        "SELECT thread_id, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata "
                        "FROM checkpoints WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id = %s",
                        (thread_id, checkpoint_ns, ckpt_id),
                    )
                else:
                    await cur.execute(
                        "SELECT thread_id, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata "
                        "FROM checkpoints WHERE thread_id = %s AND checkpoint_ns = %s "
                        "ORDER BY checkpoint_id DESC LIMIT 1",
                        (thread_id, checkpoint_ns),
                    )
                row = await cur.fetchone()
                if row is None:
                    return None

                (
                    thread_id_db,
                    checkpoint_id_db,
                    parent_checkpoint_id,
                    type_,
                    checkpoint_blob,
                    metadata_blob,
                ) = row
                if not ckpt_id:
                    config = {
                        "configurable": {
                            "thread_id": thread_id_db,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint_id_db,
                        }
                    }

                # Pending writes for the same checkpoint.
                await cur.execute(
                    "SELECT task_id, channel, type, value FROM writes "
                    "WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id = %s "
                    "ORDER BY task_id, idx",
                    (thread_id_db, checkpoint_ns, str(config["configurable"]["checkpoint_id"])),
                )
                write_rows = await cur.fetchall()

        pending = [
            (task_id, channel, self.serde.loads_typed((wtype, value)))
            for (task_id, channel, wtype, value) in write_rows
        ]
        return CheckpointTuple(
            config,
            self.serde.loads_typed((type_, _coerce_blob(checkpoint_blob))),
            cast(
                CheckpointMetadata,
                json.loads(metadata_blob) if metadata_blob is not None else {},
            ),
            (
                {
                    "configurable": {
                        "thread_id": thread_id_db,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": parent_checkpoint_id,
                    }
                }
                if parent_checkpoint_id
                else None
            ),
            pending,
        )

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        await self.setup()
        where_sql, params = _build_search_where(config, filter, before)
        query = (
            "SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata "
            "FROM checkpoints " + where_sql + " ORDER BY checkpoint_id DESC"
        )
        if limit is not None:
            query += " LIMIT %s"
            params = (*params, int(limit))

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()

                for (
                    thread_id,
                    checkpoint_ns,
                    checkpoint_id,
                    parent_checkpoint_id,
                    type_,
                    checkpoint_blob,
                    metadata_blob,
                ) in rows:
                    await cur.execute(
                        "SELECT task_id, channel, type, value FROM writes "
                        "WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id = %s "
                        "ORDER BY task_id, idx",
                        (thread_id, checkpoint_ns, checkpoint_id),
                    )
                    write_rows = await cur.fetchall()
                    yield CheckpointTuple(
                        {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                            }
                        },
                        self.serde.loads_typed((type_, _coerce_blob(checkpoint_blob))),
                        cast(
                            CheckpointMetadata,
                            json.loads(metadata_blob) if metadata_blob is not None else {},
                        ),
                        (
                            {
                                "configurable": {
                                    "thread_id": thread_id,
                                    "checkpoint_ns": checkpoint_ns,
                                    "checkpoint_id": parent_checkpoint_id,
                                }
                            }
                            if parent_checkpoint_id
                            else None
                        ),
                        [
                            (task_id, channel, self.serde.loads_typed((wtype, value)))
                            for (task_id, channel, wtype, value) in write_rows
                        ],
                    )

    # ------------------------------------------------------------------
    # Write APIs
    # ------------------------------------------------------------------

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        await self.setup()
        thread_id = str(config["configurable"]["thread_id"])
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        type_, serialized_checkpoint = self.serde.dumps_typed(checkpoint)
        serialized_metadata = json.dumps(
            get_checkpoint_metadata(config, metadata), ensure_ascii=False
        ).encode("utf-8", "ignore")

        sql = (
            "INSERT INTO checkpoints "
            "(thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "parent_checkpoint_id = VALUES(parent_checkpoint_id), "
            "type = VALUES(type), "
            "checkpoint = VALUES(checkpoint), "
            "metadata = VALUES(metadata)"
        )
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql,
                    (
                        thread_id,
                        checkpoint_ns,
                        checkpoint["id"],
                        config["configurable"].get("checkpoint_id"),
                        type_,
                        serialized_checkpoint,
                        serialized_metadata,
                    ),
                )

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await self.setup()
        # Special-write channels (ERROR / SCHEDULED / INTERRUPT / RESUME) use
        # negative idx slots and should overwrite; regular writes are
        # append-once (first-write-wins on the composite key).
        if all(w[0] in WRITES_IDX_MAP for w in writes):
            sql = (
                "INSERT INTO writes "
                "(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE "
                "channel = VALUES(channel), type = VALUES(type), value = VALUES(value)"
            )
        else:
            sql = (
                "INSERT IGNORE INTO writes "
                "(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            )

        params = [
            (
                str(config["configurable"]["thread_id"]),
                str(config["configurable"]["checkpoint_ns"]),
                str(config["configurable"]["checkpoint_id"]),
                task_id,
                WRITES_IDX_MAP.get(channel, idx),
                channel,
                *self.serde.dumps_typed(value),
            )
            for idx, (channel, value) in enumerate(writes)
        ]
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, params)

    async def adelete_thread(self, thread_id: str) -> None:
        await self.setup()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM checkpoints WHERE thread_id = %s", (str(thread_id),))
                await cur.execute("DELETE FROM writes WHERE thread_id = %s", (str(thread_id),))

    # ------------------------------------------------------------------
    # Version helper (string version compatible with SQLite saver)
    # ------------------------------------------------------------------

    def get_next_version(self, current: str | None, channel: None = None) -> str:
        """Same monotonic ``"{int:032}.{rand:016}"`` format as the SQLite saver."""
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"


def _coerce_blob(value: Any) -> bytes:
    """Normalise the various BLOB-like types asyncmy can hand back to bytes.

    asyncmy returns ``bytes`` for LONGBLOB on most rows but can occasionally
    return ``bytearray`` or ``memoryview``. The serde expects ``bytes``.
    """
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(f"unexpected blob type from asyncmy: {type(value).__name__}")


def _metadata_predicate(metadata_filter: dict[str, Any]) -> tuple[list[str], list[Any]]:
    """Return ``(predicates, params)`` for the WHERE clause of a metadata filter.

    Mirrors langgraph's SQLite saver in spirit (extract scalar value, compare
    with the bind parameter), but uses the MySQL JSON function family and
    runs ``CONVERT(... USING utf8mb4)`` against the raw BLOB column.
    """
    predicates: list[str] = []
    params: list[Any] = []

    for query_key, query_value in metadata_filter.items():
        _validate_filter_key(query_key)
        path = f"$.\"{query_key}\""
        extract = f"JSON_UNQUOTE(JSON_EXTRACT(CONVERT(metadata USING utf8mb4), '{path}'))"

        if query_value is None:
            predicates.append(f"({extract} IS NULL OR {extract} = 'null')")
            continue
        if isinstance(query_value, bool):
            predicates.append(f"{extract} = %s")
            params.append("true" if query_value else "false")
            continue
        if isinstance(query_value, (int, float)):
            predicates.append(f"{extract} = %s")
            params.append(str(query_value))
            continue
        if isinstance(query_value, str):
            predicates.append(f"{extract} = %s")
            params.append(query_value)
            continue
        # dict / list / other: serialise compactly (matches the SQLite saver's
        # behaviour where it serialises with no spaces so it round-trips with
        # json_extract output).
        predicates.append(f"{extract} = %s")
        params.append(json.dumps(query_value, separators=(",", ":"), ensure_ascii=False))

    return predicates, params


def _build_search_where(
    config: RunnableConfig | None,
    filter: dict[str, Any] | None,
    before: RunnableConfig | None,
) -> tuple[str, tuple[Any, ...]]:
    wheres: list[str] = []
    params: list[Any] = []

    if config is not None:
        wheres.append("thread_id = %s")
        params.append(str(config["configurable"]["thread_id"]))
        ns = config["configurable"].get("checkpoint_ns")
        if ns is not None:
            wheres.append("checkpoint_ns = %s")
            params.append(ns)
        ckpt_id = get_checkpoint_id(config)
        if ckpt_id:
            wheres.append("checkpoint_id = %s")
            params.append(ckpt_id)

    if filter:
        metadata_predicates, metadata_values = _metadata_predicate(filter)
        wheres.extend(metadata_predicates)
        params.extend(metadata_values)

    if before is not None:
        wheres.append("checkpoint_id < %s")
        params.append(get_checkpoint_id(before))

    return ("WHERE " + " AND ".join(wheres) if wheres else ""), tuple(params)
