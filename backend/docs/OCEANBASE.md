# OceanBase Backend

DeerFlow's persistence layer ships with three reference backends — `memory`,
`sqlite`, `postgres` — and now a fourth: `oceanbase` (MySQL-compatible).
This page explains when to use it, how to configure it, and what to watch out
for in production.

## When to use OceanBase

Choose `oceanbase` when:

- Your organisation already operates an OceanBase cluster and prefers a single
  HA-managed database for everything DeerFlow stores.
- You need horizontal scale beyond what a single-node Postgres can give without
  sharding above the ORM.
- You want MySQL wire-protocol tooling (`mysql` CLI, BI dashboards, etc.).

Stick with `sqlite` for single-node deployments and `postgres` when you
already run a PG instance — both are simpler.

## What is supported today

| Layer | Status | Notes |
|---|---|---|
| Application ORM (5 tables) | **GA** (phase 1) | `runs`, `run_events`, `threads_meta`, `feedback`, `users` |
| LangGraph Checkpointer (2 tables) | **GA** (phase 2) | Self-built `AsyncOceanBaseSaver` in `runtime/checkpointer/oceanbase_saver.py`. Tables: `checkpoints`, `writes`. Async-only — sync callers see `NotImplementedError`. |
| Auto-detected via `UV_EXTRAS` | ✅ | `make dev` reads `database.backend: oceanbase` and adds `--extra oceanbase` automatically |
| Alembic migrations | ✅ | `render_as_batch` is now dialect-aware |

Set `database.backend: oceanbase` once and both layers move together — no need
to mix backends.

## Quick start

1. Lock the cluster at **OceanBase ≥ 4.2.1** so JSON, CTE, window functions,
   and `ON DUPLICATE KEY UPDATE ... AS new` are available.

2. Create the database (the engine can auto-create it on first start too, but
   doing it explicitly avoids the privilege ceremony):

   ```sql
   CREATE DATABASE IF NOT EXISTS deerflow
     CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   ```

3. Put the connection string in `.env`:

   ```bash
   OCEANBASE_URL="mysql://deerflow:secret@ob-proxy:2881/deerflow"
   ```

4. Wire `config.yaml`:

   ```yaml
   database:
     backend: oceanbase
     oceanbase_url: $OCEANBASE_URL
     oceanbase_charset: utf8mb4
     oceanbase_pool_recycle: 3600
     pool_size: 10
   ```

5. Run the bootstrap:

   ```bash
   cd backend && uv sync --all-packages --extra oceanbase
   ```

6. Start: `make dev` from the project root (the extra is now auto-detected on
   every restart by `scripts/detect_uv_extras.py`).

## Configuration reference

| Field | Default | Description |
|---|---|---|
| `oceanbase_url` | `""` | SQLAlchemy URL. `mysql://` is rewritten to `mysql+asyncmy://` automatically. |
| `oceanbase_charset` | `utf8mb4` | Must be `utf8mb4` for full Unicode; OB rejects 4-byte chars on `utf8`. |
| `oceanbase_pool_recycle` | `3600` | Seconds before idle connections are recycled. OBProxy default is `7200`; we recycle at half that to stay clear. |
| `pool_size` | `5` | SQLAlchemy connection pool size. |
| `echo_sql` | `false` | Log every SQL statement. Debug only. |

## How DeerFlow adapts to MySQL

| Concern | Adaptation |
|---|---|
| Driver | `asyncmy` (Cython, async-first). `aiomysql` works at the URL level but is slower and not recommended. |
| Timezone | `DateTime(timezone=True)` compiles to `DATETIME` on MySQL (no TZ). Every connection runs `SET SESSION time_zone='+00:00'`; the app layer stores `datetime.now(UTC)` so timestamps round-trip correctly. |
| SQL mode | `STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION` is set on every connection so silent truncation becomes a hard error. |
| JSON queries | `JsonMatch` emits `JSON_EXTRACT`/`JSON_UNQUOTE`/`JSON_TYPE` on the MySQL dialect (see `persistence/json_compat.py`). |
| Partial unique index | `idx_users_oauth_identity` was simplified to a plain `UNIQUE`. SQL-standard UNIQUE allows multiple `NULL/NULL` rows on all three backends, so the SQLite-only `WHERE` clause was redundant. |
| Alembic | `render_as_batch` is now dialect-aware — SQLite gets batch mode, OceanBase/PG use native `ALTER TABLE`. |
| Auto-create database | `_auto_create_mysql_db()` issues `CREATE DATABASE IF NOT EXISTS ... CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci` when init hits MySQL error 1049. |

## Operations

### Connection pool

- `pool_pre_ping=True` so dead connections are reaped on checkout.
- `pool_recycle=oceanbase_pool_recycle` (default 1h) defeats OBProxy's 2h
  idle cut.
- `pool_size=10` is a reasonable starting point for a single Gateway process.
  Multiply by your replica count.

### `max_allowed_packet`

Phase 2 will store LangGraph checkpoint blobs as `LONGBLOB`. Make sure your
OceanBase tenant raises `max_allowed_packet` to at least 16 MB (the default is
often 4 MB).

### Monitoring

Watch:

- Connection pool water level (SQLAlchemy events, or Prometheus exporter).
- Slow queries — JSON path lookups can be expensive without functional
  indexes.
- BLOB column sizes once checkpoints land on OceanBase (phase 2).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ImportError: asyncmy is not installed` | Extra not installed | `uv sync --all-packages --extra oceanbase` or `UV_EXTRAS=oceanbase` |
| `Unknown database 'X' (1049)` | Database does not exist | Engine auto-creates it on first start; if it loops, check the user has `CREATE` privilege |
| Timestamps off by N hours | Session time_zone not set | Verify your driver supports `SET SESSION time_zone` (asyncmy does) and check the connect-event listener fired |
| `Data too long for column` | `sql_mode` set to STRICT (intentional) | Audit the row — the app should never store oversized data |
| `Connection was killed` after long idle | OBProxy cut the connection | Lower `oceanbase_pool_recycle`; verify `pool_pre_ping` is on |

## Testing

- Unit tests: `backend/tests/test_database_config_oceanbase.py` and
  `backend/tests/test_json_compat_oceanbase.py` run in every CI job and need
  no external dependencies.
- Integration tests: `backend/tests/test_engine_oceanbase.py` and
  `backend/tests/test_persistence_oceanbase_e2e.py` are marked
  `pytest.mark.oceanbase`. Run them with a real container:

  ```bash
  docker run -d --name ob -p 2881:2881 oceanbase/oceanbase-ce:4.2.1
  cd backend && OCEANBASE_TEST_URL='mysql://root@127.0.0.1:2881/deerflow_test' \
    PYTHONPATH=. uv run pytest -m oceanbase -v
  ```

## Roadmap

* Native sync `OceanBaseSaver` (currently async-only).
* Batched `aput_writes` with chunking for very large multi-write turns.
* `vendoring` the upstream langgraph checkpoint test suite to guard against
  protocol drift across langgraph versions.

## Why a self-built Saver?

OceanBase speaks the MySQL wire protocol, not Postgres, so
`langgraph-checkpoint-postgres` cannot connect even in Oracle compatibility
mode. The self-built saver in `runtime/checkpointer/oceanbase_saver.py` mirrors
the SQLite reference saver's 2-table schema and adapts the SQL to MySQL:

* `BLOB` → `LONGBLOB`
* `INSERT OR REPLACE INTO` → `INSERT … ON DUPLICATE KEY UPDATE`
* `INSERT OR IGNORE INTO` → `INSERT IGNORE INTO`
* `json_extract(CAST(metadata AS TEXT), '$.k')` →
  `JSON_UNQUOTE(JSON_EXTRACT(CONVERT(metadata USING utf8mb4), '$."k"'))`

The DDL lives in `runtime/checkpointer/_oceanbase_schema.sql` and is applied
lazily on the first call to `setup()`.
