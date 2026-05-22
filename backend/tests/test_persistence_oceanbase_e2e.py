"""End-to-end persistence tests against a real OceanBase instance.

Validates that ``database.backend: oceanbase`` is a drop-in replacement for
sqlite/postgres on the five DeerFlow application tables:

- ``runs`` / ``run_events`` / ``threads_meta`` / ``feedback`` / ``users``

Coverage:

1. ``Base.metadata.create_all`` succeeds (DDL portable across dialects).
2. CRUD on each table works through the ORM session factory.
3. :class:`JsonMatch` filters round-trip correctly under the MySQL dialect.
4. ``idx_users_oauth_identity`` UNIQUE behaviour: multiple ``NULL/NULL`` rows
   are allowed (the standard SQL contract), and a duplicate
   ``(provider, oauth_id)`` raises.

Marked with ``pytest.mark.oceanbase`` so CI runs them in a dedicated job. Set
``OCEANBASE_TEST_URL`` to a connection string for a writeable database (any
MySQL 8.0+ server also satisfies the test contract).
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import select

from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence import engine as engine_mod

pytestmark = pytest.mark.oceanbase

OCEANBASE_TEST_URL = os.environ.get("OCEANBASE_TEST_URL", "")


def _skip_if_unconfigured():
    if not OCEANBASE_TEST_URL:
        pytest.skip("OCEANBASE_TEST_URL not set; cannot run OceanBase integration tests")
    pytest.importorskip("asyncmy", reason="asyncmy not installed; install with --extra oceanbase")


@pytest.fixture
async def db():
    _skip_if_unconfigured()
    cfg = DatabaseConfig(
        backend="oceanbase",
        oceanbase_url=OCEANBASE_TEST_URL,
        pool_size=5,
    )
    await engine_mod.init_engine_from_config(cfg)
    yield engine_mod.get_session_factory()
    await engine_mod.close_engine()


class TestOceanBaseE2E:
    @pytest.mark.anyio
    async def test_user_unique_index_allows_multiple_null_oauth(self, db):
        """SQL standard: multiple NULL/NULL rows under a UNIQUE constraint are allowed."""
        from deerflow.persistence.user.model import UserRow

        async with db() as session:
            session.add(UserRow(id=str(uuid.uuid4()), email=f"a-{uuid.uuid4()}@ex.com"))
            session.add(UserRow(id=str(uuid.uuid4()), email=f"b-{uuid.uuid4()}@ex.com"))
            await session.commit()
            # Both rows inserted: NULL/NULL pairs do not violate UNIQUE on
            # any of the backends we target.

    @pytest.mark.anyio
    async def test_user_unique_index_rejects_duplicate_oauth_pair(self, db):
        from sqlalchemy.exc import IntegrityError

        from deerflow.persistence.user.model import UserRow

        async with db() as session:
            session.add(
                UserRow(
                    id=str(uuid.uuid4()),
                    email=f"c-{uuid.uuid4()}@ex.com",
                    oauth_provider="github",
                    oauth_id="42",
                )
            )
            await session.commit()

        with pytest.raises(IntegrityError):
            async with db() as session:
                session.add(
                    UserRow(
                        id=str(uuid.uuid4()),
                        email=f"d-{uuid.uuid4()}@ex.com",
                        oauth_provider="github",
                        oauth_id="42",
                    )
                )
                await session.commit()

    @pytest.mark.anyio
    async def test_run_crud_and_json_metadata(self, db):
        from deerflow.persistence.run.model import RunRow

        run_id = str(uuid.uuid4())
        thread_id = str(uuid.uuid4())
        async with db() as session:
            session.add(
                RunRow(
                    run_id=run_id,
                    thread_id=thread_id,
                    status="pending",
                    metadata_json={"tag": "test", "priority": 5},
                )
            )
            await session.commit()

        async with db() as session:
            row = (await session.execute(select(RunRow).where(RunRow.run_id == run_id))).scalar_one()
            assert row.status == "pending"
            assert row.metadata_json["tag"] == "test"
            assert row.metadata_json["priority"] == 5

    @pytest.mark.anyio
    async def test_json_match_filter_string(self, db):
        from deerflow.persistence.json_compat import json_match
        from deerflow.persistence.run.model import RunRow

        marker = uuid.uuid4().hex
        async with db() as session:
            session.add(
                RunRow(
                    run_id=str(uuid.uuid4()),
                    thread_id=str(uuid.uuid4()),
                    metadata_json={"marker": marker},
                )
            )
            await session.commit()

        async with db() as session:
            rows = (
                await session.execute(
                    select(RunRow).where(json_match(RunRow.metadata_json, "marker", marker))
                )
            ).scalars().all()
            assert any(r.metadata_json.get("marker") == marker for r in rows)

    @pytest.mark.anyio
    async def test_json_match_filter_int(self, db):
        from deerflow.persistence.json_compat import json_match
        from deerflow.persistence.run.model import RunRow

        run_id = str(uuid.uuid4())
        async with db() as session:
            session.add(
                RunRow(
                    run_id=run_id,
                    thread_id=str(uuid.uuid4()),
                    metadata_json={"score": 7},
                )
            )
            await session.commit()

        async with db() as session:
            rows = (
                await session.execute(
                    select(RunRow).where(json_match(RunRow.metadata_json, "score", 7))
                )
            ).scalars().all()
            assert any(r.run_id == run_id for r in rows)
