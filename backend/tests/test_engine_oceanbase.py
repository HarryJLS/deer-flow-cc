"""Integration tests for the OceanBase branch of :func:`init_engine`.

These tests require a real OceanBase server (or any MySQL-compatible server
listening on the MySQL wire protocol) plus the ``asyncmy`` driver. They are
marked with ``pytest.mark.oceanbase`` so CI can run them in a dedicated job
that starts an ``oceanbase/oceanbase-ce:4.2.1`` container.

Run locally::

    docker run -d --name ob -p 2881:2881 oceanbase/oceanbase-ce:4.2.1
    cd backend && OCEANBASE_TEST_URL='mysql://root@127.0.0.1:2881/deerflow_test' \\
        PYTHONPATH=. uv run pytest -m oceanbase -v
"""

from __future__ import annotations

import os

import pytest

from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence import engine as engine_mod

pytestmark = pytest.mark.oceanbase

OCEANBASE_TEST_URL = os.environ.get("OCEANBASE_TEST_URL", "")


def _skip_if_unconfigured():
    if not OCEANBASE_TEST_URL:
        pytest.skip("OCEANBASE_TEST_URL not set; cannot run OceanBase integration tests")
    pytest.importorskip("asyncmy", reason="asyncmy not installed; install with --extra oceanbase")


class TestOceanBaseEngine:
    @pytest.mark.anyio
    async def test_init_engine_and_close(self):
        _skip_if_unconfigured()
        cfg = DatabaseConfig(
            backend="oceanbase",
            oceanbase_url=OCEANBASE_TEST_URL,
            pool_size=5,
        )
        try:
            await engine_mod.init_engine_from_config(cfg)
            assert engine_mod.get_engine() is not None
            assert engine_mod.get_session_factory() is not None
        finally:
            await engine_mod.close_engine()
            assert engine_mod.get_engine() is None

    @pytest.mark.anyio
    async def test_auto_create_database(self):
        """Engine init should auto-create a missing database (1049 retry path)."""
        _skip_if_unconfigured()
        # Point at a database name that does not yet exist; engine should
        # CREATE DATABASE IF NOT EXISTS and retry create_all.
        from sqlalchemy.engine.url import make_url

        parsed = make_url(OCEANBASE_TEST_URL)
        synthetic_db = "deerflow_autocreate_test"
        synthetic_url = str(parsed.set(database=synthetic_db).render_as_string(hide_password=False))
        # asyncmy URL needs the +asyncmy driver suffix
        if synthetic_url.startswith("mysql://"):
            synthetic_url = synthetic_url.replace("mysql://", "mysql+asyncmy://", 1)

        cfg = DatabaseConfig(
            backend="oceanbase",
            oceanbase_url=synthetic_url,
        )
        try:
            await engine_mod.init_engine_from_config(cfg)
            assert engine_mod.get_engine() is not None
        finally:
            await engine_mod.close_engine()

    def test_missing_asyncmy_raises_actionable_error(self, monkeypatch):
        """When asyncmy is not installed, init_engine surfaces the install hint."""
        import sys

        # Force ImportError when asyncmy is imported, regardless of whether
        # the extra is installed in the test environment.
        monkeypatch.setitem(sys.modules, "asyncmy", None)

        import asyncio

        async def _run():
            await engine_mod.init_engine(
                "oceanbase",
                url="mysql+asyncmy://u:p@h:2881/db",
            )

        with pytest.raises(ImportError, match="asyncmy is not installed"):
            asyncio.run(_run())
