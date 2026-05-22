"""Unit tests for the OceanBase branch of :class:`DatabaseConfig`.

Pure unit tests — no database connection required. Validates:

1. ``backend="oceanbase"`` is accepted by the Pydantic Literal.
2. New fields (``oceanbase_url`` / ``oceanbase_charset`` /
   ``oceanbase_pool_recycle``) have correct defaults.
3. ``app_sqlalchemy_url`` rewrites the ``mysql://`` scheme to add the
   ``+asyncmy`` driver suffix, and is idempotent if the user already supplied
   ``mysql+asyncmy://``.
4. Empty / unset ``oceanbase_url`` is preserved verbatim (the engine layer is
   responsible for the actionable error message).
"""

from __future__ import annotations

import pytest

from deerflow.config.database_config import DatabaseConfig


class TestOceanBaseConfig:
    def test_backend_oceanbase_accepted(self):
        c = DatabaseConfig(backend="oceanbase", oceanbase_url="mysql://u:p@h:2881/db")
        assert c.backend == "oceanbase"

    def test_defaults(self):
        c = DatabaseConfig(backend="oceanbase", oceanbase_url="mysql://u:p@h:2881/db")
        assert c.oceanbase_charset == "utf8mb4"
        assert c.oceanbase_pool_recycle == 3600
        assert c.pool_size == 5

    def test_url_rewrite_adds_asyncmy(self):
        c = DatabaseConfig(
            backend="oceanbase",
            oceanbase_url="mysql://u:p@h:2881/db",
        )
        url = c.app_sqlalchemy_url
        assert url.startswith("mysql+asyncmy://")
        assert "u:p@h:2881/db" in url

    def test_url_rewrite_idempotent(self):
        c = DatabaseConfig(
            backend="oceanbase",
            oceanbase_url="mysql+asyncmy://u:p@h:2881/db",
        )
        url = c.app_sqlalchemy_url
        assert url.count("asyncmy") == 1

    def test_empty_url_passes_through(self):
        # The engine layer surfaces an actionable error; the config layer
        # must not silently inject a default URL.
        c = DatabaseConfig(backend="oceanbase")
        assert c.app_sqlalchemy_url == ""

    def test_custom_pool_recycle(self):
        c = DatabaseConfig(
            backend="oceanbase",
            oceanbase_url="mysql://u:p@h:2881/db",
            oceanbase_pool_recycle=1800,
        )
        assert c.oceanbase_pool_recycle == 1800

    def test_invalid_backend_rejected(self):
        with pytest.raises(ValueError):
            DatabaseConfig(backend="mysql")  # type: ignore[arg-type]

    def test_memory_backend_unchanged(self):
        # Adding oceanbase support must not regress the memory branch.
        c = DatabaseConfig(backend="memory")
        with pytest.raises(ValueError, match="No SQLAlchemy URL"):
            _ = c.app_sqlalchemy_url
