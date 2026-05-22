"""Unit tests for the MySQL/OceanBase branch of :class:`JsonMatch`.

OceanBase identifies itself to SQLAlchemy as the ``mysql`` dialect (it speaks
the MySQL wire protocol), so registration under ``"mysql"`` covers both. Pure
unit tests — no database connection required.

Each case compiles ``JsonMatch(col, key, value)`` against the MySQL dialect and
asserts the resulting SQL string, covering the five value categories that
:func:`json_match` supports: ``None`` / ``bool`` / ``int`` / ``float`` / ``str``.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.dialects import mysql
from sqlalchemy.types import JSON

from deerflow.persistence.json_compat import json_match


@pytest.fixture
def jcol():
    """A standalone JSON column under the MySQL dialect."""
    md = MetaData()
    t = Table("t", md, Column("meta", JSON))
    return t.c.meta


def _render(expr) -> str:
    return str(
        expr.compile(
            dialect=mysql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


class TestJsonMatchMySQL:
    def test_none_compiles_to_json_type_equals_null(self, jcol):
        sql = _render(json_match(jcol, "deleted", None))
        # MySQL JSON_TYPE returns uppercase 'NULL' for JSON null.
        assert "JSON_TYPE(JSON_EXTRACT" in sql
        assert "'NULL'" in sql

    def test_bool_true_compiles_to_boolean_check(self, jcol):
        sql = _render(json_match(jcol, "enabled", True))
        assert "= 'BOOLEAN'" in sql
        assert "= 'true'" in sql

    def test_bool_false(self, jcol):
        sql = _render(json_match(jcol, "enabled", False))
        assert "= 'BOOLEAN'" in sql
        assert "= 'false'" in sql

    def test_int_compiles_to_signed_cast(self, jcol):
        sql = _render(json_match(jcol, "count", 42))
        # MySQL uses SIGNED for integer casts, not BIGINT.
        assert "'INTEGER'" in sql
        assert "CAST(" in sql
        assert "AS SIGNED" in sql
        assert "= 42" in sql

    def test_float_compiles_to_double_cast(self, jcol):
        sql = _render(json_match(jcol, "ratio", 0.5))
        assert "AS DOUBLE" in sql
        assert "= 0.5" in sql

    def test_string_compiles_against_unquoted_extract(self, jcol):
        sql = _render(json_match(jcol, "name", "alice"))
        # JSON_UNQUOTE is needed so the string literal compares against the
        # raw value, not the JSON-quoted form ('"alice"').
        assert "JSON_UNQUOTE(JSON_EXTRACT" in sql
        assert "= 'STRING'" in sql
        assert "= 'alice'" in sql

    def test_path_uses_doubly_quoted_key(self, jcol):
        sql = _render(json_match(jcol, "user_id", "u1"))
        # MySQL JSON path: '$."key"' — the same format used for SQLite.
        assert '$."user_id"' in sql

    def test_int_uses_no_regex_guard(self, jcol):
        sql = _render(json_match(jcol, "count", 7))
        # PG needs the ~ regex guard because json_typeof collapses int/float
        # into "number". MySQL JSON_TYPE distinguishes them natively, so the
        # regex literal '^-?[0-9]+$' should NOT appear in the MySQL output.
        assert "'^-?[0-9]+$'" not in sql

    def test_default_dialect_still_raises(self, jcol):
        # Older code raised NotImplementedError for any dialect other than
        # sqlite/postgresql. The error message was extended to list "mysql"
        # — verify the runtime behaviour kept the raise for unsupported ones.
        from sqlalchemy.dialects import oracle

        expr = json_match(jcol, "x", 1)
        with pytest.raises(NotImplementedError, match="sqlite, postgresql, and mysql"):
            str(expr.compile(dialect=oracle.dialect()))
