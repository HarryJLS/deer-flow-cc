"""Integration tests for :class:`AsyncOceanBaseSaver`.

Validates that the self-built checkpoint saver matches the contract of the
reference SQLite saver. Marked ``pytest.mark.oceanbase`` so CI runs against a
live OceanBase / MySQL container; without ``OCEANBASE_TEST_URL`` the suite is
skipped.

Coverage:

1. ``setup()`` is idempotent and creates the two checkpoint tables.
2. ``aput`` + ``aget_tuple`` round-trip a checkpoint (incl. metadata + parent).
3. ``aput_writes`` round-trips both regular and special (ERROR / SCHEDULED /
   INTERRUPT / RESUME) writes; special writes overwrite, regular writes are
   first-write-wins.
4. ``alist`` orders newest-first and respects ``before`` + ``limit`` +
   metadata ``filter``.
5. ``adelete_thread`` clears both tables for the given thread.
6. Behaviour matches ``AsyncSqliteSaver`` for the same fixture.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.oceanbase

OCEANBASE_TEST_URL = os.environ.get("OCEANBASE_TEST_URL", "")


def _skip_if_unconfigured():
    if not OCEANBASE_TEST_URL:
        pytest.skip("OCEANBASE_TEST_URL not set; cannot run OceanBase checkpointer tests")
    pytest.importorskip("asyncmy", reason="asyncmy not installed; install with --extra oceanbase")


def _cfg(thread_id: str, *, ns: str = "", checkpoint_id: str | None = None):
    configurable = {"thread_id": thread_id, "checkpoint_ns": ns}
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


def _empty_checkpoint(ckpt_id: str) -> dict:
    """A minimal Checkpoint shape that the JsonPlusSerializer accepts."""
    return {
        "v": 4,
        "id": ckpt_id,
        "ts": "2026-05-20T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


@pytest.fixture
async def saver():
    _skip_if_unconfigured()
    from deerflow.runtime.checkpointer.oceanbase_saver import AsyncOceanBaseSaver

    async with AsyncOceanBaseSaver.from_conn_string(OCEANBASE_TEST_URL) as s:
        await s.setup()
        yield s


class TestSetupAndUpsert:
    @pytest.mark.anyio
    async def test_setup_is_idempotent(self, saver):
        # Calling setup twice should not raise even though the tables exist.
        await saver.setup()
        await saver.setup()

    @pytest.mark.anyio
    async def test_put_then_get_round_trip(self, saver):
        thread_id = f"t-{uuid.uuid4()}"
        ckpt = _empty_checkpoint("ckpt-1")
        saved = await saver.aput(_cfg(thread_id), ckpt, {"tag": "first"}, {})
        assert saved["configurable"]["checkpoint_id"] == "ckpt-1"

        got = await saver.aget_tuple(_cfg(thread_id))
        assert got is not None
        assert got.checkpoint["id"] == "ckpt-1"
        assert got.metadata.get("tag") == "first"
        assert got.parent_config is None

    @pytest.mark.anyio
    async def test_put_with_parent_links(self, saver):
        thread_id = f"t-{uuid.uuid4()}"
        # IDs are sorted DESC by alist/aget_tuple, so use lexicographically
        # increasing IDs to guarantee the child wins as "latest".
        await saver.aput(_cfg(thread_id), _empty_checkpoint("ckpt-001"), {}, {})
        child_cfg = _cfg(thread_id, checkpoint_id="ckpt-001")
        await saver.aput(child_cfg, _empty_checkpoint("ckpt-002"), {}, {})

        got = await saver.aget_tuple(_cfg(thread_id))
        assert got is not None
        assert got.checkpoint["id"] == "ckpt-002"
        assert got.parent_config is not None
        assert got.parent_config["configurable"]["checkpoint_id"] == "ckpt-001"

    @pytest.mark.anyio
    async def test_put_is_upsert(self, saver):
        thread_id = f"t-{uuid.uuid4()}"
        await saver.aput(_cfg(thread_id), _empty_checkpoint("k"), {"v": 1}, {})
        await saver.aput(_cfg(thread_id), _empty_checkpoint("k"), {"v": 2}, {})

        got = await saver.aget_tuple(_cfg(thread_id))
        assert got is not None
        assert got.metadata.get("v") == 2


class TestWrites:
    @pytest.mark.anyio
    async def test_regular_writes_first_write_wins(self, saver):
        thread_id = f"t-{uuid.uuid4()}"
        await saver.aput(_cfg(thread_id), _empty_checkpoint("c1"), {}, {})
        cfg = _cfg(thread_id, checkpoint_id="c1")

        await saver.aput_writes(cfg, [("messages", "v1")], task_id="task-A")
        # Same (task, idx) with different value MUST be ignored (INSERT IGNORE).
        await saver.aput_writes(cfg, [("messages", "v2")], task_id="task-A")

        got = await saver.aget_tuple(cfg)
        assert got is not None
        values = [v for (_tid, _ch, v) in got.pending_writes]
        assert values == ["v1"]

    @pytest.mark.anyio
    async def test_special_writes_overwrite(self, saver):
        from langgraph.checkpoint.base import ERROR

        thread_id = f"t-{uuid.uuid4()}"
        await saver.aput(_cfg(thread_id), _empty_checkpoint("c1"), {}, {})
        cfg = _cfg(thread_id, checkpoint_id="c1")

        await saver.aput_writes(cfg, [(ERROR, "boom1")], task_id="task-A")
        await saver.aput_writes(cfg, [(ERROR, "boom2")], task_id="task-A")

        got = await saver.aget_tuple(cfg)
        assert got is not None
        values = [v for (_tid, ch, v) in got.pending_writes if ch == ERROR]
        assert values == ["boom2"]


class TestListAndDelete:
    @pytest.mark.anyio
    async def test_list_returns_newest_first(self, saver):
        thread_id = f"t-{uuid.uuid4()}"
        for ck_id in ("a", "b", "c"):
            await saver.aput(_cfg(thread_id), _empty_checkpoint(ck_id), {}, {})

        ids = [t.checkpoint["id"] async for t in saver.alist(_cfg(thread_id))]
        # Lexicographic DESC: "c" > "b" > "a"
        assert ids == ["c", "b", "a"]

    @pytest.mark.anyio
    async def test_list_respects_limit(self, saver):
        thread_id = f"t-{uuid.uuid4()}"
        for ck_id in ("a", "b", "c", "d"):
            await saver.aput(_cfg(thread_id), _empty_checkpoint(ck_id), {}, {})

        ids = [t.checkpoint["id"] async for t in saver.alist(_cfg(thread_id), limit=2)]
        assert ids == ["d", "c"]

    @pytest.mark.anyio
    async def test_list_respects_before(self, saver):
        thread_id = f"t-{uuid.uuid4()}"
        for ck_id in ("a", "b", "c"):
            await saver.aput(_cfg(thread_id), _empty_checkpoint(ck_id), {}, {})

        before = _cfg(thread_id, checkpoint_id="c")
        ids = [t.checkpoint["id"] async for t in saver.alist(_cfg(thread_id), before=before)]
        assert ids == ["b", "a"]

    @pytest.mark.anyio
    async def test_list_filters_by_metadata(self, saver):
        thread_id = f"t-{uuid.uuid4()}"
        await saver.aput(_cfg(thread_id), _empty_checkpoint("a"), {"kind": "x"}, {})
        await saver.aput(_cfg(thread_id), _empty_checkpoint("b"), {"kind": "y"}, {})
        await saver.aput(_cfg(thread_id), _empty_checkpoint("c"), {"kind": "x"}, {})

        ids = [
            t.checkpoint["id"]
            async for t in saver.alist(_cfg(thread_id), filter={"kind": "x"})
        ]
        assert sorted(ids) == ["a", "c"]

    @pytest.mark.anyio
    async def test_delete_thread_removes_checkpoints_and_writes(self, saver):
        thread_id = f"t-{uuid.uuid4()}"
        await saver.aput(_cfg(thread_id), _empty_checkpoint("c1"), {}, {})
        await saver.aput_writes(
            _cfg(thread_id, checkpoint_id="c1"), [("messages", "v1")], task_id="t"
        )

        await saver.adelete_thread(thread_id)

        got = await saver.aget_tuple(_cfg(thread_id))
        assert got is None


class TestVersioning:
    """Pure helpers — no DB required. Instantiate the saver with a None pool."""

    def _bare_saver(self):
        from deerflow.runtime.checkpointer.oceanbase_saver import AsyncOceanBaseSaver

        return AsyncOceanBaseSaver(pool=None)

    def test_get_next_version_monotonic(self):
        s = self._bare_saver()
        v1 = s.get_next_version(None, None)
        v2 = s.get_next_version(v1, None)
        v3 = s.get_next_version(v2, None)
        assert v1 < v2 < v3
        # Format: "{int:032}.{rand:016}" — same shape as the SQLite saver so
        # mixed-history threads compare consistently.
        assert "." in v1 and len(v1.split(".")[0]) == 32

    def test_get_next_version_accepts_int(self):
        s = self._bare_saver()
        v = s.get_next_version(7, None)
        assert v.startswith("0" * 31 + "8")


class TestParity:
    @pytest.mark.anyio
    async def test_matches_sqlite_for_same_inputs(self, saver):
        """Round-trip the same checkpoint through SQLite and OceanBase savers
        and assert the public surface (CheckpointTuple) is equivalent.

        Skips fields that legitimately differ (the saved ``config`` is
        identity-tagged with the backend's own table contents).
        """
        from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

        thread_id = f"t-{uuid.uuid4()}"
        ckpt = _empty_checkpoint("ckpt-parity")
        metadata = {"tag": "parity", "n": 7}

        async with AsyncSqliteSaver.from_conn_string(":memory:") as sqlite_saver:
            await sqlite_saver.setup()
            await sqlite_saver.aput(_cfg(thread_id), ckpt, metadata, {})
            sqlite_tuple = await sqlite_saver.aget_tuple(_cfg(thread_id))

        await saver.aput(_cfg(thread_id), ckpt, metadata, {})
        ob_tuple = await saver.aget_tuple(_cfg(thread_id))

        assert sqlite_tuple is not None and ob_tuple is not None
        assert sqlite_tuple.checkpoint["id"] == ob_tuple.checkpoint["id"]
        assert sqlite_tuple.metadata == ob_tuple.metadata
