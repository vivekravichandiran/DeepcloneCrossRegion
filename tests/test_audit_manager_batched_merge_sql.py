"""
test_audit_manager_batched_merge_sql.py — Regression tests for the batched
set-based MERGE statements added to AuditManager (P2/P3 performance work).

Root cause guarded here:
  Databricks does NOT allow the column-alias form
      USING (VALUES ...) AS s(mid, cid)
  directly in a MERGE USING clause — it raises
      [COLUMN_ALIASES_NOT_ALLOWED] ... SQLSTATE: 42601
  The VALUES list must be wrapped in a SELECT subquery instead:
      USING (SELECT ... FROM (VALUES ...) AS v(mid, cid)) AS s

These tests build the real SQL via a fake SqlClient and assert none of the
generated MERGE statements use the disallowed form.

No live Databricks / Spark / dbutils needed.
"""

from __future__ import annotations

import re
import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Stub databricks.sdk.core.Config so orchestrator.sql_client imports cleanly
# without the real databricks-sdk installed (we only use FakeSql here).
if "databricks" not in sys.modules:
    _d = types.ModuleType("databricks")
    _sdk = types.ModuleType("databricks.sdk")
    _core = types.ModuleType("databricks.sdk.core")
    _core.Config = object  # type: ignore[attr-defined]
    _sdk.core = _core  # type: ignore[attr-defined]
    _d.sdk = _sdk  # type: ignore[attr-defined]
    sys.modules["databricks"] = _d
    sys.modules["databricks.sdk"] = _sdk
    sys.modules["databricks.sdk.core"] = _core

from types import SimpleNamespace

from orchestrator.audit_manager import AuditManager
from orchestrator.models import ChunkAssignment


class FakeSql:
    """Captures every SQL string sent to execute / execute_ddl."""

    def __init__(self):
        self.ddl: list[str] = []
        self.queries: list[str] = []

    def execute_ddl(self, sql: str):
        self.ddl.append(sql)
        return None

    def execute(self, sql: str):
        self.queries.append(sql)
        return []


# The exact anti-pattern Databricks rejects inside a MERGE USING clause:
#   USING (VALUES ...) AS <alias>(<col>, <col>, ...)
_BAD_MERGE_ALIAS = re.compile(
    r"USING\s*\(\s*VALUES\b.*?\)\s*AS\s+\w+\s*\(",
    re.IGNORECASE | re.DOTALL,
)


def _make_audit(sql):
    cfg = SimpleNamespace(meta_catalog="mc", meta_schema="ms")
    return AuditManager(cfg, sql, run_id="run-1")


def _assert_no_bad_alias(sql: str):
    assert not _BAD_MERGE_ALIAS.search(sql), (
        "MERGE USING uses the disallowed `(VALUES ...) AS alias(cols)` form "
        "(COLUMN_ALIASES_NOT_ALLOWED / SQLSTATE 42601); wrap VALUES in a "
        f"SELECT subquery instead.\nSQL was:\n{sql}"
    )


def test_assign_batch_chunks_merge_wraps_values_in_select():
    sql = FakeSql()
    audit = _make_audit(sql)

    chunks = [
        ChunkAssignment(chunk_id=1, batch_id="b1",
                        migration_ids=[f"id-{i}" for i in range(3)]),
        ChunkAssignment(chunk_id=2, batch_id="b1",
                        migration_ids=[f"id-{i}" for i in range(3, 5)]),
    ]
    n = audit.assign_batch_chunks("b1", chunks)

    assert n == 5
    assert len(sql.ddl) == 1
    merge = sql.ddl[0]
    _assert_no_bad_alias(merge)
    # Positive: the safe nested form is used.
    assert "FROM (VALUES" in merge
    assert re.search(r"\)\s*AS\s+v\s*\(\s*mid\s*,\s*cid\s*\)", merge)
    assert "chunk_id   = s.cid" in merge or "chunk_id = s.cid" in merge


def test_assign_batch_chunks_splits_into_multiple_merges():
    sql = FakeSql()
    audit = _make_audit(sql)
    # 1200 ids across one chunk -> ceil(1200/500) = 3 batched MERGEs.
    chunk = ChunkAssignment(chunk_id=7, batch_id="b1",
                            migration_ids=[f"id-{i}" for i in range(1200)])
    n = audit.assign_batch_chunks("b1", [chunk])
    assert n == 1200
    assert len(sql.ddl) == 3
    for merge in sql.ddl:
        _assert_no_bad_alias(merge)


def test_mark_validation_batch_merge_is_alias_safe():
    sql = FakeSql()
    audit = _make_audit(sql)
    outcomes = [
        {"migration_id": "id-1", "status": "VALIDATED", "message": "ok",
         "source_row_count": 10, "target_row_count": 10},
        {"migration_id": "id-2", "status": "VALIDATION_FAILED",
         "message": "count mismatch", "source_row_count": 5,
         "target_row_count": 4},
    ]
    written = audit.mark_validation_batch(outcomes)
    assert written == 2
    assert len(sql.ddl) == 1
    _assert_no_bad_alias(sql.ddl[0])


def test_assign_batch_chunks_escapes_single_quotes_in_ids():
    sql = FakeSql()
    audit = _make_audit(sql)
    chunk = ChunkAssignment(chunk_id=1, batch_id="b1",
                            migration_ids=["o'brien-id"])
    audit.assign_batch_chunks("b1", [chunk])
    merge = sql.ddl[0]
    _assert_no_bad_alias(merge)
    assert "o\\'brien-id" in merge
