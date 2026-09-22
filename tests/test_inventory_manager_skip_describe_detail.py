"""
test_inventory_manager_skip_describe_detail.py — Unit tests for the
OrchestratorConfig.skip_describe_detail opt-out flag.

Covers:
  1. skip_describe_detail=True never calls DESCRIBE DETAIL / DESCRIBE HISTORY
     on the source at all.
  2. The onboarded MigrationRecord has NULL (None) size_in_bytes/size_gb/
     source_num_files/source_version, and workload_class='UNKNOWN' /
     workload_weight=0 — never 0/"SMALL" (which would misleadingly imply
     "measured and found empty/small").
  3. The generated MERGE SQL persists real SQL `NULL` literals for those
     columns (not the Python string "None", and not 0) — both in the
     WHEN MATCHED UPDATE SET clause and the WHEN NOT MATCHED INSERT VALUES
     clause.
  4. Regression: skip_describe_detail=False (default) is completely
     unaffected — DESCRIBE DETAIL still runs and numeric metadata is still
     captured exactly as before.
  5. run_inventory() end-to-end stats are correct under skip_describe_detail
     for a small multi-table batch (no failures, no crashes).
  6. _sql_num / _sql_str helper behavior directly.

Run with:  python3 -m pytest tests/test_inventory_manager_skip_describe_detail.py -v

No live Databricks connection, Spark, or dbutils required — SqlClient calls
are faked in-process. `databricks.sdk.core` is stubbed in sys.modules before
import since it is not installed in this local dev environment (it is only
ever imported for its Config class, which orchestrator.sql_client uses at
module scope but this test never instantiates).
"""

from __future__ import annotations

import sys
import types
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

# ── Make `orchestrator` importable when run directly (repo root on sys.path) ──
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ── Stub databricks.sdk.core.Config so orchestrator.sql_client imports cleanly
# without the real `databricks-sdk` package installed (not needed here — we
# never instantiate the real SqlClient, only FakeSqlClient below). ──
if "databricks" not in sys.modules:
    databricks_mod       = types.ModuleType("databricks")
    databricks_sdk_mod   = types.ModuleType("databricks.sdk")
    databricks_core_mod  = types.ModuleType("databricks.sdk.core")

    class _StubConfig:
        def __init__(self, host: str = ""):
            self.host = host or "https://stub.azuredatabricks.net"

        def authenticate(self) -> Dict[str, str]:
            return {"Authorization": "Bearer stub"}

    databricks_core_mod.Config = _StubConfig
    databricks_sdk_mod.core = databricks_core_mod
    databricks_mod.sdk = databricks_sdk_mod
    sys.modules["databricks"]     = databricks_mod
    sys.modules["databricks.sdk"] = databricks_sdk_mod
    sys.modules["databricks.sdk.core"] = databricks_core_mod

from orchestrator.config import OrchestratorConfig                    # noqa: E402
from orchestrator.models import TableSelection, WorkloadClass, MigrationStatus  # noqa: E402
from orchestrator.workload_classifier import WorkloadClassifier       # noqa: E402
from orchestrator.inventory_manager import InventoryManager, _sql_num, _sql_str  # noqa: E402


# ── Fake SqlClient ──────────────────────────────────────────────────────────

class FakeSqlClient:
    """
    Minimal stand-in for orchestrator.sql_client.SqlClient.

    `responses` maps a substring that must appear in the SQL to either a
    fixed list-of-dict-rows or a zero-arg callable returning one. The FIRST
    matching key wins. Unmatched `execute()` calls return [] (e.g. so
    InventoryManager._get_existing() finds "no existing row").
    """

    def __init__(self, responses: Optional[Dict[str, Any]] = None):
        self.responses: Dict[str, Any] = responses or {}
        self.execute_calls: List[str] = []
        self.ddl_calls: List[str] = []

    def execute(self, sql: str, timeout_s: int = 300) -> List[Dict[str, Any]]:
        self.execute_calls.append(sql)
        for key, rows in self.responses.items():
            if key in sql:
                return rows() if callable(rows) else rows
        return []

    def execute_ddl(self, sql: str) -> None:
        self.ddl_calls.append(sql)

    def execute_one(self, sql: str) -> Optional[Dict[str, Any]]:
        rows = self.execute(sql)
        return rows[0] if rows else None

    def called_with(self, needle: str) -> bool:
        return any(needle in c for c in self.execute_calls)


# ── Fixtures ──────────────────────────────────────────────────────────────

def make_cfg(skip_describe_detail: bool, batch_id: str = "unit-test-batch") -> OrchestratorConfig:
    cfg = OrchestratorConfig()
    cfg.meta_catalog = "test_meta_catalog"
    cfg.meta_schema  = "test_meta_schema"
    cfg.batch_id     = batch_id
    cfg.max_retries  = 3
    cfg.clone_type   = "delta_share"
    cfg.skip_describe_detail = skip_describe_detail
    return cfg


def make_selection(table: str = "t1") -> TableSelection:
    return TableSelection(
        source_catalog="src_cat", source_schema="src_sch", source_table=table,
        target_catalog="tgt_cat", target_schema="tgt_sch", target_table=table,
    )


DESCRIBE_DETAIL_RESPONSE = [{
    "format": "delta",
    "location": "abfss://x@y.dfs.core.windows.net/t1",
    "sizeInBytes": 123456789,
    "numFiles": 42,
    "createdAt": "2026-01-01T00:00:00Z",
    "lastModified": "2026-01-02T00:00:00Z",
}]
DESCRIBE_HISTORY_RESPONSE = [{"version": 7}]


# ── 1 & 2 & 3: skip_describe_detail=True ──────────────────────────────────

def test_skip_describe_detail_never_calls_source():
    cfg = make_cfg(skip_describe_detail=True)
    src = FakeSqlClient()   # no responses configured — any call would be a bug
    tgt = FakeSqlClient()
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    rec, target_missing = mgr._process_one(make_selection(), force=False)

    assert target_missing is False
    assert rec is not None
    # The whole point: DESCRIBE DETAIL / DESCRIBE HISTORY must never be sent.
    assert src.execute_calls == [], f"source was queried but should not have been: {src.execute_calls}"


def test_skip_describe_detail_record_fields_are_null_not_zero():
    cfg = make_cfg(skip_describe_detail=True)
    src = FakeSqlClient()
    tgt = FakeSqlClient()
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    rec, _ = mgr._process_one(make_selection(), force=False)

    assert rec.size_in_bytes is None
    assert rec.size_gb is None
    assert rec.source_num_files is None
    assert rec.source_version is None
    assert rec.workload_class == WorkloadClass.UNKNOWN.value
    assert rec.workload_weight == 0
    assert rec.status == MigrationStatus.QUEUED.value
    assert rec.source_path is None


def test_skip_describe_detail_merge_sql_uses_real_null_literals():
    cfg = make_cfg(skip_describe_detail=True)
    src = FakeSqlClient()
    tgt = FakeSqlClient()
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    mgr._process_one(make_selection(), force=False)

    assert len(tgt.ddl_calls) == 1, "expected exactly one MERGE (the _upsert call)"
    merge_sql = tgt.ddl_calls[0]
    # Normalize whitespace so assertions don't depend on exact column
    # alignment/indentation in the SQL-building f-string.
    flat = " ".join(merge_sql.split())

    # Must be MERGE INTO migration_control, and must literally say NULL for
    # every metadata field that requires DESCRIBE DETAIL to compute (size/
    # files/version/location) — these are genuinely "never measured".
    assert "MERGE INTO" in flat
    assert "size_in_bytes = NULL" in flat
    assert "size_gb = NULL" in flat
    assert "source_num_files = NULL" in flat
    assert "source_version = NULL" in flat
    assert "source_path = NULL" in flat

    # workload_class/workload_weight are a DELIBERATE exception: they are
    # NOT left NULL — they're set to the literal sentinel 'UNKNOWN' / 0 (see
    # WorkloadClass.UNKNOWN in models.py) so that audit queries like
    # `GROUP BY workload_class` stay meaningful instead of grouping under a
    # SQL NULL bucket.
    assert "workload_class = 'UNKNOWN'" in flat
    assert "workload_weight = 0" in flat

    # Never the Python string "None" leaking into SQL (would be a silent
    # data-corruption bug — 'None' is a non-NULL string value in the column).
    assert "'None'" not in flat
    assert "= None" not in flat

    # The INSERT VALUES branch must also use bare NULL, not a quoted string.
    assert "'NULL'" not in flat  # must be bare NULL, never quoted


# ── 4: Regression — skip_describe_detail=False behaves exactly as before ──

def test_normal_path_still_calls_describe_detail_and_populates_size():
    cfg = make_cfg(skip_describe_detail=False)
    src = FakeSqlClient(responses={
        "DESCRIBE DETAIL":  DESCRIBE_DETAIL_RESPONSE,
        "DESCRIBE HISTORY": DESCRIBE_HISTORY_RESPONSE,
    })
    tgt = FakeSqlClient()
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    rec, target_missing = mgr._process_one(make_selection(), force=False)

    assert target_missing is False
    assert src.called_with("DESCRIBE DETAIL")
    assert src.called_with("DESCRIBE HISTORY")
    assert rec.size_in_bytes == 123456789
    assert rec.size_gb == pytest.approx(123456789 / (1024 ** 3))
    assert rec.source_num_files == 42
    assert rec.source_version == 7
    assert rec.workload_class == WorkloadClass.SMALL.value   # ~0.115 GB < 10 GB
    assert rec.workload_weight == 1
    assert rec.status == MigrationStatus.QUEUED.value

    merge_sql = tgt.ddl_calls[0]
    flat = " ".join(merge_sql.split())
    assert "size_in_bytes = 123456789" in flat
    assert "size_gb = 0.114978" in flat  # 123456789 bytes, formatted to 6dp
    assert "workload_class = 'SMALL'" in flat
    assert "workload_weight = 1" in flat
    assert "source_num_files = 42" in flat
    assert "source_version = 7" in flat
    # No NULLs at all should appear for these populated fields anywhere.
    for field in ("size_in_bytes", "size_gb", "workload_class", "workload_weight",
                  "source_num_files", "source_version"):
        segment = flat.split(f"{field} =")[1].split(",")[0]
        assert "NULL" not in segment, f"{field} unexpectedly NULL: {segment}"


def test_normal_path_source_not_found_marks_failed_permanent():
    """Non-Delta / missing source still hits the pre-existing FAILED_PERMANENT
    path when skip_describe_detail=False — must be untouched by this change."""
    cfg = make_cfg(skip_describe_detail=False)
    src = FakeSqlClient(responses={})  # no DESCRIBE DETAIL rows → treated as not found
    tgt = FakeSqlClient()
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    rec, target_missing = mgr._process_one(make_selection(), force=False)

    assert rec is None
    assert target_missing is False
    assert len(tgt.ddl_calls) == 1
    assert "FAILED_PERMANENT" in tgt.ddl_calls[0]


# ── 5: run_inventory() end-to-end batch stats under skip_describe_detail ──

def test_run_inventory_batch_with_skip_describe_detail():
    cfg = make_cfg(skip_describe_detail=True)
    src = FakeSqlClient()
    tgt = FakeSqlClient()
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    selections = [make_selection("t1"), make_selection("t2"), make_selection("t3")]
    stats = mgr.run_inventory(selections, force=False)

    assert stats["total"] == 3
    assert stats["inserted"] == 3
    assert stats["failed"] == 0
    assert stats["skipped"] == 0
    assert src.execute_calls == [], "skip_describe_detail must never touch the source at all"
    assert len(tgt.ddl_calls) == 3   # one MERGE per table
    for rec in stats["records"]:
        assert rec.size_in_bytes is None
        assert rec.workload_class == WorkloadClass.UNKNOWN.value


def test_run_inventory_idempotent_rerun_same_batch_skips():
    """Re-running INVENTORY for the same batch_id a second time should find
    the existing QUEUED row and skip re-onboarding (existing idempotency
    contract) — must hold identically whether or not describe detail was
    skipped the first time."""
    cfg = make_cfg(skip_describe_detail=True)
    src = FakeSqlClient()

    # First run: no existing row.
    existing_row = {}

    def get_existing_lookup():
        return [existing_row] if existing_row else []

    tgt = FakeSqlClient(responses={"SELECT migration_id, status, attempt_number": get_existing_lookup})
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    sel = make_selection("t1")
    rec1, _ = mgr._process_one(sel, force=False)
    assert rec1 is not None

    # Simulate that the row now exists as QUEUED (not yet COMPLETED/VALIDATED/
    # FAILED_PERMANENT/SKIPPED) — QUEUED re-onboarding is NOT blocked by the
    # existing-status check (only terminal states are), so this call still
    # re-touches (not duplicates) the same row idempotently.
    existing_row.clear()
    existing_row.update({"migration_id": rec1.migration_id, "status": "QUEUED", "attempt_number": 0})

    rec2, _ = mgr._process_one(sel, force=False)
    assert rec2 is not None
    assert rec2.migration_id == rec1.migration_id  # same row, not a new one

    # Now simulate a genuinely terminal state — must be skipped entirely.
    existing_row["status"] = "VALIDATED"
    rec3, _ = mgr._process_one(sel, force=False)
    assert rec3 is None


# ── 6: _sql_num / _sql_str helpers ────────────────────────────────────────

@pytest.mark.parametrize("value,expected", [
    (None, "NULL"),
    (0, "0"),
    (42, "42"),
    (3.14, "3.14"),
])
def test_sql_num_helper(value, expected):
    assert _sql_num(value) == expected


@pytest.mark.parametrize("value,expected", [
    (None, "NULL"),
    ("", "''"),
    ("SMALL", "'SMALL'"),
    ("O'Brien", "'O\\'Brien'"),
])
def test_sql_str_helper(value, expected):
    assert _sql_str(value) == expected


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
