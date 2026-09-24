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
import threading
import time
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

    `sleep_s`, when set, makes execute()/execute_ddl() hold the "connection"
    for that long — used to prove/disprove concurrency deterministically via
    the max_inflight/max_ddl_inflight high-water marks below, instead of
    relying on flaky wall-clock timing assertions.
    """

    def __init__(self, responses: Optional[Dict[str, Any]] = None, sleep_s: float = 0.0):
        self.responses: Dict[str, Any] = responses or {}
        self.sleep_s = sleep_s
        self.execute_calls: List[str] = []
        self.ddl_calls: List[str] = []

        self._lock = threading.Lock()
        self._inflight = 0
        self.max_inflight = 0        # high-water mark of concurrent execute() calls
        self._ddl_inflight = 0
        self.max_ddl_inflight = 0    # high-water mark of concurrent execute_ddl() calls

    def execute(self, sql: str, timeout_s: int = 300) -> List[Dict[str, Any]]:
        with self._lock:
            self._inflight += 1
            self.max_inflight = max(self.max_inflight, self._inflight)
        try:
            self.execute_calls.append(sql)
            if self.sleep_s:
                time.sleep(self.sleep_s)
            for key, rows in self.responses.items():
                if key in sql:
                    return rows() if callable(rows) else rows
            return []
        finally:
            with self._lock:
                self._inflight -= 1

    def execute_ddl(self, sql: str) -> None:
        with self._lock:
            self._ddl_inflight += 1
            self.max_ddl_inflight = max(self.max_ddl_inflight, self._ddl_inflight)
        try:
            self.ddl_calls.append(sql)
            if self.sleep_s:
                time.sleep(self.sleep_s)
        finally:
            with self._lock:
                self._ddl_inflight -= 1

    def execute_one(self, sql: str) -> Optional[Dict[str, Any]]:
        rows = self.execute(sql)
        return rows[0] if rows else None

    def called_with(self, needle: str) -> bool:
        return any(needle in c for c in self.execute_calls)


# ── Fixtures ──────────────────────────────────────────────────────────────

def make_cfg(
    skip_describe_detail: bool,
    batch_id: str = "unit-test-batch",
    inventory_parallel_threads: int = 1,
) -> OrchestratorConfig:
    cfg = OrchestratorConfig()
    cfg.meta_catalog = "test_meta_catalog"
    cfg.meta_schema  = "test_meta_schema"
    cfg.batch_id     = batch_id
    cfg.max_retries  = 3
    cfg.clone_type   = "delta_share"
    cfg.skip_describe_detail = skip_describe_detail
    # Existing tests above were written against the original strictly-
    # sequential behavior — default here to 1 thread so they keep testing
    # _process_one()/_upsert() in isolation without incidental concurrency.
    # Dedicated parallelism tests below override this explicitly.
    cfg.inventory_parallel_threads = inventory_parallel_threads
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

    rec, _ = mgr._process_one(make_selection(), force=False)
    # P1: _process_one no longer writes — the control-table MERGE is batched and
    # issued by run_inventory()/_upsert_batch() after the parallel read phase.
    assert tgt.ddl_calls == [], "_process_one must not write anymore (batched)"
    mgr._upsert_batch([rec])

    assert len(tgt.ddl_calls) == 1, "expected exactly one batched MERGE"
    merge_sql = tgt.ddl_calls[0]
    # Normalize whitespace so assertions don't depend on exact column
    # alignment/indentation in the SQL-building f-string.
    flat = " ".join(merge_sql.split())

    # Must be MERGE INTO migration_control. Unmeasured metadata (size/files/
    # version/location) is carried as bare SQL NULL in the VALUES row — never
    # the Python string 'None', and never a quoted 'NULL' string.
    assert "MERGE INTO" in flat
    assert "NULL" in flat
    assert "'None'" not in flat
    assert "= None" not in flat
    assert "'NULL'" not in flat

    # The typed source SELECT casts the numeric columns explicitly so a
    # whole-batch-NULL column still has an unambiguous type.
    assert "CAST(size_in_bytes AS BIGINT)" in flat
    assert "CAST(source_version AS BIGINT)" in flat

    # workload_class/workload_weight are a DELIBERATE exception: they are the
    # literal sentinel 'UNKNOWN' / 0 (see WorkloadClass.UNKNOWN), present in the
    # VALUES row so audit `GROUP BY workload_class` stays meaningful.
    assert "'UNKNOWN'" in flat


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

    # P1: write is batched — drive it explicitly and check the VALUES row +
    # the batched SET (which references the source alias, `col = s.col`).
    assert tgt.ddl_calls == []
    mgr._upsert_batch([rec])
    merge_sql = tgt.ddl_calls[0]
    flat = " ".join(merge_sql.split())
    assert "123456789" in flat            # size_in_bytes value present in VALUES row
    assert "0.114978" in flat             # size_gb formatted to 6dp
    assert "'SMALL'" in flat
    assert "size_in_bytes = s.size_in_bytes" in flat   # batched SET references source
    assert "source_version = s.source_version" in flat
    # No Python 'None' string ever leaks into SQL for a populated record.
    assert "'None'" not in flat


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
    assert len(tgt.ddl_calls) == 1   # P1: all 3 rows in ONE batched MERGE
    for rec in stats["records"]:
        assert rec.size_in_bytes is None
        assert rec.workload_class == WorkloadClass.UNKNOWN.value


def test_upsert_batch_splits_into_multiple_merges():
    """P1: _upsert_batch collapses many records into ceil(N/batch_size) MERGE
    statements (not one-per-table), and each is a single MERGE INTO."""
    cfg = make_cfg(skip_describe_detail=True)
    src = FakeSqlClient()
    tgt = FakeSqlClient()
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    recs = [mgr._process_one(make_selection(f"t{i}"), force=False)[0] for i in range(250)]
    assert tgt.ddl_calls == []          # building records writes nothing
    mgr._upsert_batch(recs, batch_size=200)
    assert len(tgt.ddl_calls) == 2      # 250 → 200 + 50
    assert all("MERGE INTO" in c for c in tgt.ddl_calls)
    # Every record's identity key must appear across the batched statements.
    joined = " ".join(tgt.ddl_calls)
    for r in recs:
        assert f"'{r.source_table}'" in joined


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


# ── 6: inventory_parallel_threads ──────────────────────────────────────────
# These use FakeSqlClient(sleep_s=...) high-water-mark counters instead of
# wall-clock timing assertions, so they are deterministic (never flaky under
# CI load) while still proving REAL concurrency occurred (not just "the code
# didn't crash").

N_TABLES = 8
THREAD_SLEEP_S = 0.05


def make_many_selections(n: int) -> List[TableSelection]:
    return [make_selection(f"t{i}") for i in range(n)]


def test_inventory_parallel_threads_reads_run_concurrently():
    cfg = make_cfg(skip_describe_detail=False, inventory_parallel_threads=4)
    src = FakeSqlClient(
        responses={"DESCRIBE DETAIL": DESCRIBE_DETAIL_RESPONSE, "DESCRIBE HISTORY": DESCRIBE_HISTORY_RESPONSE},
        sleep_s=THREAD_SLEEP_S,
    )
    tgt = FakeSqlClient(sleep_s=THREAD_SLEEP_S)
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    stats = mgr.run_inventory(make_many_selections(N_TABLES), force=False)

    assert stats["total"] == N_TABLES
    assert stats["inserted"] == N_TABLES
    # The whole point: with 4 worker threads, more than one DESCRIBE DETAIL/
    # HISTORY/existing-lookup call must have been in flight simultaneously.
    assert src.max_inflight > 1, (
        f"expected concurrent source reads with inventory_parallel_threads=4, "
        f"got max_inflight={src.max_inflight}"
    )
    # But migration_control MERGE writes must NEVER overlap — that's the
    # entire reason _write_lock exists (avoids Delta concurrent-write
    # conflicts on the shared control table). With P1 batching the write is a
    # single set-based MERGE issued after the parallel read phase.
    assert tgt.max_ddl_inflight == 1, (
        f"migration_control writes must be fully serialized, "
        f"got max_ddl_inflight={tgt.max_ddl_inflight}"
    )
    assert len(tgt.ddl_calls) == 1          # P1: all N rows in ONE batched MERGE
    assert len(stats["records"]) == N_TABLES  # no records lost or duplicated


def test_inventory_parallel_threads_one_is_fully_sequential():
    """inventory_parallel_threads=1 must fully restore the original
    strictly-sequential behavior — no concurrency at all, anywhere."""
    cfg = make_cfg(skip_describe_detail=False, inventory_parallel_threads=1)
    src = FakeSqlClient(
        responses={"DESCRIBE DETAIL": DESCRIBE_DETAIL_RESPONSE, "DESCRIBE HISTORY": DESCRIBE_HISTORY_RESPONSE},
        sleep_s=THREAD_SLEEP_S,
    )
    tgt = FakeSqlClient(sleep_s=THREAD_SLEEP_S)
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    stats = mgr.run_inventory(make_many_selections(N_TABLES), force=False)

    assert stats["total"] == N_TABLES
    assert stats["inserted"] == N_TABLES
    assert src.max_inflight == 1, "inventory_parallel_threads=1 must never overlap source reads"
    assert tgt.max_ddl_inflight == 1
    assert len(tgt.ddl_calls) == 1   # P1: single batched MERGE regardless of thread count


def test_inventory_parallel_threads_no_lost_or_duplicated_records_under_high_concurrency():
    """Stress the aggregation path: many tables, more threads than tables,
    zero artificial delay (maximizes race-condition exposure) — stats and
    the records list must still be exactly correct, with no lost/duplicated
    entries from the concurrent ThreadPoolExecutor + as_completed() loop."""
    n = 40
    cfg = make_cfg(skip_describe_detail=True, inventory_parallel_threads=16)
    src = FakeSqlClient()
    tgt = FakeSqlClient()
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    stats = mgr.run_inventory(make_many_selections(n), force=False)

    assert stats["total"] == n
    assert stats["inserted"] == n
    assert stats["failed"] == 0
    assert stats["skipped"] == 0
    assert len(stats["records"]) == n
    assert len({r.source_table for r in stats["records"]}) == n  # all distinct, none dropped/duplicated
    # P1: n=40 rows fit in a single batched MERGE (batch_size=200).
    assert len(tgt.ddl_calls) == 1


def test_inventory_parallel_threads_zero_or_negative_falls_back_to_one():
    """orchestrator_notebook.py wraps the widget parse in try/except — a
    non-numeric inventory_parallel_threads must not crash INVENTORY. This
    test exercises the config-level guard directly: InventoryManager treats
    any non-positive/invalid value as 1 (never 0 or negative, which would
    make ThreadPoolExecutor raise)."""
    cfg = make_cfg(skip_describe_detail=True)
    cfg.inventory_parallel_threads = 0   # e.g. a bad override slipping through
    src = FakeSqlClient()
    tgt = FakeSqlClient()
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-1")

    # Must not raise ValueError("max_workers must be greater than 0") from
    # ThreadPoolExecutor — run_inventory() clamps to at least 1.
    stats = mgr.run_inventory(make_many_selections(3), force=False)
    assert stats["total"] == 3
    assert stats["inserted"] == 3


# ── 7: _sql_num / _sql_str helpers ────────────────────────────────────────

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
