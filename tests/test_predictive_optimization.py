"""
test_predictive_optimization.py — Unit tests for the INVENTORY-time Predictive
Optimization (PO) gate.

Covers:
  1. POState parsing from the UC REST API `effective_predictive_optimization_flag`:
     - ENABLE set directly on the table           → blocked, level TABLE
     - ENABLE inherited from SCHEMA / CATALOG      → blocked, correct level/source
     - DISABLE                                     → allowed
     - missing/unrecognised effective flag         → blocked (fail closed, UNKNOWN)
     - API/permission error                        → blocked (fail closed, UNKNOWN)
  2. check_selections() runs in parallel and returns a per-fqn verdict map.
  3. InventoryManager integration: a PO-enabled source is recorded as PO_BLOCKED
     (not QUEUED), clean tables are still onboarded as QUEUED, and the stats
     carry po_blocked / po_blocked_list.

No live Databricks / Spark / dbutils needed — the UC API and SqlClient are
faked in-process. `databricks.sdk.core.Config` is stubbed before import (same
as the other orchestrator unit tests).

Run with:  python3 -m pytest tests/test_predictive_optimization.py -v
"""

from __future__ import annotations

import sys
import threading
import types
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ── Stub databricks.sdk.core.Config (real SDK not installed in local dev) ─────
if "databricks" not in sys.modules:
    databricks_mod      = types.ModuleType("databricks")
    databricks_sdk_mod  = types.ModuleType("databricks.sdk")
    databricks_core_mod = types.ModuleType("databricks.sdk.core")

    class _StubConfig:
        def __init__(self, host: str = ""):
            self.host = host or "https://stub.azuredatabricks.net"

        def authenticate(self) -> Dict[str, str]:
            return {"Authorization": "Bearer stub"}

    databricks_core_mod.Config = _StubConfig
    databricks_sdk_mod.core = databricks_core_mod
    databricks_mod.sdk = databricks_sdk_mod
    sys.modules["databricks"] = databricks_mod
    sys.modules["databricks.sdk"] = databricks_sdk_mod
    sys.modules["databricks.sdk.core"] = databricks_core_mod

from orchestrator.config import OrchestratorConfig                     # noqa: E402
from orchestrator.models import TableSelection, MigrationStatus        # noqa: E402
from orchestrator.workload_classifier import WorkloadClassifier        # noqa: E402
from orchestrator.inventory_manager import InventoryManager            # noqa: E402
from orchestrator.predictive_optimization import (                     # noqa: E402
    PredictiveOptimizationChecker, check_selections,
    PO_ENABLED_CODE, PO_UNKNOWN_CODE,
)


# ── Fakes ─────────────────────────────────────────────────────────────────────

class FakeApi:
    """Stand-in for ApiClient.unity_catalog_get keyed by table full name."""

    def __init__(self, responses: Optional[Dict[str, dict]] = None,
                 errors: Optional[set] = None):
        self.responses = responses or {}
        self.errors = errors or set()
        self.calls: List[str] = []
        self._lock = threading.Lock()

    def unity_catalog_get(self, kind: str, full_name: str) -> dict:
        with self._lock:
            self.calls.append(full_name)
        if full_name in self.errors:
            raise RuntimeError("PERMISSION_DENIED reading PO")
        return self.responses.get(full_name, {})


class FakeSqlClient:
    def __init__(self, responses: Optional[Dict[str, Any]] = None):
        self.responses = responses or {}
        self.execute_calls: List[str] = []
        self.ddl_calls: List[str] = []
        self._lock = threading.Lock()

    def execute(self, sql: str, timeout_s: int = 300) -> List[Dict[str, Any]]:
        with self._lock:
            self.execute_calls.append(sql)
        for key, rows in self.responses.items():
            if key in sql:
                return rows() if callable(rows) else rows
        return []

    def execute_ddl(self, sql: str) -> None:
        with self._lock:
            self.ddl_calls.append(sql)

    def execute_one(self, sql: str) -> Optional[Dict[str, Any]]:
        rows = self.execute(sql)
        return rows[0] if rows else None


def eff(value=None, inherited_type=None, inherited_name=None) -> dict:
    flag: Dict[str, Any] = {}
    if value is not None:
        flag["value"] = value
    if inherited_type is not None:
        flag["inherited_from_type"] = inherited_type
    if inherited_name is not None:
        flag["inherited_from_name"] = inherited_name
    return {"effective_predictive_optimization_flag": flag}


def make_selection(table: str = "t1") -> TableSelection:
    return TableSelection(
        source_catalog="src_cat", source_schema="src_sch", source_table=table,
        target_catalog="tgt_cat", target_schema="tgt_sch", target_table=table,
    )


# ── 1. POState parsing ─────────────────────────────────────────────────────────

def test_enabled_directly_on_table_is_blocked():
    api = FakeApi({"src_cat.src_sch.t1": eff("ENABLE")})
    st = PredictiveOptimizationChecker(api).check("src_cat", "src_sch", "t1")
    assert st.blocked is True
    assert st.value == "ENABLE"
    assert st.level == "TABLE"            # no inherited_from ⇒ set directly
    assert st.error_code == PO_ENABLED_CODE


def test_enabled_inherited_from_schema():
    api = FakeApi({"src_cat.src_sch.t1": eff("ENABLE", "SCHEMA", "src_sch")})
    st = PredictiveOptimizationChecker(api).check("src_cat", "src_sch", "t1")
    assert st.blocked is True
    assert st.level == "SCHEMA"
    assert st.source_name == "src_sch"
    assert st.error_code == PO_ENABLED_CODE


def test_enabled_inherited_from_catalog():
    api = FakeApi({"src_cat.src_sch.t1": eff("ENABLE", "CATALOG", "src_cat")})
    st = PredictiveOptimizationChecker(api).check("src_cat", "src_sch", "t1")
    assert st.blocked is True
    assert st.level == "CATALOG"
    assert st.source_name == "src_cat"


def test_disabled_is_allowed():
    api = FakeApi({"src_cat.src_sch.t1": eff("DISABLE")})
    st = PredictiveOptimizationChecker(api).check("src_cat", "src_sch", "t1")
    assert st.blocked is False
    assert st.value == "DISABLE"
    assert st.error_code == ""


def test_missing_flag_fails_closed():
    api = FakeApi({"src_cat.src_sch.t1": {}})  # no effective flag at all
    st = PredictiveOptimizationChecker(api).check("src_cat", "src_sch", "t1")
    assert st.blocked is True
    assert st.error_code == PO_UNKNOWN_CODE


def test_api_error_fails_closed():
    api = FakeApi(errors={"src_cat.src_sch.t1"})
    st = PredictiveOptimizationChecker(api).check("src_cat", "src_sch", "t1")
    assert st.blocked is True
    assert st.error_code == PO_UNKNOWN_CODE


# ── 2. Parallel check_selections ───────────────────────────────────────────────

def test_check_selections_parallel_map():
    api = FakeApi({
        "src_cat.src_sch.a": eff("DISABLE"),
        "src_cat.src_sch.b": eff("ENABLE", "SCHEMA", "src_sch"),
        "src_cat.src_sch.c": eff("DISABLE"),
    })
    sels = [make_selection("a"), make_selection("b"), make_selection("c")]
    out = check_selections(PredictiveOptimizationChecker(api), sels, threads=3)
    assert set(out.keys()) == {"src_cat.src_sch.a", "src_cat.src_sch.b", "src_cat.src_sch.c"}
    assert out["src_cat.src_sch.a"].blocked is False
    assert out["src_cat.src_sch.b"].blocked is True
    assert out["src_cat.src_sch.c"].blocked is False
    assert len(api.calls) == 3


# ── 3. InventoryManager integration ────────────────────────────────────────────

def _make_cfg() -> OrchestratorConfig:
    cfg = OrchestratorConfig()
    cfg.meta_catalog = "meta_cat"
    cfg.meta_schema = "meta_sch"
    cfg.batch_id = "po-unit-batch"
    cfg.clone_type = "delta_share"
    cfg.skip_describe_detail = True          # avoid needing DESCRIBE DETAIL fakes
    cfg.inventory_parallel_threads = 2
    return cfg


def test_inventory_blocks_po_enabled_and_queues_clean():
    cfg = _make_cfg()
    api = FakeApi({
        "src_cat.src_sch.clean": eff("DISABLE"),
        "src_cat.src_sch.blocked": eff("ENABLE", "CATALOG", "src_cat"),
    })
    src = FakeSqlClient()
    tgt = FakeSqlClient()
    mgr = InventoryManager(
        cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-po",
        po_checker=PredictiveOptimizationChecker(api),
    )

    stats = mgr.run_inventory([make_selection("clean"), make_selection("blocked")])

    assert stats["po_blocked"] == 1
    assert stats["inserted"] == 1
    blocked_fqns = [sel.source_fqn for sel, _ in stats["po_blocked_list"]]
    assert blocked_fqns == ["src_cat.src_sch.blocked"]

    all_ddl = "\n".join(tgt.ddl_calls)
    # PO-blocked row written with the dedicated status + error code
    assert "PO_BLOCKED" in all_ddl
    assert PO_ENABLED_CODE in all_ddl
    # Clean table still onboarded as QUEUED
    assert MigrationStatus.QUEUED.value in all_ddl


def test_inventory_all_blocked_queues_nothing():
    cfg = _make_cfg()
    api = FakeApi({
        "src_cat.src_sch.a": eff("ENABLE"),
        "src_cat.src_sch.b": eff("ENABLE", "SCHEMA", "src_sch"),
    })
    src = FakeSqlClient()
    tgt = FakeSqlClient()
    mgr = InventoryManager(
        cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-po2",
        po_checker=PredictiveOptimizationChecker(api),
    )

    stats = mgr.run_inventory([make_selection("a"), make_selection("b")])

    assert stats["po_blocked"] == 2
    assert stats["inserted"] == 0
    assert "PO_BLOCKED" in "\n".join(tgt.ddl_calls)


def test_inventory_without_checker_skips_gate():
    """Backward-compat: po_checker=None (as in older callers) → no PO gate."""
    cfg = _make_cfg()
    src = FakeSqlClient()
    tgt = FakeSqlClient()
    mgr = InventoryManager(cfg, src, tgt, WorkloadClassifier(cfg), run_id="run-po3")

    stats = mgr.run_inventory([make_selection("x")])

    assert stats["po_blocked"] == 0
    assert stats["inserted"] == 1
