"""
test_retry_permanent.py — Unit tests for the OPT-IN FAILED_PERMANENT re-drive
(retry_permanent) feature.

Covers:
  1. State machine: models.py now PERMITS FAILED_PERMANENT -> RETRY_PENDING
     (and -> QUEUED) so the reset transition is legal, while FAILED_PERMANENT
     remains in TERMINAL_STATES (default semantics unchanged).
  2. RetryManager.run_retry: with retry_permanent=True it calls
     AuditManager.reset_permanent_failures FIRST (batch-scoped); with the flag
     off (default) it never touches FAILED_PERMANENT rows.
  3. After a reset (attempt_number=0, error_code cleared) the two guards in
     run_retry do NOT instantly re-kill the row — it gets requeued once.

No live warehouse / SDK needed: databricks.sdk.core is stubbed (same approach
as tests/test_sql_client_warehouse_start.py) and AuditManager is a fake.

Run with:  python3 -m pytest tests/test_retry_permanent.py -v
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

# ── Make `orchestrator` importable when run directly (repo root on sys.path) ──
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ── Stub databricks-sdk so importing orchestrator.audit_manager (-> sql_client)
# works without the SDK installed (mirrors tests/test_sql_client_warehouse_start.py).
if "databricks.sdk.core" not in sys.modules:
    _dbx = types.ModuleType("databricks")
    _sdk = types.ModuleType("databricks.sdk")
    _core = types.ModuleType("databricks.sdk.core")

    class _Config:  # minimal stand-in
        def __init__(self, *a, **k):
            self.host = k.get("host", "https://example.databricks.com")

        def authenticate(self):
            return {}

    _core.Config = _Config
    _sdk.core = _core
    _dbx.sdk = _sdk
    sys.modules.setdefault("databricks", _dbx)
    sys.modules.setdefault("databricks.sdk", _sdk)
    sys.modules.setdefault("databricks.sdk.core", _core)

from orchestrator.models import (  # noqa: E402
    MigrationStatus,
    VALID_TRANSITIONS,
    TERMINAL_STATES,
    MigrationRecord,
)
from orchestrator.retry_manager import RetryManager  # noqa: E402


# ── 1. State machine ─────────────────────────────────────────────────────────

def test_failed_permanent_now_transitions_to_retry_pending():
    allowed = VALID_TRANSITIONS[MigrationStatus.FAILED_PERMANENT]
    assert MigrationStatus.RETRY_PENDING in allowed
    assert MigrationStatus.QUEUED in allowed


def test_failed_permanent_still_terminal_state():
    # Default semantics preserved: it's still a terminal state (only the opt-in
    # reset path uses the new edge).
    assert MigrationStatus.FAILED_PERMANENT in TERMINAL_STATES


def test_record_can_transition_permanent_to_retry_pending():
    rec = MigrationRecord(
        migration_id="m1", run_id="r1", clone_type="delta_share",
        source_workspace="", target_workspace="",
        source_catalog="c", source_schema="s", source_table="t",
        target_catalog="c", target_schema="s", target_table="t",
        status=MigrationStatus.FAILED_PERMANENT.value,
    )
    assert rec.can_transition_to(MigrationStatus.RETRY_PENDING) is True
    assert rec.can_transition_to(MigrationStatus.QUEUED) is True
    # but NOT to an unrelated state
    assert rec.can_transition_to(MigrationStatus.COMPLETED) is False


# ── 2 & 3. RetryManager wiring ───────────────────────────────────────────────

class _FakeAudit:
    """Records calls; returns a configurable retryable-record set."""

    def __init__(self, retryable=None, reset_count=0):
        self._retryable = retryable or []
        self._reset_count = reset_count
        self.calls = []
        self.reset_batch = None

    def reset_permanent_failures(self, batch_id: str = ""):
        self.calls.append(("reset", batch_id))
        self.reset_batch = batch_id
        return self._reset_count

    def get_retryable_records(self, batch_id: str = ""):
        self.calls.append(("get_retryable", batch_id))
        return list(self._retryable)

    def requeue_for_retry(self, migration_id: str):
        self.calls.append(("requeue", migration_id))

    def mark_permanent_failure(self, migration_id: str, reason: str):
        self.calls.append(("permanent", migration_id))


class _Cfg:
    max_retries = 3
    retry_backoff_base_s = 30


def _call_names(audit):
    return [c[0] for c in audit.calls]


def test_flag_off_does_not_reset_permanent():
    audit = _FakeAudit(retryable=[])
    rm = RetryManager(_Cfg(), audit)
    stats = rm.run_retry(backoff=False, batch_id="b1", retry_permanent=False)
    assert "reset" not in _call_names(audit)
    assert stats["reset_permanent"] == 0


def test_flag_on_resets_permanent_first_and_is_batch_scoped():
    audit = _FakeAudit(retryable=[], reset_count=2)
    rm = RetryManager(_Cfg(), audit)
    stats = rm.run_retry(backoff=False, batch_id="b1", retry_permanent=True)
    names = _call_names(audit)
    # reset must happen, and BEFORE selection
    assert "reset" in names
    assert names.index("reset") < names.index("get_retryable")
    assert audit.reset_batch == "b1"          # batch scope respected
    assert stats["reset_permanent"] == 2


def test_reset_row_gets_requeued_not_re_killed():
    # A just-reset row (attempt_number=0, error_code cleared) surfaces via
    # get_retryable_records — the two guards must NOT re-mark it permanent; it
    # should be requeued exactly once.
    reset_row = {
        "migration_id": "m-reset",
        "error_code": None,       # cleared by reset_permanent_failures
        "attempt_number": 0,      # fresh budget
        "max_attempts": 3,
    }
    audit = _FakeAudit(retryable=[reset_row], reset_count=1)
    rm = RetryManager(_Cfg(), audit)
    stats = rm.run_retry(backoff=False, batch_id="b1", retry_permanent=True)
    assert ("requeue", "m-reset") in audit.calls
    assert ("permanent", "m-reset") not in audit.calls
    assert stats["requeued"] == 1
    assert stats["permanent"] == 0
