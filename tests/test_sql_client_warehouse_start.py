"""
Regression tests for SqlClient.start_warehouse() cold-start wait.

Context: a retry (and any orchestrator run) failed because start_warehouse()
gave up after only 150s (2.5 min). The wait is now 30 min. These tests use a
*fake clock* (time.time / time.sleep are monkeypatched) so a simulated 10-minute
warehouse cold start is verified instantly, with no real network or sleeping.
"""
from __future__ import annotations

import sys
import types

import pytest

# ── Stub databricks-sdk so this test runs without the SDK installed ──────────
# start_warehouse() never constructs Config() (we build the client via __new__),
# so a lightweight stand-in for databricks.sdk.core.Config is all that's needed
# to import orchestrator.sql_client on a plain dev box / CI.
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

from orchestrator import sql_client as sc
from orchestrator.sql_client import SqlClient


class _Resp:
    """Minimal stand-in for a requests.Response."""

    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p

    def raise_for_status(self):
        pass


def _make_client():
    """Build a SqlClient without touching Databricks auth/network."""
    c = SqlClient.__new__(SqlClient)
    c._url = "https://example.databricks.com"
    c._wh = "wh_test"
    c._throttle = 0
    c._cfg = None
    c._headers = lambda: {}          # stub out auth
    return c


def _install_fake_clock(monkeypatch):
    """Make time.time() advance only when time.sleep() is called."""
    clock = {"t": 0.0}
    monkeypatch.setattr(sc.time, "time", lambda: clock["t"])
    monkeypatch.setattr(sc.time, "sleep", lambda s: clock.__setitem__("t", clock["t"] + s))
    return clock


def test_warehouse_up_after_10min_succeeds(monkeypatch):
    """Warehouse stays STARTING for ~10 min, then RUNNING -> must NOT raise."""
    clock = _install_fake_clock(monkeypatch)
    monkeypatch.setattr(sc.requests, "post", lambda *a, **k: _Resp({}))

    def fake_get(*a, **k):
        # Not RUNNING until 10 minutes (600s) of simulated wait have elapsed.
        state = "RUNNING" if clock["t"] >= 600 else "STARTING"
        return _Resp({"state": state})

    monkeypatch.setattr(sc.requests, "get", fake_get)

    c = _make_client()
    c.start_warehouse()   # should return cleanly, not raise

    # Proves it actually waited through the ~10-minute cold start...
    assert clock["t"] >= 600
    # ...and did so within the 30-minute budget.
    assert clock["t"] <= sc.SqlClient._WAREHOUSE_START_TIMEOUT_S


def test_warehouse_never_starts_times_out_at_30min(monkeypatch):
    """Warehouse never reaches RUNNING -> raises TimeoutError near 1800s."""
    clock = _install_fake_clock(monkeypatch)
    monkeypatch.setattr(sc.requests, "post", lambda *a, **k: _Resp({}))
    monkeypatch.setattr(sc.requests, "get", lambda *a, **k: _Resp({"state": "STARTING"}))

    c = _make_client()
    with pytest.raises(TimeoutError) as ei:
        c.start_warehouse()

    assert "did not start" in str(ei.value)
    assert "1800" in str(ei.value)
    assert clock["t"] >= sc.SqlClient._WAREHOUSE_START_TIMEOUT_S


def test_timeout_constant_is_30_minutes():
    """Guardrail: the wait budget is 30 min (was 150s), poll every 5s."""
    assert SqlClient._WAREHOUSE_START_TIMEOUT_S == 1800
    assert SqlClient._WAREHOUSE_START_POLL_S == 5
    # A 10-minute cold start (600s) must fit comfortably in the budget,
    # and would have failed under the old 150s limit.
    assert 600 <= SqlClient._WAREHOUSE_START_TIMEOUT_S
    assert 600 > 150
