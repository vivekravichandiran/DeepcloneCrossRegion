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

    def __init__(self, payload, status_code=200):
        self._p = payload
        self.status_code = status_code
        self.ok = 200 <= status_code < 400
        self.headers = {}
        self.text = ""

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

    def fake_request(method, url, *a, **k):
        if method == "POST":            # /start
            return _Resp({})
        # GET /warehouses/{id}: not RUNNING until 10 min (600s) simulated.
        state = "RUNNING" if clock["t"] >= 600 else "STARTING"
        return _Resp({"state": state})

    monkeypatch.setattr(sc.requests, "request", fake_request)

    c = _make_client()
    c.start_warehouse()   # should return cleanly, not raise

    # Proves it actually waited through the ~10-minute cold start...
    assert clock["t"] >= 600
    # ...and did so within the 30-minute budget.
    assert clock["t"] <= sc.SqlClient._WAREHOUSE_START_TIMEOUT_S


def test_warehouse_never_starts_times_out_at_30min(monkeypatch):
    """Warehouse never reaches RUNNING -> raises TimeoutError near 1800s."""
    clock = _install_fake_clock(monkeypatch)

    def fake_request(method, url, *a, **k):
        return _Resp({}) if method == "POST" else _Resp({"state": "STARTING"})

    monkeypatch.setattr(sc.requests, "request", fake_request)

    c = _make_client()
    with pytest.raises(TimeoutError) as ei:
        c.start_warehouse()

    assert "did not start" in str(ei.value)
    assert "1800" in str(ei.value)
    assert clock["t"] >= sc.SqlClient._WAREHOUSE_START_TIMEOUT_S


def test_submit_retries_on_transient_500_then_succeeds(monkeypatch):
    """Regression: a POST /api/2.0/sql/statements that returns a transient
    500 INTERNAL_ERROR must be retried (with backoff), not raised on the first
    attempt. Two 500s followed by a 200 must ultimately succeed."""
    monkeypatch.setattr(sc.time, "sleep", lambda s: None)   # no real backoff waits

    calls = {"n": 0}

    def fake_request(method, url, *a, **k):
        calls["n"] += 1
        if calls["n"] <= 2:
            return _Resp(
                {"error_code": "INTERNAL_ERROR", "message": "unexpected condition"},
                status_code=500,
            )
        return _Resp({"statement_id": "stmt-123", "status": {"state": "SUCCEEDED"}})

    monkeypatch.setattr(sc.requests, "request", fake_request)

    c = _make_client()
    stmt_id, payload = c._submit("CREATE SCHEMA IF NOT EXISTS a.b")
    assert stmt_id == "stmt-123"
    assert calls["n"] == 3            # 2 transient failures + 1 success


def test_submit_raises_after_exhausting_retries_on_persistent_500(monkeypatch):
    """A persistently failing 500 raises only AFTER _MAX_RETRIES attempts."""
    monkeypatch.setattr(sc.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def fake_request(method, url, *a, **k):
        calls["n"] += 1
        return _Resp({"error_code": "INTERNAL_ERROR"}, status_code=500)

    monkeypatch.setattr(sc.requests, "request", fake_request)

    c = _make_client()
    with pytest.raises(RuntimeError) as ei:
        c._submit("SELECT 1")
    assert calls["n"] == sc.SqlClient._MAX_RETRIES
    assert "after" in str(ei.value).lower()


def test_submit_does_not_retry_non_transient_4xx(monkeypatch):
    """A non-transient 400 (e.g. bad SQL) is returned/raised immediately —
    retrying a client error would be pointless."""
    monkeypatch.setattr(sc.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def fake_request(method, url, *a, **k):
        calls["n"] += 1
        return _Resp({"error_code": "BAD_REQUEST"}, status_code=400)

    monkeypatch.setattr(sc.requests, "request", fake_request)

    c = _make_client()
    with pytest.raises(RuntimeError):
        c._submit("SELCT 1")          # typo -> 400
    assert calls["n"] == 1            # NOT retried


def test_timeout_constant_is_30_minutes():
    """Guardrail: the wait budget is 30 min (was 150s), poll every 5s."""
    assert SqlClient._WAREHOUSE_START_TIMEOUT_S == 1800
    assert SqlClient._WAREHOUSE_START_POLL_S == 5
    # A 10-minute cold start (600s) must fit comfortably in the budget,
    # and would have failed under the old 150s limit.
    assert 600 <= SqlClient._WAREHOUSE_START_TIMEOUT_S
    assert 600 > 150
