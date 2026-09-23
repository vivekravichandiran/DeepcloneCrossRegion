"""
sql_client.py — SQL Statement Execution API client.

Authentication
--------------
Uses the Databricks SDK's native/unified authentication (`databricks.sdk.core.Config`)
instead of a hand-rolled OAuth client_id/client_secret flow. When this code runs
inside a Databricks job/notebook (the only place it ever runs in production),
`Config()` automatically picks up the run's own native auth context — there is
NO client_id, client_secret, or Databricks Secret scope to provision or manage.
`workspace_url` is optional (blank = current/attached workspace, auto-detected).

Features
--------
• Polls statement until SUCCEEDED / FAILED with configurable timeout.
• Returns typed rows as List[Dict[str, Any]].
• Separate source and target clients, each with their own Config/auth context.
• Rate-limit awareness (backs off on 429).
"""

from __future__ import annotations
import time
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests
from databricks.sdk.core import Config

log = logging.getLogger(__name__)


class SqlClient:
    """
    Wraps the Databricks SQL Statement Execution API.

    Usage
    -----
    client = SqlClient(warehouse_id=warehouse_id)                 # current workspace (default)
    client = SqlClient(workspace_url=url, warehouse_id=wh_id)     # explicit workspace override
    rows   = client.execute("SELECT * FROM t WHERE status = 'PENDING'")
    client.execute_ddl("CREATE SCHEMA IF NOT EXISTS cat.sch")
    """

    _POLL_INTERVAL_S = 2
    _MAX_WAIT_S      = 300   # 5 minutes per statement

    # Warehouse cold-start can be slow; wait up to 30 minutes for RUNNING.
    _WAREHOUSE_START_TIMEOUT_S = 1800  # 30 minutes
    _WAREHOUSE_START_POLL_S    = 5

    def __init__(
        self,
        warehouse_id:   str,
        workspace_url:  str = "",
        throttle_s:     float = 0.2,
    ):
        # Config() with no host auto-detects the current Databricks workspace
        # (via the notebook/job's own runtime auth) and requires zero secrets.
        # Passing an explicit workspace_url is only meaningful when combined
        # with a standard Databricks SDK auth env var / CLI profile set
        # externally by the caller (DATABRICKS_HOST/TOKEN/CLIENT_ID/... —
        # none of which this codebase provisions or stores itself).
        self._cfg  = Config(host=workspace_url) if workspace_url else Config()
        self._url  = self._cfg.host.rstrip("/")
        self._wh   = warehouse_id
        self._throttle = throttle_s

    # ── Public API ────────────────────────────────────────────────────────────

    def execute(self, sql: str, timeout_s: int = 300) -> List[Dict[str, Any]]:
        """
        Execute a SQL statement. Returns list of rows as dicts.
        For DDL / DML with no result set, returns [].
        Raises RuntimeError on failure.
        Handles multi-chunk (paginated) results transparently.
        """
        if self._throttle:
            time.sleep(self._throttle)

        stmt_id, initial = self._submit(sql)

        state = initial.get("status", {}).get("state", "")
        data  = initial

        deadline = time.time() + timeout_s
        while state not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            if time.time() > deadline:
                self._cancel(stmt_id)
                raise TimeoutError(f"SQL statement timed out after {timeout_s}s: {sql[:80]}")
            time.sleep(self._POLL_INTERVAL_S)
            data  = self._poll(stmt_id)
            state = data.get("status", {}).get("state", "")

        if state == "SUCCEEDED":
            return self._extract_rows_paginated(stmt_id, data)

        err = data.get("status", {}).get("error", {})
        raise RuntimeError(
            f"[{err.get('error_code','SQL_ERROR')}] {err.get('message','Unknown SQL error')} "
            f"| SQL: {sql[:120]}"
        )

    def execute_ddl(self, sql: str) -> None:
        """Execute DDL / DML. Swallows the empty row result."""
        self.execute(sql)

    def execute_one(self, sql: str) -> Optional[Dict[str, Any]]:
        """Execute SQL that should return exactly one row. Returns None if empty."""
        rows = self.execute(sql)
        return rows[0] if rows else None

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _headers(self) -> Dict[str, str]:
        headers = self._cfg.authenticate()
        headers["Content-Type"] = "application/json"
        return headers

    def _submit(self, sql: str) -> Tuple[str, Dict]:
        for attempt in range(3):
            resp = requests.post(
                f"{self._url}/api/2.0/sql/statements",
                headers=self._headers(),
                json={
                    "statement":       sql,
                    "warehouse_id":    self._wh,
                    "wait_timeout":    "50s",
                    "on_wait_timeout": "CONTINUE",
                },
                timeout=65,
            )
            if resp.status_code == 429:
                log.warning("Rate limited — backing off 30s")
                time.sleep(30)
                continue
            if not resp.ok:
                raise RuntimeError(
                    f"{resp.status_code} POST /api/2.0/sql/statements: {resp.text[:2000]}"
                )
            d = resp.json()
            return d["statement_id"], d
        raise RuntimeError("Failed to submit SQL after 3 retries (rate limited)")

    def _poll(self, stmt_id: str) -> Dict:
        resp = requests.get(
            f"{self._url}/api/2.0/sql/statements/{stmt_id}",
            headers=self._headers(),
            timeout=30,
        )
        if not resp.ok:
            raise RuntimeError(
                f"{resp.status_code} GET /api/2.0/sql/statements/{stmt_id}: {resp.text[:2000]}"
            )
        return resp.json()

    def _cancel(self, stmt_id: str) -> None:
        try:
            requests.post(
                f"{self._url}/api/2.0/sql/statements/{stmt_id}/cancel",
                headers=self._headers(),
                timeout=15,
            )
        except Exception:
            pass

    @staticmethod
    def _extract_rows(data: Dict) -> List[Dict[str, Any]]:
        """Extract rows from the first chunk only (kept for legacy callers)."""
        result = data.get("result", {})
        if not result.get("data_array"):
            return []
        cols = [c["name"] for c in data["manifest"]["schema"]["columns"]]
        return [dict(zip(cols, row)) for row in result["data_array"]]

    def _extract_rows_paginated(self, stmt_id: str, data: Dict) -> List[Dict[str, Any]]:
        """
        Extract ALL rows across multiple result chunks.

        The SQL Statement Execution API paginates large result sets into chunks.
        Each chunk is fetched via:
          GET /api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}
        """
        manifest = data.get("manifest", {})
        cols = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
        if not cols:
            return []

        total_chunks = manifest.get("total_chunk_count", 1)
        rows: List[Dict[str, Any]] = []

        # First chunk is embedded in the initial response
        first_result = data.get("result", {})
        if first_result.get("data_array"):
            rows.extend(dict(zip(cols, row)) for row in first_result["data_array"])

        # Fetch remaining chunks if any
        for chunk_idx in range(1, total_chunks):
            resp = requests.get(
                f"{self._url}/api/2.0/sql/statements/{stmt_id}/result/chunks/{chunk_idx}",
                headers=self._headers(),
                timeout=60,
            )
            if not resp.ok:
                raise RuntimeError(
                    f"{resp.status_code} GET /api/2.0/sql/statements/{stmt_id}/result/chunks/{chunk_idx}: "
                    f"{resp.text[:2000]}"
                )
            chunk = resp.json()
            chunk_data = chunk.get("data_array", [])
            if chunk_data:
                rows.extend(dict(zip(cols, row)) for row in chunk_data)
            log.debug("Fetched chunk %d/%d — %d rows", chunk_idx + 1, total_chunks, len(chunk_data))

        return rows

    # ── Warehouse management ──────────────────────────────────────────────────

    def start_warehouse(self) -> None:
        """Start the warehouse if not already RUNNING."""
        requests.post(
            f"{self._url}/api/2.0/sql/warehouses/{self._wh}/start",
            headers=self._headers(),
            timeout=30,
        )
        log.info("Warehouse %s start requested", self._wh)
        # A cold/auto-stopped warehouse (esp. serverless with capacity waits or
        # a classic warehouse that must provision clusters) can take many
        # minutes to reach RUNNING. Poll for up to 30 minutes so a slow start
        # doesn't fail the run prematurely.
        deadline = time.time() + self._WAREHOUSE_START_TIMEOUT_S
        while time.time() < deadline:
            r = requests.get(
                f"{self._url}/api/2.0/sql/warehouses/{self._wh}",
                headers=self._headers(),
                timeout=20,
            ).json()
            if r.get("state") == "RUNNING":
                log.info("Warehouse %s is RUNNING", self._wh)
                return
            time.sleep(self._WAREHOUSE_START_POLL_S)
        raise TimeoutError(
            f"Warehouse {self._wh} did not start within "
            f"{self._WAREHOUSE_START_TIMEOUT_S}s"
        )
