"""
api_client.py — Databricks REST API client (clusters, jobs, Unity Catalog).

Authentication
--------------
Uses the Databricks SDK's native/unified authentication (`databricks.sdk.core.Config`)
— the same mechanism as `sql_client.SqlClient`. No client_id/client_secret or
Databricks Secret scope is required: when running inside a Databricks job/
notebook, `Config()` auto-detects the run's own native auth context.

Covers
------
• UC Tables API (list tables with full pagination)
• Cluster state queries
• Jobs Run Submit (one-off worker runs)
• Jobs Run Get (poll worker status)
• Jobs Run Cancel
"""

from __future__ import annotations
import time
import logging
from typing import Any, Dict, Generator, List, Optional

import requests

from databricks.sdk.core import Config

log = logging.getLogger(__name__)


class ApiError(RuntimeError):
    """
    RuntimeError subclass raised on non-2xx responses that preserves the
    HTTP status code and (truncated) response body, so callers/logs can see
    the *real* Databricks error instead of the opaque message from a bare
    requests.Response.raise_for_status() call.
    """

    def __init__(self, status_code: int, path: str, body: str):
        self.status_code = status_code
        self.response_text = body
        super().__init__(f"{status_code} {path}: {body[:2000]}")


class ApiClient:
    """
    Thin wrapper around the Databricks REST APIs needed by the orchestrator.
    Auth comes from `databricks.sdk.core.Config` — no secrets to manage.
    """

    def __init__(self, workspace_url: str = ""):
        self._cfg = Config(host=workspace_url) if workspace_url else Config()
        self._url = self._cfg.host.rstrip("/")

    # ── Internal ──────────────────────────────────────────────────────────────

    def _h(self) -> Dict[str, str]:
        headers = self._cfg.authenticate()
        headers["Content-Type"] = "application/json"
        return headers

    def _get(self, path: str, params: Optional[Dict] = None, timeout: int = 30) -> Dict:
        r = requests.get(f"{self._url}{path}", headers=self._h(), params=params, timeout=timeout)
        if r.status_code == 429:
            log.warning("Rate limited on GET %s — sleeping 30s", path)
            time.sleep(30)
            r = requests.get(f"{self._url}{path}", headers=self._h(), params=params, timeout=timeout)
        if not r.ok:
            raise ApiError(r.status_code, path, r.text)
        return r.json()

    def _post(self, path: str, body: Dict, timeout: int = 60) -> Dict:
        r = requests.post(f"{self._url}{path}", headers=self._h(), json=body, timeout=timeout)
        if not r.ok:
            raise ApiError(r.status_code, path, r.text)
        return r.json()

    # ── Unity Catalog — Tables API ────────────────────────────────────────────

    def list_schemas(self, catalog_name: str) -> List[Dict]:
        """List all schemas in a catalog (paginated)."""
        schemas = []
        token: Optional[str] = None
        while True:
            params: Dict[str, Any] = {"catalog_name": catalog_name, "max_results": 50}
            if token:
                params["page_token"] = token
            d = self._get("/api/2.1/unity-catalog/schemas", params=params)
            schemas.extend(d.get("schemas", []))
            token = d.get("next_page_token")
            if not token:
                break
        return schemas

    def list_tables(self, catalog_name: str, schema_name: str) -> List[Dict]:
        """List all tables in a schema (paginated)."""
        tables = []
        token: Optional[str] = None
        while True:
            params: Dict[str, Any] = {
                "catalog_name": catalog_name,
                "schema_name":  schema_name,
                "max_results":  200,
            }
            if token:
                params["page_token"] = token
            d = self._get("/api/2.1/unity-catalog/tables", params=params)
            tables.extend(d.get("tables", []))
            token = d.get("next_page_token")
            if not token:
                break
        return tables

    def get_table(self, full_name: str) -> Optional[Dict]:
        """Get a single table's UC metadata."""
        try:
            return self._get(f"/api/2.1/unity-catalog/tables/{full_name}")
        except ApiError as e:
            if e.status_code == 404:
                return None
            raise

    # ── Cluster management ────────────────────────────────────────────────────

    def get_cluster_state(self, cluster_id: str) -> str:
        """Return cluster state string (RUNNING / TERMINATED / etc.)."""
        try:
            d = self._get("/api/2.0/clusters/get", params={"cluster_id": cluster_id})
            return d.get("state", "UNKNOWN")
        except Exception as e:
            log.warning("Could not get cluster %s state: %s", cluster_id, e)
            return "UNKNOWN"

    def is_cluster_available(self, cluster_id: str) -> bool:
        return self.get_cluster_state(cluster_id) == "RUNNING"

    # ── Jobs — Run Submit (ephemeral per-table worker run) ────────────────────

    def submit_worker_run(
        self,
        cluster_id:        str,
        notebook_path:     str,
        migration_id:      str,
        meta_catalog:      str,
        meta_schema:       str,
        run_name:          str = "migration-worker",
        timeout_minutes:   int = 10080,   # 7 days (fallback; real value = cfg.worker_timeout_minutes)
        new_cluster_config: Optional[Dict] = None,
        extra_params:       Optional[Dict] = None,
    ) -> int:
        """
        Submit a one-time notebook task.

        Priority:
          1. new_cluster_config (ephemeral job cluster) — best for production/DAB
          2. existing_cluster_id — when a running interactive cluster is provided
             (cluster_id == "" or "auto" → new_cluster_config must be provided)

        extra_params: additional notebook base_parameters (e.g. batch_id, chunk_id
                      for the chunk_worker_notebook).

        Returns the Databricks run_id.
        """
        base_params: Dict[str, str] = {}
        if migration_id:
            base_params["migration_id"] = migration_id
        base_params["meta_catalog"] = meta_catalog
        base_params["meta_schema"]  = meta_schema
        if extra_params:
            base_params.update({k: str(v) for k, v in extra_params.items()})

        if new_cluster_config:
            # Ephemeral job cluster — no cluster_id required
            cluster_spec = {"new_cluster": new_cluster_config}
        elif cluster_id and cluster_id not in ("", "auto"):
            cluster_spec = {"existing_cluster_id": cluster_id}
        else:
            raise ValueError(
                "Either new_cluster_config or a valid existing cluster_id must be provided"
            )

        body = {
            "run_name":       run_name,
            "notebook_task":  {
                "notebook_path":   notebook_path,
                "base_parameters": base_params,
            },
            "timeout_seconds": timeout_minutes * 60,
            **cluster_spec,
        }
        d = self._post("/api/2.1/jobs/runs/submit", body, timeout=30)
        return d["run_id"]

    def get_run_output(self, run_id: int) -> str:
        """Return the notebook output message for a completed run."""
        try:
            d = self._get(f"/api/2.1/jobs/runs/get-output", params={"run_id": run_id})
            return d.get("notebook_output", {}).get("result", "")
        except Exception as e:
            log.warning("Could not fetch run output for %d: %s", run_id, e)
            return ""

    def get_run_state(self, run_id: int) -> Dict[str, Any]:
        """
        Returns dict with 'life_cycle_state', 'result_state', 'state_message'.
        life_cycle_state: PENDING | RUNNING | TERMINATING | TERMINATED | SKIPPED | INTERNAL_ERROR
        result_state:     SUCCESS | FAILED | TIMEDOUT | CANCELED
        """
        d = self._get(f"/api/2.1/jobs/runs/get", params={"run_id": run_id})
        state = d.get("state", {})
        return {
            "life_cycle_state": state.get("life_cycle_state", ""),
            "result_state":     state.get("result_state", ""),
            "state_message":    state.get("state_message", ""),
            "run_page_url":     d.get("run_page_url", ""),
        }

    def cancel_run(self, run_id: int) -> None:
        """Cancel a running job run."""
        try:
            self._post("/api/2.1/jobs/runs/cancel", {"run_id": run_id})
        except Exception as e:
            log.warning("Could not cancel run %s: %s", run_id, e)

    def poll_run_until_done(
        self,
        run_id:       int,
        interval_s:   int = 30,
        timeout_s:    int = 604800,   # 7 days (fallback; real value = cfg.worker_timeout_minutes * 60)
    ) -> Dict[str, Any]:
        """
        Block until the run reaches a terminal life_cycle_state.
        Returns the final run state dict.
        """
        deadline = time.time() + timeout_s
        while True:
            state = self.get_run_state(run_id)
            lc = state["life_cycle_state"]
            if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                # Enrich with notebook output for chunk result parsing
                state["notebook_output"] = {"result": self.get_run_output(run_id)}
                return state
            if time.time() > deadline:
                self.cancel_run(run_id)
                raise TimeoutError(f"Run {run_id} did not complete within {timeout_s}s")
            time.sleep(interval_s)
