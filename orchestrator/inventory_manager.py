"""
inventory_manager.py — DESCRIBE DETAIL execution and migration_control upserts.

INVENTORY mode responsibilities:
1. Validate source table existence.
2. Execute DESCRIBE DETAIL (direct_adls only) on the source warehouse.
3. Capture location, sizeInBytes, numFiles, format, Delta version.
4. Classify workload.
5. Idempotently upsert one migration_control record per table.
6. Transition: DISCOVERED → ONBOARDED → WAITING_FOR_LOAD → QUEUED.

Steps 1–4 can be skipped entirely via OrchestratorConfig.skip_describe_detail
(opt-in, default off) — see that field's docstring for when/why. Tables are
still onboarded as QUEUED, just with NULL size/file/version metadata.

This module NEVER executes a clone. It only reads source metadata and
writes to the control table.
"""

from __future__ import annotations
import logging
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from orchestrator.models import (
    MigrationRecord, MigrationStatus, TableSelection, TableInventory, WorkloadClass
)
from orchestrator.config import OrchestratorConfig
from orchestrator.sql_client import SqlClient
from orchestrator.workload_classifier import WorkloadClassifier

log = logging.getLogger(__name__)

_TS = lambda: datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _sql_num(v) -> str:
    """Render an Optional numeric value as a NULL-safe SQL literal (unquoted)."""
    return "NULL" if v is None else str(v)


def _sql_str(v) -> str:
    """Render an Optional string value as a NULL-safe, quote-escaped SQL literal."""
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "\\'") + "'"


class InventoryManager:
    """
    Idempotent inventory: upserts migration_control records.

    Identity key = (source_catalog, source_schema, source_table, batch_id).
    batch_id is a first-class part of that key — NOT just a descriptive
    column — so every batch_id gets its OWN row per source table:
      - Re-running INVENTORY again for the SAME batch_id is idempotent
        (finds and updates that batch's own row; no duplicates).
      - Onboarding the SAME source table under a DIFFERENT/new batch_id
        always creates a brand-new row, leaving every earlier batch's row
        for that table completely untouched — full per-batch history is
        preserved in migration_control itself, not just in the separate
        immutable migration_attempts/migration_validation_history tables.

    Consequently, `force=True` (force_reonboard) is scoped to the CURRENT
    batch_id only: an existing COMPLETED/VALIDATED/FAILED_PERMANENT/SKIPPED
    row for THIS SAME batch_id is not re-onboarded unless `force=True`. A
    genuinely different batch_id is never blocked by this check at all — it
    always proceeds (existing lookup is batch-scoped and finds nothing).
    """

    def __init__(
        self,
        config:       OrchestratorConfig,
        src_sql:      SqlClient,   # for DESCRIBE DETAIL (direct_adls)
        tgt_sql:      SqlClient,   # for control-table reads/writes
        classifier:   WorkloadClassifier,
        run_id:       str,
    ):
        self._cfg       = config
        self._src       = src_sql
        self._tgt       = tgt_sql
        self._cls       = classifier
        self._run_id    = run_id
        self._ctrl      = f"{config.meta_catalog}.{config.meta_schema}.migration_control"

    # ── Main entry ────────────────────────────────────────────────────────────

    def run_inventory(
        self,
        selections: List[TableSelection],
        force:      bool = False,
        require_target_precreated: bool = False,
    ) -> dict:
        """
        Process a list of TableSelections.

        Args:
            require_target_precreated: when True, a table whose
                target_catalog.target_schema.target_table does not already
                exist in the target workspace is still enlisted into
                migration_control, but with status=SKIPPED (error_code
                TARGET_NOT_PRECREATED) instead of QUEUED — it will NOT be
                picked up by DEEP_CLONE (which only ever selects
                status='QUEUED'). Governance mode for scenarios where target
                tables must be pre-provisioned (schema/partitioning/grants)
                by another team before DEEP_CLONE is allowed to land data.
                Default False preserves the original behavior (DEEP_CLONE
                auto-creates the target table via CREATE OR REPLACE ... DEEP
                CLONE — used by every prior test in this repo).

            self._cfg.skip_describe_detail (read directly off config, not a
                parameter here): when True, bypasses DESCRIBE DETAIL entirely
                for every table — see OrchestratorConfig.skip_describe_detail
                for the full rationale (storage firewall / ABAC errors that
                block DESCRIBE DETAIL specifically). Tables are still
                onboarded as QUEUED, but size_in_bytes/size_gb/
                source_num_files/workload_class/workload_weight/
                source_version are all left NULL in migration_control.

        Returns:
            {
              "total":    int,
              "inserted": int,
              "skipped":  int,  # already completed OR target not pre-created
              "skipped_target_missing": int,  # subset of "skipped" specifically
                                               # due to require_target_precreated
              "failed":   int,  # source not found or DESCRIBE error
              "records":  List[MigrationRecord]  # newly onboarded
            }
        """
        stats = {
            "total": 0, "inserted": 0, "skipped": 0,
            "skipped_target_missing": 0, "failed": 0, "records": [],
        }
        for sel in selections:
            stats["total"] += 1
            try:
                rec, target_missing = self._process_one(sel, force, require_target_precreated)
                if rec is None:
                    stats["skipped"] += 1
                    if target_missing:
                        stats["skipped_target_missing"] += 1
                else:
                    stats["inserted"] += 1
                    stats["records"].append(rec)
            except Exception as e:
                stats["failed"] += 1
                log.error("Inventory failed for %s: %s", sel.source_fqn, e)
                self._mark_permanent_failure(sel, str(e))
        return stats

    # ── Per-table logic ───────────────────────────────────────────────────────

    def _process_one(
        self, sel: TableSelection, force: bool, require_target_precreated: bool = False,
    ) -> "tuple[Optional[MigrationRecord], bool]":
        """Returns (record_or_None, target_was_missing)."""
        # Check if already in control table — scoped to (source table,
        # THIS batch_id) so different batches never collide on one row.
        batch_id = getattr(self._cfg, "batch_id", "") or ""
        existing = self._get_existing(sel.source_fqn, batch_id)

        if existing and not force:
            if existing.get("status") in (
                MigrationStatus.COMPLETED.value,
                MigrationStatus.VALIDATED.value,
                MigrationStatus.FAILED_PERMANENT.value,
                MigrationStatus.SKIPPED.value,
            ):
                log.info("Skipping %s — already %s", sel.source_fqn, existing["status"])
                return None, False

        # ── Governance gate: target must already exist ──────────────────────
        # Cheap short-circuit BEFORE touching the source at all — if the
        # target isn't pre-created, there is no point describing the source.
        if require_target_precreated and not self._target_exists(sel):
            msg = (
                f"Target table {sel.target_fqn} does not exist yet in the target "
                f"catalog. Pre-create it (schema/partitioning/grants) so it can be "
                f"queued for DEEP_CLONE, or set require_target_precreated=false to "
                f"let DEEP_CLONE auto-create it."
            )
            log.warning(
                "Target %s not found — marking SKIPPED (require_target_precreated=true)",
                sel.target_fqn,
            )
            self._mark_skipped_target_missing(sel, msg)
            return None, True

        # Validate source existence (UC metadata check via API) — unless the
        # operator has explicitly opted out (skip_describe_detail=True).
        if self._cfg.skip_describe_detail:
            # DESCRIBE DETAIL bypassed entirely for this run — see
            # OrchestratorConfig.skip_describe_detail docstring for when/why
            # (e.g. Azure storage firewall or Delta Share ABAC errors that
            # block DESCRIBE DETAIL specifically). No source existence/format
            # check happens here in this mode — a bad source table will only
            # surface later, as a DEEP_CLONE-time failure instead.
            inv = TableInventory()   # every field defaults to None
            wl_class, wl_weight = WorkloadClass.UNKNOWN.value, self._cfg.workload_weights.get("UNKNOWN", 0)
            log.info(
                "skip_describe_detail=true — onboarding %s without source metadata "
                "(size/files/workload_class left NULL)", sel.source_fqn,
            )
        else:
            inv = self._run_describe_detail(sel)
            if inv is None:
                log.warning("Source table %s not found or not Delta — marking FAILED_PERMANENT", sel.source_fqn)
                self._mark_permanent_failure(sel, "Source table not found or not a Delta table")
                return None, False

            # Classify workload
            wl_class, wl_weight = self._cls.classify(inv.size_in_bytes)

        # Build record. `existing` is already scoped to THIS batch_id (see
        # _get_existing above), so reusing its migration_id here only ever
        # re-touches THIS batch's own row (idempotent re-run of the same
        # batch). A different/new batch_id for the same source table always
        # finds existing=None and gets a fresh migration_id → a brand-new
        # row, leaving every other batch's row for this table untouched.
        mid = (existing or {}).get("migration_id") or str(uuid.uuid4())
        now = _TS()
        rec = MigrationRecord(
            migration_id      = mid,
            run_id            = self._run_id,
            clone_type        = self._cfg.clone_type,
            source_workspace  = self._cfg.source_workspace_url,
            target_workspace  = self._cfg.target_workspace_url,
            source_catalog    = sel.source_catalog,
            source_schema     = sel.source_schema,
            source_table      = sel.source_table,
            target_catalog    = sel.target_catalog,
            target_schema     = sel.target_schema,
            target_table      = sel.target_table,
            source_path       = inv.source_path,
            size_in_bytes     = inv.size_in_bytes,
            size_gb           = (inv.size_in_bytes / (1024 ** 3)) if inv.size_in_bytes is not None else None,
            workload_class    = wl_class,
            workload_weight   = wl_weight,
            status            = MigrationStatus.QUEUED.value,
            attempt_number    = 0,
            max_attempts      = self._cfg.max_retries,
            source_num_files  = inv.num_files,
            source_version    = inv.source_version,
            discovered_at     = now,
            onboarded_at      = now,
            queued_at         = now,
            created_at        = now,
            updated_at        = now,
            # Set batch_id immediately so parallel inventories don't steal each other's records
            batch_id          = getattr(self._cfg, "batch_id", "") or "",
        )

        # Upsert to control table
        self._upsert(rec)
        if rec.size_gb is not None:
            log.info(
                "Onboarded %s → %s [%s %.2f GB w=%d]",
                sel.source_fqn, sel.target_fqn, wl_class, rec.size_gb, wl_weight
            )
        else:
            log.info(
                "Onboarded %s → %s [%s — size unknown, DESCRIBE DETAIL skipped]",
                sel.source_fqn, sel.target_fqn, wl_class,
            )
        return rec, False

    # ── DESCRIBE DETAIL ───────────────────────────────────────────────────────

    def _run_describe_detail(self, sel: TableSelection) -> Optional[TableInventory]:
        """
        Execute DESCRIBE DETAIL on source. For delta_share mode, the source FQN
        is available directly; for direct_adls we need the ADLS path.

        Missed scenario handled:
        - Non-Delta objects (views, foreign tables) → return None → SKIPPED.
        - Empty tables (0 rows/files) → still valid, return inventory with zeros.
        """
        try:
            # Use backtick-quoted per-component to handle catalog names with dashes
            fqn = f"`{sel.source_catalog}`.`{sel.source_schema}`.`{sel.source_table}`"
            rows = self._src.execute(f"DESCRIBE DETAIL {fqn}")
            if not rows:
                return None
            r = rows[0]
            fmt = (r.get("format") or "").upper()
            if fmt != "DELTA":
                log.warning("%s is %s, not Delta — will skip", sel.source_fqn, fmt or "UNKNOWN")
                return None
            return TableInventory(
                source_path    = r.get("location") or "",
                size_in_bytes  = int(r.get("sizeInBytes") or 0),
                size_gb        = int(r.get("sizeInBytes") or 0) / (1024 ** 3),
                num_files      = int(r.get("numFiles") or 0),
                format         = "DELTA",
                source_version = self._get_delta_version(sel),
                created_at     = str(r.get("createdAt") or ""),
                last_modified  = str(r.get("lastModified") or ""),
                is_delta       = True,
            )
        except RuntimeError as e:
            err = str(e)
            if "TABLE_OR_VIEW_NOT_FOUND" in err or "SCHEMA_NOT_FOUND" in err:
                return None
            raise

    def _get_delta_version(self, sel: TableSelection) -> Optional[int]:
        """Get current Delta version via DESCRIBE HISTORY LIMIT 1."""
        try:
            fqn = f"`{sel.source_catalog}`.`{sel.source_schema}`.`{sel.source_table}`"
            rows = self._src.execute(f"DESCRIBE HISTORY {fqn} LIMIT 1")
            if rows:
                return int(rows[0].get("version") or 0)
        except Exception:
            pass
        return None

    # ── Target pre-existence gate (require_target_precreated) ──────────────────

    def _target_exists(self, sel: TableSelection) -> bool:
        """
        True iff target_catalog.target_schema.target_table already exists in
        the target workspace. Uses a lightweight DESCRIBE TABLE (no size
        stats needed — this is purely an existence probe) against tgt_sql.

        Any *_NOT_FOUND error (table, schema, or catalog missing) is treated
        as "doesn't exist yet". Any other error (e.g. a permission problem)
        is NOT swallowed — it propagates so it surfaces as a real inventory
        failure instead of being silently misclassified as "missing".
        """
        fqn = f"`{sel.target_catalog}`.`{sel.target_schema}`.`{sel.target_table}`"
        try:
            self._tgt.execute(f"DESCRIBE TABLE {fqn}")
            return True
        except RuntimeError as e:
            err = str(e)
            if any(code in err for code in (
                "TABLE_OR_VIEW_NOT_FOUND", "SCHEMA_NOT_FOUND", "CATALOG_NOT_FOUND",
            )):
                return False
            raise

    def _mark_skipped_target_missing(self, sel: TableSelection, msg: str) -> None:
        """
        Enlist sel into migration_control with status=SKIPPED (terminal,
        never picked up by DEEP_CLONE/RETRY — both only ever select
        status='QUEUED') and error_code=TARGET_NOT_PRECREATED, instead of
        the generic FAILED_PERMANENT used by _mark_permanent_failure().

        Modeled closely on _mark_permanent_failure()'s MERGE — same minimal
        column set (no source DESCRIBE DETAIL was run, since the whole point
        of checking the target FIRST is to avoid that round-trip when we're
        going to skip anyway).
        """
        now = _TS()
        msg_esc = msg.replace("'", "\\'")[:500]
        mid = str(uuid.uuid4())
        batch_id_val = (getattr(self._cfg, "batch_id", "") or "").replace("'", "\\'")
        try:
            # Same batch_id-scoped identity key as _upsert()/_get_existing()/
            # _mark_permanent_failure() — a table SKIPPED under batch_id=A
            # must not collide with / block onboarding of the same table
            # under a different batch_id=B (that new batch gets its own row,
            # unaffected by this one being SKIPPED).
            q = f"""
            MERGE INTO {self._ctrl} AS t
            USING (SELECT '{sel.source_catalog}' AS sc, '{sel.source_schema}' AS ss,
                          '{sel.source_table}' AS st, '{batch_id_val}' AS bid) AS s
            ON t.source_catalog = s.sc AND t.source_schema = s.ss
               AND t.source_table = s.st AND t.batch_id = s.bid
            WHEN MATCHED THEN UPDATE SET
              status = 'SKIPPED', error_code = 'TARGET_NOT_PRECREATED',
              error_message = '{msg_esc}', batch_id = '{batch_id_val}',
              -- Refresh the target mapping too — the row may pre-date this
              -- CSV/YAML's current target mapping (e.g. an earlier batch
              -- pointed this same source table at a different target).
              -- Without this, the persisted row would show a stale/unrelated
              -- target_catalog/schema/table while error_message (built fresh
              -- from sel.target_fqn) correctly names the CURRENT target —
              -- a misleading mismatch for anyone auditing this row.
              target_catalog   = '{sel.target_catalog}',
              target_schema    = '{sel.target_schema}',
              target_table     = '{sel.target_table}',
              -- Same rationale as _upsert()'s reset-on-reonboard block: if
              -- this source table was previously COMPLETED/VALIDATED against
              -- a DIFFERENT target (e.g. an earlier batch/CSV mapping), those
              -- stale validation_status='VALIDATED'/row-count/timestamp
              -- values must NOT survive onto this SKIPPED row — otherwise a
              -- table that was never cloned under THIS mapping would
              -- misleadingly show as already validated.
              validation_status = NULL,
              source_row_count  = NULL,
              target_row_count  = NULL,
              started_at        = NULL,
              completed_at      = NULL,
              failed_at         = NULL,
              updated_at = TIMESTAMP '{now}'
            WHEN NOT MATCHED THEN INSERT (
              migration_id, run_id, clone_type, source_workspace, target_workspace,
              source_catalog, source_schema, source_table,
              target_catalog, target_schema, target_table,
              status, error_code, error_message, batch_id,
              discovered_at, created_at, updated_at
            ) VALUES (
              '{mid}', '{self._run_id}', '{self._cfg.clone_type}',
              '{self._cfg.source_workspace_url}', '{self._cfg.target_workspace_url}',
              '{sel.source_catalog}', '{sel.source_schema}', '{sel.source_table}',
              '{sel.target_catalog}', '{sel.target_schema}', '{sel.target_table}',
              'SKIPPED', 'TARGET_NOT_PRECREATED', '{msg_esc}', '{batch_id_val}',
              TIMESTAMP '{now}', TIMESTAMP '{now}', TIMESTAMP '{now}'
            )
            """
            self._tgt.execute_ddl(q)
        except Exception as e:
            log.error("Could not record SKIPPED (target missing) for %s: %s", sel.source_fqn, e)

    # ── Control table operations ──────────────────────────────────────────────

    def _get_existing(self, source_fqn: str, batch_id: str) -> Optional[dict]:
        """
        Look up an existing migration_control row for this EXACT
        (source table, batch_id) pair.

        batch_id is part of the identity key here — deliberately. Each
        batch_id gets its own row per source table, so that:
          - Re-running INVENTORY twice for the SAME batch_id is idempotent
            (finds and updates its own row, doesn't duplicate it).
          - Onboarding the SAME source table under a DIFFERENT/new batch_id
            always finds nothing here (existing=None) and therefore always
            gets a brand-new row + a brand-new migration_id in
            _process_one() — preserving that earlier batch's row (and its
            batch_id, timestamps, row counts, error info) untouched forever.
            Without batch_id in this lookup, a second batch touching the
            same table would find and silently overwrite the first batch's
            row (see git history for the pre-fix behavior) — the very bug
            this scoping fixes.
        """
        parts = source_fqn.split(".")
        if len(parts) != 3:
            return None
        cat, sch, tbl = parts
        safe_bid = (batch_id or "").replace("'", "\\'")
        try:
            rows = self._tgt.execute(f"""
                SELECT migration_id, status, attempt_number
                FROM {self._ctrl}
                WHERE source_catalog = '{cat}'
                  AND source_schema  = '{sch}'
                  AND source_table   = '{tbl}'
                  AND batch_id       = '{safe_bid}'
                LIMIT 1
            """)
            return rows[0] if rows else None
        except Exception:
            return None

    def _upsert(self, rec: MigrationRecord) -> None:
        """
        MERGE into migration_control.

        Identity key is (source_catalog, source_schema, source_table,
        batch_id) — NOT bare migration_id. This is the authoritative
        enforcement of "one row per table PER BATCH": even if Python-side
        migration_id resolution in _process_one() ever got out of sync,
        this MERGE's own match condition still guarantees a different
        batch_id can never collide with / overwrite another batch's row for
        the same source table — it will always fall to the INSERT branch
        and create a new row instead.
        """
        loc = _sql_str(rec.source_path)
        err_msg = (rec.error_message or "").replace("'", "\\'")
        batch_id_val = (rec.batch_id or "").replace("'", "\\'")
        # NULL-safe: when onboarded with skip_describe_detail=True, these are
        # all None on `rec` — must persist as real SQL NULL (not 0/"None"),
        # so migration_control can distinguish "never measured" from
        # "measured as zero". See OrchestratorConfig.skip_describe_detail.
        size_bytes_sql = _sql_num(rec.size_in_bytes)
        size_gb_sql    = "NULL" if rec.size_gb is None else f"{rec.size_gb:.6f}"
        wl_class_sql   = _sql_str(rec.workload_class)
        wl_weight_sql  = _sql_num(rec.workload_weight)
        num_files_sql  = _sql_num(rec.source_num_files)
        version_sql    = _sql_num(rec.source_version)
        q = f"""
        MERGE INTO {self._ctrl} AS t
        USING (SELECT '{rec.source_catalog}' AS sc, '{rec.source_schema}' AS ss,
                      '{rec.source_table}' AS st, '{batch_id_val}' AS bid) AS s
        ON t.source_catalog = s.sc AND t.source_schema = s.ss
           AND t.source_table = s.st AND t.batch_id = s.bid
        WHEN MATCHED THEN UPDATE SET
          run_id            = '{rec.run_id}',
          status            = '{rec.status}',
          source_path       = {loc},
          size_in_bytes     = {size_bytes_sql},
          size_gb           = {size_gb_sql},
          workload_class    = {wl_class_sql},
          workload_weight   = {wl_weight_sql},
          attempt_number    = 0,
          max_attempts      = {rec.max_attempts},
          source_num_files  = {num_files_sql},
          source_version    = {version_sql},
          batch_id          = '{batch_id_val}',
          -- Target mapping can legitimately change between onboards (e.g. a
          -- CSV/YAML edit renames the target table) — without updating these,
          -- a re-onboarded (esp. force_reonboard=true) row silently kept
          -- whichever target_catalog/schema/table it was FIRST inserted
          -- with, ignoring the current CSV/YAML's mapping.
          target_catalog    = '{rec.target_catalog}',
          target_schema     = '{rec.target_schema}',
          target_table      = '{rec.target_table}',
          -- Re-onboarding resets this row to a genuinely fresh QUEUED state —
          -- clear out any started_at/completed_at/failed_at/error_code/
          -- error_message left over from a PREVIOUS run's abandoned attempt
          -- (e.g. a stale IN_PROGRESS/ASSIGNED record reconciled by
          -- reconcile_stale_records()). Without this, a table that is now
          -- freshly QUEUED under a brand-new batch_id kept showing hours-old
          -- "STALE_EXECUTION" error info from an unrelated past run, even
          -- though nothing is currently wrong with it.
          started_at        = NULL,
          completed_at      = NULL,
          failed_at         = NULL,
          error_code        = NULL,
          error_message     = NULL,
          -- validation_status/row counts also MUST be cleared on re-onboard:
          -- mark_validated() sets validation_status='VALIDATED' and stamps
          -- source_row_count/target_row_count once. The VALIDATE loop
          -- (orchestrator_notebook.py) explicitly SKIPS any record whose
          -- validation_status is already 'VALIDATED' (it assumes that means
          -- "already checked, nothing changed"). But a force_reonboard re-run
          -- physically re-clones the table — the old VALIDATED verdict and
          -- row counts are stale and must not be trusted (or displayed) until
          -- VALIDATE genuinely re-checks the fresh clone.
          validation_status = NULL,
          source_row_count  = NULL,
          target_row_count  = NULL,
          onboarded_at      = TIMESTAMP '{rec.onboarded_at}',
          queued_at         = TIMESTAMP '{rec.queued_at}',
          updated_at        = TIMESTAMP '{rec.updated_at}'
        WHEN NOT MATCHED THEN INSERT (
          migration_id, run_id, clone_type,
          source_workspace, target_workspace,
          source_catalog, source_schema, source_table,
          target_catalog, target_schema, target_table,
          source_path, size_in_bytes, size_gb,
          workload_class, workload_weight,
          status, attempt_number, max_attempts,
          source_num_files, source_version,
          batch_id,
          discovered_at, onboarded_at, queued_at,
          created_at, updated_at
        ) VALUES (
          '{rec.migration_id}', '{rec.run_id}', '{rec.clone_type}',
          '{rec.source_workspace}', '{rec.target_workspace}',
          '{rec.source_catalog}', '{rec.source_schema}', '{rec.source_table}',
          '{rec.target_catalog}', '{rec.target_schema}', '{rec.target_table}',
          {loc}, {size_bytes_sql}, {size_gb_sql},
          {wl_class_sql}, {wl_weight_sql},
          '{rec.status}', 0, {rec.max_attempts},
          {num_files_sql}, {version_sql},
          '{batch_id_val}',
          TIMESTAMP '{rec.discovered_at}', TIMESTAMP '{rec.onboarded_at}', TIMESTAMP '{rec.queued_at}',
          TIMESTAMP '{rec.created_at}', TIMESTAMP '{rec.updated_at}'
        )
        """
        self._tgt.execute_ddl(q)

    def _mark_permanent_failure(self, sel: TableSelection, msg: str) -> None:
        """
        Same batch_id-scoped identity key as _upsert()/_get_existing() —
        a source table that fails permanently under batch_id=A must not
        collide with / block a later attempt of the same table under a
        different batch_id=B. Also now stamps batch_id on both the UPDATE
        and INSERT branches (previously omitted entirely, which is why a
        FAILED_PERMANENT row never carried any batch_id at all).
        """
        now = _TS()
        msg_esc = msg.replace("'", "\\'")[:500]
        mid = str(uuid.uuid4())
        batch_id_val = (getattr(self._cfg, "batch_id", "") or "").replace("'", "\\'")
        try:
            q = f"""
            MERGE INTO {self._ctrl} AS t
            USING (SELECT '{sel.source_catalog}' AS sc, '{sel.source_schema}' AS ss,
                          '{sel.source_table}' AS st, '{batch_id_val}' AS bid) AS s
            ON t.source_catalog = s.sc AND t.source_schema = s.ss
               AND t.source_table = s.st AND t.batch_id = s.bid
            WHEN MATCHED THEN UPDATE SET
              status = 'FAILED_PERMANENT', error_message = '{msg_esc}',
              updated_at = TIMESTAMP '{now}'
            WHEN NOT MATCHED THEN INSERT (
              migration_id, run_id, clone_type, source_workspace, target_workspace,
              source_catalog, source_schema, source_table,
              target_catalog, target_schema, target_table,
              batch_id, status, error_message, created_at, updated_at
            ) VALUES (
              '{mid}', '{self._run_id}', '{self._cfg.clone_type}',
              '{self._cfg.source_workspace_url}', '{self._cfg.target_workspace_url}',
              '{sel.source_catalog}', '{sel.source_schema}', '{sel.source_table}',
              '{sel.target_catalog}', '{sel.target_schema}', '{sel.target_table}',
              '{batch_id_val}', 'FAILED_PERMANENT', '{msg_esc}',
              TIMESTAMP '{now}', TIMESTAMP '{now}'
            )
            """
            self._tgt.execute_ddl(q)
        except Exception as e:
            log.error("Could not record permanent failure for %s: %s", sel.source_fqn, e)
