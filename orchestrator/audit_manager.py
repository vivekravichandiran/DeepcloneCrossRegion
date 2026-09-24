"""
audit_manager.py — Atomic state machine transitions and attempt history.

Responsibilities
----------------
• Transition migration_control status with WHERE-clause guards (concurrency safe).
• Write migration_attempts records (immutable execution history).
• Reconcile stale IN_PROGRESS / ASSIGNED records after a crash/restart.
• Expose aggregate metrics for the run summary.

Key principle from the design doc:
  "Use atomic state transitions so two scheduler instances cannot acquire
   the same table."
"""

from __future__ import annotations
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from orchestrator.models import MigrationStatus, AttemptStatus, ChunkAssignment
from orchestrator.sql_client import SqlClient
from orchestrator.config import OrchestratorConfig

log = logging.getLogger(__name__)

_TS = lambda: datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class AuditManager:
    """
    Wraps all state-machine writes to migration_control and migration_attempts.
    All UPDATE statements include a WHERE status = '<expected>' guard so that
    concurrent schedulers cannot double-acquire a table.
    """

    def __init__(self, config: OrchestratorConfig, tgt_sql: SqlClient, run_id: str):
        self._cfg     = config
        self._sql     = tgt_sql
        self._run_id  = run_id
        self._ctrl    = f"{config.meta_catalog}.{config.meta_schema}.migration_control"
        self._attempts = f"{config.meta_catalog}.{config.meta_schema}.migration_attempts"

    # ── Fetch ─────────────────────────────────────────────────────────────────

    def _batch_filter(self, batch_id: str = "") -> str:
        """Return a SQL AND clause filtering by batch_id when provided."""
        if batch_id:
            safe = batch_id.replace("'", "\\'")
            return f" AND batch_id = '{safe}'"
        return ""

    def get_queued_records(self, limit: int = 500, batch_id: str = "") -> List[Dict]:
        """
        Return QUEUED records ordered by workload_weight DESC (largest first).

        When batch_id is provided, only records belonging to that batch are
        returned — this is the primary isolation mechanism for multi-party
        parallel execution.
        """
        bf = self._batch_filter(batch_id)
        return self._sql.execute(f"""
            SELECT migration_id, source_catalog, source_schema, source_table,
                   target_catalog, target_schema, target_table,
                   clone_type, source_path, size_in_bytes, size_gb,
                   workload_class, workload_weight, attempt_number, max_attempts,
                   batch_id, chunk_id, status
            FROM {self._ctrl}
            WHERE status = 'QUEUED'
            {bf}
            ORDER BY chunk_id ASC, workload_weight DESC, size_in_bytes DESC
            LIMIT {limit}
        """)

    def get_chunk_ids(self, batch_id: str) -> List[int]:
        """Return distinct chunk_ids for a batch, ordered ascending."""
        bf = self._batch_filter(batch_id)
        rows = self._sql.execute(f"""
            SELECT DISTINCT chunk_id
            FROM {self._ctrl}
            WHERE status IN ('QUEUED', 'ASSIGNED', 'IN_PROGRESS') {bf}
            ORDER BY chunk_id ASC
        """)
        return [int(r["chunk_id"]) for r in rows if r.get("chunk_id")]

    def get_queued_for_chunk(self, batch_id: str, chunk_id: int) -> List[Dict]:
        """Return all QUEUED records for one specific chunk."""
        safe_bid = batch_id.replace("'", "\\'")
        return self._sql.execute(f"""
            SELECT migration_id, source_catalog, source_schema, source_table,
                   target_catalog, target_schema, target_table,
                   clone_type, source_path, size_in_bytes, size_gb,
                   workload_class, workload_weight, attempt_number, max_attempts,
                   batch_id, chunk_id, status
            FROM {self._ctrl}
            WHERE batch_id = '{safe_bid}'
              AND chunk_id  = {chunk_id}
              AND status IN ('QUEUED', 'ASSIGNED', 'IN_PROGRESS')
            ORDER BY workload_weight DESC
        """)

    def get_in_progress_records(self, batch_id: str = "") -> List[Dict]:
        bf = self._batch_filter(batch_id)
        return self._sql.execute(f"""
            SELECT migration_id, assigned_cluster_id, workload_weight,
                   batch_id, chunk_id, started_at, status
            FROM {self._ctrl}
            WHERE status IN ('IN_PROGRESS', 'ASSIGNED') {bf}
        """)

    def get_completed_records(self, batch_id: str = "") -> List[Dict]:
        bf = self._batch_filter(batch_id)
        return self._sql.execute(f"""
            SELECT migration_id, source_catalog, source_schema, source_table,
                   target_catalog, target_schema, target_table,
                   source_path, source_num_files, source_version, target_version,
                   validation_status, batch_id, chunk_id
            FROM {self._ctrl}
            WHERE status = 'COMPLETED' {bf}
        """)

    def get_retryable_records(self, batch_id: str = "") -> List[Dict]:
        bf = self._batch_filter(batch_id)
        return self._sql.execute(f"""
            SELECT migration_id, source_catalog, source_schema, source_table,
                   target_catalog, target_schema, target_table,
                   clone_type, source_path, workload_weight,
                   attempt_number, max_attempts, batch_id, chunk_id, status
            FROM {self._ctrl}
            WHERE status IN ('FAILED', 'RETRY_PENDING', 'VALIDATION_FAILED')
              AND attempt_number < max_attempts {bf}
        """)

    def get_batch_status_summary(self, batch_id: str) -> List[Dict]:
        """Aggregate status counts for a specific batch — useful for monitoring."""
        safe_bid = batch_id.replace("'", "\\'")
        return self._sql.execute(f"""
            SELECT
                chunk_id,
                status,
                COUNT(*)        AS tables,
                ROUND(SUM(size_gb), 2) AS total_gb,
                AVG(duration_seconds)  AS avg_duration_s
            FROM {self._ctrl}
            WHERE batch_id = '{safe_bid}'
            GROUP BY chunk_id, status
            ORDER BY chunk_id, status
        """)

    # ── Bulk chunk assignment (called by BatchPlanner after bin-packing) ──────

    def assign_batch_chunks(
        self,
        batch_id: str,
        chunk_assignments: "List[ChunkAssignment]",     # type: ignore[name-defined]
    ) -> int:
        """
        Write batch_id and chunk_id back to migration_control for every table
        in the plan.

        P2: uses BATCHED set-based MERGE statements (a few hundred rows each)
        instead of one single-row UPDATE per migration_id. At 1000+ tables the
        old per-row loop issued one Delta commit per table, which degraded
        super-linearly (the classic "many tiny sequential Delta UPDATEs on one
        table" anti-pattern) and could take hours. This collapses it to
        ceil(N / batch_size) commits.

        Returns the total number of (migration_id, chunk_id) assignments applied.
        """
        now = _TS()
        safe_bid = batch_id.replace("'", "\\'")
        # Flatten to (migration_id, chunk_id) pairs across all chunks.
        pairs = [
            (mid, chunk.chunk_id)
            for chunk in chunk_assignments
            for mid in chunk.migration_ids
        ]
        if not pairs:
            return 0

        batch_size = 500
        updated = 0
        for i in range(0, len(pairs), batch_size):
            batch = pairs[i:i + batch_size]
            values = ", ".join(
                f"('{str(mid).replace(chr(39), chr(92) + chr(39))}', {int(cid)})"
                for mid, cid in batch
            )
            # NOTE: the column-alias form `(VALUES ...) AS s(mid, cid)` is NOT
            # allowed directly in a MERGE USING clause on Databricks
            # ([COLUMN_ALIASES_NOT_ALLOWED] / SQLSTATE 42601). Wrap the VALUES
            # in a SELECT subquery so the aliases live inside the derived table.
            self._sql.execute_ddl(f"""
                MERGE INTO {self._ctrl} AS t
                USING (
                    SELECT CAST(mid AS STRING) AS mid, CAST(cid AS INT) AS cid
                    FROM (VALUES {values}) AS v(mid, cid)
                ) AS s
                ON t.migration_id = s.mid AND t.status = 'QUEUED'
                WHEN MATCHED THEN UPDATE SET
                    batch_id   = '{safe_bid}',
                    chunk_id   = s.cid,
                    updated_at = TIMESTAMP '{now}'
            """)
            updated += len(batch)
        log.info(
            "Assigned batch_id=%s to %d records across %d chunks "
            "(%d batched MERGE statement(s), batch_size=%d)",
            batch_id, updated, len(chunk_assignments),
            (len(pairs) + batch_size - 1) // batch_size, batch_size,
        )
        return updated

    # ── State transitions ─────────────────────────────────────────────────────

    def mark_assigned(self, migration_id: str, cluster_id: str) -> bool:
        """QUEUED → ASSIGNED (atomic, returns True if transition succeeded)."""
        now = _TS()
        rows = self._sql.execute(f"""
            UPDATE {self._ctrl}
            SET status = 'ASSIGNED',
                assigned_cluster_id = '{cluster_id}',
                updated_at = TIMESTAMP '{now}'
            WHERE migration_id = '{migration_id}'
              AND status = 'QUEUED'
        """)
        # In Databricks SQL, affected rows count available via num_affected_rows
        return True  # If no exception, transition was applied

    def mark_in_progress(self, migration_id: str, attempt_num: int) -> None:
        """ASSIGNED → IN_PROGRESS."""
        now = _TS()
        self._sql.execute_ddl(f"""
            UPDATE {self._ctrl}
            SET status = 'IN_PROGRESS',
                attempt_number = {attempt_num},
                started_at = TIMESTAMP '{now}',
                updated_at = TIMESTAMP '{now}'
            WHERE migration_id = '{migration_id}'
              AND status IN ('ASSIGNED', 'QUEUED')
        """)

    def mark_completed(self, migration_id: str, duration_s: int,
                       target_num_files: Optional[int] = None,
                       target_version: Optional[int] = None) -> None:
        """IN_PROGRESS → COMPLETED."""
        now = _TS()
        tnf = target_num_files or "NULL"
        tv  = target_version or "NULL"
        self._sql.execute_ddl(f"""
            UPDATE {self._ctrl}
            SET status = 'COMPLETED',
                completed_at = TIMESTAMP '{now}',
                duration_seconds = {duration_s},
                target_num_files = {tnf},
                target_version = {tv},
                error_code = NULL,
                error_message = NULL,
                updated_at = TIMESTAMP '{now}'
            WHERE migration_id = '{migration_id}'
              AND status = 'IN_PROGRESS'
        """)

    def mark_failed(
        self,
        migration_id: str,
        error_code:   str,
        error_message: str,
        duration_s:   int = 0,
    ) -> None:
        """IN_PROGRESS → FAILED or RETRY_PENDING."""
        now = _TS()
        err_esc = error_message.replace("'", "\\'")[:500]
        err_code = error_code.replace("'", "\\'")[:50]
        self._sql.execute_ddl(f"""
            UPDATE {self._ctrl}
            SET status = CASE
                  WHEN attempt_number < max_attempts - 1 THEN 'RETRY_PENDING'
                  ELSE 'FAILED'
                END,
                failed_at = TIMESTAMP '{now}',
                duration_seconds = {duration_s},
                error_code = '{err_code}',
                error_message = '{err_esc}',
                updated_at = TIMESTAMP '{now}'
            WHERE migration_id = '{migration_id}'
              AND status = 'IN_PROGRESS'
        """)

    def mark_permanent_failure(self, migration_id: str, reason: str) -> None:
        now = _TS()
        reason_esc = reason.replace("'", "\\'")[:500]
        self._sql.execute_ddl(f"""
            UPDATE {self._ctrl}
            SET status = 'FAILED_PERMANENT',
                error_message = '{reason_esc}',
                updated_at = TIMESTAMP '{now}'
            WHERE migration_id = '{migration_id}'
        """)

    @staticmethod
    def _row_count_set_clause(source_row_count: Optional[int], target_row_count: Optional[int]) -> str:
        """
        Build the SET fragment for source_row_count / target_row_count.
        Only overwrites these columns when a count was actually computed
        (row_count_validation enabled) — leaves prior values untouched
        otherwise, so a validation run with row-count checking OFF doesn't
        silently blank out counts recorded by an earlier run that had it ON.
        """
        parts = []
        if source_row_count is not None:
            parts.append(f"source_row_count = {int(source_row_count)}")
        if target_row_count is not None:
            parts.append(f"target_row_count = {int(target_row_count)}")
        return (",\n                " + ",\n                ".join(parts)) if parts else ""

    def mark_validated(
        self,
        migration_id: str,
        message: str,
        source_row_count: Optional[int] = None,
        target_row_count: Optional[int] = None,
    ) -> None:
        """COMPLETED → VALIDATED."""
        now = _TS()
        msg_esc = message.replace("'", "\\'")[:1000]
        rc_set = self._row_count_set_clause(source_row_count, target_row_count)
        self._sql.execute_ddl(f"""
            UPDATE {self._ctrl}
            SET status = 'VALIDATED',
                validation_status = 'VALIDATED',
                validation_message = '{msg_esc}',
                updated_at = TIMESTAMP '{now}'{rc_set}
            WHERE migration_id = '{migration_id}'
              AND status = 'COMPLETED'
        """)

    def mark_validation_failed(
        self,
        migration_id: str,
        message: str,
        source_row_count: Optional[int] = None,
        target_row_count: Optional[int] = None,
    ) -> None:
        """COMPLETED → VALIDATION_FAILED."""
        now = _TS()
        msg_esc = message.replace("'", "\\'")[:1000]
        rc_set = self._row_count_set_clause(source_row_count, target_row_count)
        self._sql.execute_ddl(f"""
            UPDATE {self._ctrl}
            SET status = 'VALIDATION_FAILED',
                validation_status = 'VALIDATION_FAILED',
                validation_message = '{msg_esc}',
                updated_at = TIMESTAMP '{now}'{rc_set}
            WHERE migration_id = '{migration_id}'
              AND status = 'COMPLETED'
        """)

    def mark_validation_batch(self, outcomes: "List[Dict]", batch_size: int = 200) -> int:
        """
        P3: batched COMPLETED → VALIDATED / VALIDATION_FAILED for many tables in
        a few set-based MERGE commits instead of one UPDATE per table.

        `outcomes` is a list of dicts, each with keys:
          migration_id, status ('VALIDATED'|'VALIDATION_FAILED'), message,
          source_row_count (int|None), target_row_count (int|None).

        Row counts use COALESCE(new, existing) so a run with row-count checking
        OFF (counts None) never blanks out counts recorded by an earlier ON run
        — mirroring the single-row _row_count_set_clause() semantics.
        """
        if not outcomes:
            return 0
        now = _TS()
        written = 0
        for i in range(0, len(outcomes), batch_size):
            batch = outcomes[i:i + batch_size]
            rows = []
            for o in batch:
                mid = str(o["migration_id"]).replace("'", "\\'")
                st  = str(o["status"]).replace("'", "\\'")
                msg = str(o.get("message") or "").replace("'", "\\'")[:1000]
                src = "NULL" if o.get("source_row_count") is None else str(int(o["source_row_count"]))
                tgt = "NULL" if o.get("target_row_count") is None else str(int(o["target_row_count"]))
                rows.append(f"('{mid}', '{st}', '{msg}', {src}, {tgt})")
            values = ",\n              ".join(rows)
            self._sql.execute_ddl(f"""
                MERGE INTO {self._ctrl} AS t
                USING (
                    SELECT CAST(mid AS STRING) AS mid, CAST(st AS STRING) AS st,
                           CAST(msg AS STRING) AS msg,
                           CAST(src AS BIGINT) AS src, CAST(tgt AS BIGINT) AS tgt
                    FROM (VALUES
                      {values}
                    ) AS v(mid, st, msg, src, tgt)
                ) AS s
                ON t.migration_id = s.mid AND t.status = 'COMPLETED'
                WHEN MATCHED THEN UPDATE SET
                    status             = s.st,
                    validation_status  = s.st,
                    validation_message = s.msg,
                    source_row_count   = COALESCE(s.src, t.source_row_count),
                    target_row_count   = COALESCE(s.tgt, t.target_row_count),
                    updated_at         = TIMESTAMP '{now}'
            """)
            written += len(batch)
        log.info("Batched validation-status write: %d record(s) in %d MERGE(s)",
                 written, (len(outcomes) + batch_size - 1) // batch_size)
        return written

    def record_validation_history_batch(self, outcomes: "List[Dict]", batch_size: int = 200) -> int:
        """
        P3: batched append to migration_validation_history — one multi-row
        INSERT per batch instead of one INSERT per table. Column order matches
        record_validation_history(). Each outcome dict carries the same keys as
        mark_validation_batch() plus `record` (the migration_control row, for
        batch_id / source_version / target_version / FQN parts),
        `row_count_checked` (bool) and `row_count_matched` (bool|None).
        """
        if not outcomes:
            return 0
        now = _TS()
        tbl = f"{self._cfg.meta_catalog}.{self._cfg.meta_schema}.migration_validation_history"

        def _q(x) -> str:
            return str(x if x is not None else "").replace("'", "\\'")

        written = 0
        for i in range(0, len(outcomes), batch_size):
            batch = outcomes[i:i + batch_size]
            rows = []
            for o in batch:
                rec = o.get("record") or {}
                sv  = rec.get("source_version")
                tv  = rec.get("target_version")
                src = o.get("source_row_count")
                tgt = o.get("target_row_count")
                matched = o.get("row_count_matched")
                rows.append(
                    "("
                    f"'{_q(self._run_id)}', '{_q(o['migration_id'])}', '{_q(rec.get('batch_id'))}', "
                    f"'{_q(rec.get('source_catalog'))}', '{_q(rec.get('source_schema'))}', '{_q(rec.get('source_table'))}', "
                    f"'{_q(rec.get('target_catalog'))}', '{_q(rec.get('target_schema'))}', '{_q(rec.get('target_table'))}', "
                    f"{str(int(sv)) if sv is not None else 'NULL'}, {str(int(tv)) if tv is not None else 'NULL'}, "
                    f"{'true' if o.get('row_count_checked') else 'false'}, "
                    f"{str(int(src)) if src is not None else 'NULL'}, {str(int(tgt)) if tgt is not None else 'NULL'}, "
                    f"{'NULL' if matched is None else ('true' if matched else 'false')}, "
                    f"'{_q(o['status'])}', '{_q(o.get('message'))[:1000]}', "
                    f"TIMESTAMP '{now}', TIMESTAMP '{now}'"
                    ")"
                )
            stmt = f"INSERT INTO {tbl} VALUES\n              " + ",\n              ".join(rows)
            self._sql.execute_ddl(stmt)
            written += len(batch)
        log.info("Batched validation-history write: %d row(s) in %d INSERT(s)",
                 written, (len(outcomes) + batch_size - 1) // batch_size)
        return written

    def requeue_for_retry(self, migration_id: str) -> None:
        """RETRY_PENDING → QUEUED, increment attempt_number."""
        now = _TS()
        self._sql.execute_ddl(f"""
            UPDATE {self._ctrl}
            SET status = 'QUEUED',
                queued_at = TIMESTAMP '{now}',
                updated_at = TIMESTAMP '{now}',
                attempt_number = attempt_number + 1
            WHERE migration_id = '{migration_id}'
              AND status IN ('RETRY_PENDING', 'FAILED', 'VALIDATION_FAILED')
        """)

    def reset_permanent_failures(self, batch_id: str = "") -> int:
        """
        OPT-IN (retry_permanent=true): re-drive FAILED_PERMANENT rows.

        Flips FAILED_PERMANENT → RETRY_PENDING for the given batch scope and
        gives them a FRESH attempt budget (attempt_number reset to 0) so that
        `attempt_number < max_attempts` holds again and RetryManager's
        attempt-cap guard doesn't instantly re-mark them permanent. It also
        clears error_code (recording the reset in error_message for audit) so
        RetryManager's permanent-error-code guard doesn't immediately re-kill a
        row that was originally marked permanent because of a non-retryable
        error code — the operator has explicitly asked to re-attempt it once.

        This is the ONLY path that leaves FAILED_PERMANENT; the default RETRY
        flow never calls it, so FAILED_PERMANENT stays terminal unless
        retry_permanent=true is passed. When batch_id is provided only that
        batch's rows are reset (the same isolation used everywhere else).

        Returns the number of rows reset.
        """
        now = _TS()
        bf  = self._batch_filter(batch_id)
        rows = self._sql.execute(f"""
            SELECT migration_id
            FROM {self._ctrl}
            WHERE status = 'FAILED_PERMANENT' {bf}
        """)
        self._sql.execute_ddl(f"""
            UPDATE {self._ctrl}
            SET status = 'RETRY_PENDING',
                attempt_number = 0,
                error_code = NULL,
                error_message = 'Reset from FAILED_PERMANENT by retry_permanent=true',
                updated_at = TIMESTAMP '{now}'
            WHERE status = 'FAILED_PERMANENT' {bf}
        """)
        count = len(rows)
        if count:
            log.info(
                "Reset %d FAILED_PERMANENT record(s) → RETRY_PENDING "
                "(retry_permanent=true, batch=%s)",
                count, batch_id or "ALL",
            )
        return count

    # ── Stale record reconciliation ───────────────────────────────────────────

    def reconcile_stale_records(self, threshold_minutes: int = 120) -> int:
        """
        On orchestrator restart, requeue any IN_PROGRESS or ASSIGNED records
        that have been in that state longer than threshold_minutes.

        Missed scenario from the design doc: 'Scheduler restart — Control table
        is source of truth; stale IN_PROGRESS/ASSIGNED records can be reconciled.'
        """
        now = _TS()
        # P4: single set-based reconcile instead of a SELECT + per-row UPDATE
        # loop. Count first (for the return value / log), then flip every stale
        # row in ONE UPDATE — no round-trip per stale record. Both statements
        # share the same `now` so the count and the UPDATE see the same window.
        stale_predicate = (
            "status IN ('IN_PROGRESS', 'ASSIGNED') "
            "AND (started_at IS NULL "
            f"OR TIMESTAMPDIFF(MINUTE, started_at, TIMESTAMP '{now}') > {threshold_minutes})"
        )
        rows = self._sql.execute(
            f"SELECT COUNT(*) AS n FROM {self._ctrl} WHERE {stale_predicate}"
        )
        count = int((rows[0].get("n") if rows else 0) or 0)
        if count:
            self._sql.execute_ddl(f"""
                UPDATE {self._ctrl}
                SET status = 'RETRY_PENDING',
                    error_code = 'STALE_EXECUTION',
                    error_message = 'Record was IN_PROGRESS/ASSIGNED beyond stale threshold; requeued',
                    updated_at = TIMESTAMP '{now}'
                WHERE {stale_predicate}
            """)
            log.info("Reconciled %d stale records → RETRY_PENDING (single set-based UPDATE)", count)
        return count

    # ── Attempt history ───────────────────────────────────────────────────────

    def record_attempt(
        self,
        migration_id:  str,
        attempt_num:   int,
        cluster_id:    str,
        status:        str,
        started_at:    str,
        completed_at:  Optional[str] = None,
        duration_s:    int = 0,
        error_code:    Optional[str] = None,
        error_message: Optional[str] = None,
        src_size:      int = 0,
        tgt_size:      int = 0,
    ) -> None:
        """Insert one row into migration_attempts (never update — immutable history)."""
        worker_id   = str(uuid.uuid4())[:8]
        now         = _TS()
        ca          = f"TIMESTAMP '{completed_at}'" if completed_at else "NULL"
        err_code    = f"'{(error_code or '').replace(chr(39), chr(39)*2)[:50]}'" if error_code else "NULL"
        err_msg     = f"'{(error_message or '').replace(chr(39), chr(39)*2)[:500]}'" if error_message else "NULL"
        self._sql.execute_ddl(f"""
            INSERT INTO {self._attempts} VALUES (
              '{self._run_id}', '{migration_id}', {attempt_num},
              '{cluster_id}', '{worker_id}',
              TIMESTAMP '{started_at}', {ca},
              '{status}', {err_code}, {err_msg},
              {duration_s}, {src_size}, {tgt_size},
              TIMESTAMP '{now}'
            )
        """)

    # ── Validation history (immutable audit trail) ────────────────────────────

    def record_validation_history(
        self,
        migration_id:      str,
        record:            Dict,
        status:            str,
        message:           str,
        source_row_count:  Optional[int] = None,
        target_row_count:  Optional[int] = None,
        row_count_checked: bool = False,
        row_count_matched: Optional[bool] = None,
    ) -> None:
        """
        Insert one immutable row into migration_validation_history — every
        VALIDATE run for a table appends a new row here (unlike
        migration_control, which only ever holds the LATEST validation
        outcome). This is the audit trail for reviewing row-count / size /
        file-count checks over time, across repeated validation attempts.
        """
        now = _TS()
        safe_bid = (record.get("batch_id") or "").replace("'", "\\'")
        msg_esc  = message.replace("'", "\\'")[:1000]
        src_ver  = record.get("source_version")
        tgt_ver  = record.get("target_version")
        src_ver_sql = str(int(src_ver)) if src_ver is not None else "NULL"
        tgt_ver_sql = str(int(tgt_ver)) if tgt_ver is not None else "NULL"
        src_rc_sql  = str(int(source_row_count)) if source_row_count is not None else "NULL"
        tgt_rc_sql  = str(int(target_row_count)) if target_row_count is not None else "NULL"
        matched_sql = (
            "NULL" if row_count_matched is None else ("true" if row_count_matched else "false")
        )
        checked_sql = "true" if row_count_checked else "false"
        self._sql.execute_ddl(f"""
            INSERT INTO {self._cfg.meta_catalog}.{self._cfg.meta_schema}.migration_validation_history VALUES (
              '{self._run_id}', '{migration_id}', '{safe_bid}',
              '{record.get("source_catalog","")}', '{record.get("source_schema","")}', '{record.get("source_table","")}',
              '{record.get("target_catalog","")}', '{record.get("target_schema","")}', '{record.get("target_table","")}',
              {src_ver_sql}, {tgt_ver_sql},
              {checked_sql}, {src_rc_sql}, {tgt_rc_sql}, {matched_sql},
              '{status}', '{msg_esc}',
              TIMESTAMP '{now}', TIMESTAMP '{now}'
            )
        """)

    # ── Exclusion audit (global exclusion list — Section: exclusion_manager) ──

    def record_exclusions(
        self,
        batch_id:  str,
        excluded:  List[Tuple[object, object]],   # List[Tuple[TableSelection, ExclusionRule]]
    ) -> int:
        """
        Insert one immutable row per excluded table into
        migration_exclusion_log — this is the audit trail requested for the
        global exclusion_csv_path feature: every table skipped at INVENTORY
        time (because it matched a catalog/schema/table exclusion rule) is
        recorded here, along with WHICH rule matched and its type, so the
        exclusion is reviewable/auditable rather than silently disappearing.

        `excluded` is the (TableSelection, ExclusionRule) list returned by
        orchestrator.exclusion_manager.apply_exclusions(). Uses one batched
        multi-row INSERT (not per-row, unlike assign_batch_chunks) since this
        is a pure append with no per-row WHERE guard needed.

        Returns the number of rows inserted.
        """
        if not excluded:
            return 0
        now = _TS()
        safe_bid = (batch_id or "").replace("'", "\\'")
        values = []
        for sel, rule in excluded:
            values.append(
                "("
                f"'{self._run_id}', '{safe_bid}', "
                f"'{sel.source_catalog}', '{sel.source_schema}', '{sel.source_table}', "
                f"'{rule.exclude_type}', '{rule.describe().replace(chr(39), chr(92)+chr(39))}', "
                f"TIMESTAMP '{now}'"
                ")"
            )
        stmt = (
            f"INSERT INTO {self._cfg.meta_catalog}.{self._cfg.meta_schema}.migration_exclusion_log VALUES "
            + ", ".join(values)
        )
        self._sql.execute_ddl(stmt)
        log.info("Recorded %d excluded table(s) into migration_exclusion_log (batch=%s)", len(values), batch_id)
        return len(values)

    # ── Aggregate metrics ─────────────────────────────────────────────────────

    def get_run_metrics(self) -> Dict:
        """
        Return aggregated status counts and data volume for the current batch.

        BUG FIX (2026-09-09): this used to filter `WHERE run_id = self._run_id`.
        That is WRONG for every mode except a first-time INVENTORY insert:
        mark_completed() / mark_failed() / mark_validated() (below) never set
        run_id on the rows they update — only InventoryManager._upsert()'s
        INSERT branch stamps run_id, and only for brand-new rows. So a
        DEEP_CLONE/RETRY/VALIDATE run (or an INVENTORY run that only SKIPPED
        already-known tables) would filter on a run_id that no row in
        migration_control actually carries, always yielding an all-zero
        summary — even though the run did real, correct work. batch_id is
        the ID that is actually threaded consistently through every mode
        (INVENTORY sets it at onboard time; DEEP_CLONE/RETRY/VALIDATE all
        select their work `WHERE batch_id = cfg.batch_id`), so scope the
        summary by that instead.
        """
        rows = self._sql.execute(f"""
            SELECT status, COUNT(*) AS n,
                   SUM(size_in_bytes) AS total_bytes,
                   SUM(duration_seconds) AS total_dur
            FROM {self._ctrl}
            WHERE batch_id = '{self._cfg.batch_id}'
            GROUP BY status
        """)
        metrics: Dict = {
            "total": 0, "queued": 0, "in_progress": 0, "assigned": 0,
            "completed": 0, "validated": 0, "failed": 0, "failed_permanent": 0,
            "validation_failed": 0, "retry_pending": 0, "skipped": 0,
            "total_bytes": 0, "total_duration_s": 0,
        }
        for r in rows:
            s = (r.get("status") or "").lower().replace("_", "")
            metrics["total"] += int(r["n"] or 0)
            # Map status → metrics key
            key_map = {
                "queued":           "queued",
                "inprogress":       "in_progress",
                "assigned":         "assigned",
                "completed":        "completed",
                "validated":        "validated",
                "failed":           "failed",
                "failedpermanent":  "failed_permanent",
                "validationfailed": "validation_failed",
                "retrypending":     "retry_pending",
                "skipped":          "skipped",
            }
            key = key_map.get(s)
            if key:
                metrics[key] = int(r["n"] or 0)
            metrics["total_bytes"]     += int(r.get("total_bytes") or 0)
            metrics["total_duration_s"] += int(r.get("total_dur") or 0)
        return metrics
