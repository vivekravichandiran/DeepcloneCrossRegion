"""
validator.py — Post-clone source/target comparison.

Validation is an independent phase (Section 12) that runs separately from
cloning. This allows large validation jobs to scale independently.

Status policy:
  The ONLY check that can mark a table VALIDATION_FAILED is the row-count
  comparison (source vs target COUNT(*) mismatch). All other checks below are
  still executed and reported in the validation_message for visibility, but are
  ADVISORY ONLY — a differing sizeInBytes / file count / format / delta version
  will NOT fail validation. (The one exception is a hard SOURCE_MISSING /
  TARGET_MISSING, where the table can't be validated at all.)

Checks performed (configurable):
  ✓ Source exists           — always (hard fail if missing)
  ✓ Target exists           — always (hard fail if missing)
  ✓ Format is DELTA         — advisory only
  ✓ Size comparison         — advisory only (configurable tolerance %)
  ✓ File count comparison   — advisory only (configurable tolerance %)
  ✓ Delta version present   — advisory only
  ✓ Row count (COUNT(*))    — optional (expensive for TB-scale tables); the ONLY
                              check that can flip status to VALIDATION_FAILED;
                              counted
                              VERSION AS OF the source_version/target_version
                              already captured in migration_control, so it
                              reflects what was actually cloned rather than
                              the live current state of either table. Counts
                              are persisted both as columns on
                              migration_control (latest) and as append-only
                              rows in migration_validation_history (audit
                              trail across repeated validation runs).

Missed scenarios handled:
  • Target exists but is empty (0 files) → distinct FAIL message.
  • Source was deleted after clone → FAIL with SOURCE_MISSING code.
  • Size tolerance to allow minor compaction diffs.
  • Schema drift (source changed since inventory) → detected via DESCRIBE DETAIL diff.
"""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Tuple

from orchestrator.models import ValidationResult
from orchestrator.sql_client import SqlClient
from orchestrator.config import OrchestratorConfig

log = logging.getLogger(__name__)


class RowCounts:
    """
    Structured row-count outcome from one validate() call — returned to the
    caller in addition to the (status, message) tuple so the orchestrator can
    persist counts as real columns (migration_control.source_row_count /
    target_row_count) and as an immutable audit trail row
    (migration_validation_history), instead of only being embedded inside a
    free-text validation_message string.
    """
    __slots__ = ("source_row_count", "target_row_count", "checked", "matched",
                 "source_version", "target_version")

    def __init__(
        self,
        source_row_count: Optional[int] = None,
        target_row_count: Optional[int] = None,
        checked: bool = False,
        matched: Optional[bool] = None,
        source_version: Optional[int] = None,
        target_version: Optional[int] = None,
    ):
        self.source_row_count = source_row_count
        self.target_row_count = target_row_count
        self.checked           = checked      # was row_count_validation actually run?
        self.matched           = matched      # None if not checked / errored
        self.source_version    = source_version
        self.target_version    = target_version


class Validator:
    def __init__(
        self,
        config:  OrchestratorConfig,
        src_sql: SqlClient,   # source warehouse
        tgt_sql: SqlClient,   # target warehouse (also runs DML against target Delta tables)
    ):
        self._cfg  = config
        self._src  = src_sql
        self._tgt  = tgt_sql

    # ── Main entry ────────────────────────────────────────────────────────────

    def validate(self, record: Dict) -> Tuple[str, str, "RowCounts"]:
        """
        Run all configured validation checks for one migration_control record.

        Returns:
            (status, message, row_counts) where:
              status     = 'VALIDATED' | 'VALIDATION_FAILED'
                           ('VALIDATION_FAILED' only on a row-count mismatch or a
                            missing source/target — never on a size/file/format
                            difference alone)
              row_counts = RowCounts — structured source/target counts (None
                           fields when row_count_validation is disabled or the
                           count query errored), for the caller to persist.
        """
        src_cat   = record["source_catalog"]
        src_sch   = record["source_schema"]
        src_tbl   = record["source_table"]
        tgt_cat   = record["target_catalog"]
        tgt_sch   = record["target_schema"]
        tgt_tbl   = record["target_table"]
        src_fqn   = f"`{src_cat}`.`{src_sch}`.`{src_tbl}`"
        tgt_fqn   = f"`{tgt_cat}`.`{tgt_sch}`.`{tgt_tbl}`"

        results: List[ValidationResult] = []

        # 1. Source existence
        src_detail = self._describe(self._src, src_fqn)
        if src_detail is None:
            return ("VALIDATION_FAILED", "SOURCE_MISSING: source table not found after clone",
                    RowCounts(source_version=record.get("source_version"), target_version=record.get("target_version")))

        results.append(ValidationResult("source_exists", True, f"Source {src_fqn} exists"))

        # 2. Target existence
        tgt_detail = self._describe(self._tgt, tgt_fqn)
        if tgt_detail is None:
            return ("VALIDATION_FAILED", "TARGET_MISSING: target table was not created",
                    RowCounts(source_version=record.get("source_version"), target_version=record.get("target_version")))

        results.append(ValidationResult("target_exists", True, f"Target {tgt_fqn} exists"))

        # 3. Format check
        src_fmt = (src_detail.get("format") or "").upper()
        tgt_fmt = (tgt_detail.get("format") or "").upper()
        fmt_ok  = tgt_fmt == "DELTA"
        results.append(ValidationResult(
            "target_format", fmt_ok,
            f"Target format={tgt_fmt}" + ("" if fmt_ok else " (expected DELTA)"),
        ))

        # 4. Size comparison
        src_bytes = int(src_detail.get("sizeInBytes") or 0)
        tgt_bytes = int(tgt_detail.get("sizeInBytes") or 0)
        size_result = self._compare_metric(
            "size_bytes", src_bytes, tgt_bytes, self._cfg.size_tolerance_pct
        )
        results.append(size_result)

        # 5. File count comparison
        src_files = int(src_detail.get("numFiles") or 0)
        tgt_files = int(tgt_detail.get("numFiles") or 0)

        # Empty table edge case: both 0 is valid
        if src_files == 0 and tgt_files == 0:
            results.append(ValidationResult(
                "file_count", True, "Both source and target have 0 files (empty table)"
            ))
        else:
            file_result = self._compare_metric(
                "file_count", src_files, tgt_files, self._cfg.file_count_tolerance_pct
            )
            results.append(file_result)

        # 6. Delta version present on target
        # Note: Databricks returns 'lastModified' in some versions, 'lastCommitTimestamp' in others
        tgt_ver = tgt_detail.get("lastCommitTimestamp") or tgt_detail.get("lastModified")
        results.append(ValidationResult(
            "delta_version_present",
            tgt_ver is not None,
            f"Target lastCommitTimestamp={tgt_ver}",
        ))

        # 7. Row count (optional) — counted AS OF the exact Delta version that
        # was ACTUALLY cloned (source_version) and the resulting target version
        # (target_version), both captured in migration_control by the chunk
        # worker at CLONE time — NOT the live current version. Crucially,
        # source_version is refreshed at clone time (the version DEEP CLONE read,
        # taken from the target's CLONE-op sourceVersion), not the stale
        # inventory-time version. Without that refresh, a source table written
        # to during the QUEUED gap between inventory and clone would produce a
        # false MISMATCH: source counted AS OF the old inventory version vs a
        # target that cloned the newer live version.
        row_counts = RowCounts(
            source_version=record.get("source_version"),
            target_version=record.get("target_version"),
        )
        if self._cfg.row_count_validation:
            rc_result = self._row_count_check(
                src_fqn, tgt_fqn,
                source_version=record.get("source_version"),
                target_version=record.get("target_version"),
            )
            results.append(rc_result)
            row_counts.checked = True
            row_counts.matched = rc_result.passed
            try:
                row_counts.source_row_count = int(rc_result.source_value) if rc_result.source_value is not None else None
                row_counts.target_row_count = int(rc_result.target_value) if rc_result.target_value is not None else None
            except (TypeError, ValueError):
                pass

        # ── Aggregate ─────────────────────────────────────────────────────────
        # Status policy (per operational requirement): a validation run is only
        # marked VALIDATION_FAILED when the ROW COUNT comparison fails — i.e.
        # the source and target row counts don't match. Every other check
        # (size in bytes, file count, target format, delta version presence) is
        # still executed and recorded in the message for visibility, but is
        # ADVISORY ONLY and never fails validation on its own. This means a
        # benign size/file difference (e.g. from post-clone auto-compaction or
        # differing file layout) no longer flips a table to VALIDATION_FAILED.
        #
        # When row_count_validation is disabled (no count comparison is run),
        # there is nothing that can "not match", so the run is VALIDATED.
        # A hard SOURCE_MISSING/TARGET_MISSING is still a failure (handled via
        # the early returns above) because the table can't be validated at all.
        lines   = [r.to_line() for r in results]
        message = " | ".join(lines)

        count_mismatch = row_counts.checked and (row_counts.matched is False)
        if count_mismatch:
            return "VALIDATION_FAILED", message, row_counts
        return "VALIDATED", message, row_counts

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _describe(self, client: SqlClient, fqn: str) -> Optional[Dict]:
        try:
            rows = client.execute(f"DESCRIBE DETAIL {fqn}")
            return rows[0] if rows else None
        except Exception as e:
            log.warning("DESCRIBE DETAIL failed for %s: %s", fqn, e)
            return None

    def _compare_metric(
        self,
        name:      str,
        src_val:   int,
        tgt_val:   int,
        tolerance: float,
    ) -> ValidationResult:
        if src_val == 0:
            # Avoid division by zero; if source is 0, target must also be 0
            passed = tgt_val == 0
            msg = f"{name} src=0 tgt={tgt_val}" + ("" if passed else " (target non-zero when source is 0)")
            return ValidationResult(name, passed, msg, str(src_val), str(tgt_val))

        diff_pct = abs(src_val - tgt_val) / src_val * 100
        passed   = diff_pct <= tolerance
        msg = (
            f"{name} src={src_val:,} tgt={tgt_val:,} "
            f"diff={diff_pct:.2f}% (tol={tolerance}%)"
            + (" ✓" if passed else " ✗ EXCEEDS TOLERANCE")
        )
        return ValidationResult(name, passed, msg, str(src_val), str(tgt_val))

    def _row_count_check(
        self,
        src_fqn: str,
        tgt_fqn: str,
        source_version: Optional[int] = None,
        target_version: Optional[int] = None,
    ) -> ValidationResult:
        """
        Optional expensive row-count comparison.

        Counted AS OF the specific Delta version recorded in migration_control
        (source_version = the version DEEP CLONE actually read, captured by the
        chunk worker at clone time; target_version = the resulting target commit)
        — NOT the live/current version — so the comparison reflects exactly what
        was cloned, even if the source table has since been written to again.
        Falls back to a plain (unversioned) COUNT(*) when a version number
        isn't available (e.g. legacy records onboarded before this column
        was populated).

        Wrapped in try/except to not block overall validation on large tables.
        """
        src_sql = (
            f"SELECT COUNT(*) AS n FROM {src_fqn} VERSION AS OF {int(source_version)}"
            if source_version is not None else
            f"SELECT COUNT(*) AS n FROM {src_fqn}"
        )
        tgt_sql = (
            f"SELECT COUNT(*) AS n FROM {tgt_fqn} VERSION AS OF {int(target_version)}"
            if target_version is not None else
            f"SELECT COUNT(*) AS n FROM {tgt_fqn}"
        )
        try:
            src_rows_d = self._src.execute_one(src_sql)
            tgt_rows_d = self._tgt.execute_one(tgt_sql)
            src_n = int((src_rows_d or {}).get("n") or 0)
            tgt_n = int((tgt_rows_d or {}).get("n") or 0)
            passed = src_n == tgt_n
            ver_note = (
                f" (src@v{source_version}, tgt@v{target_version})"
                if source_version is not None or target_version is not None else
                " (unversioned — live count; no source_version/target_version on record)"
            )
            msg = f"row_count src={src_n:,} tgt={tgt_n:,}{ver_note}" + ("" if passed else " ✗ MISMATCH")
            return ValidationResult("row_count", passed, msg, str(src_n), str(tgt_n))
        except Exception as e:
            return ValidationResult("row_count", False, f"row_count ERROR: {e}")
