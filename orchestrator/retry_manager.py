"""
retry_manager.py — Retry eligibility checks and requeue logic.

Retry operates ONLY on existing control records. It must not repeat the
inventory/discovery process unless explicitly requested. (Section 13)

Eligible records:
  status IN ('FAILED', 'RETRY_PENDING', 'VALIDATION_FAILED')
  AND attempt_number < max_attempts

Missed scenarios handled:
  • Permanent error codes (e.g. TABLE_NOT_FOUND) → skip retry → FAILED_PERMANENT.
  • Exponential backoff: optional sleep between requeueing batches.
  • Max-retry guard: exceed max_attempts → FAILED_PERMANENT.
"""

from __future__ import annotations
import logging
import time
from typing import List, Set

from orchestrator.audit_manager import AuditManager
from orchestrator.config import OrchestratorConfig

log = logging.getLogger(__name__)

# These error codes indicate unrecoverable failures that should not be retried.
PERMANENT_ERROR_CODES: Set[str] = {
    "TABLE_OR_VIEW_NOT_FOUND",
    "SCHEMA_NOT_FOUND",
    "CATALOG_NOT_FOUND",
    "PERMISSION_DENIED",
    "DELTA_NOT_SUPPORTED",
    "NON_DELTA_SOURCE",
    "SOURCE_IS_VIEW",
    "FAILED_PERMANENT",
}


class RetryManager:
    def __init__(self, config: OrchestratorConfig, audit: AuditManager):
        self._cfg   = config
        self._audit = audit

    def run_retry(self, backoff: bool = True, batch_id: str = "",
                  retry_permanent: bool = False) -> dict:
        """
        Requeue all eligible records for another clone attempt.

        When batch_id is provided, only records from that batch are retried.

        When retry_permanent=True (OPT-IN, default False), this FIRST resets the
        batch's FAILED_PERMANENT rows back to RETRY_PENDING with a fresh attempt
        budget (AuditManager.reset_permanent_failures) so they get re-driven,
        THEN runs the normal selection below. Because the reset zeroes
        attempt_number and clears error_code, the two guards below
        (permanent-error-code / attempt-cap) do NOT instantly re-kill the
        just-reset rows — so a single run resets-then-attempts-once (no loop).
        Default behaviour is unchanged: FAILED_PERMANENT stays terminal.

        Returns: {"eligible": int, "requeued": int, "permanent": int,
                  "reset_permanent": int}
        """
        reset_permanent = 0
        if retry_permanent:
            reset_permanent = self._audit.reset_permanent_failures(batch_id=batch_id)
            log.info(
                "retry_permanent=true — reset %d FAILED_PERMANENT record(s) "
                "back into the retry pipeline before selection", reset_permanent,
            )

        records = self._audit.get_retryable_records(batch_id=batch_id)
        stats   = {"eligible": len(records), "requeued": 0, "permanent": 0,
                   "reset_permanent": reset_permanent}

        log.info("Retry manager found %d eligible records", len(records))

        for rec in records:
            mid        = rec["migration_id"]
            err_code   = (rec.get("error_code") or "").upper()
            attempt    = int(rec.get("attempt_number") or 0)
            max_att    = int(rec.get("max_attempts") or self._cfg.max_retries)

            # Check for permanent errors
            if err_code in PERMANENT_ERROR_CODES:
                log.warning("Marking %s FAILED_PERMANENT (permanent error: %s)", mid[:8], err_code)
                self._audit.mark_permanent_failure(mid, f"Non-retryable error: {err_code}")
                stats["permanent"] += 1
                continue

            # Check attempt limit
            if attempt >= max_att:
                log.warning("Marking %s FAILED_PERMANENT (max attempts=%d reached)", mid[:8], max_att)
                self._audit.mark_permanent_failure(mid, f"Max attempts ({max_att}) exhausted")
                stats["permanent"] += 1
                continue

            # Exponential backoff (optional)
            if backoff and attempt > 0:
                sleep_s = self._cfg.retry_backoff_base_s * (2 ** (attempt - 1))
                log.info("Backoff %ds for %s (attempt %d)", sleep_s, mid[:8], attempt + 1)
                time.sleep(sleep_s)

            self._audit.requeue_for_retry(mid)
            log.info("Requeued %s for attempt %d/%d", mid[:8], attempt + 1, max_att)
            stats["requeued"] += 1

        return stats

    @staticmethod
    def is_permanent_error(error_code: str) -> bool:
        return (error_code or "").upper() in PERMANENT_ERROR_CODES
