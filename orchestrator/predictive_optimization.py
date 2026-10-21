"""
predictive_optimization.py — INVENTORY-time Predictive Optimization (PO) gate.

Why this exists
---------------
Unity Catalog Predictive Optimization runs background OPTIMIZE / VACUUM on a
table. If PO is enabled on a SOURCE table while it is being migrated, those
background rewrites mutate the source's files/Delta versions mid-flight —
exactly the class of churn that causes DEEP CLONE version drift and concurrent-
write conflicts. The migration therefore requires PO to be DISABLED on every
source table before onboarding. INVENTORY enforces this: any source table whose
*effective* PO is ENABLED (or whose PO status cannot be confirmed disabled) is
recorded with status=PO_BLOCKED and the INVENTORY step is failed.

Detection
---------
Uses the Unity Catalog REST API single-object GET (see
ApiClient.unity_catalog_get). A single table-level GET returns the fully
resolved `effective_predictive_optimization_flag` — catalog → schema → table
inheritance is resolved server-side — so one call per table covers the
"catalog, schema, or table level" requirement automatically, and
`inherited_from_type`/`inherited_from_name` tell us WHERE it was enabled (used
in the recorded error message).

Verified live (UC REST API 2.1):
  • catalog ENABLE  → child table effective ENABLE, inherited_from CATALOG
  • schema  ENABLE  → child table effective ENABLE, inherited_from SCHEMA
  • table   ENABLE  → table effective ENABLE (set directly, no inherited_from)
  • all DISABLE/INHERIT → effective DISABLE

Fail-closed
-----------
If the PO status cannot be read (API/permission error) or the effective flag is
missing/unrecognised, the table is treated as BLOCKED (error_code
PREDICTIVE_OPTIMIZATION_UNKNOWN) — we never onboard a table we can't prove has
PO disabled.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# error_code values persisted to migration_control.error_code for PO_BLOCKED rows
PO_ENABLED_CODE = "PREDICTIVE_OPTIMIZATION_ENABLED"
PO_UNKNOWN_CODE = "PREDICTIVE_OPTIMIZATION_UNKNOWN"


@dataclass
class POState:
    """Resolved Predictive Optimization verdict for one source table."""
    fqn:         str
    blocked:     bool                    # True ⇒ do NOT onboard; fail the step
    value:       Optional[str]           # "ENABLE" | "DISABLE" | None (unknown)
    level:       Optional[str]           # where enabled: "TABLE" | "SCHEMA" | "CATALOG"
    source_name: Optional[str]           # name of the inheriting securable (if any)
    reason:      str                     # human-readable message
    error_code:  str                     # "" when allowed; PO_*_CODE when blocked


class PredictiveOptimizationChecker:
    """
    Resolves effective PO for source tables via the UC REST API.

    `api_client` must be the DISCOVERY-workspace ApiClient (tgt_api for
    delta_share, src_api for direct_adls) — i.e. the workspace where the SOURCE
    tables being onboarded actually live.
    """

    def __init__(self, api_client):
        self._api = api_client

    def check(self, source_catalog: str, source_schema: str, source_table: str) -> POState:
        fqn = f"{source_catalog}.{source_schema}.{source_table}"
        try:
            obj = self._api.unity_catalog_get("tables", fqn)
        except Exception as e:  # fail closed — cannot confirm disabled
            return POState(
                fqn=fqn, blocked=True, value=None, level=None, source_name=None,
                reason=f"Could not read predictive optimization status: {str(e)[:300]}",
                error_code=PO_UNKNOWN_CODE,
            )

        eff = obj.get("effective_predictive_optimization_flag") or {}
        value = eff.get("value")

        if value == "ENABLE":
            level = eff.get("inherited_from_type") or "TABLE"
            src   = eff.get("inherited_from_name")
            where = f"{level} {src}".strip() if src else "set directly on the table"
            return POState(
                fqn=fqn, blocked=True, value="ENABLE", level=level, source_name=src,
                reason=f"Predictive optimization is ENABLED ({where}) — disable it "
                       f"before migrating this source table.",
                error_code=PO_ENABLED_CODE,
            )

        if value == "DISABLE":
            return POState(
                fqn=fqn, blocked=False, value="DISABLE", level=None, source_name=None,
                reason="Predictive optimization disabled.", error_code="",
            )

        # Unrecognised / missing effective flag → fail closed.
        return POState(
            fqn=fqn, blocked=True, value=value, level=None, source_name=None,
            reason=f"Could not confirm predictive optimization is disabled "
                   f"(effective flag: {eff!r}).",
            error_code=PO_UNKNOWN_CODE,
        )


def check_selections(
    checker: PredictiveOptimizationChecker,
    selections: List,
    threads: int = 4,
) -> Dict[str, POState]:
    """
    Run the PO check across many TableSelections in parallel (respecting the
    same thread budget as INVENTORY's reads). Returns {source_fqn: POState}.

    Used by the DRY_RUN path (which has no per-table onboarding loop of its
    own); the INVENTORY path calls `checker.check(...)` inline inside its
    existing ThreadPoolExecutor worker instead of adding a second pass.
    """
    results: Dict[str, POState] = {}
    if not selections:
        return results

    def _one(sel) -> Tuple[str, POState]:
        return sel.source_fqn, checker.check(
            sel.source_catalog, sel.source_schema, sel.source_table
        )

    with ThreadPoolExecutor(max_workers=max(1, threads), thread_name_prefix="po") as ex:
        for fut in as_completed([ex.submit(_one, s) for s in selections]):
            fqn, state = fut.result()
            results[fqn] = state
    return results
