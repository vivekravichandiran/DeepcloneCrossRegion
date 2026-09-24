# Delta DeepClone — Migration Telemetry Dashboard (AI/BI) — Design

**Purpose:** An Ops-facing Databricks AI/BI (Lakeview) dashboard to monitor the
cross-region Delta Deep Clone migration in real time, from the four audit
tables. Answers the questions an on-call/migration operator actually asks:
*Are we on track? What's stuck? What failed and why? Is the data correct?
How much is left and how fast are we going?*

- **Data source:** `ril_migration_catalog.migration_meta.*`
  (`migration_control`, `migration_attempts`, `migration_validation_history`,
  `migration_exclusion_log`)
- **SQL warehouse:** `5fe1692f119e2528` (target warehouse from `databricks.yml`
  / `create_jobs_notebook`)
- **Cross-dashboard filters:** `batch_id`, `clone_type`, `status` (filter
  widgets on page 1 apply to all widgets sharing `ds_control`).

Status vocabulary (from `migration_control.status`):
`DISCOVERED · ONBOARDED · WAITING_FOR_LOAD · QUEUED · ASSIGNED · IN_PROGRESS ·
COMPLETED · FAILED · FAILED_PERMANENT · RETRY_PENDING · SKIPPED · VALIDATED ·
VALIDATION_FAILED`. "Done" = `COMPLETED` or `VALIDATED`; "Failed" =
`FAILED` + `FAILED_PERMANENT` + `VALIDATION_FAILED`; "Active" =
`ASSIGNED` + `IN_PROGRESS`.

---

## Datasets

| Dataset | Purpose | Source |
|---|---|---|
| `ds_control` | Flexible base for breakdowns, tables, filters | `migration_control` |
| `ds_kpis` | Single-row headline KPIs | `migration_control` (aggregated) |
| `ds_throughput` | Hourly completion time-series | `migration_control` (COMPLETED/VALIDATED) |
| `ds_valhist` | Validation audit trail (trend + mismatches) | `migration_validation_history` |
| `ds_attempts` | Retry / attempt telemetry | `migration_attempts` |
| `ds_excl` | Exclusion audit | `migration_exclusion_log` |

---

## Page 1 — Executive Overview (at-a-glance)

**KPI counters (top row):**
1. Total tables
2. Done (COMPLETED+VALIDATED)
3. % Complete
4. Active (ASSIGNED+IN_PROGRESS)
5. Queued
6. Failed (FAILED+FAILED_PERMANENT+VALIDATION_FAILED)
7. Total GB onboarded
8. GB migrated (done)

**Charts:**
- **Status distribution** — bar (count by `status`). The single most-used Ops view.
- **GB by status** — bar (SUM `size_gb` by `status`) — where the *data volume* sits.
- **Tables by batch, stacked by status** — bar (`batch_id` × count, color=`status`) — per-team/per-run progress.
- **Clone type split** — donut (`clone_type`).

**Filters:** `batch_id`, `clone_type`, `status`.

## Page 2 — Progress & Throughput

- **Cumulative tables completed over time** — line (`completed_at` hourly, running total) — the "burn-up" curve.
- **Tables completed per hour** — bar (`ds_throughput`).
- **GB migrated per hour** — bar (`ds_throughput`).
- **Avg clone duration by workload class** — bar (AVG `duration_seconds` by `workload_class`).
- **Longest-running clones** — table top 20 by `duration_seconds` (source/target, GB, duration, batch).

## Page 3 — Failures & Retries

- **Failures by error code** — bar (count by `error_code`, failed statuses only).
- **Attempts distribution** — bar (count by `attempt_number`).
- **Retry outcomes over time** — bar from `ds_attempts` (`status` by day).
- **Failure detail** — table (source table, status, error_code, error_message, attempt_number/max_attempts, updated_at) for FAILED/FAILED_PERMANENT/VALIDATION_FAILED.

## Page 4 — Validation & Data Correctness

- **Validation status breakdown** — donut (`validation_status`).
- **Row-count check outcome** — bar: NOT CHECKED / MATCH / MISMATCH (derived from source/target row counts).
- **Row-count mismatches** — table (schemas/tables, source/target versions, source/target row counts) where counts differ.
- **Validation attempts over time** — line from `ds_valhist` (`validated_at` × count, color=`status`).

## Page 5 — Inventory, Chunks & Exclusions

- **Workload class distribution** — bar (count + SUM GB by `workload_class`).
- **Size distribution (GB)** — histogram of `size_gb`.
- **Chunk fan-out per batch** — table (`batch_id`, `chunk_id`, tables, GB) — validates bin-packing/parallelism.
- **Exclusions by type** — bar (count by `exclusion_type` from `ds_excl`).
- **Excluded tables** — table (catalog/schema/table, rule, excluded_at).

---

## Notes / Ops rationale
- Row counts are shown VERSION-AS-OF the recorded `source_version`/`target_version`
  (see `migration_control` comments) so validation figures are stable.
- The mismatch view is the primary "is the migration *correct*" signal, distinct
  from the "did it *run*" signals on pages 1–3.
- `migration_validation_history` (append-only) powers trend charts; `migration_control`
  holds only the latest per-table state.
