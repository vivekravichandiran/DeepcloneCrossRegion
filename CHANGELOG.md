# Changelog

All notable changes to the DeepClone CrossRegion orchestrator are documented in
this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Convention (from now on):** every future change — feature, fix, config/DAB
> change, or behavioural tweak — MUST add an entry here under a new version
> heading (most recent first), with the date and the files touched. Keep entries
> concise and factual.

## [1.8.0] - 2026-09-24

### Added
- **AI/BI (Lakeview) Ops telemetry dashboard** for the migration, built from the
  four audit tables (`migration_control`, `migration_attempts`,
  `migration_validation_history`, `migration_exclusion_log`) on warehouse
  `5fe1692f119e2528`. 5 pages / 33 widgets: Overview (KPIs, status/GB/batch
  breakdowns, filters), Progress & Throughput (burn-up, per-hour, durations),
  Failures & Retries (error codes, attempts, detail), Validation (row-count
  match/mismatch, trend), Inventory/Chunks/Exclusions.
  - `docs/AIBI_Dashboard_Design.md` — design (Ops rationale, datasets, charts).
  - `scripts/build_migration_dashboard.py` — generator for the serialized dashboard.
  - `dashboards/migration_telemetry.lvdash.json` — serialized dashboard.
  - Deployed dashboard_id `01f1b7b6bcbe1b18adeec1598cd1dc76`.

## [1.7.0] - 2026-09-24

### Changed
- **Chunk-worker clusters now ALWAYS provision from a pre-warmed instance pool**
  (`0924-004044-comic1-pool-63p33jj6`, `Standard_D8ds_v4`, min_idle=25) instead
  of on-demand VMs. On-demand provisioning in `eastus` was failing at cluster
  launch with `AZURE_QUOTA_EXCEEDED_EXCEPTION` (E32ds_v5 × 9 = ~288 cores/chunk,
  over the 350-core regional limit) and, after downsizing, with
  `CLOUD_PROVIDER_RESOURCE_STOCKOUT` (`Standard_D8ds_v5` SKU not available).
  Pre-warmed pool instances sidestep both. Node type is now governed by the
  pool, so `worker_node_type` is ignored while a worker pool id is set.
  - `orchestrator/job_factory.py`: new `worker_instance_pool_id` param (default
    is the pool); `_worker_cluster_json` emits `instance_pool_id` (dropping
    `node_type_id`/`azure_attributes`) when set, and falls back to on-demand
    `node_type_id` only when blank.
  - `databricks.yml`: new `worker_instance_pool_id` variable.
  - `resources/03_deep_clone_job.yml`, `resources/05_retry_job.yml`: chunk
    `worker_cluster_json` switched to `instance_pool_id: ${var.worker_instance_pool_id}`.
  - Tests: updated `test_worker_cluster_json_embedded` and added
    `test_worker_cluster_json_falls_back_to_on_demand_when_no_pool`.

## [1.6.1] - 2026-09-23

### Fixed
- **INVENTORY batch-planning crash: `[COLUMN_ALIASES_NOT_ALLOWED] ... SQLSTATE:
  42601`.** The P2 batched chunk-assignment MERGE in
  `orchestrator/audit_manager.py::assign_batch_chunks` used the
  `USING (VALUES ...) AS s(mid, cid)` column-alias form, which Databricks
  rejects directly inside a MERGE `USING` clause. Wrapped the `VALUES` list in a
  `SELECT ... FROM (VALUES ...) AS v(mid, cid)` subquery (the same safe pattern
  already used by `mark_validation_batch` and `inventory_manager._upsert_batch`).
  INVENTORY onboarding itself already succeeded (its `_upsert_batch` was already
  alias-safe); only the subsequent chunk assignment failed.
- Tests: added `tests/test_audit_manager_batched_merge_sql.py` — asserts none of
  the batched MERGEs use the disallowed `(VALUES ...) AS alias(cols)` form,
  covers batch-splitting and single-quote escaping.

## [1.6.0] - 2026-09-23

### Performance
- **Eliminated the per-row Delta-commit anti-pattern that stalled runs at 1000+
  tables.** Several control-table write loops issued one single-row `UPDATE`/
  `MERGE` (= one Delta commit) per table, which degrades super-linearly on a
  single table and could take hours for ~1662 tables. All are now batched
  set-based statements.
  - **P1 — INVENTORY control-table writes** (`orchestrator/inventory_manager.py`):
    `_process_one()` no longer writes; `run_inventory()` collects the onboarded
    records and writes them via new `_upsert_batch()` — a batched `MERGE`
    (default 200 rows/statement, typed-CAST source SELECT so all-NULL numeric
    columns still type-check) with identical semantics to `_upsert()`. Reads
    stay parallel; writes drop from N commits to ceil(N/200).
  - **P2 — chunk assignment** (`orchestrator/audit_manager.py::assign_batch_chunks`):
    replaced the per-`migration_id` `UPDATE` loop with batched `MERGE`
    statements (500 pairs each) — this was the step the 1662-table INVENTORY
    was visibly stuck on.
  - **P3 — VALIDATE** (`notebooks/orchestrator_notebook.py` +
    `orchestrator/audit_manager.py`): `validator.validate()` (read-only) now runs
    across a `ThreadPoolExecutor`; results are written with two batched calls,
    `mark_validation_batch()` (batched `MERGE`, `COALESCE`-preserving row counts)
    and `record_validation_history_batch()` (batched multi-row `INSERT`),
    instead of ~6 serial SQL round-trips per table.
  - **P4 — stale reconcile** (`orchestrator/audit_manager.py::reconcile_stale_records`):
    replaced the `SELECT` + per-row `UPDATE` loop with a single set-based
    `UPDATE` (plus one `COUNT(*)` for the return value/log).
  - Tests: updated `tests/test_inventory_manager_skip_describe_detail.py` for the
    batched write path and added `test_upsert_batch_splits_into_multiple_merges`.

## [1.5.0] - 2026-09-23

### Fixed
- **Row-count validation false MISMATCH from a stale `source_version`.** The
  source Delta version was captured at INVENTORY time, but DEEP CLONE reads the
  source's *live* version at clone time. When a source table received writes
  during the (potentially long) QUEUED gap between inventory and clone, the
  target reflected the newer version while VALIDATE still counted the source
  `VERSION AS OF` the old inventory version — producing a false row-count
  mismatch (and, since [1.4.0], a `VALIDATION_FAILED`).
  - `notebooks/chunk_worker_notebook.py`: `clone_table()` now records the source
    version **actually cloned** and writes it back to
    `migration_control.source_version` on `COMPLETED`. It reads the source
    `DESCRIBE HISTORY` immediately before the clone as a fallback, then prefers
    the exact, race-free `operationParameters.sourceVersion` from the target's
    CLONE-op history entry. The column is only overwritten when a value is
    captured (a capture failure never clobbers the inventory value with NULL).
  - `orchestrator/validator.py`: docstrings updated to reflect that
    `source_version` is now the clone-time version (the version DEEP CLONE read),
    not the inventory-time version.

## [1.4.0] - 2026-09-23

### Changed
- **Validation status is now driven solely by the row-count check.** A table is
  only marked `VALIDATION_FAILED` when the source/target `COUNT(*)` comparison
  mismatches (or the source/target table is missing entirely). Size-in-bytes,
  file-count, target-format, and delta-version checks still run and are recorded
  in `validation_message` for visibility, but are **advisory only** and no
  longer fail validation. This fixes tables being flagged `VALIDATION_FAILED`
  purely because `sizeInBytes` differed (e.g. from post-clone compaction / file
  layout differences).
  - `orchestrator/validator.py`: `Validator.validate()` aggregation now returns
    `VALIDATION_FAILED` only on a row-count mismatch (`row_counts.checked and
    row_counts.matched is False`); all other checks are informational. Updated
    module/method docstrings to document the new policy.
- **VALIDATE never fails the job run.** A `VALIDATION_FAILED` table is surfaced
  via logs, the run summary, and `migration_control` /
  `migration_validation_history`, but the Databricks job task now completes
  `SUCCESS` so downstream tasks and the overall workflow are not blocked.
  - `notebooks/orchestrator_notebook.py`: replaced the post-VALIDATE
    `raise Exception(...)` on `summary.val_failed > 0` with a warning log +
    printed notice; the job is intentionally left `SUCCESS`.

## [1.3.0] - 2026-09-23

### Added
- **Opt-in re-drive of `FAILED_PERMANENT` tables on RETRY** via a new
  `retry_permanent` boolean parameter (default `false`). When `true`, a RETRY
  run first resets the batch's `FAILED_PERMANENT` rows back to `RETRY_PENDING`
  with a fresh attempt budget, then attempts them once in the same run; a table
  that re-fails simply returns to `FAILED_PERMANENT` (reset-then-attempt-once,
  no infinite loop). Default `false` keeps `FAILED_PERMANENT` terminal — the
  safe, unchanged behaviour.
  - `orchestrator/audit_manager.py`: new `reset_permanent_failures(batch_id)` —
    flips `FAILED_PERMANENT → RETRY_PENDING`, resets `attempt_number` to 0, and
    clears `error_code` (recording the reset in `error_message`) so the
    RetryManager guards don't instantly re-kill the reset rows. Batch-scoped.
  - `orchestrator/retry_manager.py`: `run_retry(..., retry_permanent=False)`
    calls the reset first when the flag is set, then runs normal selection;
    adds `reset_permanent` to the returned stats.
  - `orchestrator/models.py`: `VALID_TRANSITIONS` now permits
    `FAILED_PERMANENT → {RETRY_PENDING, QUEUED}` (the only edges out of
    `FAILED_PERMANENT`); it remains in `TERMINAL_STATES` for default semantics.
  - `orchestrator/config.py`: new `OrchestratorConfig.retry_permanent` (default
    `False`), also read from YAML `execution.retry_permanent`.
  - Threaded end-to-end like `max_retries`: widget in
    `notebooks/orchestrator_notebook.py`; job parameter + `base_parameters`
    templating on the RETRY job and the workflow's RETRY task in
    `orchestrator/job_factory.py`, `resources/05_retry_job.yml`, and
    `resources/06_full_migration_workflow.yml`; `retry_permanent` variable in
    `databricks.yml`; and `ABSTRACTED_DEFAULTS["retry_permanent"] = "false"` in
    `notebooks/create_jobs_notebook.py`.

### Tests
- `tests/test_job_factory.py`: assert the `retry_permanent` job parameter is
  present (default `false`) and wired via `{{job.parameters.retry_permanent}}`
  on both the RETRY job and the Full Workflow job.
- `tests/test_retry_permanent.py` (new): the `FAILED_PERMANENT → RETRY_PENDING`
  transition is now legal (and still terminal); RetryManager calls the reset
  when the flag is on (batch-scoped) and not when off; a just-reset row is
  requeued once rather than re-marked permanent.

## [1.2.0] - 2026-09-23

### Changed
- **Warehouse cold-start wait increased from 150s to 30 minutes** so long SQL
  warehouse cold starts no longer abort a run (`orchestrator/sql_client.py`;
  `_WAREHOUSE_START_TIMEOUT_S = 1800`, poll every 5s). Regression coverage in
  `tests/test_sql_client_warehouse_start.py` (fake-clock, no real sleeping).

## [1.1.0] - 2026-09-23

### Added
- **In-workspace CSV-mode job-creator notebook** `notebooks/create_jobs_notebook.py`
  that reproduces `databricks bundle deploy` from inside the workspace via the
  Jobs REST API, backed by the pure, dependency-free builder
  `orchestrator/job_factory.py` (`tests/test_job_factory.py`). Existing jobs are
  patched in place via `jobs/reset` (same `job_id`/history) — never deleted and
  recreated. Creator widgets are numbered `01..21` for a stable, logical order,
  with per-run invariants abstracted into `ABSTRACTED_DEFAULTS`.

## [1.0.1] - 2026-09-09

### Confirmed
- **UC Volumes support**: the CSV table-mapping path and the global exclusion
  CSV path both support `/Volumes/...` (Unity Catalog Volumes) locations.
