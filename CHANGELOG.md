# Changelog

All notable changes to the DeepClone CrossRegion orchestrator are documented in
this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Convention (from now on):** every future change — feature, fix, config/DAB
> change, or behavioural tweak — MUST add an entry here under a new version
> heading (most recent first), with the date and the files touched. Keep entries
> concise and factual.

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
