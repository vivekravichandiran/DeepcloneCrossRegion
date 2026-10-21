# Databricks notebook source
"""
setup_control_tables.py — DDL script to create all control tables.

Run this ONCE before using the orchestrator. Idempotent (CREATE IF NOT EXISTS).

Tables created in {meta_catalog}.{meta_schema}:
  migration_control   — per-table state machine (Section 10.1)
  migration_attempts  — immutable execution history (Section 10.2)

Authentication: no client_id/client_secret/Databricks Secret scope needed.
SqlClient authenticates natively via databricks.sdk.core.Config() — automatic
when this runs inside a Databricks job/notebook. Only a plain (non-secret)
SQL warehouse id is required, passed via the `target_warehouse_id` job
parameter/widget (or the AZ2AZ_TGT_WH_ID env var for standalone script usage).

Usage (standalone, outside a Databricks job — requires standard Databricks
SDK auth configured, e.g. `databricks auth login` / DATABRICKS_CONFIG_PROFILE):
  export AZ2AZ_TGT_WH_ID="<warehouse-id>"
  python3 setup_control_tables.py
"""

import os, sys, time

# ── Path setup: repo_root widget (DAB-deployed) → PYTHONPATH env var → __file__ ──
_repo_root = ""
try:
    _repo_root = dbutils.widgets.get("repo_root")  # noqa: F821 — injected by Databricks notebooks
except Exception:
    pass
for _p in [
    _repo_root,
    os.environ.get("PYTHONPATH", ""),
    os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else "",
]:
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

from orchestrator.sql_client import SqlClient

def _get_param(name: str, default: str) -> str:
    """Read a value from the Databricks job's notebook widget (base_parameters)
    first — that's how meta_catalog/meta_schema are actually passed by the DAB
    job — falling back to an UPPER_CASE env var for standalone script usage."""
    try:
        return dbutils.widgets.get(name)  # noqa: F821 — injected by Databricks notebooks
    except Exception:
        return os.environ.get(name.upper(), default)

# ── Config ────────────────────────────────────────────────────────────────────
# TGT_WH_ID is a plain (non-secret) SQL warehouse identifier — NOT a
# credential. No client_id/client_secret/workspace_url needed: SqlClient
# authenticates natively via databricks.sdk.core.Config(), which auto-detects
# the current workspace/run's own auth context.
TGT_WH_ID    = _get_param("target_warehouse_id", "")
META_CATALOG = _get_param("meta_catalog", "azure_uc_demo_region1")
META_SCHEMA  = _get_param("meta_schema",  "migration_meta")

if not TGT_WH_ID:
    raise ValueError(
        "target_warehouse_id widget/job-parameter (or AZ2AZ_TGT_WH_ID env var) "
        "is required — it's a plain SQL warehouse id, not a secret."
    )

sql = SqlClient(warehouse_id=TGT_WH_ID)

print(f"Setting up control tables in {META_CATALOG}.{META_SCHEMA} ...")
print("Starting warehouse ...")
sql.start_warehouse()

# ── Create schema ─────────────────────────────────────────────────────────────
print(f"Creating schema {META_CATALOG}.{META_SCHEMA} ...")
sql.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS `{META_CATALOG}`.`{META_SCHEMA}`")

# ── migration_control ─────────────────────────────────────────────────────────
print("Creating migration_control ...")
sql.execute_ddl(f"""
CREATE TABLE IF NOT EXISTS `{META_CATALOG}`.`{META_SCHEMA}`.migration_control (
  -- Identity
  migration_id        STRING        NOT NULL COMMENT 'UUID per source table',
  run_id              STRING        NOT NULL COMMENT 'Orchestrator run identifier',
  clone_type          STRING        NOT NULL COMMENT 'delta_share | direct_adls',
  source_workspace    STRING        COMMENT 'Source workspace URL',
  target_workspace    STRING        COMMENT 'Target workspace URL',

  -- Source
  source_catalog      STRING        NOT NULL,
  source_schema       STRING        NOT NULL,
  source_table        STRING        NOT NULL,

  -- Target
  target_catalog      STRING        NOT NULL,
  target_schema       STRING        NOT NULL,
  target_table        STRING        NOT NULL,

  -- Batch / Chunk assignment (Section 8 — Batch-Chunk model)
  batch_id            STRING        COMMENT 'User isolation key — multiple parties use different batch_ids to run concurrently without interference',
  chunk_id            INT           COMMENT 'Chunk index within the batch (0 = unassigned). Tables with the same chunk_id share one ephemeral cluster job.',

  -- Inventory
  source_path         STRING        COMMENT 'Source ADLS abfss:// path',
  size_in_bytes       BIGINT        COMMENT 'Source table size at inventory time',
  size_gb             DOUBLE        COMMENT 'Source table size in GB',
  workload_class      STRING        COMMENT 'SMALL | MEDIUM | LARGE | XLARGE',
  workload_weight     INT           COMMENT 'Scheduling weight unit',
  assigned_cluster_id STRING        COMMENT 'Cluster assigned by scheduler',

  -- State machine
  status              STRING        NOT NULL
                      COMMENT 'DISCOVERED|ONBOARDED|WAITING_FOR_LOAD|QUEUED|ASSIGNED|IN_PROGRESS|COMPLETED|FAILED|FAILED_PERMANENT|RETRY_PENDING|SKIPPED|PO_BLOCKED|VALIDATED|VALIDATION_FAILED',
  attempt_number      INT           DEFAULT 0 COMMENT 'Current attempt count',
  max_attempts        INT           DEFAULT 3 COMMENT 'Maximum retry attempts',

  -- Timestamps
  discovered_at       TIMESTAMP,
  onboarded_at        TIMESTAMP,
  queued_at           TIMESTAMP,
  started_at          TIMESTAMP,
  completed_at        TIMESTAMP,
  failed_at           TIMESTAMP,
  duration_seconds    BIGINT        COMMENT 'Actual clone duration in seconds',

  -- Errors
  error_code          STRING,
  error_message       STRING,

  -- Validation
  source_num_files    BIGINT        COMMENT 'Source file count at inventory',
  target_num_files    BIGINT        COMMENT 'Target file count after clone',
  source_version      BIGINT        COMMENT 'Source Delta version at inventory',
  target_version      BIGINT        COMMENT 'Target Delta version after clone',
  validation_status   STRING        COMMENT 'PENDING|VALIDATED|VALIDATION_FAILED|SKIPPED',
  validation_message  STRING        COMMENT 'Detailed validation check results',

  -- Audit
  created_at          TIMESTAMP,
  updated_at          TIMESTAMP
)
USING DELTA
COMMENT 'Migration Orchestrator — per-table state machine (Batch/Chunk model)'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported'
)
""")

# ── Add batch_id / chunk_id / row-count columns to existing tables (idempotent) ─
# NOTE: `ADD COLUMN IF NOT EXISTS` is NOT valid syntax on this SQL warehouse
# (PARSE_SYNTAX_ERROR at 'EXISTS') — must use `ADD COLUMNS (col type ...)`
# (no IF NOT EXISTS) and rely on the try/except below to make it idempotent
# by swallowing the "already exists" error on reruns.
print("Ensuring batch_id, chunk_id, and row-count columns exist (idempotent ALTER) ...")
for col_stmt in [
    f"ALTER TABLE `{META_CATALOG}`.`{META_SCHEMA}`.migration_control ADD COLUMNS (batch_id STRING COMMENT 'Batch isolation key')",
    f"ALTER TABLE `{META_CATALOG}`.`{META_SCHEMA}`.migration_control ADD COLUMNS (chunk_id INT COMMENT 'Chunk index within batch')",
    f"ALTER TABLE `{META_CATALOG}`.`{META_SCHEMA}`.migration_control ADD COLUMNS (source_row_count BIGINT COMMENT 'Source row count, counted VERSION AS OF source_version (latest VALIDATE run with row_count_validation=true)')",
    f"ALTER TABLE `{META_CATALOG}`.`{META_SCHEMA}`.migration_control ADD COLUMNS (target_row_count BIGINT COMMENT 'Target row count, counted VERSION AS OF target_version (latest VALIDATE run with row_count_validation=true)')",
]:
    try:
        sql.execute_ddl(col_stmt)
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            pass  # column already present
        else:
            print(f"  Warning: {e}")
            raise  # surface real errors instead of silently continuing with a half-migrated schema

# ── migration_attempts ─────────────────────────────────────────────────────────
print("Creating migration_attempts ...")
sql.execute_ddl(f"""
CREATE TABLE IF NOT EXISTS `{META_CATALOG}`.`{META_SCHEMA}`.migration_attempts (
  run_id              STRING        NOT NULL COMMENT 'Orchestrator run_id',
  migration_id        STRING        NOT NULL COMMENT 'FK → migration_control.migration_id',
  attempt_number      INT           NOT NULL COMMENT 'Attempt index (1-based)',
  cluster_id          STRING        COMMENT 'Databricks cluster that ran the worker',
  worker_id           STRING        COMMENT 'Short UUID identifying the worker run',
  started_at          TIMESTAMP,
  completed_at        TIMESTAMP,
  status              STRING        COMMENT 'SUCCESS | FAILED',
  error_code          STRING,
  error_message       STRING,
  duration_seconds    BIGINT,
  source_size_bytes   BIGINT,
  target_size_bytes   BIGINT,
  created_at          TIMESTAMP
)
USING DELTA
COMMENT 'Migration Orchestrator — immutable execution attempt history'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# ── migration_validation_history ────────────────────────────────────────────
# Immutable audit trail: unlike migration_control (which only holds the LATEST
# validation outcome per table, overwritten on every re-validate), this table
# APPENDS one row per VALIDATE attempt per table — including the source/target
# row counts (counted VERSION AS OF the exact Delta version captured at
# inventory/clone time) — so counts can be reviewed/audited historically even
# after a table has been re-validated multiple times.
print("Creating migration_validation_history ...")
sql.execute_ddl(f"""
CREATE TABLE IF NOT EXISTS `{META_CATALOG}`.`{META_SCHEMA}`.migration_validation_history (
  run_id              STRING        NOT NULL COMMENT 'Orchestrator run_id that performed this validation',
  migration_id        STRING        NOT NULL COMMENT 'FK -> migration_control.migration_id',
  batch_id            STRING        COMMENT 'Batch isolation key',

  source_catalog      STRING,
  source_schema       STRING,
  source_table        STRING,
  target_catalog      STRING,
  target_schema       STRING,
  target_table        STRING,

  source_version      BIGINT        COMMENT 'Source Delta version the row count (if checked) was counted AS OF',
  target_version      BIGINT        COMMENT 'Target Delta version the row count (if checked) was counted AS OF',

  row_count_checked   BOOLEAN       COMMENT 'Whether row_count_validation was enabled for this attempt',
  source_row_count    BIGINT        COMMENT 'COUNT(*) on source VERSION AS OF source_version (NULL if not checked)',
  target_row_count    BIGINT        COMMENT 'COUNT(*) on target VERSION AS OF target_version (NULL if not checked)',
  row_count_matched   BOOLEAN       COMMENT 'source_row_count = target_row_count (NULL if not checked)',

  status              STRING        COMMENT 'VALIDATED | VALIDATION_FAILED',
  message             STRING        COMMENT 'Full validation check results (size, file count, delta version, row count)',

  validated_at        TIMESTAMP,
  created_at          TIMESTAMP
)
USING DELTA
COMMENT 'Migration Orchestrator — immutable per-attempt VALIDATE audit trail (row counts, versions, pass/fail)'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# ── migration_exclusion_log ─────────────────────────────────────────────────
# Audit trail for the global exclusion_csv_path feature (orchestrator/
# exclusion_manager.py): every table skipped at INVENTORY time because it
# matched a catalog/schema/table exclusion rule gets one immutable row here
# — independent of migration_control, since excluded tables never get a
# migration_control row at all (they're filtered out BEFORE onboarding).
print("Creating migration_exclusion_log ...")
sql.execute_ddl(f"""
CREATE TABLE IF NOT EXISTS `{META_CATALOG}`.`{META_SCHEMA}`.migration_exclusion_log (
  run_id              STRING        NOT NULL COMMENT 'Orchestrator run_id that performed this INVENTORY pass',
  batch_id            STRING        COMMENT 'Batch isolation key for the INVENTORY run that excluded this table',

  source_catalog      STRING        NOT NULL,
  source_schema       STRING        NOT NULL,
  source_table        STRING        NOT NULL,

  exclusion_type      STRING        COMMENT 'catalog | schema | table — the granularity of the rule that matched',
  exclusion_rule      STRING        COMMENT 'Human-readable description of the exact rule that matched, e.g. schema:ril_bulk_02.iot',

  excluded_at         TIMESTAMP
)
USING DELTA
COMMENT 'Migration Orchestrator — audit trail of tables skipped by the global exclusion_csv_path list at INVENTORY time'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

print()
print("✓ Control tables ready:")
print(f"  {META_CATALOG}.{META_SCHEMA}.migration_control")
print(f"  {META_CATALOG}.{META_SCHEMA}.migration_attempts")
print(f"  {META_CATALOG}.{META_SCHEMA}.migration_validation_history")
print(f"  {META_CATALOG}.{META_SCHEMA}.migration_exclusion_log")

# ── Sample monitoring queries ──────────────────────────────────────────────────
print()
print("Sample monitoring queries:")
print(f"""
-- Overall status summary
SELECT status, COUNT(*) AS n, SUM(size_gb) AS total_gb
FROM `{META_CATALOG}`.`{META_SCHEMA}`.migration_control
GROUP BY status ORDER BY n DESC;

-- Current throughput (bytes/hour)
SELECT
  DATE_TRUNC('hour', completed_at) AS hour,
  COUNT(*) AS tables,
  SUM(size_gb) AS gb_completed,
  AVG(duration_seconds) AS avg_duration_s
FROM `{META_CATALOG}`.`{META_SCHEMA}`.migration_control
WHERE status IN ('COMPLETED','VALIDATED')
GROUP BY 1 ORDER BY 1 DESC;

-- Failed tables
SELECT source_catalog, source_schema, source_table,
       attempt_number, max_attempts, error_code, error_message
FROM `{META_CATALOG}`.`{META_SCHEMA}`.migration_control
WHERE status IN ('FAILED','FAILED_PERMANENT','VALIDATION_FAILED')
ORDER BY updated_at DESC;

-- Workload class distribution
SELECT workload_class, COUNT(*) AS n, SUM(size_gb) AS total_gb
FROM `{META_CATALOG}`.`{META_SCHEMA}`.migration_control
GROUP BY workload_class ORDER BY total_gb DESC;

-- Latest row-count validation outcome per table (from migration_control)
SELECT source_schema, source_table, target_schema, target_table,
       source_version, target_version, source_row_count, target_row_count,
       CASE WHEN source_row_count IS NULL THEN 'NOT CHECKED'
            WHEN source_row_count = target_row_count THEN 'MATCH'
            ELSE 'MISMATCH' END AS row_count_status,
       validation_status, updated_at
FROM `{META_CATALOG}`.`{META_SCHEMA}`.migration_control
ORDER BY updated_at DESC;

-- Full row-count validation audit history (every VALIDATE attempt, not just latest)
SELECT validated_at, batch_id, source_schema, source_table,
       source_version, target_version, source_row_count, target_row_count,
       row_count_checked, row_count_matched, status
FROM `{META_CATALOG}`.`{META_SCHEMA}`.migration_validation_history
ORDER BY validated_at DESC;

-- Tables skipped by the global exclusion list (exclusion_csv_path)
SELECT excluded_at, batch_id, source_catalog, source_schema, source_table,
       exclusion_type, exclusion_rule
FROM `{META_CATALOG}`.`{META_SCHEMA}`.migration_exclusion_log
ORDER BY excluded_at DESC;
""")

if __name__ == "__main__":
    print("Setup complete.")
