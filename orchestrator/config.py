"""
config.py — Configuration loading and defaults.

Precedence (highest → lowest):
  1. Explicit job parameters (Databricks widgets)
  2. YAML configuration file
  3. CSV table mapping (extends/overrides selection only)
  4. Framework defaults (this file)

Authentication: this framework does NOT store or reference any client_id /
client_secret / Databricks Secret scope. `orchestrator/sql_client.py` and
`orchestrator/api_client.py` authenticate via the Databricks SDK's native
runtime auth (`databricks.sdk.core.Config()`), which is automatic when the
code runs inside a Databricks job/notebook. The fields below
(workspace_url / warehouse_id) are plain, non-sensitive identifiers — not
secrets — and may be left blank to auto-detect the current workspace.
"""

from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


# ── Workload classification thresholds (bytes) ────────────────────────────────

DEFAULT_WORKLOAD_THRESHOLDS: Dict[str, int] = {
    "SMALL":  10 * (1024 ** 3),         # < 10 GB
    "MEDIUM": 100 * (1024 ** 3),        # 10–100 GB
    "LARGE":  1_000 * (1024 ** 3),      # 100 GB–1 TB
    # XLARGE: anything above LARGE
}

DEFAULT_WORKLOAD_WEIGHTS: Dict[str, int] = {
    "SMALL":   1,
    "MEDIUM":  3,
    "LARGE":   8,
    "XLARGE":  20,
    "UNKNOWN": 0,   # skip_describe_detail=true — size never measured
}


# ── Cluster pool entry ────────────────────────────────────────────────────────

@dataclass
class ClusterConfig:
    cluster_id:      str
    capacity_units:  int   = 10     # logical workload units


# ── Top-level configuration ───────────────────────────────────────────────────

@dataclass
class OrchestratorConfig:
    # ── Workspace connection info (plain, non-secret identifiers) ──
    # No client_id/client_secret here by design — SqlClient/ApiClient
    # authenticate natively via databricks.sdk.core.Config() (no external
    # secrets). workspace_url blank = auto-detect the current/attached
    # workspace at runtime.
    source_workspace_url:    str = ""     # [direct_adls only] leave blank to auto-detect
    source_warehouse_id:     str = ""     # required for DIRECT_ADLS inventory

    target_workspace_url:    str = ""     # leave blank to auto-detect (current workspace)
    target_warehouse_id:     str = ""     # for control-table reads/writes

    # ── Control table location (on target workspace) ──
    meta_catalog:  str = "azure_uc_demo_region1"
    meta_schema:   str = "migration_meta"

    # ── Clone settings ──
    clone_type:            str = "direct_adls"   # delta_share | direct_adls
    default_target_catalog: str = ""             # override target catalog

    # exclusion_csv_path: optional CSV of catalog/schema/table-level exclusion
    #   rules (see orchestrator/exclusion_manager.py for the format). Applied
    #   uniformly on top of whatever input_type resolved the selection list
    #   (JOB/YAML/CSV) — independent of that mechanism's own exclude_schemas/
    #   exclude_tables (which only apply inside a single YAML mapping/CSV row).
    #   Every excluded table is skipped at INVENTORY time and recorded into
    #   migration_exclusion_log for audit/review. Blank = no global exclusions.
    exclusion_csv_path:     str = ""

    # ── Cluster pool ──
    cluster_pool: List[ClusterConfig] = field(default_factory=list)

    # ── Workload classification ──
    workload_thresholds: Dict[str, int]  = field(default_factory=lambda: dict(DEFAULT_WORKLOAD_THRESHOLDS))
    workload_weights:    Dict[str, int]  = field(default_factory=lambda: dict(DEFAULT_WORKLOAD_WEIGHTS))

    # ── Retry ──
    max_retries:         int = 3
    retry_backoff_base_s: int = 30      # exponential backoff base (seconds)

    # ── Validation ──
    validation_enabled:        bool = True
    row_count_validation:      bool = False    # expensive — opt-in
    size_tolerance_pct:        float = 1.0     # acceptable size diff %
    file_count_tolerance_pct:  float = 5.0

    # skip_describe_detail: when True, INVENTORY bypasses DESCRIBE DETAIL on
    #   the source entirely for every table in the run. size_in_bytes,
    #   size_gb, source_num_files, workload_class and workload_weight are all
    #   left NULL in migration_control (not 0 — 0 would misleadingly imply
    #   "verified empty"; NULL means "never checked"). source_version is also
    #   left NULL, which the VALIDATE row-count check already tolerates by
    #   falling back to an unversioned COUNT(*).
    #   Use case: DESCRIBE DETAIL needs to list files in the source's
    #   underlying storage, which can fail for reasons entirely outside this
    #   framework's control — e.g. an Azure storage account firewall
    #   rejecting the request, or a Delta Share entity governed by an ABAC
    #   policy that Databricks does not yet support for DESCRIBE DETAIL. When
    #   that happens on a whole batch of tables, this flag lets INVENTORY
    #   still onboard them as QUEUED (identity/mapping only, no metadata) so
    #   DEEP_CLONE can proceed — `CREATE TABLE ... DEEP CLONE` does not need
    #   this metadata upfront. Trade-off: the non-Delta/"does the source even
    #   exist" gate normally provided by DESCRIBE DETAIL is skipped too, so a
    #   bad source table now only surfaces as a DEEP_CLONE-time failure
    #   instead of an INVENTORY-time one. Default False preserves full
    #   inventory metadata collection — only enable this when DESCRIBE DETAIL
    #   itself is the thing failing.
    skip_describe_detail:      bool = False

    # ── Batch / Chunk model ──
    # batch_id: user-defined isolation key. All INVENTORY, DEEP_CLONE and VALIDATE
    #   jobs for one migration owner use the same batch_id. Multiple parties can
    #   run concurrent batches without interference.
    batch_id:                  str   = ""       # auto-generated if empty

    # max_concurrent_chunks: upper bound on simultaneously active cluster jobs
    #   PER BATCH. A value of 3 means at most 3 ephemeral clusters are live at
    #   the same time for this batch.  Other batches are unaffected.
    max_concurrent_chunks:     int   = 3

    # parallel_threads_per_chunk: threads inside each chunk cluster job that run
    #   DEEP CLONE statements concurrently. 4 means 4 tables copy in parallel on
    #   the same 8-executor cluster.
    parallel_threads_per_chunk: int  = 4

    # chunk_capacity_gb: greedy bin-packing target size per chunk. Tables are
    #   sorted by size DESC and packed until this limit is reached. A single
    #   table that exceeds this threshold gets its own dedicated chunk/cluster.
    chunk_capacity_gb:         float = 50.0

    # min_executors_per_chunk: minimum number of Spark worker nodes per chunk
    #   cluster.  Overrides num_workers in worker_cluster_config if lower.
    min_executors_per_chunk:   int   = 8

    # inventory_parallel_threads: number of tables processed CONCURRENTLY
    #   during INVENTORY, via a ThreadPoolExecutor in InventoryManager.
    #   run_inventory(). A dedicated knob — separate from
    #   parallel_threads_per_chunk — because INVENTORY runs before any chunk
    #   exists, so it can't reuse that DEEP_CLONE-specific setting.
    #   Only the read-heavy round-trips (existing-row lookup, DESCRIBE
    #   DETAIL, DESCRIBE HISTORY — ~75% of the per-table SQL round-trips)
    #   are actually parallelized; the final migration_control MERGE write
    #   is always fully serialized (see InventoryManager._write_lock) to
    #   avoid Delta concurrent-write conflicts on the shared control table.
    #   Set to 1 to fully restore the original strictly-sequential behavior
    #   (e.g. for debugging). Default 4 — the reads/writes are cheap
    #   metadata-only SQL statements, so a warehouse comfortably handles
    #   this level of concurrency.
    inventory_parallel_threads: int   = 4

    # ── Execution ──
    stale_threshold_minutes:   int  = 120      # reconcile stuck IN_PROGRESS records
    poll_interval_s:           int  = 30
    worker_timeout_minutes:    int  = 180
    api_throttle_delay_s:      float = 0.2

    # ── Exclusion ──
    exclude_catalogs:  List[str] = field(default_factory=list)                   # catalog names (exact, lower-case)
    exclude_schemas:   List[str] = field(default_factory=lambda: ["information_schema", "__databricks_internal"])
    exclude_patterns:  List[str] = field(default_factory=list)   # glob/regex on table name
    include_only_delta: bool     = True        # skip views / non-Delta sources

    # ── Worker notebook path (on target workspace) ──
    # Last-resort fallback only. JOB-mode runs should always pass the real
    # deployed path via the "worker_notebook_path" widget/job-parameter
    # (wired from the ${var.worker_notebook} bundle variable), since this
    # hardcoded path will not match the actual deployed workspace location
    # for other users/targets.
    worker_notebook_path: str = "/Users/vivek.ravichandiran@databricks.com/DeepcloneCrossRegion/worker_notebook"

    # ── Worker cluster config (used when cluster_pool is empty or cluster unavailable) ──
    # If set, each worker dispatch creates an ephemeral job cluster.
    # Dict matching Databricks NewCluster spec: spark_version, node_type_id, num_workers, etc.
    worker_cluster_config: Dict[str, Any] = field(default_factory=dict)

    # ── Run tracking ──
    run_id: str = ""    # Set at runtime


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_env(config: OrchestratorConfig) -> None:
    """
    Populate plain (non-secret) workspace connection overrides from
    environment variables, for standalone/local runs only. Inside a
    Databricks job these are normally supplied via job parameters/widgets
    instead (see orchestrator_notebook.py's target_warehouse_id /
    source_warehouse_id widgets) — no client_id/client_secret exist here,
    auth is handled natively by databricks.sdk.core.Config().
    """
    config.source_workspace_url   = os.environ.get("AZ2AZ_SRC_URL", config.source_workspace_url)
    config.source_warehouse_id    = os.environ.get("AZ2AZ_SRC_WH_ID", config.source_warehouse_id)

    config.target_workspace_url   = os.environ.get("AZ2AZ_TGT_URL", config.target_workspace_url)
    config.target_warehouse_id    = os.environ.get("AZ2AZ_TGT_WH_ID", config.target_warehouse_id)


def load_from_yaml(path: str) -> OrchestratorConfig:
    """Load configuration from a YAML file, then overlay env-var overrides (non-secret)."""
    import yaml  # lazy import — only needed when YAML input_type is used
    with open(path) as f:
        raw: Dict[str, Any] = yaml.safe_load(f) or {}

    cfg = OrchestratorConfig()

    mig = raw.get("migration", {})
    cfg.clone_type           = mig.get("clone_type", cfg.clone_type)
    cfg.source_warehouse_id  = mig.get("source_warehouse_id", cfg.source_warehouse_id)

    src = raw.get("source", {})
    cfg.source_workspace_url = src.get("workspace_url", cfg.source_workspace_url)

    tgt = raw.get("target", {})
    cfg.target_workspace_url = tgt.get("workspace_url", cfg.target_workspace_url)
    cfg.target_warehouse_id  = tgt.get("warehouse_id", cfg.target_warehouse_id)
    cfg.default_target_catalog = tgt.get("catalog", cfg.default_target_catalog)

    meta = raw.get("control_tables", {})
    cfg.meta_catalog = meta.get("catalog", cfg.meta_catalog)
    cfg.meta_schema  = meta.get("schema",  cfg.meta_schema)

    exec_ = raw.get("execution", {})
    cfg.max_retries             = exec_.get("max_retries",             cfg.max_retries)
    cfg.validation_enabled      = exec_.get("validation_enabled",      cfg.validation_enabled)
    cfg.row_count_validation    = exec_.get("row_count_validation",    cfg.row_count_validation)
    cfg.stale_threshold_minutes = exec_.get("stale_threshold_minutes", cfg.stale_threshold_minutes)
    cfg.worker_timeout_minutes  = exec_.get("worker_timeout_minutes",  cfg.worker_timeout_minutes)
    cfg.worker_notebook_path    = exec_.get("worker_notebook_path",    cfg.worker_notebook_path)
    cfg.exclude_schemas         = exec_.get("exclude_schemas",         cfg.exclude_schemas)
    cfg.exclude_patterns        = exec_.get("exclude_patterns",        cfg.exclude_patterns)
    cfg.include_only_delta      = exec_.get("include_only_delta",      cfg.include_only_delta)

    # ── Global exclusions from migration.exclude block (new-style YAML) ──────
    # Merge into exclude_schemas and exclude_patterns so _expand_catalog /
    # _expand_schema honour them even when called from JOB or CSV modes.
    g_excl = mig.get("exclude", {})
    if g_excl:
        # catalogs — stored separately on cfg for InputResolver to check
        cfg.exclude_catalogs = list({
            *getattr(cfg, "exclude_catalogs", []),
            *[c.lower() for c in g_excl.get("catalogs", [])],
        })
        # schemas — merge glob patterns into exclude_schemas
        extra_schs = g_excl.get("schemas", [])
        if extra_schs:
            cfg.exclude_schemas = list({*cfg.exclude_schemas, *extra_schs})
        # tables — merge glob patterns into exclude_patterns
        extra_tbls = g_excl.get("tables", [])
        if extra_tbls:
            cfg.exclude_patterns = list({*cfg.exclude_patterns, *extra_tbls})

    batch_ = raw.get("batch", {})
    cfg.batch_id                = batch_.get("batch_id",                cfg.batch_id)
    cfg.max_concurrent_chunks   = batch_.get("max_concurrent_chunks",   cfg.max_concurrent_chunks)
    cfg.parallel_threads_per_chunk = batch_.get("parallel_threads_per_chunk", cfg.parallel_threads_per_chunk)
    cfg.chunk_capacity_gb       = batch_.get("chunk_capacity_gb",       cfg.chunk_capacity_gb)
    cfg.min_executors_per_chunk = batch_.get("min_executors_per_chunk", cfg.min_executors_per_chunk)

    wl = raw.get("workload", {})
    if wl.get("thresholds"):
        cfg.workload_thresholds.update({k: v * (1024**3) for k, v in wl["thresholds"].items()})
    if wl.get("weights"):
        cfg.workload_weights.update(wl["weights"])

    pool = raw.get("cluster_pool", [])
    cfg.cluster_pool = [
        ClusterConfig(cluster_id=c["cluster_id"], capacity_units=c.get("capacity_units", 10))
        for c in pool
    ]

    wc = raw.get("worker_cluster", {})
    if wc:
        cfg.worker_cluster_config = wc

    _load_env(cfg)
    return cfg


def load_defaults() -> OrchestratorConfig:
    """Return a default config with plain (non-secret) overrides from env vars."""
    cfg = OrchestratorConfig()
    _load_env(cfg)
    return cfg


def validate_config(cfg: OrchestratorConfig, mode: str) -> List[str]:
    """Return list of validation errors (empty = OK).

    NOTE on source_* fields: for clone_type=delta_share, the SOURCE workspace
    is NEVER contacted directly — tables are read via the shared catalog that
    already lives on the TARGET workspace (see orchestrator_notebook.py's
    _disc_sql/_val_src_sql routing and the conditional src_sql.start_warehouse()
    guard). So source_warehouse_id is ONLY required when clone_type=direct_adls,
    where the source SQL warehouse must be queried (DESCRIBE DETAIL) to
    resolve the underlying abfss:// path.

    NOTE on auth: there are no client_id/client_secret fields to validate —
    SqlClient/ApiClient authenticate natively via databricks.sdk.core.Config()
    (no external secrets). workspace_url fields are optional everywhere
    (blank = auto-detect the current/attached workspace); only the SQL
    warehouse *id* (a plain, non-secret identifier) is actually required, so
    the client knows which warehouse to route statements to.
    """
    errors = []
    if cfg.clone_type == "direct_adls" and not cfg.source_warehouse_id:
        errors.append("source_warehouse_id is required for direct_adls clone type")
    if not cfg.target_warehouse_id:
        errors.append("target_warehouse_id is required for control-table access")
    if mode == "DEEP_CLONE" and not cfg.cluster_pool and not cfg.worker_cluster_config:
        errors.append(
            "For DEEP_CLONE mode, provide either cluster_pool_config (existing cluster IDs) "
            "or worker_cluster_json (new ephemeral job cluster spec)"
        )
    return errors
