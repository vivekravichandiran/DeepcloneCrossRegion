# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Migration Orchestrator
# MAGIC **Version 1.0** — Cross-region Deep Clone framework
# MAGIC
# MAGIC ## Widgets
# MAGIC | Widget | Description |
# MAGIC |--------|-------------|
# MAGIC | `mode` | INVENTORY / DEEP_CLONE / VALIDATE / RETRY / DRY_RUN |
# MAGIC | `clone_type` | delta_share / direct_adls |
# MAGIC | `input_type` | JOB / YAML / CSV |
# MAGIC | `selection_type` | catalog / schema / table |
# MAGIC | `source_catalogs` | JSON array e.g. `["azure_uc_demo_region2"]` |
# MAGIC | `source_schemas` | JSON array e.g. `["azure_uc_demo_region2.deepclone_src"]` |
# MAGIC | `source_tables` | JSON array of FQNs |
# MAGIC | `target_catalog` | Default target catalog override |
# MAGIC | `cluster_pool_config` | JSON array of `{cluster_id, capacity_units}` |
# MAGIC | `yaml_config_path` | YAML config path (YAML input_type) |
# MAGIC | `csv_path` | CSV mapping path (CSV input_type) |
# MAGIC | `exclusion_csv_path` | Global catalog/schema/table exclusion list — applied on top of JOB/YAML/CSV, regardless of input_type. See `orchestrator/exclusion_manager.py`. Excluded tables are skipped at INVENTORY and logged to `migration_exclusion_log`. |
# MAGIC | `max_retries` | Max retry attempts (default 3) |
# MAGIC | `validation_enabled` | true/false |
# MAGIC | `row_count_validation` | true/false (expensive) |
# MAGIC | `run_id` | Optional explicit run identifier |
# MAGIC | `target_warehouse_id` | SQL warehouse id (plain, non-secret) for control-table SQL. No client_id/client_secret needed — auth is native (`databricks.sdk.core.Config()`). |
# MAGIC | `source_warehouse_id` | [direct_adls only] source SQL warehouse id. Unused for delta_share. |

# COMMAND ----------

# ── 0. Notebook widgets ────────────────────────────────────────────────────────
try:
    dbutils.widgets.dropdown("mode", "INVENTORY",
        ["INVENTORY", "DEEP_CLONE", "VALIDATE", "RETRY", "DRY_RUN"])
    dbutils.widgets.dropdown("clone_type", "direct_adls",
        ["direct_adls", "delta_share"])
    dbutils.widgets.dropdown("input_type", "JOB", ["JOB", "YAML", "CSV"])
    dbutils.widgets.dropdown("selection_type", "schema",
        ["catalog", "schema", "table"])
    dbutils.widgets.text("source_catalogs",  "[]")
    dbutils.widgets.text("source_schemas",   '["azure_uc_demo_region2.deepclone_src"]')
    dbutils.widgets.text("source_tables",    "[]")
    dbutils.widgets.text("target_catalog",   "azure_uc_demo_region1")
    dbutils.widgets.text("cluster_pool_config", "[]")
    dbutils.widgets.text("yaml_config_path", "")
    dbutils.widgets.text("csv_path",         "")
    # exclusion_csv_path: GLOBAL catalog/schema/table exclusion list, applied
    # on top of whatever input_type resolved (JOB/YAML/CSV) — see
    # orchestrator/exclusion_manager.py for the CSV format. Blank = no
    # exclusions (default, zero behavior change).
    dbutils.widgets.text("exclusion_csv_path", "")
    dbutils.widgets.text("max_retries",      "3")
    dbutils.widgets.dropdown("validation_enabled",   "true",  ["true", "false"])
    dbutils.widgets.dropdown("row_count_validation", "false", ["true", "false"])
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("meta_catalog", "hive_metastore")
    dbutils.widgets.text("meta_schema",  "migration_meta")
    # target_warehouse_id / source_warehouse_id: plain (non-secret) SQL
    # warehouse identifiers — NOT credentials. SqlClient/ApiClient authenticate
    # natively via databricks.sdk.core.Config() (no client_id/client_secret,
    # no Databricks Secret scope). source_warehouse_id is only consulted when
    # clone_type=direct_adls; leave it blank for delta_share.
    dbutils.widgets.text("target_warehouse_id", "")
    dbutils.widgets.text("source_warehouse_id", "")
    dbutils.widgets.text("worker_cluster_json", "")
    dbutils.widgets.text("worker_notebook_path", "")  # deployed path to chunk_worker_notebook
    # ── Batch / Chunk parameters (Section 8 — new) ────────────────────────────
    dbutils.widgets.text("batch_id",              "")   # blank = auto-generated
    dbutils.widgets.text("max_concurrent_chunks", "3")  # max clusters per batch
    dbutils.widgets.text("parallel_threads",      "4")  # threads per chunk cluster
    dbutils.widgets.text("chunk_capacity_gb",     "50") # GB target per chunk
    dbutils.widgets.text("min_executors",         "8")  # worker nodes per cluster
    # ── Target schema override (for same-workspace self-clone tests) ───────────
    dbutils.widgets.text("target_schema", "")  # if set, overrides target schema for all schema-selections
    # force_reonboard: INVENTORY normally skips tables already COMPLETED/
    # VALIDATED/FAILED_PERMANENT/SKIPPED (idempotency — see InventoryManager
    # docstring). Set to "true" to re-onboard them anyway under a fresh
    # batch_id/run_id (e.g. to re-test the same CSV/YAML end-to-end).
    dbutils.widgets.dropdown("force_reonboard", "false", ["true", "false"])
    # require_target_precreated: governance gate for INVENTORY. When "true",
    # a table whose target_catalog.target_schema.target_table does NOT
    # already exist in the target workspace is enlisted into
    # migration_control with status=SKIPPED (error_code=TARGET_NOT_PRECREATED)
    # instead of QUEUED, so DEEP_CLONE/RETRY (which only ever pick up
    # status='QUEUED') never touch it. Use this when target tables must be
    # pre-provisioned (schema/partitioning/grants) by another team before
    # DEEP_CLONE is allowed to land data. Default "false" preserves the
    # original behavior — DEEP_CLONE auto-creates the target table via
    # CREATE OR REPLACE ... DEEP CLONE.
    dbutils.widgets.dropdown("require_target_precreated", "false", ["true", "false"])
    # skip_describe_detail: opt-out of DESCRIBE DETAIL on the source entirely
    # during INVENTORY. Tables are still onboarded as QUEUED (identity/
    # mapping only), but size_in_bytes/size_gb/source_num_files/
    # workload_class/workload_weight/source_version are all left NULL in
    # migration_control instead of being measured. Use this when DESCRIBE
    # DETAIL itself is failing for reasons outside this framework's control
    # (e.g. an Azure storage account firewall rejecting the storage listing,
    # or a Delta Share entity governed by an ABAC policy Databricks doesn't
    # yet support for DESCRIBE DETAIL) — see
    # OrchestratorConfig.skip_describe_detail for the full rationale.
    # Default "false" preserves full inventory metadata collection.
    dbutils.widgets.dropdown("skip_describe_detail", "false", ["true", "false"])
    # inventory_parallel_threads: number of tables INVENTORY processes
    # CONCURRENTLY (ThreadPoolExecutor) — the slow read round-trips
    # (existing-row lookup, DESCRIBE DETAIL, DESCRIBE HISTORY) run in
    # parallel; the final migration_control MERGE write is still fully
    # serialized (see InventoryManager._write_lock) to avoid Delta
    # concurrent-write conflicts. Dedicated knob — separate from
    # parallel_threads (DEEP_CLONE's per-chunk thread count), since
    # INVENTORY runs before any chunk exists. Set to "1" to fully restore
    # the original strictly-sequential behavior. Default "4".
    dbutils.widgets.text("inventory_parallel_threads", "4")
except Exception:
    pass  # widgets already exist or running outside Databricks

# COMMAND ----------

# ── 1. Imports and setup ───────────────────────────────────────────────────────
import sys, os, uuid, logging, json, math
from datetime import datetime, timezone
from typing import Optional

# ── Path setup: the bundle-deployed root MUST be found FIRST on sys.path ────
# ROOT CAUSE (found via DEBUG instrumentation): `spark_env_vars.PYTHONPATH`
# set on the job cluster does NOT reliably show up in `os.environ` inside the
# notebook's own Python kernel process — os.environ.get("PYTHONPATH") comes
# back empty here even though the cluster spec sets it. Relying on that env
# var (as this block used to) silently fell through to the legacy fallback
# paths below, which contain STALE pre-bundle copies of `orchestrator/` —
# every recent fix (CSV/YAML input_resolver changes, etc.) landed in the
# bundle-deployed copy but silently kept running against the stale one.
#
# Fix: derive the bundle root directly and deterministically from the
# CURRENTLY EXECUTING notebook's own workspace path (same mechanism already
# used below for worker_notebook_path auto-detection) — this is always
# accurate regardless of env var propagation quirks. `orchestrator/` is a
# sibling of `notebooks/`, so the bundle root is one level up.
_BUNDLE_ROOT = None
try:
    _nb_ctx_path = (
        dbutils.notebook.entry_point.getDbutils().notebook()
        .getContext().notebookPath().get()
    )
    if _nb_ctx_path:
        # .../<bundle_root>/notebooks/orchestrator_notebook -> <bundle_root>
        _BUNDLE_ROOT = _nb_ctx_path.rsplit("/notebooks/", 1)[0]
except Exception:
    _BUNDLE_ROOT = None

if _BUNDLE_ROOT:
    _ws_bundle_root = "/Workspace" + _BUNDLE_ROOT if not _BUNDLE_ROOT.startswith("/Workspace") else _BUNDLE_ROOT
    for _p in (_BUNDLE_ROOT, _ws_bundle_root):
        if _p in sys.path:
            sys.path.remove(_p)
        sys.path.insert(0, _p)

# Legacy fallbacks — ONLY for ad-hoc / non-bundle runs where the above
# auto-detection fails entirely. Appended (never inserted at 0) so they can
# never shadow the bundle-deployed orchestrator package on sys.path.
_DBFS_PATH = "/dbfs/deepclone_orchestrator"
_WS_PATH   = "/Workspace/Users/vivek.ravichandiran@databricks.com/DeepcloneCrossRegion"
for _p in [_DBFS_PATH, _WS_PATH,
           os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else ""]:
    if _p and _p not in sys.path:
        sys.path.append(_p)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("orchestrator")

from orchestrator.config        import load_defaults, load_from_yaml, validate_config
from orchestrator.parameter_parser import JobParameters
from orchestrator.sql_client    import SqlClient
from orchestrator.api_client    import ApiClient
from orchestrator.input_resolver  import InputResolver
import orchestrator.input_resolver as _ir_mod
log.info("orchestrator package loaded from: %s", os.path.dirname(getattr(_ir_mod, "__file__", "?")))
from orchestrator.inventory_manager import InventoryManager
from orchestrator.workload_classifier import WorkloadClassifier
from orchestrator.cluster_pool  import ClusterPool
from orchestrator.scheduler     import Scheduler, BatchChunkScheduler
from orchestrator.audit_manager import AuditManager
from orchestrator.clone_worker  import CloneWorker
from orchestrator.validator     import Validator
from orchestrator.retry_manager import RetryManager
from orchestrator.batch_planner import plan_chunks
from orchestrator.exclusion_manager import load_exclusion_rules, apply_exclusions
from orchestrator.models        import RunSummary


def _derive_max_tables_per_chunk(n_records: int, max_concurrent_chunks: int) -> Optional[int]:
    """
    Guarantee at least `max_concurrent_chunks` chunks (real parallelism) even
    when tables are tiny/GB-bin-packing alone would collapse them into one
    chunk. ceil(n_records / max_concurrent_chunks) tables per chunk means the
    bin-packer opens a new chunk as soon as a chunk hits that COUNT, on top
    of (in addition to) the existing GB cap. Returns None if inputs are
    degenerate (nothing to plan / no concurrency configured).
    """
    if n_records <= 0 or max_concurrent_chunks <= 0:
        return None
    return math.ceil(n_records / max_concurrent_chunks)

# COMMAND ----------

# ── 2. Parse parameters and build config ───────────────────────────────────────
# Read all widget values DIRECTLY in the notebook cell (dbutils is in notebook scope)
def _get_widget(name, default=""):
    try:
        val = dbutils.widgets.get(name)
        return val if val else default
    except Exception:
        import os
        return os.environ.get(name, default)

# Capture all widget values here in notebook scope
_wmode      = _get_widget("mode",               "INVENTORY")
_clone_type = _get_widget("clone_type",          "direct_adls")
_input_type = _get_widget("input_type",          "JOB")
_sel_type   = _get_widget("selection_type",      "schema")
_src_cats   = _get_widget("source_catalogs",     "[]")
_src_schs   = _get_widget("source_schemas",      "[]")
_src_tbls   = _get_widget("source_tables",       "[]")
_tgt_cat    = _get_widget("target_catalog",      "")
_cluster_pool = _get_widget("cluster_pool_config","[]")
_yaml_path  = _get_widget("yaml_config_path",    "")
_csv_path   = _get_widget("csv_path",            "")
_exclusion_csv_path = _get_widget("exclusion_csv_path", "")

# ── Auto-detect effective input source ──────────────────────────────────────
# `input_type` used to be a separate widget that had to be kept manually in
# sync with which path was actually supplied — when it drifted (e.g.
# yaml_config_path carrying a standing non-blank default while input_type
# said CSV, or vice versa), the wrong file got loaded silently. It is now
# DERIVED from which path is non-blank, so the two can never disagree:
#   1. csv_path is non-blank           → CSV  (explicit table list, highest priority)
#   2. else yaml_config_path non-blank  → YAML (may itself delegate to a CSV via
#                                               migration.csv_path inside that file)
#   3. else                             → JOB  (source_catalogs/schemas/tables widgets)
_input_type_requested = _input_type  # original widget/job-param value, kept for logging only
if _csv_path.strip():
    _input_type = "CSV"
elif _yaml_path.strip():
    _input_type = "YAML"
else:
    _input_type = _input_type_requested or "JOB"
log.info(
    "Input type auto-detected: requested=%s csv_path=%r yaml_config_path=%r -> effective=%s",
    _input_type_requested, _csv_path, _yaml_path, _input_type,
)

_max_retries = _get_widget("max_retries",        "3")
_val_enabled = _get_widget("validation_enabled", "true")
_row_cnt_val = _get_widget("row_count_validation","false")
_run_id_wg          = _get_widget("run_id",              "")
_meta_catalog       = _get_widget("meta_catalog",       "hive_metastore")
_meta_schema        = _get_widget("meta_schema",        "migration_meta")
_worker_cluster     = _get_widget("worker_cluster_json", "")
_worker_nb_path     = _get_widget("worker_notebook_path", "")
_target_wh_id       = _get_widget("target_warehouse_id", "")
_source_wh_id       = _get_widget("source_warehouse_id", "")
# Batch / Chunk parameters
_batch_id           = _get_widget("batch_id",              "")

# ── Fallback: fetch batch_id directly via the taskValues SDK ────────────────
# resources/06_full_migration_workflow.yml sets deep_clone/retry/validate's
# "batch_id" base_parameter to the dynamic value reference
# "{{tasks.inventory.values.batch_id}}" so they pick up whatever batch_id
# INVENTORY actually used (explicit or auto-generated) — see the long
# comment where cfg.batch_id is finalized below for why this matters. In
# testing, that dynamic-reference substitution did NOT always resolve to the
# published value inside base_parameters (each downstream task ended up with
# a genuinely BLANK widget and therefore auto-generated its OWN unrelated
# batch_id instead of inventory's — confirmed via driver logs: deep_clone and
# validate each published a different batch_id than inventory did in the
# same run). Databricks' own docs/community threads note the SDK call
# (`dbutils.jobs.taskValues.get`) is the more reliable mechanism, so use it
# directly as a fallback here whenever the widget came back blank and we're
# in a mode that only ever runs downstream of inventory in this one workflow.
if not _batch_id.strip() and _wmode in ("DEEP_CLONE", "RETRY", "VALIDATE"):
    try:
        _tv_batch_id = dbutils.jobs.taskValues.get(
            taskKey="inventory", key="batch_id", default="", debugValue="",
        )
        if _tv_batch_id:
            _batch_id = _tv_batch_id
            log.info("batch_id widget was blank — recovered %s via taskValues.get(taskKey='inventory')", _batch_id)
    except Exception as _e:
        # Standalone jobs (03_deep_clone_job.yml / 04_validate_job.yml /
        # 05_retry_job.yml) have no "inventory" task at all — this always
        # raises there, which is expected; fall through to auto-generation.
        log.info("taskValues.get(taskKey='inventory') unavailable (%s) — falling back to batch_id widget/auto-generation", _e)
_max_chunks         = _get_widget("max_concurrent_chunks", "3")
_parallel_threads   = _get_widget("parallel_threads",      "4")
_chunk_capacity_gb  = _get_widget("chunk_capacity_gb",     "50")
_min_executors      = _get_widget("min_executors",         "8")
# Target schema override (for same-workspace self-clone tests)
_tgt_schema_override = _get_widget("target_schema", "")
_force_reonboard = _get_widget("force_reonboard", "false").strip().lower() == "true"
_require_target_precreated = _get_widget("require_target_precreated", "false").strip().lower() == "true"
_skip_describe_detail = _get_widget("skip_describe_detail", "false").strip().lower() == "true"
_inventory_parallel_threads = _get_widget("inventory_parallel_threads", "4")

log.info("Widget values read directly: mode=%s meta=%s.%s", _wmode, _meta_catalog, _meta_schema)

# Load config
yaml_path = _yaml_path if _input_type == "YAML" else ""
cfg = load_from_yaml(yaml_path) if yaml_path else load_defaults()

# Apply widget overrides directly (do not rely on _widget() in imported module)
# ── clone_type: YAML owns this when input_type=YAML ──────────────────────────
# migration.yaml's `migration.clone_type:` is already loaded into cfg above
# (load_from_yaml). The clone_type widget/job-parameter (databricks.yml's
# var.clone_type, blank by default) only exists to optionally force a mode
# for JOB/CSV runs (neither has any other place to specify it) — it must
# NOT clobber a YAML file's own clone_type, and a blank value must NOT
# clobber OrchestratorConfig's built-in default ("direct_adls") either.
if _input_type != "YAML" and _clone_type.strip():
    cfg.clone_type = _clone_type
cfg.meta_catalog         = _meta_catalog
cfg.meta_schema          = _meta_schema
# skip_describe_detail is always widget-driven (like require_target_precreated) —
# not owned by YAML/CSV, applies uniformly regardless of input_type.
cfg.skip_describe_detail = _skip_describe_detail
try:
    cfg.inventory_parallel_threads = max(1, int(_inventory_parallel_threads))
except ValueError:
    log.warning(
        "inventory_parallel_threads=%r is not a valid int — keeping default %d",
        _inventory_parallel_threads, cfg.inventory_parallel_threads,
    )
# Plain (non-secret) SQL warehouse ids — only override cfg if the widget was
# actually supplied, so a YAML file's own target.warehouse_id (if set) isn't
# silently clobbered by a blank job-parameter default.
if _target_wh_id.strip():
    cfg.target_warehouse_id = _target_wh_id.strip()
if _source_wh_id.strip():
    cfg.source_warehouse_id = _source_wh_id.strip()
cfg.validation_enabled   = _val_enabled.lower() == "true"
cfg.row_count_validation = _row_cnt_val.lower() == "true"
try: cfg.max_retries     = int(_max_retries)
except ValueError: pass
# ── JOB-mode-only override ───────────────────────────────────────────────────
# target_catalog (and source_catalogs/source_schemas/source_tables, applied
# later via `params`) are databricks.yml bundle variables that exist ONLY to
# drive JOB-mode (widget-based) table selection. When input_type is YAML or
# CSV, source/target catalog.schema.table MUST come entirely from that file
# (InputResolver._resolve_yaml / _resolve_csv already ignore these widgets for
# selection purposes) — but this default_target_catalog assignment used to run
# unconditionally and would silently clobber a YAML file's own
# `target.catalog:` setting with databricks.yml's var.target_catalog default.
# Gate it to JOB mode only so YAML/CSV configs are never shadowed.
if _input_type == "JOB" and _tgt_cat:
    cfg.default_target_catalog = _tgt_cat
# Global exclusion list — applies regardless of input_type (see
# orchestrator/exclusion_manager.py). Independent knob, not tied to
# JOB/YAML/CSV precedence rules above.
cfg.exclusion_csv_path = _exclusion_csv_path
if _cluster_pool and _cluster_pool.strip() not in ("[]", ""):
    import json as _json
    from orchestrator.config import ClusterConfig
    cfg.cluster_pool = [ClusterConfig(c["cluster_id"], c.get("capacity_units", 10))
                        for c in _json.loads(_cluster_pool)]

# Worker cluster config (ephemeral job cluster for chunk workers)
if _worker_cluster and _worker_cluster.strip() not in ("", "{}"):
    import json as _json2
    cfg.worker_cluster_config = _json2.loads(_worker_cluster)
    # Defensive default: without an explicit data_security_mode, some
    # workspaces create these ephemeral chunk clusters in a non-UC
    # ("No Isolation Shared" / NONE) mode, which then fails the moment the
    # chunk worker touches a Unity Catalog table/catalog. This safety net
    # patches any client-supplied worker_cluster_json that omits the field —
    # AUTO picks the best UC-compatible mode for the workspace/policy without
    # needing a single_user_name.
    if "data_security_mode" not in cfg.worker_cluster_config:
        cfg.worker_cluster_config["data_security_mode"] = "DATA_SECURITY_MODE_AUTO"
        log.info("worker_cluster_json had no data_security_mode — defaulted to DATA_SECURITY_MODE_AUTO for Unity Catalog compatibility")
    log.info("Worker cluster config loaded (%d keys)", len(cfg.worker_cluster_config))

# Worker/chunk-worker notebook path — MUST match the actual deployed workspace
# path (varies per bundle target/user). Falls back to auto-detecting the
# currently-running notebook's own path (same directory as chunk_worker_notebook)
# if the widget wasn't supplied, rather than trusting a hardcoded default.
if _worker_nb_path.strip():
    cfg.worker_notebook_path = _worker_nb_path.strip()
    log.info("Worker notebook path (from widget): %s", cfg.worker_notebook_path)
else:
    try:
        _ctx_path = (
            dbutils.notebook.entry_point.getDbutils().notebook()
            .getContext().notebookPath().get()
        )
        if _ctx_path:
            cfg.worker_notebook_path = _ctx_path.rsplit("/", 1)[0] + "/worker_notebook"
            log.info("Worker notebook path (auto-detected from context): %s", cfg.worker_notebook_path)
    except Exception as _e:
        log.warning(
            "Could not auto-detect notebook path (%s) — falling back to default: %s",
            _e, cfg.worker_notebook_path,
        )

# When using ephemeral new_cluster config (no fixed cluster_pool), inject a virtual pool entry.
if cfg.worker_cluster_config and not cfg.cluster_pool:
    from orchestrator.config import ClusterConfig
    _virtual_capacity = 20
    cfg.cluster_pool = [ClusterConfig(cluster_id="auto", capacity_units=_virtual_capacity)]
    log.info("Injected virtual 'auto' cluster (capacity=%d) for ephemeral dispatch", _virtual_capacity)

# ── Batch / Chunk settings ────────────────────────────────────────────────────
# auto-generate batch_id if not provided: <user>-<date>-<short-uuid>
import getpass as _gp
_default_batch = f"batch-{datetime.now(tz=timezone.utc).strftime('%Y%m%d')}-{str(uuid.uuid4())[:6]}"
cfg.batch_id                  = _batch_id or _default_batch

# Publish the effective batch_id as a Databricks Jobs task value so that
# DOWNSTREAM tasks in the SAME job run (e.g. deep_clone/retry/validate in
# resources/06_full_migration_workflow.yml) can pick up whatever batch_id
# THIS task actually used — whether it was explicitly passed in or just
# auto-generated above. Without this, each task would independently
# auto-generate its OWN random batch_id whenever the job-level "batch_id"
# parameter is left blank, so INVENTORY's rows would never be found by
# DEEP_CLONE/VALIDATE (they all use the SAME job-level parameter value, but
# each falls back to a DIFFERENT random UUID when it's blank). The matching
# downstream tasks reference this via "{{tasks.inventory.values.batch_id}}"
# in their base_parameters instead of "{{job.parameters.batch_id}}".
# Harmless/no-op outside a job run (e.g. interactive/local) or on task types
# that don't support taskValues.
try:
    dbutils.jobs.taskValues.set(key="batch_id", value=cfg.batch_id)
    log.info("Published task value batch_id=%s for downstream tasks", cfg.batch_id)
except Exception as _e:
    log.info("Could not publish batch_id task value (likely not running as a job task): %s", _e)

try: cfg.max_concurrent_chunks     = int(_max_chunks)
except ValueError: pass
try: cfg.parallel_threads_per_chunk = int(_parallel_threads)
except ValueError: pass
try: cfg.chunk_capacity_gb          = float(_chunk_capacity_gb)
except ValueError: pass
try: cfg.min_executors_per_chunk    = int(_min_executors)
except ValueError: pass

log.info("Batch settings: batch_id=%s  max_chunks=%d  threads/chunk=%d  chunk_cap=%.0f GB  min_exec=%d",
         cfg.batch_id, cfg.max_concurrent_chunks, cfg.parallel_threads_per_chunk,
         cfg.chunk_capacity_gb, cfg.min_executors_per_chunk)

# Build a minimal params object for input_resolver
class _Params:
    mode            = _wmode
    clone_type      = _clone_type
    input_type      = _input_type
    selection_type  = _sel_type
    source_catalogs = json.loads(_src_cats) if _src_cats.strip() not in ("[]","") else []
    source_schemas  = json.loads(_src_schs) if _src_schs.strip() not in ("[]","") else []
    source_tables   = json.loads(_src_tbls) if _src_tbls.strip() not in ("[]","") else []
    target_catalog  = _tgt_cat
    target_schema   = _tgt_schema_override   # ← new: overrides all target schemas for SCHEMA-sel

    @property
    def effective_input_path(self):
        # MUST be driven by input_type, not just "whichever path widget happens
        # to be non-blank". yaml_config_path now has a standing non-blank
        # default (${var}/configs/migration.yaml), so a naive `_yaml_path or
        # _csv_path` fallback silently ignored input_type=CSV runs and always
        # resolved against the YAML file instead of the requested CSV file.
        it = (self.input_type or "").strip().upper()
        if it == "CSV":
            return _csv_path
        if it == "YAML":
            return _yaml_path
        return _yaml_path or _csv_path  # JOB mode (or unset) doesn't use this path at all

    @property
    def max_retries(self): return cfg.max_retries

params = _Params()

# Validate mode
valid_modes = {"INVENTORY", "DEEP_CLONE", "VALIDATE", "RETRY", "DRY_RUN"}
if params.mode.upper() not in valid_modes:
    raise ValueError(f"Invalid mode '{params.mode}'. Must be one of {valid_modes}")

# Generate run_id
cfg.run_id = _run_id_wg or f"run-{datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%S')}-{str(uuid.uuid4())[:6]}"

MODE = params.mode.upper()
log.info("="*60)
log.info("Migration Orchestrator — Mode=%s  run_id=%s", MODE, cfg.run_id)
log.info("Source: %s", cfg.source_workspace_url)
log.info("Target: %s", cfg.target_workspace_url)
log.info("Clone type: %s  |  Input type: %s", cfg.clone_type, params.input_type)
log.info("="*60)

# Row-count validation aggregates — only populated when MODE == VALIDATE
# (defaulted here so the final RunSummary can reference them unconditionally
# regardless of which mode actually ran).
rc_checked = rc_matched = rc_mismatched = 0

# Config validation
cfg_errors = validate_config(cfg, MODE)
if cfg_errors:
    raise ValueError("Configuration errors:\n" + "\n".join(cfg_errors))

# COMMAND ----------

# ── 3. Initialise clients ──────────────────────────────────────────────────────
# No client_id/client_secret/Databricks Secret scope anywhere here — auth is
# native via databricks.sdk.core.Config() inside SqlClient/ApiClient. Blank
# workspace_url auto-detects the current/attached workspace.
src_sql = SqlClient(
    warehouse_id  = cfg.source_warehouse_id,
    workspace_url = cfg.source_workspace_url,
    throttle_s    = cfg.api_throttle_delay_s,
)

tgt_sql = SqlClient(
    warehouse_id  = cfg.target_warehouse_id,
    workspace_url = cfg.target_workspace_url,
    throttle_s    = cfg.api_throttle_delay_s,
)

src_api = ApiClient(cfg.source_workspace_url)
tgt_api = ApiClient(cfg.target_workspace_url)

# Start warehouses if needed.
# The SOURCE warehouse is only ever touched by INVENTORY/DRY_RUN/VALIDATE, and
# even then only when clone_type=direct_adls — for clone_type=delta_share those
# modes route entirely through tgt_sql instead (source tables are read via the
# shared catalog on the TARGET workspace; see _disc_sql/_val_src_sql above).
# DEEP_CLONE and RETRY NEVER use src_sql/src_api at all (chunk workers run the
# actual CLONE statement directly via Spark on their own cluster, not through
# this REST client) — so starting the source warehouse for those modes is both
# unnecessary and a needless extra point of failure (e.g. a transient 500 from
# the source workspace's OIDC token endpoint used to crash DEEP_CLONE/RETRY
# runs outright, even though they never needed that warehouse).
_needs_src_warehouse = MODE in ("INVENTORY", "DRY_RUN", "VALIDATE") and cfg.clone_type != "delta_share"
if _needs_src_warehouse:
    log.info("Ensuring source warehouse is running...")
    src_sql.start_warehouse()
else:
    log.info(
        "Skipping source warehouse start — not needed for mode=%s clone_type=%s",
        MODE, cfg.clone_type,
    )
log.info("Ensuring target warehouse is running...")
tgt_sql.start_warehouse()

# COMMAND ----------

# ── 4. Core components ─────────────────────────────────────────────────────────
classifier = WorkloadClassifier(cfg)
audit      = AuditManager(cfg, tgt_sql, cfg.run_id)
pool       = ClusterPool(cfg, tgt_api)
scheduler  = Scheduler(cfg, pool, audit)
worker     = CloneWorker(cfg, tgt_api, audit)
# For delta_share, source tables live on the TARGET workspace — validate against tgt_sql.
_val_src_sql = tgt_sql if cfg.clone_type == "delta_share" else src_sql
validator_ = Validator(cfg, _val_src_sql, tgt_sql)
retry_mgr  = RetryManager(cfg, audit)

started_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# ── 5. MODE dispatch ───────────────────────────────────────────────────────────
_inventory_stats = None   # populated below only when MODE == INVENTORY; see the
                           # "metrics" override near the final summary section.

# ─────────────────────────────── INVENTORY ────────────────────────────────────
if MODE in ("INVENTORY", "DRY_RUN"):
    log.info("PHASE 1 — INVENTORY / TABLE DISCOVERY")
    log.info("Input: type=%s sel=%s schemas=%s effective_input_path=%s",
              _input_type, _sel_type, _src_schs, params.effective_input_path)

    # For delta_share clone_type, source tables reside on the TARGET workspace
    # (same-metastore self-clone). Use tgt_api/tgt_sql so SHOW TABLES succeeds.
    # For direct_adls, source tables are on the SOURCE workspace (cross-region).
    _disc_api = tgt_api if cfg.clone_type == "delta_share" else src_api
    _disc_sql = tgt_sql if cfg.clone_type == "delta_share" else src_sql
    log.info("Table discovery client: %s workspace (%s mode)",
             "TARGET" if cfg.clone_type == "delta_share" else "SOURCE", cfg.clone_type)

    # Pass disc_sql as fallback for hive_metastore / non-UC catalogs
    src_resolver = InputResolver(cfg, _disc_api, source_sql=_disc_sql)
    selections   = src_resolver.resolve(params)
    log.info("Resolved %d table selections", len(selections))
    log.info("Resolved %d table(s) for migration", len(selections))

    # ── Global exclusion list — applied on top of whatever input_type just
    # resolved, regardless of whether it was JOB/YAML/CSV. Excluded tables
    # never reach migration_control; they're recorded in
    # migration_exclusion_log instead (INVENTORY mode only — see below).
    _excl_rules = load_exclusion_rules(cfg.exclusion_csv_path)
    selections, _excluded = apply_exclusions(selections, _excl_rules)
    if _excl_rules:
        log.info(
            "Exclusion list applied (%s): %d table(s) excluded, %d remain",
            cfg.exclusion_csv_path, len(_excluded), len(selections),
        )
        print(f"\n{'─'*68}")
        print(f"  EXCLUSION LIST — {cfg.exclusion_csv_path}")
        print(f"  {len(_excluded)} table(s) excluded, {len(selections)} remain")
        for _sel, _rule in _excluded:
            print(f"  SKIP  {_sel.source_fqn}  (matched {_rule.describe()})")
        print(f"{'─'*68}\n")

    if MODE == "DRY_RUN":
        print(f"\n{'─'*68}")
        print(f"  DRY RUN — {len(selections)} tables selected (no data will be moved)")
        print(f"{'─'*68}")
        for s in selections:
            print(f"  {s.source_fqn} → {s.target_fqn}")
        print(f"{'─'*68}\n")

        # Show workload classification for each
        print(f"{'─'*68}")
        print("  Workload classification")
        print(f"{'─'*68}")
        class_summary = {}
        for s in selections:
            wl_cls, wl_wt = classifier.classify_gb(0)  # 0 = unknown size; use DESCRIBE DETAIL for real
            class_summary[wl_cls] = class_summary.get(wl_cls, 0) + 1
        for cls, cnt in class_summary.items():
            print(f"  {cls:<8}: {cnt} table(s)")
        print(f"{'─'*68}\n")

    else:
        # Reconcile stale records from any crashed prior run
        stale = audit.reconcile_stale_records(cfg.stale_threshold_minutes)
        if stale:
            log.info("Reconciled %d stale IN_PROGRESS records", stale)

        inv_mgr = InventoryManager(cfg, _disc_sql, tgt_sql, classifier, cfg.run_id)
        stats   = inv_mgr.run_inventory(
            selections, force=_force_reonboard,
            require_target_precreated=_require_target_precreated,
        )
        log.info(
            "Inventory complete: total=%d onboarded=%d skipped=%d (target_missing=%d) failed=%d "
            "force_reonboard=%s require_target_precreated=%s skip_describe_detail=%s "
            "inventory_parallel_threads=%d",
            stats["total"], stats["inserted"], stats["skipped"],
            stats["skipped_target_missing"], stats["failed"],
            _force_reonboard, _require_target_precreated, _skip_describe_detail,
            cfg.inventory_parallel_threads,
        )
        if _skip_describe_detail and stats["inserted"]:
            print(
                f"  ⚠ skip_describe_detail=true — {stats['inserted']} table(s) onboarded "
                f"WITHOUT source metadata: size_in_bytes/size_gb/source_num_files/"
                f"workload_class are NULL in migration_control, and the source "
                f"existence/Delta-format check was skipped (a bad source table will "
                f"only surface as a DEEP_CLONE-time failure instead of an INVENTORY-time one)."
            )
        if _require_target_precreated and stats["skipped_target_missing"]:
            print(
                f"  ⚠ {stats['skipped_target_missing']} table(s) SKIPPED — target not "
                f"pre-created (require_target_precreated=true). Pre-create the target "
                f"table(s) and re-run INVENTORY with force_reonboard=true to queue them."
            )
        # Audit trail for the global exclusion list (DRY_RUN never writes to
        # migration_* tables — this only fires for real INVENTORY runs).
        if _excluded:
            audit.record_exclusions(cfg.batch_id, _excluded)
        # Ground truth for THIS invocation's own summary — see the
        # "_inventory_stats" override applied to `metrics` further below for why
        # this can't just rely on get_run_metrics()'s run_id-scoped DB query.
        _inventory_stats = stats

        # ── Bin-packing: assign chunk_id to THIS BATCH's QUEUED records only ───
        # Filter by cfg.batch_id so parallel inventories don't steal each other's
        # records. batch_id is now set during the inventory INSERT (inventory_manager._upsert).
        log.info("PHASE 1b — BATCH PLANNING (bin-packing into chunks)")
        all_queued = audit.get_queued_records(limit=50000, batch_id=cfg.batch_id)
        if all_queued:
            _max_tables_per_chunk = _derive_max_tables_per_chunk(len(all_queued), cfg.max_concurrent_chunks)
            log.info(
                "Derived max_tables_per_chunk=%s from %d records / max_concurrent_chunks=%d "
                "(guarantees >= %d chunks so cluster parallelism isn't defeated by tiny tables)",
                _max_tables_per_chunk, len(all_queued), cfg.max_concurrent_chunks, cfg.max_concurrent_chunks,
            )
            chunks = plan_chunks(
                records               = all_queued,
                batch_id              = cfg.batch_id,
                chunk_capacity_gb     = cfg.chunk_capacity_gb,
                max_tables_per_chunk  = _max_tables_per_chunk,
            )
            assigned = audit.assign_batch_chunks(cfg.batch_id, chunks)
            log.info(
                "Batch plan: %d chunks | %d tables assigned to batch_id=%s",
                len(chunks), assigned, cfg.batch_id,
            )
            print(f"\n{'─'*68}")
            print(f"  Batch ID : {cfg.batch_id}")
            print(f"  Chunks   : {len(chunks)}  (max {cfg.max_concurrent_chunks} run concurrently)")
            print(f"  Tables   : {assigned}")
            print(f"  Cluster  : {cfg.min_executors_per_chunk} workers per chunk cluster")
            print(f"  Threads  : {cfg.parallel_threads_per_chunk} parallel copies per cluster")
            print(f"{'─'*68}")
            for c in chunks:
                print(f"  Chunk {c.chunk_id:02d}: {c.size:3d} tables | {c.total_gb:7.2f} GB")
            print(f"{'─'*68}\n")
        else:
            log.info("No QUEUED records to plan (inventory may have been empty or all skipped)")


# ────────────────────────────── DEEP_CLONE ────────────────────────────────────
elif MODE == "DEEP_CLONE":
    log.info("PHASE 2 — DEEP CLONE (Batch/Chunk model)")
    log.info("  batch_id             = %s", cfg.batch_id)
    log.info("  max_concurrent_chunks= %d", cfg.max_concurrent_chunks)
    log.info("  parallel_threads     = %d", cfg.parallel_threads_per_chunk)
    log.info("  chunk_capacity_gb    = %.0f GB", cfg.chunk_capacity_gb)
    log.info("  min_executors        = %d", cfg.min_executors_per_chunk)

    # Reconcile stale IN_PROGRESS records from prior crashed runs (for this batch)
    stale = audit.reconcile_stale_records(cfg.stale_threshold_minutes)
    if stale:
        log.info("Reconciled %d stale IN_PROGRESS records", stale)

    # Fetch all QUEUED records for THIS batch
    queued = audit.get_queued_records(limit=50000, batch_id=cfg.batch_id)
    log.info("QUEUED records for batch %s: %d", cfg.batch_id, len(queued))

    if not queued:
        log.warning(
            "No QUEUED records found for batch_id='%s'. "
            "Run INVENTORY first, or check that batch_id matches.",
            cfg.batch_id,
        )
    else:
        # Check whether chunks are already assigned (re-run / retry scenario)
        # If chunk_id is 0 or NULL for any records, re-plan
        unplanned = [r for r in queued if not r.get("chunk_id")]
        if unplanned:
            log.info("%d records lack chunk assignment — running bin-packing now", len(unplanned))
            _max_tables_per_chunk = _derive_max_tables_per_chunk(len(queued), cfg.max_concurrent_chunks)
            chunks = plan_chunks(
                records               = queued,
                batch_id              = cfg.batch_id,
                chunk_capacity_gb     = cfg.chunk_capacity_gb,
                max_tables_per_chunk  = _max_tables_per_chunk,
            )
            audit.assign_batch_chunks(cfg.batch_id, chunks)
        else:
            # Reconstruct ChunkAssignment objects from the existing chunk_id grouping
            from orchestrator.models import ChunkAssignment, ChunkStatus
            chunk_map = {}
            for r in queued:
                cid = int(r.get("chunk_id") or 0)
                if cid not in chunk_map:
                    chunk_map[cid] = ChunkAssignment(
                        chunk_id=cid, batch_id=cfg.batch_id,
                        status=ChunkStatus.PENDING.value)
                chunk_map[cid].migration_ids.append(r["migration_id"])
                chunk_map[cid].total_gb += float(r.get("size_gb") or 0)
            chunks = sorted(chunk_map.values(), key=lambda c: c.chunk_id)
            log.info("Resuming %d existing chunks", len(chunks))

        print(f"\n{'─'*68}")
        print(f"  DEEP CLONE — Batch: {cfg.batch_id}")
        print(f"  Chunks: {len(chunks)}  |  Max concurrent: {cfg.max_concurrent_chunks}")
        print(f"{'─'*68}")
        for c in chunks:
            print(f"  Chunk {c.chunk_id:02d}: {c.size:3d} tables | {c.total_gb:7.2f} GB")
        print(f"{'─'*68}\n")

        # Ensure worker_cluster_config has at least min_executors
        if cfg.worker_cluster_config:
            cc = dict(cfg.worker_cluster_config)
            if cc.get("num_workers", 0) < cfg.min_executors_per_chunk:
                cc["num_workers"] = cfg.min_executors_per_chunk
                cfg.worker_cluster_config = cc

        # Define the chunk dispatch function (called by BatchChunkScheduler per thread)
        def dispatch_chunk_fn(chunk):
            return worker.dispatch_chunk(chunk)

        # Run the batch scheduler — slides max_concurrent_chunks window across all chunks
        chunk_scheduler = BatchChunkScheduler(cfg, audit, dispatch_chunk_fn)
        dc_stats = chunk_scheduler.run(chunks)

        log.info("DEEP_CLONE complete: %s", dc_stats)
        print(f"\n{'─'*68}")
        print(f"  DEEP CLONE Summary — batch={cfg.batch_id}")
        print(f"  Chunks completed : {dc_stats['completed_chunks']}")
        print(f"  Chunks failed    : {dc_stats['failed_chunks']}")
        print(f"  Tables ok        : {dc_stats['completed_tables']}")
        print(f"  Tables failed    : {dc_stats['failed_tables']}")
        print(f"{'─'*68}\n")

        # Fail the job run itself if any chunk/table failed to dispatch or clone —
        # otherwise the job would falsely report SUCCESS while cloning 0 tables
        # (mirrors the VALIDATE branch's "fail job on val_failed > 0" pattern below).
        if dc_stats.get('failed_chunks', 0) > 0 or dc_stats.get('failed_tables', 0) > 0:
            raise RuntimeError(
                f"DEEP_CLONE had {dc_stats['failed_chunks']} failed chunk(s) / "
                f"{dc_stats['failed_tables']} failed table(s): {dc_stats}"
            )


# ─────────────────────────────── VALIDATE ─────────────────────────────────────
elif MODE == "VALIDATE":
    log.info("PHASE 3 — VALIDATE (batch=%s)", cfg.batch_id)

    # Fetch COMPLETED records filtered to this batch (or all if no batch_id)
    records = audit.get_completed_records(batch_id=cfg.batch_id)
    log.info("Validating %d COMPLETED records for batch=%s", len(records), cfg.batch_id or "ALL")

    val_pass = val_fail = 0
    rc_checked = rc_matched = rc_mismatched = 0
    for rec in records:
        mid = rec["migration_id"]
        if rec.get("validation_status") == "VALIDATED":
            log.debug("Skipping already-validated %s", mid[:8])
            continue
        try:
            v_status, v_msg, rc = validator_.validate(rec)
            if rc.checked:
                rc_checked += 1
                if rc.matched is True:
                    rc_matched += 1
                elif rc.matched is False:
                    rc_mismatched += 1
            if v_status == "VALIDATED":
                audit.mark_validated(mid, v_msg, rc.source_row_count, rc.target_row_count)
                val_pass += 1
            else:
                audit.mark_validation_failed(mid, v_msg, rc.source_row_count, rc.target_row_count)
                val_fail += 1
                log.warning("VALIDATION_FAILED %s: %s", mid[:8], v_msg[:120])
            # Immutable audit trail — one row per VALIDATE attempt, regardless
            # of pass/fail, so row counts are reviewable historically even
            # after migration_control's single "latest" row gets overwritten
            # by a later re-validation.
            audit.record_validation_history(
                mid, rec, v_status, v_msg,
                source_row_count=rc.source_row_count,
                target_row_count=rc.target_row_count,
                row_count_checked=rc.checked,
                row_count_matched=rc.matched,
            )
        except Exception as e:
            log.error("Validation exception for %s: %s", mid[:8], e)
            audit.mark_validation_failed(mid, f"EXCEPTION: {e}")
            audit.record_validation_history(mid, rec, "VALIDATION_FAILED", f"EXCEPTION: {e}")
            val_fail += 1

    log.info(
        "Validation complete: passed=%d failed=%d | row_count_checked=%d matched=%d mismatched=%d",
        val_pass, val_fail, rc_checked, rc_matched, rc_mismatched,
    )


# ─────────────────────────────── RETRY ────────────────────────────────────────
elif MODE == "RETRY":
    log.info("PHASE 4 — RETRY (batch=%s)", cfg.batch_id)
    retry_stats = retry_mgr.run_retry(backoff=True, batch_id=cfg.batch_id)
    log.info("Retry complete: %s", retry_stats)

    # After requeuing, run the chunk scheduler for retry tables
    if retry_stats.get("requeued", 0) > 0:
        log.info("Running chunk scheduling pass for requeued tables")
        queued = audit.get_queued_records(limit=50000, batch_id=cfg.batch_id)
        if queued:
            from orchestrator.models import ChunkAssignment, ChunkStatus
            chunk_map = {}
            for r in queued:
                cid = int(r.get("chunk_id") or 0)
                if cid not in chunk_map:
                    chunk_map[cid] = ChunkAssignment(
                        chunk_id=cid, batch_id=cfg.batch_id,
                        status=ChunkStatus.PENDING.value)
                chunk_map[cid].migration_ids.append(r["migration_id"])
                chunk_map[cid].total_gb += float(r.get("size_gb") or 0)
            retry_chunks = sorted(chunk_map.values(), key=lambda c: c.chunk_id)

            def dispatch_chunk_fn_retry(chunk):
                return worker.dispatch_chunk(chunk)

            retry_scheduler = BatchChunkScheduler(cfg, audit, dispatch_chunk_fn_retry)
            r_stats = retry_scheduler.run(retry_chunks)
            log.info("Post-retry dispatch: %s", r_stats)

else:
    raise ValueError(f"Unknown mode: {MODE}")

# COMMAND ----------

# ── 6. Final metrics and summary ───────────────────────────────────────────────
ended_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
metrics  = audit.get_run_metrics()

# BUG FIX: get_run_metrics() queries migration_control WHERE run_id = cfg.run_id.
# That's accurate for DEEP_CLONE/VALIDATE/RETRY (they stamp run_id on every row
# they touch, including ones whose status doesn't change). It is NOT accurate
# for INVENTORY: InventoryManager._process_one() intentionally does NOT
# re-stamp rows it SKIPS (already COMPLETED/VALIDATED/FAILED_PERMANENT/
# SKIPPED — see its docstring on idempotency), so those rows keep whatever
# run_id/batch_id a PAST run set. An INVENTORY run that resolves N tables from
# CSV/YAML but finds all N already done would then show a misleading
# "Total discovered: 0" here — even though resolution worked fine and the
# skip was the correct, intentional behavior. Override with the ground-truth
# counts INVENTORY itself just observed instead.
if MODE == "INVENTORY" and _inventory_stats is not None:
    metrics["total"]   = _inventory_stats["total"]
    metrics["skipped"] = _inventory_stats["skipped"]
    metrics["failed"]  = _inventory_stats["failed"]
    metrics["queued"]  = _inventory_stats["inserted"]   # newly (re-)onboarded this run

summary = RunSummary(
    mode       = MODE,
    run_id     = cfg.run_id,
    started_at = started_at,
    ended_at   = ended_at,
    clone_type = cfg.clone_type,      # EFFECTIVE value actually used (post YAML/JOB resolution) —
    input_type = _input_type,         # NOT the raw job parameter shown in the Databricks UI
                                       # "Parameters" panel, which always displays the widget's
                                       # configured/default value regardless of what the notebook
                                       # code actually decided to use internally.
    total      = metrics.get("total", 0),
    queued     = metrics.get("queued", 0),
    completed  = metrics.get("completed", 0),
    validated  = metrics.get("validated", 0),
    failed     = metrics.get("failed", 0),
    skipped    = metrics.get("skipped", 0),
    val_failed = metrics.get("validation_failed", 0),
    total_bytes = metrics.get("total_bytes", 0),
    row_count_checked    = rc_checked,
    row_count_matched    = rc_matched,
    row_count_mismatched = rc_mismatched,
)
summary.print_report()

# ── Cluster pool snapshot ──────────────────────────────────────────────────────
if cfg.cluster_pool:
    print("Cluster pool utilization:")
    for snap in pool.snapshot():
        print(f"  {snap['cluster_id'][:16]} | total={snap['total']} used={snap['used']} free={snap['free']} available={snap['available']}")

# ── Fail job if validation failures exist ──────────────────────────────────────
if summary.val_failed > 0:
    raise Exception(f"{summary.val_failed} table(s) failed validation — check migration_control for details")
