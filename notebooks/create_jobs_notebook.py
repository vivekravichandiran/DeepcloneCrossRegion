# Databricks notebook source
# MAGIC %md
# MAGIC # DeepClone CrossRegion — Create Jobs (CSV mode, from a notebook)
# MAGIC
# MAGIC This notebook reproduces `databricks bundle deploy` **from inside the
# MAGIC workspace** for a **CSV-driven** migration. It exposes only the widgets
# MAGIC relevant to CSV mode; everything specific to the other input modes
# MAGIC (JOB-mode filters, YAML config) and per-run invariants (input_type=CSV,
# MAGIC auto batch_id, notebook paths, …) is abstracted into `ABSTRACTED_DEFAULTS`.
# MAGIC It **create-or-updates the identical 7 jobs** via the Jobs REST API:
# MAGIC
# MAGIC | # | Job |
# MAGIC |---|-----|
# MAGIC | 0 | Setup Control Tables |
# MAGIC | 1 | INVENTORY |
# MAGIC | 2 | DRY_RUN |
# MAGIC | 3 | DEEP_CLONE |
# MAGIC | 4 | VALIDATE |
# MAGIC | 5 | RETRY |
# MAGIC | – | Full Migration Workflow |
# MAGIC
# MAGIC The pure spec-building logic lives in `orchestrator/job_factory.py`
# MAGIC (unit-tested by `tests/test_job_factory.py`). Task `base_parameters`
# MAGIC keep the exact `{{job.parameters.*}}` / `{{tasks.inventory.values.batch_id}}`
# MAGIC templating, so jobs created here behave identically to bundle-deployed ones.
# MAGIC
# MAGIC **Auth:** native (`databricks.sdk.WorkspaceClient()`), no secrets.

# COMMAND ----------

import os
import sys
import json

# ── Path setup so `orchestrator` is importable (DAB deploys repo to file_path) ──
_repo_root = ""
try:
    _repo_root = dbutils.widgets.get("repo_root")  # noqa: F821
except Exception:
    pass
for _p in [
    _repo_root,
    os.environ.get("PYTHONPATH", ""),
    os.path.dirname(os.path.abspath("__file__")) if "__file__" in dir() else "",
    "..",
]:
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

from orchestrator.job_factory import (  # noqa: E402
    build_all_job_specs,
    resolve_workspace_params,
    as_reset_body,
    DEFAULT_PARAMS,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets — mirror `databricks.yml` `variables:` (edit and re-run)

# COMMAND ----------

# This is a CSV-mode job creator. Only parameters relevant to a CSV-driven
# migration are exposed as widgets. Everything specific to the other input
# modes (JOB-mode source/target filters, YAML config path), plus values that
# should never vary per run (input_type=CSV, auto-generated batch_id, notebook
# paths, etc.) are abstracted away into ABSTRACTED_DEFAULTS further below.

# Widget labels are numbered 01..21 so the Databricks widget bar (which orders
# widgets alphabetically by label) shows them in this exact, logical sequence.

# ── General ───────────────────────────────────────────────────────────────────
dbutils.widgets.text("email", "vivek.ravichandiran@databricks.com", "01 · Notification email")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "02 · Dry run (preview only, no changes)")

# ── Control tables / meta ─────────────────────────────────────────────────────
dbutils.widgets.text("meta_catalog", "ril_migration_catalog", "03 · Meta catalog (control tables)")
dbutils.widgets.text("meta_schema", "migration_meta", "04 · Meta schema (control tables)")

# ── CSV input (the only supported input mode for this job) ────────────────────
dbutils.widgets.text("csv_path", "", "05 · CSV path (blank = <file_path>/configs/csv_scale_ril_bulk_02_full.csv)")
dbutils.widgets.text("exclusion_csv_path", "", "06 · Exclusion CSV path (blank = <file_path>/configs/exclusion_list_example.csv)")

# ── Clone behaviour ───────────────────────────────────────────────────────────
dbutils.widgets.dropdown("clone_type", "delta_share", ["delta_share", "direct_adls"], "07 · Clone type")
dbutils.widgets.dropdown("validation_enabled", "true", ["true", "false"], "08 · Validation enabled")
dbutils.widgets.dropdown("row_count_validation", "true", ["true", "false"], "09 · Row-count validation")
dbutils.widgets.dropdown("force_reonboard", "false", ["true", "false"], "10 · Force re-onboard")
dbutils.widgets.dropdown("require_target_precreated", "false", ["true", "false"], "11 · Require target pre-created")

# ── Warehouse (plain id, NOT a secret) ───────────────────────────────────────
dbutils.widgets.text("target_warehouse_id", "5fe1692f119e2528", "12 · Target SQL warehouse id")

# ── Compute ───────────────────────────────────────────────────────────────────
dbutils.widgets.text("worker_spark_version", "17.3.x-scala2.13", "13 · Worker Spark version")
dbutils.widgets.text("worker_node_type", "Standard_E32ds_v5", "14 · Worker node type")
dbutils.widgets.text("worker_num_workers", "8", "15 · Worker node count")
dbutils.widgets.text("instance_pool_id", "0908-093007-hoped4-pool-q4mi8hia", "16 · Instance pool id (orchestrator)")

# ── Chunk execution ───────────────────────────────────────────────────────────
dbutils.widgets.text("max_concurrent_chunks", "5", "17 · Max concurrent chunks")
dbutils.widgets.text("parallel_threads", "4", "18 · Parallel threads per chunk")
dbutils.widgets.text("chunk_capacity_gb", "500", "19 · Chunk capacity (GB)")
dbutils.widgets.text("min_executors", "8", "20 · Min executors per chunk")
dbutils.widgets.text("inventory_parallel_threads", "4", "21 · Inventory parallel threads")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve parameters

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Workspace file root is always auto-detected from where this notebook lives
# (abstracted — no longer a widget). This notebook sits at
# <root>/notebooks/create_jobs_notebook, so the repo root is 2 levels up.
try:
    _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _file_path = "/Workspace" + "/".join(_nb.split("/")[:-2])
except Exception:
    _file_path = DEFAULT_PARAMS["workspace_file_path"]

# User-facing widgets (the only knobs this CSV-mode job exposes).
_widget_names = [
    "email", "meta_catalog", "meta_schema",
    "csv_path", "exclusion_csv_path",
    "clone_type", "validation_enabled", "row_count_validation",
    "force_reonboard", "require_target_precreated",
    "target_warehouse_id",
    "worker_spark_version", "worker_node_type", "worker_num_workers", "instance_pool_id",
    "max_concurrent_chunks", "parallel_threads", "chunk_capacity_gb",
    "min_executors", "inventory_parallel_threads",
]

# Abstracted (non-widget) values — fixed for a CSV-mode migration. Notebook/
# config paths are left blank here and auto-derived from the workspace file
# root by resolve_workspace_params() below (mirrors databricks.yml's
# ${workspace.file_path}/... defaults).
ABSTRACTED_DEFAULTS = {
    "bundle_target":        "prod",     # job name suffix -> "... [prod]"
    "input_type":           "CSV",      # this job is CSV-only
    "batch_id":             "",         # always auto-generated per run
    "max_retries":          "3",
    "retry_permanent":      "false",    # opt-in re-drive of FAILED_PERMANENT (off by default)
    "skip_describe_detail": "false",    # always collect full source metadata
    "source_warehouse_id":  "",         # only used by clone_type=direct_adls
    # JOB-mode-only selection (unused in CSV mode; kept for spec fidelity)
    "target_catalog":        "hive_metastore",
    "source_catalog_filter": '["ril_bulk"]',
    "source_schema_filter":  "[]",
    "source_table_filter":   "[]",
    # blank -> auto-derived from workspace file root
    "yaml_config_path":     "",
    "orchestrator_notebook": "",
    "worker_notebook":       "",
    "setup_notebook":        "",
}

params = {name: dbutils.widgets.get(name) for name in _widget_names}
params.update(ABSTRACTED_DEFAULTS)
params["workspace_file_path"] = _file_path
params = resolve_workspace_params(params)   # fills notebook/config paths from file_path

DRY_RUN = dbutils.widgets.get("dry_run").lower() == "true"

print(f"Workspace file root : {params['workspace_file_path']}")
print(f"Bundle target       : {params['bundle_target']}  (input_type={params['input_type']})")
print(f"CSV path            : {params['csv_path']}")
print(f"Orchestrator NB     : {params['orchestrator_notebook']}")
print(f"Dry run (preview)   : {DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the 7 job specs

# COMMAND ----------

specs = build_all_job_specs(params)
print(f"Built {len(specs)} job specs:\n")
for name in specs:
    print(f"  • {name}")

# Preview one spec for sanity
print("\n── Sample spec: Full Migration Workflow ──")
_wf = next(v for k, v in specs.items() if "Full Migration Workflow" in k)
print(json.dumps({
    "name": _wf["name"],
    "tasks": [{"task_key": t["task_key"], "depends_on": t.get("depends_on")} for t in _wf["tasks"]],
    "num_parameters": len(_wf["parameters"]),
}, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or update jobs (idempotent by name)
# MAGIC
# MAGIC Existing jobs are **patched in place** via `jobs/reset` (full-settings
# MAGIC overwrite that keeps the same `job_id` and run history) — this notebook
# MAGIC **never deletes and recreates** a job. Only genuinely new jobs are created.

# COMMAND ----------

def _find_job_id_by_name(name: str):
    """Return the job_id of an existing job with this exact name, else None."""
    for j in w.jobs.list(name=name):
        if j.settings and j.settings.name == name:
            return j.job_id
    return None

results = []
for name, settings in specs.items():
    existing_id = _find_job_id_by_name(name)
    action = "would create" if existing_id is None else "would update"
    if not DRY_RUN:
        if existing_id is None:
            created = w.api_client.do("POST", "/api/2.1/jobs/create", body=settings)
            job_id = created["job_id"]
            action = "created"
        else:
            w.api_client.do("POST", "/api/2.1/jobs/reset", body=as_reset_body(existing_id, settings))
            job_id = existing_id
            action = "updated"
    else:
        job_id = existing_id
    results.append({"job": name, "action": action, "job_id": job_id})
    print(f"[{action:>13}] {name}  (job_id={job_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

host = w.config.host.rstrip("/")
print(f"{'DRY RUN — no changes made' if DRY_RUN else 'Jobs synced'}: {len(results)} jobs\n")
for r in results:
    link = f"{host}/jobs/{r['job_id']}" if r["job_id"] else "(not yet created)"
    print(f"  {r['action']:>13}  {r['job']}\n                 {link}")

dbutils.notebook.exit(json.dumps(results))
