"""
job_factory.py — Pure, dependency-free builder for the Databricks Jobs API
settings of every job this project's Databricks Asset Bundle (DAB) defines.

Why this exists
---------------
`resources/*.yml` + `databricks.yml` describe 7 jobs that are normally created
via `databricks bundle deploy`. `notebooks/create_jobs_notebook.py` reproduces
that deploy *from inside the workspace* — it reads the same parameters (as
notebook widgets, mirroring databricks.yml `variables:`) and create-or-updates
the identical jobs via the Jobs REST API. This module holds the pure spec-
building logic so it can be unit-tested with zero Databricks/Spark/SDK deps.

Design
------
`build_all_job_specs(params)` returns an ordered dict {job_name -> settings}
where `settings` is the exact JSON body accepted by POST /api/2.1/jobs/create
(and, wrapped as {"job_id", "new_settings"}, by /reset).

Fidelity to the DAB:
  • Job names carry the `[<target>]` suffix, like "DeepClone 1 - INVENTORY [prod]".
  • Task `base_parameters` keep the SAME `{{job.parameters.*}}` templating and
    `{{tasks.inventory.values.batch_id}}` task-value wiring as the YAML, so the
    created jobs behave identically to bundle-deployed ones.
  • Only genuine deploy-time substitutions are resolved here: job name target,
    notebook/config paths (workspace_file_path), job-parameter DEFAULTS
    (resolved var values), and cluster specs (pool id, spark version, node
    type, num workers, PYTHONPATH), plus the embedded worker_cluster_json.

`params` is a flat dict of ALREADY-RESOLVED values (no `${...}` left). See
REQUIRED_PARAMS / DEFAULT_PARAMS below and the calling notebook.
"""
from __future__ import annotations

import copy
import json
from collections import OrderedDict
from typing import Any, Dict, List

# ── Parameter contract ────────────────────────────────────────────────────────
# Defaults mirror databricks.yml `variables:` (prod/dev effective values). The
# notebook overrides any of these from its widgets before calling the factory.
DEFAULT_PARAMS: Dict[str, Any] = {
    # deploy context
    "bundle_target": "prod",
    "workspace_file_path": "/Workspace/Users/me/deepclone_orchestrator/prod/files",
    "email": "vivek.ravichandiran@databricks.com",
    # control-table location
    "meta_catalog": "ril_migration_catalog",
    "meta_schema": "migration_meta",
    # table selection (JOB-mode filters)
    "target_catalog": "hive_metastore",
    "source_catalog_filter": '["ril_bulk"]',
    "source_schema_filter": "[]",
    "source_table_filter": "[]",
    # clone behaviour
    "clone_type": "delta_share",
    "validation_enabled": "true",
    "row_count_validation": "true",
    "max_retries": "3",
    # retry_permanent: OPT-IN re-drive of FAILED_PERMANENT tables on RETRY.
    # Default "false" keeps FAILED_PERMANENT terminal (safe default).
    "retry_permanent": "false",
    # compute
    "worker_spark_version": "17.3.x-scala2.13",
    "worker_node_type": "Standard_E32ds_v5",
    "worker_num_workers": "8",
    "instance_pool_id": "0908-093007-hoped4-pool-q4mi8hia",
    # warehouses (plain ids, not secrets)
    "target_warehouse_id": "5fe1692f119e2528",
    "source_warehouse_id": "",
    # batch / chunk
    "batch_id": "",
    "max_concurrent_chunks": "5",
    "parallel_threads": "4",
    "chunk_capacity_gb": "500",
    "min_executors": "8",
    # input source
    "input_type": "CSV",
    "yaml_config_path": "",   # notebook fills with <file_path>/configs/migration.yaml
    "csv_path": "",           # notebook fills with <file_path>/configs/csv_scale_ril_bulk_02_full.csv
    "exclusion_csv_path": "",
    # inventory flags
    "force_reonboard": "false",
    "require_target_precreated": "false",
    "skip_describe_detail": "false",
    "inventory_parallel_threads": "4",
    # notebook paths (resolved from workspace_file_path by the notebook)
    "orchestrator_notebook": "",
    "worker_notebook": "",
    "setup_notebook": "",
}

REQUIRED_PARAMS: List[str] = [
    "bundle_target", "workspace_file_path",
    "orchestrator_notebook", "worker_notebook", "setup_notebook",
    "target_warehouse_id",
]

_TAGS_TEAM = "deepclone"


def _merge_defaults(params: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(DEFAULT_PARAMS)
    merged.update(params or {})
    missing = [k for k in REQUIRED_PARAMS if not str(merged.get(k, "")).strip()]
    if missing:
        raise ValueError(f"job_factory: missing required params: {missing}")
    return merged


def _job_name(base: str, target: str) -> str:
    return f"{base} [{target}]"


def _param_list(pairs: List[tuple]) -> List[Dict[str, str]]:
    """Build a Jobs-API job-level `parameters` array from (name, default) pairs."""
    return [{"name": n, "default": str(d)} for n, d in pairs]


def _orchestrator_cluster(p: Dict[str, Any], *, key: str = "orchestrator_cluster",
                          adaptive: bool = False, spark_conf: bool = True) -> Dict[str, Any]:
    conf: Dict[str, str] = {}
    if spark_conf:
        conf["spark.databricks.delta.preview.enabled"] = "true"
    if adaptive:
        conf["spark.sql.adaptive.enabled"] = "true"
    new_cluster: Dict[str, Any] = {
        "spark_version": p["worker_spark_version"],
        "instance_pool_id": p["instance_pool_id"],
        "num_workers": 2,
        "data_security_mode": "DATA_SECURITY_MODE_AUTO",
        "spark_env_vars": {"PYTHONPATH": p["workspace_file_path"]},
    }
    if conf:
        new_cluster["spark_conf"] = conf
    return {"job_cluster_key": key, "new_cluster": new_cluster}


def _worker_cluster_json(p: Dict[str, Any], *, with_spark_conf: bool = True) -> str:
    """Ephemeral chunk-worker cluster spec, embedded as a JSON *string* param
    (exactly as the DAB passes it in base_parameters.worker_cluster_json)."""
    spec: Dict[str, Any] = {
        "spark_version": p["worker_spark_version"],
        "node_type_id": p["worker_node_type"],
        "azure_attributes": {"availability": "ON_DEMAND_AZURE"},
        "num_workers": int(p["worker_num_workers"]),
        "data_security_mode": "DATA_SECURITY_MODE_AUTO",
    }
    if with_spark_conf:
        spec["spark_conf"] = {"spark.databricks.delta.preview.enabled": "true"}
    return json.dumps(spec)


def _emails(p: Dict[str, Any], *, on_success: bool = False) -> Dict[str, Any]:
    notif: Dict[str, Any] = {"on_failure": [p["email"]]}
    if on_success:
        notif["on_success"] = [p["email"]]
    return notif


# ── Individual job builders ─────────────────────────────────────────────────

def build_setup_job(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": _job_name("DeepClone 0 - Setup Control Tables", p["bundle_target"]),
        "description": "Creates migration_control and migration_attempts Delta tables on the target workspace.",
        "tags": {"team": _TAGS_TEAM, "phase": "setup"},
        "parameters": _param_list([("target_warehouse_id", p["target_warehouse_id"])]),
        "tasks": [{
            "task_key": "setup_tables",
            "notebook_task": {
                "notebook_path": p["setup_notebook"],
                "base_parameters": {
                    "meta_catalog": p["meta_catalog"],
                    "meta_schema": p["meta_schema"],
                    "repo_root": p["workspace_file_path"],
                    "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                },
            },
            "job_cluster_key": "default_cluster",
        }],
        "job_clusters": [_orchestrator_cluster(p, key="default_cluster")],
        "email_notifications": _emails(p),
    }


def build_inventory_job(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": _job_name("DeepClone 1 - INVENTORY", p["bundle_target"]),
        "description": "Discovers and onboards source tables into migration_control (QUEUED status).",
        "tags": {"team": _TAGS_TEAM, "phase": "inventory"},
        "parameters": _param_list([
            ("source_catalogs", p["source_catalog_filter"]),
            ("source_schemas", p["source_schema_filter"]),
            ("source_tables", p["source_table_filter"]),
            ("target_catalog", p["target_catalog"]),
            ("clone_type", p["clone_type"]),
            ("meta_catalog", p["meta_catalog"]),
            ("meta_schema", p["meta_schema"]),
            ("input_type", p["input_type"]),
            ("yaml_config_path", p["yaml_config_path"]),
            ("csv_path", p["csv_path"]),
            ("exclusion_csv_path", p["exclusion_csv_path"]),
            ("batch_id", ""),
            ("chunk_capacity_gb", "50"),
            ("max_concurrent_chunks", p["max_concurrent_chunks"]),
            ("parallel_threads", "4"),
            ("min_executors", p["min_executors"]),
            ("target_warehouse_id", p["target_warehouse_id"]),
            ("source_warehouse_id", p["source_warehouse_id"]),
            ("force_reonboard", p["force_reonboard"]),
            ("require_target_precreated", p["require_target_precreated"]),
            ("skip_describe_detail", p["skip_describe_detail"]),
            ("inventory_parallel_threads", p["inventory_parallel_threads"]),
        ]),
        "tasks": [{
            "task_key": "inventory",
            "notebook_task": {
                "notebook_path": p["orchestrator_notebook"],
                "base_parameters": {
                    "mode": "INVENTORY",
                    "input_type": "{{job.parameters.input_type}}",
                    "yaml_config_path": "{{job.parameters.yaml_config_path}}",
                    "csv_path": "{{job.parameters.csv_path}}",
                    "exclusion_csv_path": "{{job.parameters.exclusion_csv_path}}",
                    "selection_type": "catalog",
                    "source_catalogs": "{{job.parameters.source_catalogs}}",
                    "source_schemas": "{{job.parameters.source_schemas}}",
                    "source_tables": "{{job.parameters.source_tables}}",
                    "target_catalog": "{{job.parameters.target_catalog}}",
                    "clone_type": "{{job.parameters.clone_type}}",
                    "meta_catalog": "{{job.parameters.meta_catalog}}",
                    "meta_schema": "{{job.parameters.meta_schema}}",
                    "batch_id": "{{job.parameters.batch_id}}",
                    "chunk_capacity_gb": "{{job.parameters.chunk_capacity_gb}}",
                    "max_concurrent_chunks": "{{job.parameters.max_concurrent_chunks}}",
                    "parallel_threads": "{{job.parameters.parallel_threads}}",
                    "min_executors": "{{job.parameters.min_executors}}",
                    "force_reonboard": "{{job.parameters.force_reonboard}}",
                    "require_target_precreated": "{{job.parameters.require_target_precreated}}",
                    "skip_describe_detail": "{{job.parameters.skip_describe_detail}}",
                    "inventory_parallel_threads": "{{job.parameters.inventory_parallel_threads}}",
                    "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                    "source_warehouse_id": "{{job.parameters.source_warehouse_id}}",
                },
            },
            "job_cluster_key": "orchestrator_cluster",
        }],
        "job_clusters": [_orchestrator_cluster(p)],
        "email_notifications": _emails(p),
    }


def build_dry_run_job(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": _job_name("DeepClone 2 - DRY_RUN", p["bundle_target"]),
        "description": "Resolves table scope and prints execution plan — no data is moved.",
        "tags": {"team": _TAGS_TEAM, "phase": "dry_run"},
        "parameters": _param_list([
            ("source_catalogs", p["source_catalog_filter"]),
            ("source_schemas", p["source_schema_filter"]),
            ("source_tables", p["source_table_filter"]),
            ("target_catalog", p["target_catalog"]),
            ("clone_type", p["clone_type"]),
            ("meta_catalog", p["meta_catalog"]),
            ("meta_schema", p["meta_schema"]),
            ("selection_type", "catalog"),
            ("target_warehouse_id", p["target_warehouse_id"]),
            ("source_warehouse_id", p["source_warehouse_id"]),
        ]),
        "tasks": [{
            "task_key": "dry_run",
            "notebook_task": {
                "notebook_path": p["orchestrator_notebook"],
                "base_parameters": {
                    "mode": "DRY_RUN",
                    "input_type": "JOB",
                    "selection_type": "{{job.parameters.selection_type}}",
                    "source_catalogs": "{{job.parameters.source_catalogs}}",
                    "source_schemas": "{{job.parameters.source_schemas}}",
                    "source_tables": "{{job.parameters.source_tables}}",
                    "target_catalog": "{{job.parameters.target_catalog}}",
                    "clone_type": "{{job.parameters.clone_type}}",
                    "meta_catalog": "{{job.parameters.meta_catalog}}",
                    "meta_schema": "{{job.parameters.meta_schema}}",
                    "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                    "source_warehouse_id": "{{job.parameters.source_warehouse_id}}",
                },
            },
            "job_cluster_key": "orchestrator_cluster",
        }],
        "job_clusters": [_orchestrator_cluster(p)],
        "email_notifications": _emails(p),
    }


def build_deep_clone_job(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": _job_name("DeepClone 3 - DEEP_CLONE", p["bundle_target"]),
        "description": "Executes DEEP CLONE for all QUEUED tables in migration_control.",
        "tags": {"team": _TAGS_TEAM, "phase": "deep_clone"},
        "parameters": _param_list([
            ("meta_catalog", p["meta_catalog"]),
            ("meta_schema", p["meta_schema"]),
            ("validation_enabled", p["validation_enabled"]),
            ("row_count_validation", p["row_count_validation"]),
            ("max_retries", p["max_retries"]),
            ("worker_spark_version", p["worker_spark_version"]),
            ("batch_id", ""),
            ("max_concurrent_chunks", p["max_concurrent_chunks"]),
            ("parallel_threads", "4"),
            ("chunk_capacity_gb", "50"),
            ("min_executors", p["min_executors"]),
            ("worker_notebook_path", p["worker_notebook"]),
            ("target_warehouse_id", p["target_warehouse_id"]),
            ("source_warehouse_id", p["source_warehouse_id"]),
            ("clone_type", p["clone_type"]),
        ]),
        "tasks": [{
            "task_key": "deep_clone",
            "notebook_task": {
                "notebook_path": p["orchestrator_notebook"],
                "base_parameters": {
                    "mode": "DEEP_CLONE",
                    "input_type": "JOB",
                    "clone_type": "{{job.parameters.clone_type}}",
                    "meta_catalog": "{{job.parameters.meta_catalog}}",
                    "meta_schema": "{{job.parameters.meta_schema}}",
                    "validation_enabled": "{{job.parameters.validation_enabled}}",
                    "row_count_validation": "{{job.parameters.row_count_validation}}",
                    "max_retries": "{{job.parameters.max_retries}}",
                    "batch_id": "{{job.parameters.batch_id}}",
                    "max_concurrent_chunks": "{{job.parameters.max_concurrent_chunks}}",
                    "parallel_threads": "{{job.parameters.parallel_threads}}",
                    "chunk_capacity_gb": "{{job.parameters.chunk_capacity_gb}}",
                    "min_executors": "{{job.parameters.min_executors}}",
                    "worker_notebook_path": "{{job.parameters.worker_notebook_path}}",
                    "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                    "source_warehouse_id": "{{job.parameters.source_warehouse_id}}",
                    "worker_cluster_json": _worker_cluster_json(p),
                },
            },
            "job_cluster_key": "orchestrator_cluster",
        }],
        "job_clusters": [_orchestrator_cluster(p)],
        "email_notifications": _emails(p, on_success=True),
    }


def build_validate_job(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": _job_name("DeepClone 4 - VALIDATE", p["bundle_target"]),
        "description": "Validates COMPLETED clone records: size, file count, Delta version, and row count.",
        "tags": {"team": _TAGS_TEAM, "phase": "validate"},
        "parameters": _param_list([
            ("meta_catalog", p["meta_catalog"]),
            ("meta_schema", p["meta_schema"]),
            ("clone_type", p["clone_type"]),
            ("row_count_validation", p["row_count_validation"]),
            ("batch_id", ""),
            ("target_warehouse_id", p["target_warehouse_id"]),
            ("source_warehouse_id", p["source_warehouse_id"]),
        ]),
        "tasks": [{
            "task_key": "validate",
            "notebook_task": {
                "notebook_path": p["orchestrator_notebook"],
                "base_parameters": {
                    "mode": "VALIDATE",
                    "input_type": "JOB",
                    "clone_type": "{{job.parameters.clone_type}}",
                    "meta_catalog": "{{job.parameters.meta_catalog}}",
                    "meta_schema": "{{job.parameters.meta_schema}}",
                    "validation_enabled": "true",
                    "row_count_validation": "{{job.parameters.row_count_validation}}",
                    "batch_id": "{{job.parameters.batch_id}}",
                    "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                    "source_warehouse_id": "{{job.parameters.source_warehouse_id}}",
                },
            },
            "job_cluster_key": "orchestrator_cluster",
        }],
        "job_clusters": [_orchestrator_cluster(p)],
        "email_notifications": _emails(p),
    }


def build_retry_job(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": _job_name("DeepClone 5 - RETRY", p["bundle_target"]),
        "description": "Re-queues RETRY_PENDING tables with exponential backoff and re-dispatches clones.",
        "tags": {"team": _TAGS_TEAM, "phase": "retry"},
        "parameters": _param_list([
            ("meta_catalog", p["meta_catalog"]),
            ("meta_schema", p["meta_schema"]),
            ("max_retries", p["max_retries"]),
            ("retry_permanent", p["retry_permanent"]),
            ("worker_spark_version", p["worker_spark_version"]),
            ("batch_id", ""),
            ("max_concurrent_chunks", p["max_concurrent_chunks"]),
            ("parallel_threads", "4"),
            ("min_executors", p["min_executors"]),
            ("worker_notebook_path", p["worker_notebook"]),
            ("target_warehouse_id", p["target_warehouse_id"]),
            ("source_warehouse_id", p["source_warehouse_id"]),
            ("clone_type", p["clone_type"]),
        ]),
        "tasks": [{
            "task_key": "retry",
            "notebook_task": {
                "notebook_path": p["orchestrator_notebook"],
                "base_parameters": {
                    "mode": "RETRY",
                    "input_type": "JOB",
                    "clone_type": "{{job.parameters.clone_type}}",
                    "meta_catalog": "{{job.parameters.meta_catalog}}",
                    "meta_schema": "{{job.parameters.meta_schema}}",
                    "max_retries": "{{job.parameters.max_retries}}",
                    "retry_permanent": "{{job.parameters.retry_permanent}}",
                    "batch_id": "{{job.parameters.batch_id}}",
                    "max_concurrent_chunks": "{{job.parameters.max_concurrent_chunks}}",
                    "parallel_threads": "{{job.parameters.parallel_threads}}",
                    "min_executors": "{{job.parameters.min_executors}}",
                    "worker_notebook_path": "{{job.parameters.worker_notebook_path}}",
                    "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                    "source_warehouse_id": "{{job.parameters.source_warehouse_id}}",
                    "worker_cluster_json": _worker_cluster_json(p, with_spark_conf=False),
                },
            },
            "job_cluster_key": "orchestrator_cluster",
        }],
        "job_clusters": [_orchestrator_cluster(p, spark_conf=False)],
        "email_notifications": _emails(p),
    }


def build_full_workflow_job(p: Dict[str, Any]) -> Dict[str, Any]:
    dc_worker = _worker_cluster_json(p)
    retry_worker = _worker_cluster_json(p, with_spark_conf=False)
    return {
        "name": _job_name("DeepClone - Full Migration Workflow", p["bundle_target"]),
        "description": (
            "End-to-end migration workflow: INVENTORY -> DRY_RUN -> DEEP_CLONE -> "
            "VALIDATE -> RETRY (if needed). All tasks parameterized and modular."
        ),
        "tags": {"team": _TAGS_TEAM, "phase": "full_workflow"},
        "parameters": _param_list([
            ("input_type", p["input_type"]),
            ("yaml_config_path", p["yaml_config_path"]),
            ("csv_path", p["csv_path"]),
            ("exclusion_csv_path", p["exclusion_csv_path"]),
            ("source_catalogs", p["source_catalog_filter"]),
            ("source_schemas", p["source_schema_filter"]),
            ("source_tables", p["source_table_filter"]),
            ("target_catalog", p["target_catalog"]),
            ("clone_type", p["clone_type"]),
            ("meta_catalog", p["meta_catalog"]),
            ("meta_schema", p["meta_schema"]),
            ("validation_enabled", p["validation_enabled"]),
            ("row_count_validation", p["row_count_validation"]),
            ("max_retries", p["max_retries"]),
            ("retry_permanent", p["retry_permanent"]),
            ("selection_type", "catalog"),
            ("run_id", ""),
            ("batch_id", p["batch_id"]),
            ("max_concurrent_chunks", p["max_concurrent_chunks"]),
            ("parallel_threads", p["parallel_threads"]),
            ("chunk_capacity_gb", p["chunk_capacity_gb"]),
            ("min_executors", p["min_executors"]),
            ("worker_spark_version", p["worker_spark_version"]),
            ("worker_notebook_path", p["worker_notebook"]),
            ("force_reonboard", p["force_reonboard"]),
            ("require_target_precreated", p["require_target_precreated"]),
            ("skip_describe_detail", p["skip_describe_detail"]),
            ("inventory_parallel_threads", p["inventory_parallel_threads"]),
            ("target_warehouse_id", p["target_warehouse_id"]),
            ("source_warehouse_id", p["source_warehouse_id"]),
        ]),
        "tasks": [
            {
                "task_key": "inventory",
                "description": "Discover source tables and onboard into migration_control (QUEUED).",
                "notebook_task": {
                    "notebook_path": p["orchestrator_notebook"],
                    "base_parameters": {
                        "mode": "INVENTORY",
                        "input_type": "{{job.parameters.input_type}}",
                        "yaml_config_path": "{{job.parameters.yaml_config_path}}",
                        "csv_path": "{{job.parameters.csv_path}}",
                        "exclusion_csv_path": "{{job.parameters.exclusion_csv_path}}",
                        "selection_type": "{{job.parameters.selection_type}}",
                        "source_catalogs": "{{job.parameters.source_catalogs}}",
                        "source_schemas": "{{job.parameters.source_schemas}}",
                        "source_tables": "{{job.parameters.source_tables}}",
                        "target_catalog": "{{job.parameters.target_catalog}}",
                        "clone_type": "{{job.parameters.clone_type}}",
                        "meta_catalog": "{{job.parameters.meta_catalog}}",
                        "meta_schema": "{{job.parameters.meta_schema}}",
                        "run_id": "{{job.parameters.run_id}}",
                        "batch_id": "{{job.parameters.batch_id}}",
                        "chunk_capacity_gb": "{{job.parameters.chunk_capacity_gb}}",
                        "max_concurrent_chunks": "{{job.parameters.max_concurrent_chunks}}",
                        "parallel_threads": "{{job.parameters.parallel_threads}}",
                        "min_executors": "{{job.parameters.min_executors}}",
                        "force_reonboard": "{{job.parameters.force_reonboard}}",
                        "require_target_precreated": "{{job.parameters.require_target_precreated}}",
                        "skip_describe_detail": "{{job.parameters.skip_describe_detail}}",
                        "inventory_parallel_threads": "{{job.parameters.inventory_parallel_threads}}",
                        "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                        "source_warehouse_id": "{{job.parameters.source_warehouse_id}}",
                    },
                },
                "job_cluster_key": "orchestrator_cluster",
            },
            {
                "task_key": "dry_run",
                "description": "Print execution plan without touching data. Runs parallel to INVENTORY.",
                "notebook_task": {
                    "notebook_path": p["orchestrator_notebook"],
                    "base_parameters": {
                        "mode": "DRY_RUN",
                        "input_type": "{{job.parameters.input_type}}",
                        "yaml_config_path": "{{job.parameters.yaml_config_path}}",
                        "csv_path": "{{job.parameters.csv_path}}",
                        "exclusion_csv_path": "{{job.parameters.exclusion_csv_path}}",
                        "selection_type": "{{job.parameters.selection_type}}",
                        "source_catalogs": "{{job.parameters.source_catalogs}}",
                        "source_schemas": "{{job.parameters.source_schemas}}",
                        "source_tables": "{{job.parameters.source_tables}}",
                        "target_catalog": "{{job.parameters.target_catalog}}",
                        "clone_type": "{{job.parameters.clone_type}}",
                        "meta_catalog": "{{job.parameters.meta_catalog}}",
                        "meta_schema": "{{job.parameters.meta_schema}}",
                        "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                        "source_warehouse_id": "{{job.parameters.source_warehouse_id}}",
                    },
                },
                "job_cluster_key": "orchestrator_cluster",
            },
            {
                "task_key": "deep_clone",
                "description": "Execute DEEP CLONE for all QUEUED tables.",
                "depends_on": [{"task_key": "inventory"}],
                "notebook_task": {
                    "notebook_path": p["orchestrator_notebook"],
                    "base_parameters": {
                        "mode": "DEEP_CLONE",
                        "input_type": "JOB",
                        "clone_type": "{{job.parameters.clone_type}}",
                        "meta_catalog": "{{job.parameters.meta_catalog}}",
                        "meta_schema": "{{job.parameters.meta_schema}}",
                        "validation_enabled": "{{job.parameters.validation_enabled}}",
                        "row_count_validation": "{{job.parameters.row_count_validation}}",
                        "max_retries": "{{job.parameters.max_retries}}",
                        "run_id": "{{job.parameters.run_id}}",
                        "batch_id": "{{tasks.inventory.values.batch_id}}",
                        "max_concurrent_chunks": "{{job.parameters.max_concurrent_chunks}}",
                        "parallel_threads": "{{job.parameters.parallel_threads}}",
                        "chunk_capacity_gb": "{{job.parameters.chunk_capacity_gb}}",
                        "min_executors": "{{job.parameters.min_executors}}",
                        "worker_notebook_path": "{{job.parameters.worker_notebook_path}}",
                        "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                        "worker_cluster_json": dc_worker,
                    },
                },
                "job_cluster_key": "orchestrator_cluster",
            },
            {
                "task_key": "retry",
                "description": "Re-queue and re-clone any RETRY_PENDING tables.",
                "depends_on": [{"task_key": "deep_clone"}],
                "notebook_task": {
                    "notebook_path": p["orchestrator_notebook"],
                    "base_parameters": {
                        "mode": "RETRY",
                        "input_type": "JOB",
                        "meta_catalog": "{{job.parameters.meta_catalog}}",
                        "meta_schema": "{{job.parameters.meta_schema}}",
                        "max_retries": "{{job.parameters.max_retries}}",
                        "retry_permanent": "{{job.parameters.retry_permanent}}",
                        "run_id": "{{job.parameters.run_id}}",
                        "batch_id": "{{tasks.inventory.values.batch_id}}",
                        "max_concurrent_chunks": "{{job.parameters.max_concurrent_chunks}}",
                        "parallel_threads": "{{job.parameters.parallel_threads}}",
                        "min_executors": "{{job.parameters.min_executors}}",
                        "worker_notebook_path": "{{job.parameters.worker_notebook_path}}",
                        "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                        "worker_cluster_json": retry_worker,
                    },
                },
                "job_cluster_key": "orchestrator_cluster",
            },
            {
                "task_key": "validate",
                "description": "Validate all COMPLETED clones against source metadata.",
                "depends_on": [{"task_key": "retry"}],
                "notebook_task": {
                    "notebook_path": p["orchestrator_notebook"],
                    "base_parameters": {
                        "mode": "VALIDATE",
                        "input_type": "JOB",
                        "clone_type": "{{job.parameters.clone_type}}",
                        "meta_catalog": "{{job.parameters.meta_catalog}}",
                        "meta_schema": "{{job.parameters.meta_schema}}",
                        "validation_enabled": "true",
                        "row_count_validation": "{{job.parameters.row_count_validation}}",
                        "run_id": "{{job.parameters.run_id}}",
                        "batch_id": "{{tasks.inventory.values.batch_id}}",
                        "target_warehouse_id": "{{job.parameters.target_warehouse_id}}",
                        "source_warehouse_id": "{{job.parameters.source_warehouse_id}}",
                    },
                },
                "job_cluster_key": "orchestrator_cluster",
            },
        ],
        "job_clusters": [_orchestrator_cluster(p, adaptive=True)],
        "email_notifications": _emails(p, on_success=True),
        "health": {"rules": [{"metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": 86400}]},
    }


# ── Public entry point ────────────────────────────────────────────────────────

# Ordered like the numbered DAB resource files (setup -> workflow last).
_BUILDERS = [
    build_setup_job,
    build_inventory_job,
    build_dry_run_job,
    build_deep_clone_job,
    build_validate_job,
    build_retry_job,
    build_full_workflow_job,
]


def build_all_job_specs(params: Dict[str, Any]) -> "OrderedDict[str, Dict[str, Any]]":
    """Build every DAB job's Jobs-API settings. Returns {job_name -> settings}."""
    p = _merge_defaults(params)
    specs: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    for builder in _BUILDERS:
        spec = builder(p)
        specs[spec["name"]] = spec
    return specs


def resolve_workspace_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Fill notebook/config paths from workspace_file_path when the caller left
    them blank (mirrors databricks.yml's ${workspace.file_path}/... defaults)."""
    p = dict(params)
    fp = str(p.get("workspace_file_path", "")).rstrip("/")
    defaults = {
        "orchestrator_notebook": f"{fp}/notebooks/orchestrator_notebook",
        "worker_notebook": f"{fp}/notebooks/chunk_worker_notebook",
        "setup_notebook": f"{fp}/notebooks/setup_control_tables",
        "yaml_config_path": f"{fp}/configs/migration.yaml",
        "csv_path": f"{fp}/configs/csv_scale_ril_bulk_02_full.csv",
        "exclusion_csv_path": f"{fp}/configs/exclusion_list_example.csv",
    }
    for k, v in defaults.items():
        if not str(p.get(k, "")).strip():
            p[k] = v
    return p


def as_reset_body(job_id: int, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Wrap settings for POST /api/2.1/jobs/reset (full overwrite of an existing job)."""
    return {"job_id": job_id, "new_settings": copy.deepcopy(settings)}
