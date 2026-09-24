"""
test_job_factory.py — Unit tests for orchestrator/job_factory.py, the pure
builder behind notebooks/create_jobs_notebook.py (the DAB-equivalent
"create jobs from a notebook" flow).

Covers:
  1. All 7 DAB jobs are built, with the correct names + `[<target>]` suffix.
  2. Deploy-time substitution: notebook paths come from workspace_file_path;
     job-parameter DEFAULTS come from the resolved var values.
  3. Task `base_parameters` keep the DAB's `{{job.parameters.*}}` templating
     (i.e. they are NOT eagerly resolved) so created jobs behave identically.
  4. Full workflow: 5 tasks, correct dependency chain, dry_run independent,
     and downstream tasks read batch_id from `{{tasks.inventory.values.batch_id}}`.
  5. Clusters: UC-safe data_security_mode AUTO, pool id + PYTHONPATH wired;
     embedded worker_cluster_json carries node type / worker count.
  6. Every produced settings dict is JSON-serialisable (valid Jobs-API body)
     and reset-wrapping works.
  7. Required-parameter validation raises when a critical value is missing.

Run with:  python3 -m pytest tests/test_job_factory.py -v

No Databricks / Spark / SDK / network needed — job_factory is pure Python.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# ── Make `orchestrator` importable when run directly (repo root on sys.path) ──
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from orchestrator.job_factory import (  # noqa: E402
    build_all_job_specs,
    resolve_workspace_params,
    as_reset_body,
    build_full_workflow_job,
    _merge_defaults,
)

TARGET = "prod"
FILE_ROOT = "/Workspace/Users/tester/deepclone_orchestrator/prod/files"


@pytest.fixture()
def params():
    base = {
        "bundle_target": TARGET,
        "workspace_file_path": FILE_ROOT,
        "email": "tester@databricks.com",
        "meta_catalog": "ril_migration_catalog",
        "meta_schema": "migration_meta",
        "target_warehouse_id": "wh_abc123",
        "worker_node_type": "Standard_E32ds_v5",
        "worker_num_workers": "8",
        "clone_type": "delta_share",
    }
    # resolve_workspace_params fills notebook/config paths from file root
    return resolve_workspace_params(base)


@pytest.fixture()
def specs(params):
    return build_all_job_specs(params)


EXPECTED_NAMES = [
    "DeepClone 0 - Setup Control Tables [prod]",
    "DeepClone 1 - INVENTORY [prod]",
    "DeepClone 2 - DRY_RUN [prod]",
    "DeepClone 3 - DEEP_CLONE [prod]",
    "DeepClone 4 - VALIDATE [prod]",
    "DeepClone 5 - RETRY [prod]",
    "DeepClone - Full Migration Workflow [prod]",
]


# ── 1. All jobs built with correct names ─────────────────────────────────────

def test_all_seven_jobs_built(specs):
    assert len(specs) == 7
    assert list(specs.keys()) == EXPECTED_NAMES


def test_target_suffix_applied():
    s = build_all_job_specs({
        "bundle_target": "dev",
        "workspace_file_path": FILE_ROOT,
        "orchestrator_notebook": f"{FILE_ROOT}/notebooks/orchestrator_notebook",
        "worker_notebook": f"{FILE_ROOT}/notebooks/chunk_worker_notebook",
        "setup_notebook": f"{FILE_ROOT}/notebooks/setup_control_tables",
        "target_warehouse_id": "wh_x",
    })
    assert all(name.endswith("[dev]") for name in s)


# ── 2. Deploy-time substitution ──────────────────────────────────────────────

def test_notebook_paths_from_file_root(specs):
    inv = specs["DeepClone 1 - INVENTORY [prod]"]
    assert inv["tasks"][0]["notebook_task"]["notebook_path"] == f"{FILE_ROOT}/notebooks/orchestrator_notebook"
    setup = specs["DeepClone 0 - Setup Control Tables [prod]"]
    assert setup["tasks"][0]["notebook_task"]["notebook_path"] == f"{FILE_ROOT}/notebooks/setup_control_tables"


def test_param_defaults_resolved(specs):
    inv = specs["DeepClone 1 - INVENTORY [prod]"]
    pmap = {p["name"]: p["default"] for p in inv["parameters"]}
    assert pmap["clone_type"] == "delta_share"
    assert pmap["meta_catalog"] == "ril_migration_catalog"
    assert pmap["target_warehouse_id"] == "wh_abc123"
    # literal defaults preserved from the YAML (per-job overrides)
    assert pmap["chunk_capacity_gb"] == "50"
    assert pmap["parallel_threads"] == "4"
    assert pmap["batch_id"] == ""


# ── 3. Templating preserved (not eagerly resolved) ───────────────────────────

def test_base_parameters_keep_job_parameter_templating(specs):
    inv_bp = specs["DeepClone 1 - INVENTORY [prod]"]["tasks"][0]["notebook_task"]["base_parameters"]
    assert inv_bp["mode"] == "INVENTORY"
    assert inv_bp["clone_type"] == "{{job.parameters.clone_type}}"
    assert inv_bp["meta_catalog"] == "{{job.parameters.meta_catalog}}"
    assert inv_bp["target_warehouse_id"] == "{{job.parameters.target_warehouse_id}}"


# ── 4. Full workflow structure ───────────────────────────────────────────────

def test_workflow_task_chain(specs):
    wf = specs["DeepClone - Full Migration Workflow [prod]"]
    tasks = {t["task_key"]: t for t in wf["tasks"]}
    assert set(tasks) == {"inventory", "dry_run", "deep_clone", "retry", "validate"}
    # dependency chain: inventory <- deep_clone <- retry <- validate
    assert "depends_on" not in tasks["inventory"]
    assert "depends_on" not in tasks["dry_run"]           # runs parallel to inventory
    assert tasks["deep_clone"]["depends_on"] == [{"task_key": "inventory"}]
    assert tasks["retry"]["depends_on"] == [{"task_key": "deep_clone"}]
    assert tasks["validate"]["depends_on"] == [{"task_key": "retry"}]


def test_workflow_batch_id_taskvalue_wiring(specs):
    wf = specs["DeepClone - Full Migration Workflow [prod]"]
    tasks = {t["task_key"]: t for t in wf["tasks"]}
    # inventory reads the raw job parameter; downstream read inventory's task value
    assert tasks["inventory"]["notebook_task"]["base_parameters"]["batch_id"] == "{{job.parameters.batch_id}}"
    for tk in ("deep_clone", "retry", "validate"):
        assert tasks[tk]["notebook_task"]["base_parameters"]["batch_id"] == "{{tasks.inventory.values.batch_id}}"


def test_workflow_has_health_rule(specs):
    wf = specs["DeepClone - Full Migration Workflow [prod]"]
    assert wf["health"]["rules"][0]["metric"] == "RUN_DURATION_SECONDS"
    assert wf["health"]["rules"][0]["value"] == 86400


# ── 5. Clusters ──────────────────────────────────────────────────────────────

def test_all_clusters_uc_safe(specs):
    for name, spec in specs.items():
        for jc in spec["job_clusters"]:
            nc = jc["new_cluster"]
            assert nc["data_security_mode"] == "DATA_SECURITY_MODE_AUTO", name
            assert nc["instance_pool_id"], name
            assert nc["spark_env_vars"]["PYTHONPATH"] == FILE_ROOT, name


def test_worker_cluster_json_is_runtime_templated(specs):
    """worker_cluster_json is a JSON *string* whose compute fields are
    {{job.parameters.*}} templates so the pool / VM type / worker count / Spark
    version are overridable from the 'Run now' panel. The notebook resolves the
    pool-vs-node choice at run time (Databricks rejects a spec with both)."""
    dc = specs["DeepClone 3 - DEEP_CLONE [prod]"]
    wcj = dc["tasks"][0]["notebook_task"]["base_parameters"]["worker_cluster_json"]
    parsed = json.loads(wcj)   # must still be a valid JSON *string*
    assert parsed["spark_version"] == "{{job.parameters.worker_spark_version}}"
    assert parsed["instance_pool_id"] == "{{job.parameters.worker_instance_pool_id}}"
    assert parsed["node_type_id"] == "{{job.parameters.worker_node_type}}"
    assert parsed["num_workers"] == "{{job.parameters.worker_num_workers}}"
    assert parsed["data_security_mode"] == "DATA_SECURITY_MODE_AUTO"


def test_worker_compute_exposed_as_job_parameters(specs):
    """DEEP_CLONE, RETRY and the full workflow expose the chunk-worker compute
    knobs as job parameters (so they can be overridden at run time) with the
    factory defaults."""
    expected = {
        "worker_spark_version": "17.3.x-scala2.13",
        "worker_instance_pool_id": "0924-004044-comic1-pool-63p33jj6",
        "worker_node_type": "Standard_E32ds_v5",
        "worker_num_workers": "8",
    }
    for job_name in (
        "DeepClone 3 - DEEP_CLONE [prod]",
        "DeepClone 5 - RETRY [prod]",
        "DeepClone - Full Migration Workflow [prod]",
    ):
        spec = specs[job_name]
        got = {pp["name"]: pp["default"] for pp in spec["parameters"]}
        for k, v in expected.items():
            assert got.get(k) == v, f"{job_name}: {k}={got.get(k)!r} != {v!r}"


# ── 6. Serialisable + reset wrapping ─────────────────────────────────────────

def test_specs_are_json_serialisable(specs):
    for name, spec in specs.items():
        blob = json.dumps(spec)          # raises if any non-serialisable value
        assert '"name"' in blob, name
        # every task references an existing job_cluster_key
        cluster_keys = {jc["job_cluster_key"] for jc in spec["job_clusters"]}
        for t in spec["tasks"]:
            assert t["job_cluster_key"] in cluster_keys, name


def test_as_reset_body_wraps_settings(specs):
    setup = specs["DeepClone 0 - Setup Control Tables [prod]"]
    body = as_reset_body(4242, setup)
    assert body["job_id"] == 4242
    assert body["new_settings"]["name"] == setup["name"]
    # deep copy — mutating the wrapped copy must not touch the original
    body["new_settings"]["name"] = "MUTATED"
    assert setup["name"] != "MUTATED"


# ── 7. Required-parameter validation ─────────────────────────────────────────

def test_missing_required_param_raises():
    with pytest.raises(ValueError) as ei:
        build_full_workflow_job(_merge_defaults({
            "bundle_target": "prod",
            "workspace_file_path": FILE_ROOT,
            "orchestrator_notebook": "",   # blank -> should fail
            "worker_notebook": "x",
            "setup_notebook": "y",
            "target_warehouse_id": "wh",
        }))
    assert "orchestrator_notebook" in str(ei.value)


def test_standalone_clone_and_retry_drop_source_warehouse_and_default_worker_nb(specs):
    # CSV-oriented (applied to ALL jobs): standalone DEEP_CLONE and RETRY no
    # longer expose source_warehouse_id or worker_notebook_path as job params;
    # worker_notebook_path is the resolved deployed path (a literal).
    expected_nb = f"{FILE_ROOT}/notebooks/chunk_worker_notebook"
    for name in ("DeepClone 3 - DEEP_CLONE [prod]", "DeepClone 5 - RETRY [prod]"):
        pnames = {p["name"] for p in specs[name]["parameters"]}
        assert "source_warehouse_id" not in pnames, name
        assert "worker_notebook_path" not in pnames, name
        bp = specs[name]["tasks"][0]["notebook_task"]["base_parameters"]
        assert bp["target_warehouse_id"] == "{{job.parameters.target_warehouse_id}}", name
        assert "source_warehouse_id" not in bp, name
        assert bp["worker_notebook_path"] == expected_nb, name


def test_retry_permanent_job_param_present_and_default_false(specs):
    # OPT-IN FAILED_PERMANENT re-drive: the RETRY job and the Full Workflow job
    # must both expose a `retry_permanent` job parameter, defaulted "false" so
    # the safe terminal semantics are unchanged unless explicitly overridden.
    for name in ("DeepClone 5 - RETRY [prod]", "DeepClone - Full Migration Workflow [prod]"):
        pmap = {p["name"]: p["default"] for p in specs[name]["parameters"]}
        assert "retry_permanent" in pmap, name
        assert pmap["retry_permanent"] == "false", name


def test_retry_permanent_wired_into_retry_base_parameters(specs):
    # The RETRY task on both jobs must forward the job parameter into the
    # notebook via {{job.parameters.retry_permanent}} templating (not eagerly
    # resolved), exactly like max_retries.
    retry_bp = specs["DeepClone 5 - RETRY [prod]"]["tasks"][0]["notebook_task"]["base_parameters"]
    assert retry_bp["retry_permanent"] == "{{job.parameters.retry_permanent}}"

    wf = specs["DeepClone - Full Migration Workflow [prod]"]
    wf_retry = {t["task_key"]: t for t in wf["tasks"]}["retry"]
    wf_retry_bp = wf_retry["notebook_task"]["base_parameters"]
    assert wf_retry_bp["retry_permanent"] == "{{job.parameters.retry_permanent}}"


def test_workflow_csv_params_removed(specs):
    # CSV-oriented workflow: these are NOT exposed as job parameters anymore.
    wf = specs["DeepClone - Full Migration Workflow [prod]"]
    names = {p["name"] for p in wf["parameters"]}
    for removed in (
        "yaml_config_path", "source_catalogs", "source_schemas", "source_tables",
        "target_catalog", "source_warehouse_id", "worker_notebook_path",
        "selection_type",
    ):
        assert removed not in names, f"{removed} should not be a workflow job parameter"
    # But the CSV inputs it DOES rely on must remain.
    assert "csv_path" in names
    assert "exclusion_csv_path" in names
    assert "input_type" in names


def test_workflow_selection_type_hardcoded_in_inventory_and_dry_run(specs):
    wf = specs["DeepClone - Full Migration Workflow [prod]"]
    tasks = {t["task_key"]: t for t in wf["tasks"]}
    for tk in ("inventory", "dry_run"):
        bp = tasks[tk]["notebook_task"]["base_parameters"]
        # Fixed default value, NOT a {{job.parameters.*}} template.
        assert bp["selection_type"] == "catalog", tk
        # Removed selection inputs must not be forwarded either.
        for removed in ("yaml_config_path", "source_catalogs", "source_schemas",
                        "source_tables", "target_catalog", "source_warehouse_id"):
            assert removed not in bp, f"{removed} still forwarded in {tk}"


def test_workflow_worker_notebook_path_hardcoded(specs):
    # worker_notebook_path is now the resolved deployed path (a literal), not a
    # {{job.parameters.*}} template, on both clone-dispatching tasks.
    wf = specs["DeepClone - Full Migration Workflow [prod]"]
    tasks = {t["task_key"]: t for t in wf["tasks"]}
    expected = f"{FILE_ROOT}/notebooks/chunk_worker_notebook"
    for tk in ("deep_clone", "retry"):
        bp = tasks[tk]["notebook_task"]["base_parameters"]
        assert bp["worker_notebook_path"] == expected, tk
        assert "{{job.parameters" not in bp["worker_notebook_path"], tk


def test_workflow_validate_drops_source_warehouse(specs):
    wf = specs["DeepClone - Full Migration Workflow [prod]"]
    tasks = {t["task_key"]: t for t in wf["tasks"]}
    val_bp = tasks["validate"]["notebook_task"]["base_parameters"]
    assert "source_warehouse_id" not in val_bp
    # target warehouse is still passed through
    assert val_bp["target_warehouse_id"] == "{{job.parameters.target_warehouse_id}}"


def test_all_jobs_drop_csv_irrelevant_params(specs):
    # CSV-oriented trimming applies to ALL jobs (not just the workflow): none of
    # these should be exposed as job parameters on ANY job.
    removed_everywhere = {
        "source_catalogs", "source_schemas", "source_tables", "target_catalog",
        "yaml_config_path", "source_warehouse_id", "worker_notebook_path",
        "selection_type",
    }
    for name, spec in specs.items():
        names = {p["name"] for p in spec["parameters"]}
        leaked = removed_everywhere & names
        assert not leaked, f"{name} still exposes {leaked}"


def test_standalone_inventory_and_dry_run_are_csv_oriented(specs):
    # INVENTORY + standalone DRY_RUN drive selection from the CSV: input_type +
    # csv_path present, selection_type hardcoded to 'catalog' in base_parameters.
    for name in ("DeepClone 1 - INVENTORY [prod]", "DeepClone 2 - DRY_RUN [prod]"):
        pnames = {p["name"] for p in specs[name]["parameters"]}
        assert "input_type" in pnames, name
        assert "csv_path" in pnames, name
        bp = specs[name]["tasks"][0]["notebook_task"]["base_parameters"]
        assert bp["selection_type"] == "catalog", name
        assert bp["input_type"] == "{{job.parameters.input_type}}", name


def test_success_email_only_on_deep_clone_and_workflow(specs):
    def has_success(spec):
        return "on_success" in spec["email_notifications"]
    assert has_success(specs["DeepClone 3 - DEEP_CLONE [prod]"])
    assert has_success(specs["DeepClone - Full Migration Workflow [prod]"])
    assert not has_success(specs["DeepClone 1 - INVENTORY [prod]"])
    assert not has_success(specs["DeepClone 4 - VALIDATE [prod]"])
