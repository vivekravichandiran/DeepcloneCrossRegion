# DeepClone CrossRegion — SOP: Creating & Running Jobs from the `create_jobs_notebook`

**Document ID:** SOP-DCR-JOBS-01
**Version:** 1.0
**Owner:** Data Platform Engineering
**Applies to bundle:** `deepclone_orchestrator` (`databricks.yml`)
**Classification:** Internal — Data Engineering

---

## Table of Contents

1. [Purpose & Scope](#1-purpose--scope)
2. [Two ways to create the jobs — and why this SOP uses the notebook](#2-two-ways-to-create-the-jobs--and-why-this-sop-uses-the-notebook)
3. [Prerequisites](#3-prerequisites)
4. [Step 1 — Get the latest code from git](#step-1--get-the-latest-code-from-git)
5. [Step 2 — Sync the code into the Databricks workspace](#step-2--sync-the-code-into-the-databricks-workspace)
6. [Step 3 — Open and configure `create_jobs_notebook`](#step-3--open-and-configure-create_jobs_notebook)
7. [Step 4 — Run the notebook (create/update the 7 jobs)](#step-4--run-the-notebook-createupdate-the-7-jobs)
8. [Step 5 — Verify the jobs](#step-5--verify-the-jobs)
9. [Step 6 — Run a migration](#step-6--run-a-migration)
10. [Step 7 — Override chunk-worker compute at run time (optional)](#step-7--override-chunk-worker-compute-at-run-time-optional)
11. [Step 8 — Monitor runs](#step-8--monitor-runs)
12. [Reference: widgets, jobs & links](#12-reference-widgets-jobs--links)
13. [Troubleshooting & Gotchas](#13-troubleshooting--gotchas)
14. [Change Log](#14-change-log)

---

## 1. Purpose & Scope

This SOP describes how to **create (or update) the 7 migration jobs directly from a
Databricks notebook** (`notebooks/create_jobs_notebook`) instead of `databricks bundle
deploy`, and how to run those jobs afterwards. It is the day-to-day operating procedure
for the current workflow:

> **edit code → push to git → `databricks sync` the files into the workspace → run
> `create_jobs_notebook` → run the migration jobs.**

**In scope:** getting code into the workspace, running the job-creation notebook, verifying
the resulting jobs, and triggering/monitoring migration runs (including run-time overrides
of the chunk-worker compute parameters).

**Out of scope:** the CSV-content mechanics and the source Delta-Sharing pre-flight (see
[`SOP_CSV_Run.md`](SOP_CSV_Run.md)); Unity Catalog / network / metastore setup.

---

## 2. Two ways to create the jobs — and why this SOP uses the notebook

The bundle can be materialised into the workspace in two independent ways. **Pick one and
stick with it** — mixing them is the #1 cause of "the job shows old parameters" confusion
(see G1).

| | **A. `databricks bundle deploy`** (see `SOP_CSV_Run.md`) | **B. `create_jobs_notebook`** (this SOP) |
|---|---|---|
| Job definitions from | `resources/*.yml` | `orchestrator/job_factory.py` (unit-tested) |
| How jobs are written | DAB deploy | Jobs REST API (`jobs/create` + `jobs/reset`) |
| Config surface | edit `databricks.yml` `variables:` + redeploy | notebook **widgets** (edit + re-run) |
| Idempotency | DAB state | **by job name** — existing jobs are patched in place via `jobs/reset`, **same `job_id`, run history preserved** |
| Best for | GitOps / CI, immutable env | interactive, in-workspace, no local CLI/bundle state needed |

Both paths produce **the identical 7 jobs** with the same `{{job.parameters.*}}` /
`{{tasks.inventory.values.batch_id}}` templating, so a job created here behaves exactly like
a bundle-deployed one.

**This SOP uses path B.** `create_jobs_notebook` exposes only the widgets relevant to a
CSV-driven migration; everything else is abstracted into `ABSTRACTED_DEFAULTS`
(input_type=CSV, auto batch_id, notebook paths, fixed governance flags, …).

The 7 jobs it creates:

| # | Job name (`[prod]` suffix) | Purpose |
|---|---|---|
| 0 | `DeepClone 0 - Setup Control Tables` | One-time DDL: `migration_control`, `migration_attempts`, `migration_validation_history`, `migration_exclusion_log` |
| 1 | `DeepClone 1 - INVENTORY` | Reads the CSV, onboards rows as `QUEUED`, bin-packs into chunks |
| 2 | `DeepClone 2 - DRY_RUN` | Previews selection/exclusions without writing control rows |
| 3 | `DeepClone 3 - DEEP_CLONE` | Dispatches one ephemeral chunk-worker cluster per chunk; runs the `DEEP CLONE` SQL |
| 4 | `DeepClone 4 - VALIDATE` | Compares source vs. target and marks `VALIDATED` / `VALIDATION_FAILED` |
| 5 | `DeepClone 5 - RETRY` | Re-queues `FAILED` / `RETRY_PENDING` / `VALIDATION_FAILED` rows |
| – | `DeepClone - Full Migration Workflow` | Chains INVENTORY → DRY_RUN → DEEP_CLONE → RETRY → VALIDATE in one run |

---

## 3. Prerequisites

| # | Requirement | How to verify |
|---|---|---|
| 1 | `databricks` CLI installed and a working profile (`ril_catalog_test`) | `databricks auth env -p ril_catalog_test` |
| 2 | Local clone of the repo on the intended branch (`main`) | `git status` |
| 3 | Instance pools exist and are warm — **orchestrator** pool (`instance_pool_id`) and **chunk-worker** pool (`worker_instance_pool_id`) | Databricks UI → Compute → Instance Pools |
| 4 | Target SQL warehouse (`target_warehouse_id`) is running | `databricks warehouses get <id> -p ril_catalog_test` |
| 5 | Source catalog is already visible from the target workspace (Delta Share / same-metastore) | See [`SOP_CSV_Run.md` §3.1](SOP_CSV_Run.md) — **mandatory for a real run** |
| 6 | **No secrets to provision** — auth is native (`databricks.sdk.WorkspaceClient()`) | n/a |

**Key IDs / paths for the `prod` target** (re-confirm with `databricks bundle validate -t prod -p ril_catalog_test -o json`):

- **Host:** `https://adb-7405616318078204.4.azuredatabricks.net`
- **Profile:** `ril_catalog_test`
- **Workspace files root:** `/Workspace/Users/99ba7259-12df-4ab6-a625-547950b4a26c/deepclone_orchestrator/prod/files`
- **Notebook:** `<files root>/notebooks/create_jobs_notebook`

---

## Step 1 — Get the latest code from git

Work on a clean, up-to-date checkout so the workspace gets exactly what's in the repo.

```bash
cd /path/to/DeepcloneCrossRegion
git checkout main
git pull --ff-only origin main
```

If you made local changes, commit (or stash) them first — `databricks sync` uploads your
**working tree**, not just what's committed, so uncommitted edits *will* go to the
workspace. Keeping git and the workspace in lock-step avoids "which version is actually
running?" ambiguity.

```bash
git status            # confirm the tree is what you intend to deploy
```

> **Optional sanity gate (recommended):** run the job-factory unit tests and validate the
> bundle locally before syncing — these catch a broken spec before it ever reaches the
> workspace:
>
> ```bash
> python3 -m pytest tests/test_job_factory.py -q
> databricks bundle validate -t prod -p ril_catalog_test
> ```

---

## Step 2 — Sync the code into the Databricks workspace

`databricks sync` uploads the repo files to the workspace files root. **It only copies
files — it does NOT create or update any job.** Job creation happens in Step 4.

```bash
databricks sync . \
  "/Workspace/Users/99ba7259-12df-4ab6-a625-547950b4a26c/deepclone_orchestrator/prod/files" \
  --full -p ril_catalog_test
```

- `--full` forces a complete re-upload (use it whenever you've changed `orchestrator/*.py`
  or the notebooks — it avoids partial/stale syncs).
- The command ends with `Initial Sync Complete` (or a list of `Uploaded …` lines).

> **Alternative — Databricks Repos:** if the workspace instead pulls this repo via
> **Workspace → Repos → Git**, just `Pull` the latest `main` in the Repos UI in place of
> this `databricks sync`. The rest of the SOP is identical. Do not mix the two — either the
> files live under the synced files root **or** under a Repo, not both.

---

## Step 3 — Open and configure `create_jobs_notebook`

Open the notebook in the workspace:

[create_jobs_notebook](https://adb-7405616318078204.4.azuredatabricks.net/#workspace/Workspace/Users/99ba7259-12df-4ab6-a625-547950b4a26c/deepclone_orchestrator/prod/files/notebooks/create_jobs_notebook)

Attach it to any general-purpose cluster (it only calls the Jobs REST API — it does not run
a migration). Then set the widgets at the top. They are numbered `01…16` so the widget bar
shows them in logical order:

| # | Widget | Default | Notes |
|---|---|---|---|
| 01 | Notification email | `vivek.ravichandiran@databricks.com` | Where job alerts go |
| 02 | Meta catalog | `ril_migration_catalog` | Control-table catalog |
| 03 | Meta schema | `migration_meta` | Control-table schema |
| 04 | CSV path | *(blank → default CSV)* | Blank = `<files>/configs/csv_scale_ril_bulk_02_full.csv` |
| 05 | Exclusion CSV path | *(blank → none)* | Global exclusion list (optional) |
| 06 | Clone type | `delta_share` | `delta_share` \| `direct_adls` |
| 07 | Validation enabled | `true` | Drives `row_count_validation` too |
| 08 | Target SQL warehouse id | `5fe1692f119e2528` | Plain ID, not a secret |
| 09 | Worker node type | `Standard_E32ds_v5` | Chunk-worker VM type (fallback when no worker pool) |
| 10 | Worker node count | `8` | Chunk-worker `num_workers` |
| 11 | Instance pool id (orchestrator) | `0908-093007-hoped4-pool-q4mi8hia` | **Orchestrator** control-plane pool |
| 12 | Max concurrent chunks | `5` | Parallel chunk clusters |
| 13 | Parallel threads per chunk | `4` | |
| 14 | Chunk capacity (GB) | `500` | Bin-packing target per chunk |
| 15 | Min executors per chunk | `8` | |
| 16 | Inventory parallel threads | `4` | |

**Values that are NOT widgets** (fixed for CSV-mode job creation, in `ABSTRACTED_DEFAULTS`):
`input_type=CSV`, auto `batch_id`, `max_retries=3`, `require_target_precreated=true`,
`force_reonboard=false`, notebook/config paths (auto-derived from where the notebook lives),
and the chunk-worker **pool** (`worker_instance_pool_id`, defaults to
`0924-004044-comic1-pool-63p33jj6` from `job_factory.DEFAULT_PARAMS`).

> The workspace files root is auto-detected from the notebook's own path — you do **not**
> set it. `create_jobs_notebook` lives at `<root>/notebooks/create_jobs_notebook`, so the
> repo root is derived two levels up.

---

## Step 4 — Run the notebook (create/update the 7 jobs)

**Run all cells.** The notebook:

1. Clears any stale cached `orchestrator.*` Python modules from `sys.modules` and invalidates
   the import cache — this forces a fresh read of your just-synced code (see G2).
2. Resolves all parameters (prints the file root, CSV path, resolved orchestrator notebook).
3. Builds the 7 job specs via `build_all_job_specs()` and prints a preview of the workflow
   (task graph + `num_parameters`).
4. **Create-or-updates each job idempotently by name:**
   - new job → `POST /api/2.1/jobs/create`
   - existing job (same name) → `POST /api/2.1/jobs/reset` (full-settings overwrite, **same
     `job_id`, run history preserved**). It **never deletes and recreates** a job.
5. Prints a summary with a Jobs-UI link per job, e.g.:

```
[      updated] DeepClone 3 - DEEP_CLONE [prod]  (job_id=402143459991396)
...
Jobs synced: 7 jobs
```

The notebook returns the results as JSON via `dbutils.notebook.exit(...)`.

---

## Step 5 — Verify the jobs

From the notebook summary, click into each job, or confirm from the CLI that the parameters
resolved as intended. For the chunk-worker compute knobs that are now **runtime job
parameters** on DEEP_CLONE / RETRY / the workflow:

```bash
databricks jobs get <deep_clone_job_id> -p ril_catalog_test --output json \
  | jq -r '.settings.parameters[] | select(.name|test("worker_(spark_version|instance_pool_id|node_type|num_workers)"))'
```

Expected defaults: `worker_spark_version=17.3.x-scala2.13`,
`worker_instance_pool_id=0924-004044-comic1-pool-63p33jj6`,
`worker_node_type=Standard_E32ds_v5`, `worker_num_workers=8`.

**First-time only:** run the setup job once to create the control tables (skip if they
already exist in `<meta_catalog>.<meta_schema>`):

```bash
databricks jobs run-now -p ril_catalog_test --json '{"job_id": <setup_control_tables_job_id>}'
```

---

## Step 6 — Run a migration

**One-shot (recommended):** the full workflow chains INVENTORY → DRY_RUN → DEEP_CLONE →
RETRY → VALIDATE. `batch_id` may be omitted/blank to auto-generate one that all downstream
tasks recover via task values.

```bash
databricks jobs run-now -p ril_catalog_test --json '{
  "job_id": <full_migration_workflow_job_id>,
  "job_parameters": {"batch_id": "<your-new-batch-id>"}
}'
```

**Step-by-step** (standalone jobs — pass the **same** `batch_id` to each):

```bash
# 1) INVENTORY
databricks jobs run-now -p ril_catalog_test --json '{"job_id": <inventory_job_id>, "job_parameters": {"batch_id": "<batch>"}}'
# 2) DEEP_CLONE
databricks jobs run-now -p ril_catalog_test --json '{"job_id": <deep_clone_job_id>, "job_parameters": {"batch_id": "<batch>"}}'
# 3) VALIDATE
databricks jobs run-now -p ril_catalog_test --json '{"job_id": <validate_job_id>, "job_parameters": {"batch_id": "<batch>"}}'
```

The CSV content, exclusion list, and audit queries are covered in
[`SOP_CSV_Run.md`](SOP_CSV_Run.md) (Steps 5–10). This SOP focuses on getting the jobs
created and launched.

---

## Step 7 — Override chunk-worker compute at run time (optional)

The **ephemeral chunk-worker clusters** (used by DEEP_CLONE and RETRY) are runtime-tunable
without recreating the jobs. Pass any of these in `job_parameters` on a `run-now`:

| Parameter | Default | Effect |
|---|---|---|
| `worker_instance_pool_id` | `0924-004044-comic1-pool-63p33jj6` | Chunk-worker pool. **Blank ⇒ fall back to on-demand `worker_node_type`.** |
| `worker_node_type` | `Standard_E32ds_v5` | VM type used **only** when no pool is set |
| `worker_num_workers` | `8` | Executors per chunk cluster |
| `worker_spark_version` | `17.3.x-scala2.13` | Runtime for chunk clusters |

```bash
# Example: bigger chunk clusters, no pool (on-demand VMs) for this run only
databricks jobs run-now -p ril_catalog_test --json '{
  "job_id": <deep_clone_job_id>,
  "job_parameters": {
    "batch_id": "<batch>",
    "worker_instance_pool_id": "",
    "worker_node_type": "Standard_E64ds_v5",
    "worker_num_workers": "12"
  }
}'
```

**How it resolves:** the values are substituted into the `worker_cluster_json` base
parameter via `{{job.parameters.*}}`, and the orchestrator notebook then picks **exactly
one** compute source — a Databricks cluster spec cannot set both `instance_pool_id` and
`node_type_id`:

- `worker_instance_pool_id` **non-blank** → use the pool; `node_type_id` is dropped (VM type
  is governed by the pool).
- `worker_instance_pool_id` **blank** → drop the pool; use `node_type_id` +
  `availability: ON_DEMAND_AZURE`.

> **Limitation (by design):** the **orchestrator control-plane cluster** pool
> (`instance_pool_id`, widget 11) is *not* runtime-overridable. It's a job-cluster
> (`new_cluster`) spec, and Databricks does not substitute `{{job.parameters.*}}` in
> job-cluster definitions — only in task-level fields. To change it, edit widget 11 and
> re-run `create_jobs_notebook` (or change `databricks.yml` and redeploy on path A).

---

## Step 8 — Monitor runs

```bash
databricks jobs list-runs --job-id <job_id> -p ril_catalog_test
databricks jobs get-run <run_id> -p ril_catalog_test
```

Or open the run from the Jobs-UI link the notebook printed in Step 4. For the full workflow,
poll `get-run` until every task shows `TERMINATED / SUCCESS`. Each DEEP_CLONE chunk is its
own child job run (linked from the DEEP_CLONE task log) — see
[`SOP_CSV_Run.md` §8](SOP_CSV_Run.md) for chunk-level monitoring.

---

## 12. Reference: widgets, jobs & links

- **Notebook:** `/Workspace/Users/99ba7259-12df-4ab6-a625-547950b4a26c/deepclone_orchestrator/prod/files/notebooks/create_jobs_notebook`
- **Spec builder:** `orchestrator/job_factory.py` (`build_all_job_specs`, `DEFAULT_PARAMS`, `as_reset_body`)
- **Unit tests:** `tests/test_job_factory.py` (`python3 -m pytest tests/test_job_factory.py -q`)
- **Sync target:** `/Workspace/Users/99ba7259-12df-4ab6-a625-547950b4a26c/deepclone_orchestrator/prod/files`
- **Job IDs are environment-specific** — read them from the notebook summary (Step 4) or:

```bash
for n in "Setup Control Tables" INVENTORY DRY_RUN DEEP_CLONE VALIDATE RETRY "Full Migration Workflow"; do
  databricks jobs list -p ril_catalog_test --output json | jq -r --arg n "$n" '.[] | select(.settings.name|test($n)) | "\(.job_id)\t\(.settings.name)"'
done
```

---

## 13. Troubleshooting & Gotchas

### G1: A job still shows OLD parameters after I changed the code
Almost always a **path A vs. path B mix-up** (§2). If a job was previously created by
`databricks bundle deploy`, that deployed definition is a separate artifact from what
`create_jobs_notebook` produces. Recreate cleanly via the notebook (path B) — it patches the
same-named job in place. Don't `bundle deploy` and run the notebook against the same
environment expecting them to reconcile.

### G2: I re-ran the notebook but the recreated jobs STILL show old parameters
This was a real bug: on a **warm cluster**, Python caches imported modules in `sys.modules`,
so `import orchestrator.job_factory` returned the stale module from an earlier run and your
fresh `databricks sync` was silently ignored. **Fixed** — `create_jobs_notebook` now deletes
all cached `orchestrator.*` modules and calls `importlib.invalidate_caches()` *before* the
import. If you still see staleness on an old build, **detach & re-attach** the cluster (or
restart Python) and re-run, and confirm Step 2's `databricks sync` actually uploaded
`orchestrator/job_factory.py`.

### G3: "0 tables" / source not found on a real run
This SOP only creates and launches the jobs. Content/resolution problems (0 tables resolved,
`TABLE_OR_VIEW_NOT_FOUND`, `clone_type` resolved wrong, exclusion-list issues) are covered in
[`SOP_CSV_Run.md` §15 (G2–G8)](SOP_CSV_Run.md) and the mandatory §3.1 Delta-Share pre-flight.

### G4: `databricks sync` says complete but the notebook is unchanged in the UI
The Databricks UI caches open notebooks. Close and reopen the notebook tab (or refresh) after
a `--full` sync. Confirm the sync target path matches the files root in §3.

### G5: Chunk cluster ignores my run-time `worker_instance_pool_id`
Check the DEEP_CLONE **driver log** for `Chunk-worker compute: using instance pool <id>` (pool
path) or `no pool set — using on-demand node type <type>` (fallback path). If you passed a
pool ID but see the fallback line, the value arrived blank — re-check the `job_parameters`
JSON. Remember: setting a pool means `worker_node_type` is intentionally ignored (the pool
governs VM type).

### G6: I want a pool + a specific VM type at the same time
Not possible — Databricks rejects a cluster spec with both `instance_pool_id` and
`node_type_id` (`INVALID_PARAMETER`). Either create a second pool of the desired VM type and
pass its ID as `worker_instance_pool_id`, or clear the pool (`worker_instance_pool_id=""`) and
set `worker_node_type`.

---

## 14. Change Log

| Version | Date | Author | Change |
|---|---|---|---|
| 1.0 | 2026-09-24 | Data Platform Engineering | Initial SOP for the notebook-based job-creation workflow: git pull → `databricks sync` → run `create_jobs_notebook` (idempotent create/update of the 7 jobs via `jobs/create`+`jobs/reset`) → run/monitor migrations. Documents the widget surface, the stale-module-cache fix, and the run-time chunk-worker compute overrides (`worker_instance_pool_id` / `worker_node_type` / `worker_num_workers` / `worker_spark_version`) with the pool-vs-node resolution rule and the orchestrator-pool create-time limitation. |

---

*For questions or exceptions to this SOP, contact the Data Platform Engineering team via your internal ticketing system.*
