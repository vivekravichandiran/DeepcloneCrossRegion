# DeepClone CrossRegion — SOP: Running a CSV-based Migration (`input_type = CSV`, `clone_type = delta_share`)

**Document ID:** SOP-DCR-CSV-01
**Version:** 1.7
**Owner:** Data Platform Engineering
**Applies to bundle:** `deepclone_orchestrator` (`databricks.yml`)
**Classification:** Internal — Data Engineering

---

## Table of Contents

1. [Purpose & Scope](#1-purpose--scope)
2. [Architecture Overview](#2-architecture-overview)
3. [Prerequisites](#3-prerequisites) ([3.1 — confirm source is Delta Shared](#31--mandatory-confirm-the-source-catalogtables-are-already-delta-shared-into-the-target-metastore))
4. [Step 1 — Prepare the CSV table-mapping file](#step-1--prepare-the-csv-table-mapping-file) ([Optional — global exclusion list](#optional--global-exclusion-list-exclusion_csv_path))
5. [Step 2 — Point `databricks.yml` at your CSV](#step-2--point-databricksyml-at-your-csv)
6. [Step 3 — Deploy the bundle](#step-3--deploy-the-bundle)
7. [Step 4 — One-time setup: create control tables](#step-4--one-time-setup-create-control-tables)
8. [Step 5 — Run INVENTORY](#step-5--run-inventory)
9. [Step 6 — Verify queued tables](#step-6--verify-queued-tables)
10. [Step 7 — Run DEEP_CLONE](#step-7--run-deep_clone)
11. [Step 8 — Monitor the chunk-worker cluster](#step-8--monitor-the-chunk-worker-cluster)
12. [Step 9 — Run VALIDATE](#step-9--run-validate)
13. [Step 10 — Review audit results / sign-off](#step-10--review-audit-results--sign-off)
14. [One-shot alternative: `full_migration_workflow`](#14-one-shot-alternative-full_migration_workflow)
15. [Troubleshooting & Gotchas](#15-troubleshooting--gotchas)
16. [Cleanup / Re-running a batch](#16-cleanup--re-running-a-batch)
17. [Reference: Job IDs & Query Cheat-Sheet](#17-reference-job-ids--query-cheat-sheet)
18. [Change Log](#18-change-log)

---

## 1. Purpose & Scope

This SOP describes the exact, verified procedure to migrate a **hand-picked list of tables** from a source catalog to a target catalog using:

- **`input_type: CSV`** — the table list comes from a CSV file, instead of a YAML config or catalog/schema/table job filters. Two formats are supported (see Step 1): an explicit `source_catalog,source_schema,source_table,target_catalog,target_schema,target_table` list, or a row-level `clone_type` (catalog/schema/table) format with `exclude_schemas`/`exclude_tables` support that reuses YAML mode's expansion logic.
- **`clone_type: delta_share`** — the DEEP CLONE statement runs `CREATE OR REPLACE TABLE <target_fqn> DEEP CLONE <source_fqn>`, referencing the source table by its UC three-part name (used when the source is reachable directly or via Delta Sharing, as opposed to `direct_adls` which clones from a raw `abfss://` path).

This is the fastest onboarding path for **ad-hoc / curated table lists** (a handful of tables, a cherry-picked back-fill, a one-off DR copy) — no YAML editing, no catalog/schema filter logic, just a flat CSV.

**Out of scope:** YAML-mode and JOB-mode onboarding (see `docs/SOP_Onboarding.md` for the legacy/general SOP), initial workspace/network setup, Unity Catalog metastore creation.

---

## 2. Architecture Overview

The bundle deploys 7 jobs. A CSV run only touches **INVENTORY → DEEP_CLONE → VALIDATE** (RETRY is automatic/on-demand; the `full_migration_workflow` job chains all of them — see §14).

![Bundle deploy summary — job names, IDs, and URLs](sop_images/02_bundle_deploy_summary.png)
*`databricks bundle summary` — every job this SOP references, with its job ID and Jobs UI URL.*

| Job | Purpose |
|---|---|
| `setup_control_tables_job` | One-time DDL: creates `migration_control`, `migration_attempts`, `migration_validation_history` |
| `inventory_job` | Reads the CSV (or YAML/catalog filters), onboards rows into `migration_control` as `QUEUED`, bin-packs them into chunks |
| `deep_clone_job` | Dispatches one ephemeral cluster per chunk; each cluster runs the `DEEP CLONE` SQL for its assigned tables |
| `validate_job` | Compares source vs. target (size, file count, Delta version, row count) for `COMPLETED` rows and marks `VALIDATED`/`VALIDATION_FAILED` |
| `retry_job` | Re-queues `FAILED` / `RETRY_PENDING` / `VALIDATION_FAILED` rows (below `max_attempts`) and re-dispatches them |
| `full_migration_workflow` | One job that chains INVENTORY → DRY_RUN → DEEP_CLONE → RETRY → VALIDATE for a single `run-now` |

Everything is scoped by **`batch_id`** — the isolation key. `batch_id` is part of `migration_control`'s identity, not just a descriptive column: **the same source table onboarded under two different `batch_id`s gets two separate rows**, each with its own `migration_id`, status, timestamps, and row counts. So multiple teams/loads can run in parallel without interfering, re-running the same table list under a brand-new `batch_id` (e.g. a later reload) always gets its own fresh row and re-clones — it never silently skips because some *other* batch already completed that table — and full historical lineage ("which batches has this table been through, and what happened each time") is preserved directly in `migration_control`, not just in the separate `migration_attempts`/`migration_validation_history` audit tables. Always filter your audit queries by `batch_id`.

**Important assumption behind `clone_type=delta_share`:** the orchestrator never calls the Delta Sharing REST API itself. For every mode, it resolves and reads `source_catalog` through the **target** workspace's own Unity Catalog clients (`InputResolver`'s `_disc_api`/`_disc_sql` and `Validator`'s `_val_src_sql` are both bound to the target workspace when `clone_type=delta_share` — see `orchestrator_notebook.py`). In other words: **the pipeline assumes `source_catalog` is already a name that the target workspace's metastore can see and query** — either because source and target are literally in the same metastore, or because the source catalog has already been published as a Delta Share and mounted as a catalog in the target metastore *before you ever run this SOP*. §3.1 below is the mandatory pre-flight check for this.

---

## 3. Prerequisites

| # | Requirement | How to verify |
|---|---|---|
| 1 | Bundle already deployed at least once (`databricks bundle deploy -t prod --profile <profile>`) | `databricks bundle summary -t prod --profile <profile>` |
| 2 | `setup_control_tables_job` has been run at least once against the target `meta_catalog.meta_schema` | `SHOW TABLES IN <meta_catalog>.<meta_schema>` → `migration_control`, `migration_attempts`, `migration_validation_history` exist |
| 3 | **Source tables are already Delta Shared / visible from the TARGET workspace — see §3.1, mandatory, check first** | `DESCRIBE DETAIL <source_catalog>.<source_schema>.<source_table>` run from the **target** workspace's SQL warehouse succeeds |
| 4 | Target catalog/schema exist, or the run's service principal can create them | `SHOW GRANTS ON CATALOG <target_catalog>` |
| 5 | Instance pool configured in `databricks.yml` (`instance_pool_id`) is warm/available | Databricks UI → Compute → Instance Pools |
| 6 | `databricks` CLI configured with a working profile | `databricks bundle validate -t prod --profile <profile>` |
| 7 | **No Databricks Secret scope / client_id / client_secret to provision.** Auth is fully native (`databricks.sdk.core.Config()` — automatic inside any Databricks job/notebook). The only thing to check is that `databricks.yml`'s `target_warehouse_id` variable points at a real, running SQL warehouse in the target workspace (a plain, non-secret ID — not a credential) | `databricks jobs get <inventory_job_id> --profile <profile> --output json \| jq -r '.settings.parameters[] \| select(.name=="target_warehouse_id")'`, then `databricks warehouses get <that_id> --profile <profile>` |

### 3.1 — MANDATORY: confirm the source catalog/tables are already Delta Shared into the target metastore

**Do this before touching any CSV/YAML in Step 1.** This is the single most common reason a CSV run resolves 0 tables or fails with `TABLE_OR_VIEW_NOT_FOUND`/`SCHEMA_NOT_FOUND` (see G2) — the pipeline never provisions the Delta Share itself, it only *consumes* a `source_catalog` that must already resolve from the target workspace.

**Case A — source and target already in the same Unity Catalog metastore** (e.g. same-workspace or sibling-workspace tests, like the `ril_bulk_02`/`ril_bulk_csvtest` → `ril_tgt_02` examples used throughout this SOP): nothing extra to do — any catalog in the metastore is already visible to every workspace attached to it. Just run the verification query below to confirm.

**Case B — true cross-region / cross-metastore migration** (the general case this bundle — `DeepcloneCrossRegion` — is built for): the source catalog must be published as a Delta Share **before Step 1**. On the **source** metastore (run as a principal with `CREATE SHARE`/`USE CATALOG` privileges there):

```sql
-- 1. Create (or reuse) a share and add the schema(s)/table(s) you're migrating
CREATE SHARE IF NOT EXISTS <share_name>
  COMMENT 'DeepClone cross-region source share for <source_catalog>';
ALTER SHARE <share_name> ADD SCHEMA <source_catalog>.<source_schema>;   -- whole schema
-- or, for a hand-picked subset instead of a whole schema:
-- ALTER SHARE <share_name> ADD TABLE <source_catalog>.<source_schema>.<source_table>;

-- 2. Create a recipient for the target metastore (Databricks-to-Databricks sharing)
CREATE RECIPIENT IF NOT EXISTS <recipient_name>
  USING ID '<target_metastore_id>';   -- target's Account Console -> Metastore -> "Metastore ID"

-- 3. Grant the recipient access
GRANT SELECT ON SHARE <share_name> TO RECIPIENT <recipient_name>;
```

Then on the **target** metastore (the workspace this bundle is deployed to):

```sql
-- 4. Confirm the share was received, then mount it as a catalog
SHOW PROVIDERS;
CREATE CATALOG IF NOT EXISTS <source_catalog> USING SHARE <provider_name>.<share_name>;
```

**Verification (run this from the TARGET workspace's SQL warehouse — the same warehouse `configs/migration.yaml`'s `meta_catalog`/`meta_schema` points at) before proceeding to Step 1, for every source catalog/schema you plan to put in the CSV:**

```sql
SHOW SCHEMAS IN <source_catalog>;
SHOW TABLES IN <source_catalog>.<source_schema>;
DESCRIBE DETAIL <source_catalog>.<source_schema>.<source_table>;
```

All three must succeed and use the **exact same `source_catalog`/`source_schema`/`source_table` names** you're about to type into the CSV — `InputResolver` and `Validator` both query these names directly through the target workspace's clients, with no fallback to the "real" source workspace when `clone_type=delta_share`.

---

## Step 1 — Prepare the CSV table-mapping file

Two CSV formats are supported, auto-detected by `InputResolver._resolve_csv()` (`orchestrator/input_resolver.py`) from the presence of a `clone_type` header column.

> **✅ Use Format B (below) — it is the canonical, supported format for all migrations.** It handles single tables, whole schemas, and whole catalogs (with exclusions) in one file, so it covers every case Format A does and more. Format A is retained only for backward compatibility with old table-list files; **new CSVs should always use Format B.**

### Format B — Row-level catalog/schema/table selection with exclusions (has a `clone_type` column) — **RECOMMENDED / CANONICAL**

Header (column order matters, all 9 columns required — leave cells blank where not applicable):

```csv
clone_type,source_catalog,source_schema,source_table,target_catalog,target_schema,target_table,exclude_schemas,exclude_tables
```

Each row's `clone_type` value — `table`, `schema`, or `catalog` — sets that row's **selection granularity** (this is *not* the same `clone_type` as the global `direct_adls`/`delta_share` setting in `databricks.yml`; it just happens to reuse the column name from the source spec). It reuses the *exact same* catalog/schema expansion + exclusion logic already used by YAML `mappings:` entries (`InputResolver._expand_mapping_entry()`), so behaviour is identical between YAML and CSV modes:

| `clone_type` | Required columns | Optional columns | Behaviour |
|---|---|---|---|
| `table` | `source_catalog`, `source_schema`, `source_table` | `target_catalog`/`target_schema`/`target_table` (default = source) | One explicit table, same as Format A. `exclude_*` ignored. |
| `schema` | `source_catalog`, `source_schema` | `target_catalog`/`target_schema` (default = source), `exclude_tables` | Expands **every table** in that schema. `source_table` ignored. `exclude_tables` is a glob-pattern list (see below). `exclude_schemas` ignored. |
| `catalog` | `source_catalog` | `target_catalog` (default = source), `exclude_schemas`, `exclude_tables` | Expands **every schema and every table** in the catalog. `source_schema`/`source_table`/`target_schema`/`target_table` are ignored — a whole catalog clones schema-for-schema. Both exclusion columns are glob-pattern lists. |

`exclude_schemas` / `exclude_tables` cells hold a **Python-list-literal string**, e.g. `['table1','*lineage*']` (glob patterns, matched case-insensitively). **Because the cell contains a comma, it must be double-quoted in the CSV file** (standard CSV escaping) so the comma inside the list isn't mistaken for a column separator:

```csv
clone_type,source_catalog,source_schema,source_table,target_catalog,target_schema,target_table,exclude_schemas,exclude_tables
table,ril_bulk_csvtest,finance,dim_finance_01,ril_tgt_02,finance,dim_finance_01_csvtest,,
schema,ril_bulk_csvtest,hr,,ril_tgt_02,hr,,,['hr_table1']
catalog,ril_bulk_csvtest,,,ril_tgt_02,,,['hr'],"['*lineage*','dim_finance_03']"
```

This example (verified end-to-end, see §18 Change Log v1.1) resolves to exactly 7 tables: `finance.dim_finance_01` (explicit rename via the `table` row), `finance.dim_finance_02` + `finance.fact_finance_txn` (via the `catalog` row — `dim_finance_03` and `finance_lineage_log` excluded by pattern, `hr` schema excluded entirely so it isn't double-counted against the `schema` row below), and `hr.dim_hr_01`/`dim_hr_02`/`dim_hr_03`/`fact_hr_payroll` (via the `schema` row — `hr_table1` excluded). Rows are evaluated in file order and de-duplicated on `source_fqn` — if two rows resolve the same source table, the **first** row's target mapping wins (this is how the explicit `table` row's rename takes priority over the broader `catalog` row above).

> Tip: keep one CSV per logical load (e.g. `csv_<team>_<date>.csv`) so you can tell at a glance what a batch contains. `configs/csv_test_ril_bulk_csvtest.csv` in this repo is a working, previously-verified Format B example. `configs/csv_scale_ril_bulk_02_full.csv` is the current default (Format B, single `catalog`-type row cloning the entire `ril_bulk_02` catalog — used for the 8-cluster scale test, see §18 Change Log v1.3).

> **Common cause of "INVENTORY completed but 0 tables":** a `catalog`- or `schema`-type row is expanded **live** against the source metastore (`SHOW SCHEMAS`/`SHOW TABLES`). If the `source_catalog` isn't visible from the **target** workspace (not Delta-Shared / not same-metastore — see §3.1), it resolves to **0 tables** and INVENTORY still finishes green. Confirm with the §3.1 verification query, or use explicit `table` rows to rule out expansion. Check the driver log line `Input: type=CSV ... effective_input_path=<your csv>` to confirm the CSV you intended was actually the one loaded.

### Format A — Legacy explicit table list (no `clone_type` column) — *legacy, avoid for new CSVs*

Retained only for backward compatibility. One row = one explicit table mapping, no wildcards, no exclusions. `target_*` columns are optional (default to source). Prefer a Format B `table` row instead.

```csv
source_catalog,source_schema,source_table,target_catalog,target_schema,target_table
ril_bulk_02,iot,dim_iot_01,ril_tgt_02,iot,dim_iot_01
ril_bulk_02,marketing,dim_marketing_01,ril_tgt_02,marketing,dim_marketing_01
```

### Optional — Global exclusion list (`exclusion_csv_path`)

Independent of Format A/B above, and independent of `input_type` (JOB/YAML/CSV) — you can supply a **second, separate CSV** of catalog/schema/table-level exclusion rules that's applied on top of whatever the main CSV/YAML resolves, right before INVENTORY onboards anything. Use this for "never touch these schemas/tables, no matter what the load CSV says" — e.g. permanently excluding a legacy schema, a table under active migration by another team, or a known-problematic table you want to skip without editing the main CSV every time.

This is **not** the same mechanism as Format B's per-row `exclude_schemas`/`exclude_tables` columns — those only scope to the one `catalog`/`schema` row that declares them. The exclusion list here is global and cross-cutting: it's checked against **every** table resolved from **any** input_type, every run.

**CSV format** (`orchestrator/exclusion_manager.py`):

```csv
exclude_type,catalog,schema,table
catalog,ril_bulk_old,,
schema,ril_bulk_02,iot,
table,ril_bulk_02,finance,dim_finance_01
```

| Column | Required for | Notes |
|---|---|---|
| `exclude_type` | all rows | `catalog` \| `schema` \| `table` — the granularity of this rule |
| `catalog` | all rows | Required always. Glob pattern allowed (e.g. `ril_bulk_*`) |
| `schema` | `schema`, `table` rows | Ignored for `catalog` rows. Glob pattern allowed |
| `table` | `table` rows only | Ignored for `catalog`/`schema` rows. Glob pattern allowed |

Matching is case-insensitive glob (`fnmatch`), same semantics as Format B's `exclude_schemas`/`exclude_tables`. A table is excluded if **any** rule matches it — a `catalog` rule matches every table in that catalog, a `schema` rule matches every table in that catalog.schema, a `table` rule matches exactly one table (or a glob of them).

**What happens to excluded tables:** they are filtered out of the resolved selection list **before** `InventoryManager` ever sees them — they never get a `migration_control` row at all. Instead, every excluded table is recorded as one immutable row in **`migration_exclusion_log`** (`run_id`, `batch_id`, `source_catalog`/`schema`/`table`, `exclusion_type`, `exclusion_rule`, `excluded_at`) — this is the audit trail for "why isn't table X in migration_control", so exclusions are reviewable, not silent. `DRY_RUN` mode previews the exclusion (prints the skip list) but does **not** write to `migration_exclusion_log` — only a real `INVENTORY` run persists the audit record.

Leave `exclusion_csv_path` blank (the widget/job-parameter default) for zero exclusions / zero behavior change.

### Optional — Governance gate: `require_target_precreated`

By default, DEEP_CLONE auto-creates the target table (`CREATE TABLE ... DEEP CLONE ...`)
if it doesn't already exist. Some environments require the target table/schema to be
provisioned out-of-band first (correct storage location, grants, tags, partitioning)
**before** any data is allowed to land in it. Set the job/bundle parameter
`require_target_precreated: "true"` to enforce this: at INVENTORY, before touching the
source at all, each row's target is probed with a lightweight `DESCRIBE TABLE`. If the
target doesn't exist yet, the row is onboarded as `status=SKIPPED`,
`error_code=TARGET_NOT_PRECREATED` (not `QUEUED`) — and since DEEP_CLONE/RETRY only ever
select `status='QUEUED'` rows, a skipped row can **never** be auto-created downstream.

**Recovery:** once the target is pre-created out-of-band, re-run INVENTORY with
`force_reonboard=true` (gate still `true`) — the row is re-evaluated, the target is
found, and it flips `SKIPPED → QUEUED`, ready for the next DEEP_CLONE run.

Leave `require_target_precreated` at its default (`"false"`) for the standard
auto-create behavior — this flag is fully opt-in and does not change any existing
pipeline. Full test matrix and results:
[`Test_Report_Target_Precreated_Gate.md`](Test_Report_Target_Precreated_Gate.md).

---

## Step 2 — Point `databricks.yml` at your CSV

Edit the `variables:` block near the top of `databricks.yml` — **3 required fields** (+1 optional), no other file needs to change:

```yaml
variables:
  csv_path:
    default: "${workspace.file_path}/configs/<your_file>.csv"
  clone_type:
    default: "delta_share"
  input_type:
    default: "CSV"     # optional / cosmetic — see note below
  exclusion_csv_path:
    default: "${workspace.file_path}/configs/<your_exclusion_file>.csv"   # optional — blank = no exclusions
  require_target_precreated:
    default: "false"   # optional — set "true" to skip (not auto-create) tables whose target doesn't exist yet
```

![databricks.yml — clone_type: delta_share](sop_images/01_databricks_yml_clone_type_delta_share.png)
*Example: setting `clone_type: delta_share` for a target in `databricks.yml`. In the current single-file layout, `clone_type` and `csv_path` live under the top-level `variables:` block (not per-target) — set them once, they apply to whichever target you deploy.*

**Rules that matter:**

- **`csv_path` wins over `yaml_config_path` automatically.** The notebook auto-detects `input_type` from whichever path is non-blank (`csv_path` checked first) — see `orchestrator_notebook.py`'s `_Params.effective_input_path`. You do **not** need to blank out `yaml_config_path`; leave it alone.
- **`clone_type: delta_share` must be set** for CSV+delta_share runs. If left blank, `OrchestratorConfig` falls back to `direct_adls` for CSV/JOB mode.
- **Do not set `target_catalog` / `source_catalog_filter` / `source_schema_filter` / `source_table_filter`** to control this run — those are JOB-mode-only variables and are silently ignored once `input_type` resolves to CSV. All source/target info comes from the CSV file itself.
- **`exclusion_csv_path` is optional and orthogonal to everything above** — set it only if you need the global exclusion list described in Step 1's "Optional" subsection; leave it blank for no exclusions.
- **No secrets to configure.** There is no Databricks Secret scope, `client_id`, or `client_secret` anywhere in this bundle — `SqlClient`/`ApiClient` authenticate natively via `databricks.sdk.core.Config()`, which is automatic for any job/notebook run in Databricks. The only workspace-connection values are `databricks.yml`'s `target_warehouse_id` (always required) and `source_warehouse_id` (only for `clone_type=direct_adls`) — both plain, non-secret SQL warehouse IDs, not credentials. Change them only if your target/source SQL warehouse differs from the bundle's default.

---

## Step 3 — Deploy the bundle

```bash
databricks bundle deploy -t prod --profile <profile>
```

Confirm the deployed job parameters actually picked up your values:

```bash
databricks jobs get <inventory_job_id> --profile <profile> --output json \
  | jq -r '.settings.parameters[] | select(.name=="input_type" or .name=="csv_path" or .name=="clone_type" or .name=="exclusion_csv_path")'
```

Expected: `input_type=CSV`, `csv_path=.../configs/<your_file>.csv`, `clone_type=delta_share`, `exclusion_csv_path=` either blank (no exclusions) or `.../configs/<your_exclusion_file>.csv`.

---

## Step 4 — One-time setup: create control tables

Skip if `migration_control` / `migration_attempts` / `migration_validation_history` already exist in your `meta_catalog.meta_schema`.

```bash
databricks bundle run setup_control_tables_job -t prod --profile <profile>
```

![CLI: triggering setup_control_tables_job](sop_images/03_setup_control_tables_cli.png)
*Deploy + run from the terminal. Note the Run URL printed — click through to watch it in the UI.*

![setup_control_tables_job — Succeeded](sop_images/04_setup_control_tables_run_ui.png)
*The Databricks UI run detail — DDL output plus useful monitoring query templates (throughput, failed tables, workload distribution) printed at the bottom of the notebook.*

---

## Step 5 — Run INVENTORY

This discovers/validates every row in your CSV and inserts it into `migration_control` as `QUEUED`, then bin-packs the queued rows into chunks (`chunk_capacity_gb`, default 50–500 GB per chunk depending on target).

```bash
databricks jobs run-now --profile <profile> --json '{
  "job_id": <inventory_job_id>,
  "job_parameters": {"batch_id": "<your-new-batch-id>"}
}'
```

> Pick a fresh, descriptive `batch_id` — it's the isolation key. Don't reuse someone else's test batch. **You may also omit `batch_id` entirely** (or pass `""`) to have one auto-generated (`batch-<date>-<short-uuid>`) — INVENTORY publishes the value it actually used as a Databricks task value, and `full_migration_workflow`'s downstream tasks (`deep_clone`/`retry`/`validate`) automatically recover the same auto-generated ID (via `dbutils.jobs.taskValues.get(taskKey="inventory", ...)`), so the whole chain stays consistent without you having to read logs to find out what ID was picked. Standalone job-by-job runs (Steps 7/9) still need you to pass the same `batch_id` explicitly, since there's no "inventory" task in the same run to recover it from.

> **Re-testing an already-`VALIDATED`/`COMPLETED` table list?** This "already done, skip it" idempotency check is scoped to the **current `batch_id` only**: INVENTORY *skips* re-onboarding a row already `COMPLETED`/`VALIDATED`/`FAILED_PERMANENT`/`SKIPPED` **for that same `batch_id`**, so re-running the exact same CSV under the *same* `batch_id` twice onboards 0 new tables (this is correct behaviour, not a bug — it's what makes re-running INVENTORY for a batch safe/idempotent). To deliberately force a fresh re-clone of the same table list under the *same* `batch_id`, add `"force_reonboard": "true"` to `job_parameters` — this resets that batch's row back to fresh `QUEUED`, clearing any stale `validation_status`/row-count/target-mapping fields, so VALIDATE genuinely re-checks the fresh clone instead of skipping it as "already validated". **A different/new `batch_id` is never affected by this check at all** — pointing the same table list at a brand-new `batch_id` (e.g. an evening reload of the same tables you migrated that morning) always onboards fresh and re-clones, creating its own new row and leaving every earlier batch's row for those tables completely untouched — you don't need `force_reonboard` just to onboard the same tables again under a new batch.

> **If `exclusion_csv_path` is set** (Step 1's "Optional" subsection), the driver log prints an `EXCLUSION LIST` block listing every skipped table and which rule matched, right after `Resolved N table selections`. Excluded tables are recorded to `migration_exclusion_log` (query in Step 6) — they will **not** appear in `migration_control` at all, so don't be alarmed if your CSV's row count doesn't match the number of rows onboarded; check the exclusion log before assuming a bug.

![INVENTORY run — Parameters panel (input_type=CSV, csv_path, clone_type=delta_share)](sop_images/05_inventory_csv_parameters_pending.png)
*Right-hand Parameters panel on the run page confirms the **resolved** values actually used for this run — always double-check `input_type`, `csv_path`, and `clone_type` here before trusting the run.*

![INVENTORY run — Succeeded, full resolved parameter list](sop_images/06_inventory_csv_succeeded_parameters.png)
*A completed INVENTORY run. `input_type: CSV (resolved)`, `clone_type: delta_share (resolved)`, `csv_path` pointing at the deployed CSV file under `.../files/configs/...`.*

---

## Step 6 — Verify queued tables

```sql
-- Tables lined up for deep clone (QUEUED, ready to be picked up)
SELECT source_catalog, source_schema, source_table,
       target_catalog, target_schema, target_table,
       status, chunk_id, size_gb
FROM `<meta_catalog>`.`<meta_schema>`.migration_control
WHERE batch_id = '<your-new-batch-id>'
ORDER BY chunk_id, source_schema, source_table;
```

![migration_control — QUEUED rows for a batch](sop_images/07_migration_control_queued_query.png)
*All rows should show `status = QUEUED` with a `chunk_id` assigned and no `error_code`. If a row shows a different status or has stale error fields, re-run INVENTORY — the upsert logic resets `started_at`/`error_code`/etc. on re-onboarding.*

If you set `exclusion_csv_path` in Step 2, also check what got skipped and why:

```sql
-- Tables skipped by the global exclusion list for this batch
SELECT source_catalog, source_schema, source_table, exclusion_type, exclusion_rule, excluded_at
FROM `<meta_catalog>`.`<meta_schema>`.migration_exclusion_log
WHERE batch_id = '<your-new-batch-id>'
ORDER BY source_schema, source_table;
```

`QUEUED count + exclusion_log count` should equal the total number of tables your CSV/YAML would otherwise have resolved — if it doesn't, some rows were neither onboarded nor excluded (check the driver log for `CSV row N ... skipping` / `Source table not found` warnings, e.g. G2).

**Sign-off before proceeding:** every row you expect from the CSV is present, `status = QUEUED`, `error_code IS NULL`, and (if applicable) every intentionally-excluded table shows up in `migration_exclusion_log` with the expected `exclusion_type`/`exclusion_rule`.

---

## Step 7 — Run DEEP_CLONE

Same `batch_id` as Step 5:

```bash
databricks jobs run-now --profile <profile> --json '{
  "job_id": <deep_clone_job_id>,
  "job_parameters": {"batch_id": "<your-new-batch-id>"}
}'
```

![DEEP_CLONE run — dispatch log while running](sop_images/08_deep_clone_running_dispatch_log.png)
*Live notebook output: `QUEUED records for batch <id>: N`, chunk bin-packing summary, then `Dispatching chunk 1: N tables ... → chunk_worker_notebook`. Each chunk is dispatched as its own one-time job run on an instance-pool-backed cluster.*

![DEEP_CLONE run — Succeeded, chunk/table summary](sop_images/09_deep_clone_succeeded_summary.png)
*Completed run — `DEEP CLONE Summary`: chunks completed, chunks failed, tables ok, tables failed. The job **fails loudly** (raises `RuntimeError`) if any chunk/table failed, so a green "Succeeded" here means everything actually cloned — it won't silently report success on partial failure.*

---

## Step 8 — Monitor the chunk-worker cluster

Each chunk is its own job run (linked from the DEEP_CLONE log above, or via `Compute → <job_run_id>`). It executes the actual `DEEP CLONE` SQL for its assigned tables directly on Spark.

![Chunk worker notebook — SUCCESS](sop_images/10_chunk_worker_success.png)
*Per-table pass/fail summary, then `Notebook exited: SUCCESS:batch=<id>,chunk=<n>,ok=<n>,fail=0`. If `fail_count > 0` the chunk raises `PARTIAL_FAILURE`, which propagates back up and fails the parent DEEP_CLONE task.*

---

## Step 9 — Run VALIDATE

```bash
databricks jobs run-now --profile <profile> --json '{
  "job_id": <validate_job_id>,
  "job_parameters": {"batch_id": "<your-new-batch-id>"}
}'
```

VALIDATE checks every `COMPLETED` row for this batch: table size, file count, Delta version, and (if `row_count_validation=true`, the default) row count — counted `VERSION AS OF` the exact `source_version`/`target_version` captured at clone time, so results stay stable even if the source gets new writes afterward. Results are written both to `migration_control` (latest) and to the append-only `migration_validation_history` audit table.

---

## Step 10 — Review audit results / sign-off

```sql
-- Final status for the batch
SELECT status, COUNT(*) AS n, SUM(size_gb) AS total_gb
FROM `<meta_catalog>`.`<meta_schema>`.migration_control
WHERE batch_id = '<your-new-batch-id>'
GROUP BY status ORDER BY n DESC;

-- Row-count validation detail (per table, from the audit trail)
SELECT source_table, source_version, target_version,
       source_row_count, target_row_count, row_count_matched, status
FROM `<meta_catalog>`.`<meta_schema>`.migration_validation_history
WHERE batch_id = '<your-new-batch-id>'
ORDER BY source_table;
```

**Acceptance criteria:** every row `status = VALIDATED`, `row_count_matched = true` (or `NULL` only if you deliberately disabled `row_count_validation`). Any `VALIDATION_FAILED` or count mismatch is a blocker — investigate before signing off.

If you used `exclusion_csv_path`, also re-confirm the exclusion count as part of sign-off — `(migration_control rows) + (migration_exclusion_log rows for this batch)` should equal the total number of tables the CSV/YAML would otherwise have resolved:

```sql
SELECT
  (SELECT COUNT(*) FROM `<meta_catalog>`.`<meta_schema>`.migration_control WHERE batch_id = '<your-new-batch-id>') AS onboarded,
  (SELECT COUNT(*) FROM `<meta_catalog>`.`<meta_schema>`.migration_exclusion_log WHERE batch_id = '<your-new-batch-id>') AS excluded;
```

---

## 14. One-shot alternative: `full_migration_workflow`

For a single `run-now` that chains INVENTORY → DRY_RUN → DEEP_CLONE → RETRY → VALIDATE automatically (Steps 5–9 above in one job), use:

```bash
databricks jobs run-now --profile <profile> --json '{
  "job_id": <full_migration_workflow_job_id>,
  "job_parameters": {"batch_id": "<your-new-batch-id>"}
}'
```

It reads the same `input_type`/`csv_path`/`clone_type` bundle variables as the standalone INVENTORY job — no extra configuration needed. Poll with `databricks jobs get-run <run_id>` until every task shows `TERMINATED / SUCCESS`.

---

## 15. Troubleshooting & Gotchas

### G1: `repair-run` silently drops your `batch_id`
If a task in the workflow fails and you `databricks jobs repair-run` to retry just that task, **you must re-pass `job_parameters` explicitly** in the repair call:

```bash
databricks jobs repair-run --profile <profile> --json '{
  "run_id": <run_id>,
  "rerun_tasks": ["deep_clone"],
  "job_parameters": {"batch_id": "<your-batch-id>"}
}'
```

Without this, the repaired task falls back to the job's blank default `batch_id`, silently auto-generates a random phantom batch, finds 0 `QUEUED` rows for it, and reports a false `SUCCESS` while doing nothing. **When in doubt, trigger a fresh `run-now` instead** — it's idempotent (re-running INVENTORY for the same source/target FQNs reuses the same `migration_id`, just resets status to `QUEUED`).

### G2: "Inventory shows 0 tables" / "Total discovered: 0" after switching to CSV
Two different root causes, both fixed in the current code but worth knowing about:

- **Genuinely 0 rows resolved** — almost always one of:
  - **`source_catalog` isn't actually visible from the target workspace yet** — see §3.1. This is the #1 cause on a real cross-region run: the source catalog hasn't been Delta Shared/mounted into the target metastore, so `SHOW SCHEMAS`/`SHOW TABLES` against it returns nothing (or errors), and `InputResolver` silently resolves 0 selections for a `catalog`/`schema`-type row. Re-run the §3.1 verification query first.
  - `csv_path` resolved to a doubled/relative path (e.g. a YAML file's own `csv_path:` field is relative to *that YAML's* directory, not the repo root) — use the full bundle-deployed path.
  - `yaml_config_path` was non-blank and took precedence unexpectedly — check `effective_input_path` in the INVENTORY log line `Input: type=... effective_input_path=...`.
  - (Format B only) a row's `clone_type` cell is misspelled/blank when it shouldn't be, or a required column for that row type is empty — check the notebook log for `CSV row N ... skipping` warnings.
- **Rows *were* resolved but the summary still shows all zeros** — this happened when every resolved row was already `COMPLETED`/`VALIDATED` from a prior run, so INVENTORY correctly *skipped* re-onboarding them (see the `force_reonboard` note in Step 5) — the underlying summary query used to be scoped to `run_id`, which only ever gets stamped on brand-new rows, so a skip-everything run showed a misleading `Total discovered: 0` even though CSV parsing worked fine. Fixed by rescoping the summary to `batch_id` (which every mode reads/writes consistently) plus overriding INVENTORY's own summary counts directly from its resolver stats. Check the driver log's `Inventory complete: total=N onboarded=N skipped=N failed=N` line — that's always the ground truth regardless of what the printed summary showed on an old build.

### G3: `clone_type` shows `direct_adls` in the UI even though the CSV/YAML says `delta_share`
Check the **Parameters panel `(resolved)` value**, not just what you typed in the config — `cfg.clone_type` is only overridden by the job parameter when `input_type != YAML`. For CSV mode, `databricks.yml`'s `clone_type` variable **is** applied, so make sure it's set to `delta_share` there (Step 2). VALIDATE additionally needs `clone_type` wired into its own job parameters (already done in `04_validate_job.yml` / `06_full_migration_workflow.yml`) — for `delta_share`, VALIDATE reads "source" data via the **target** SQL client, since delta-shared tables are visible there.

### G4: DEEP_CLONE/RETRY fail immediately with an OIDC/token error
These modes never need the **source** SQL warehouse (chunk workers run `DEEP CLONE` directly via Spark, not the REST SQL client) — the orchestrator notebook skips starting it for `DEEP_CLONE`/`RETRY`, or for any mode when `clone_type=delta_share`. If you see this error on a fresh code checkout, redeploy — this is fixed in the current `orchestrator_notebook.py`.

### G5: Chunk cluster not using the instance pool
Confirm `worker_cluster_json`'s `instance_pool_id` in the relevant job YAML (`03_deep_clone_job.yml`, `05_retry_job.yml`, `06_full_migration_workflow.yml`) matches `databricks.yml`'s `instance_pool_id` variable, and redeploy. Verify on an actual chunk run via:

```bash
databricks api get "/api/2.1/jobs/runs/get?run_id=<chunk_run_id>" --profile <profile> \
  | jq '.tasks[0].new_cluster.instance_pool_id'
```

### G6: `full_migration_workflow` with an auto-generated `batch_id` — deep_clone/validate never find any `QUEUED`/`COMPLETED` rows
Check the driver log for `batch_id widget was blank — recovered <id> via taskValues.get(taskKey='inventory')` — if this line is **absent** and each task instead logs a *different* auto-generated `batch_id` in its own `Published task value batch_id=...` line, the tasks are out of sync. This was an actual bug (the `{{tasks.inventory.values.batch_id}}` dynamic value reference in `06_full_migration_workflow.yml`'s `base_parameters` did not reliably resolve) — fixed by adding a `dbutils.jobs.taskValues.get(taskKey="inventory", key="batch_id")` fallback directly in `orchestrator_notebook.py`. If you see mismatched batch IDs on a fresh checkout, redeploy — this is fixed in the current code.

### G7: (Format B CSV) `ast.literal_eval` warning / exclude pattern silently ignored
`exclude_schemas`/`exclude_tables` cells are parsed as Python list literals (`InputResolver._parse_csv_list`). If the cell isn't valid Python syntax, or a comma inside the list wasn't double-quoted in the CSV file (so the row got split into the wrong number of columns), you'll see `Could not parse exclude-list cell ... — treating as empty` in the log and the exclusion silently won't apply. Always double-quote any cell containing a comma, e.g. `"['a','b']"`, not `['a','b']` bare.

### G8: Global `exclusion_csv_path` rule isn't excluding what you expect (or excludes too much)
This is a **separate mechanism** from Format B's per-row `exclude_schemas`/`exclude_tables` (G7) — don't confuse the two CSV files. Common causes:

- **Wrong `exclude_type` for the intent.** A `schema` row with `catalog=ril_bulk_02,schema=iot` excludes *every table currently and future in that schema* — if you only meant to exclude one table, use a `table` row instead (`orchestrator/exclusion_manager.py`'s `ExclusionRule.matches()` checks `catalog` first, then falls through to `schema`/`table` only for the matching granularity — a `catalog` row with no `schema`/`table` value excludes the **entire catalog**, so a blank `schema`/`table` cell on a `catalog`-type row is correct, not a bug).
- **Case/glob mismatch.** Matching is `fnmatch`-based and case-insensitive, but glob syntax still applies — `ril_bulk_0*` matches `ril_bulk_02`, but `ril_bulk_02,ril_bulk_03` (comma-separated) does **not** — each catalog/schema/table needs its own row.
- **Rows skipped at parse time.** Check the driver log right after `Loading global exclusion list from ...` for `Exclusion CSV row N ... — skipping` warnings — a row with an invalid `exclude_type`, or missing a required column for that type (e.g. a `table` row with a blank `table` cell), is dropped silently from the rule set, not applied as a no-op wildcard.
- **Confirm what rules actually loaded** — the driver log prints one `Exclusion rule: <type>:<catalog>[.<schema>][.<table>]` line per successfully-parsed rule, right after the CSV is loaded. If your intended rule isn't in that list, the CSV row didn't parse.
- **Blank `exclusion_csv_path` is a no-op**, by design (zero exclusions) — if you expected exclusions to apply but `migration_exclusion_log` has 0 rows for your `batch_id` and `migration_control` has every table, first check the job's resolved `exclusion_csv_path` parameter (Step 3's confirmation command) isn't blank.

---

## 16. Cleanup / Re-running a batch

To wipe a batch and start over (e.g. a bad test run):

```sql
DELETE FROM `<meta_catalog>`.`<meta_schema>`.migration_control WHERE batch_id = '<batch_id>';
DELETE FROM `<meta_catalog>`.`<meta_schema>`.migration_attempts a
  WHERE NOT EXISTS (SELECT 1 FROM `<meta_catalog>`.`<meta_schema>`.migration_control c WHERE c.migration_id = a.migration_id);
```

To simply re-queue an existing batch without deleting history:

```sql
UPDATE `<meta_catalog>`.`<meta_schema>`.migration_control
SET status = 'QUEUED', attempt_number = 0,
    started_at = NULL, completed_at = NULL, failed_at = NULL,
    error_code = NULL, error_message = NULL,
    validation_status = NULL, validation_message = NULL
WHERE batch_id = '<batch_id>';
```

Then re-run DEEP_CLONE (Step 7) — target tables are cloned with `CREATE OR REPLACE TABLE`, so re-running is safe.

---

## 17. Reference: Job IDs & Query Cheat-Sheet

> Job IDs are environment-specific — always re-confirm with `databricks bundle summary -t prod --profile <profile>` (see the screenshot in §2). Example IDs from the `prod` target at time of writing:

| Job | Example Job ID |
|---|---|
| `setup_control_tables_job` | 725639514038008 |
| `inventory_job` | 575259775037773 |
| `dry_run_job` | 159969274208470 |
| `deep_clone_job` | 402143459991396 |
| `validate_job` | 711818079788694 |
| `retry_job` | 768267244489127 |
| `full_migration_workflow` | 963908754645214 |

```sql
-- Failed tables with error context
SELECT source_catalog, source_schema, source_table,
       attempt_number, max_attempts, error_code, error_message
FROM `<meta_catalog>`.`<meta_schema>`.migration_control
WHERE status IN ('FAILED','FAILED_PERMANENT','VALIDATION_FAILED')
ORDER BY updated_at DESC;

-- Attempt-level history for a specific table (debugging retries/failures)
SELECT a.* FROM `<meta_catalog>`.`<meta_schema>`.migration_attempts a
JOIN `<meta_catalog>`.`<meta_schema>`.migration_control c
  ON a.migration_id = c.migration_id
WHERE c.source_table = '<table_name>'
ORDER BY a.attempt_number;

-- Current throughput (bytes/hour)
SELECT DATE_TRUNC('hour', completed_at) AS hour,
       COUNT(*) AS tables, SUM(size_gb) AS gb_completed,
       AVG(duration_seconds) AS avg_duration_s
FROM `<meta_catalog>`.`<meta_schema>`.migration_control
WHERE status IN ('COMPLETED','VALIDATED')
GROUP BY 1 ORDER BY 1 DESC;

-- Tables skipped by the global exclusion list (exclusion_csv_path), any batch
SELECT batch_id, source_catalog, source_schema, source_table,
       exclusion_type, exclusion_rule, excluded_at
FROM `<meta_catalog>`.`<meta_schema>`.migration_exclusion_log
ORDER BY excluded_at DESC;
```

---

## 18. Change Log

| Version | Date | Author | Change |
|---|---|---|---|
| 1.7 | 2026-09-24 | Data Platform Engineering | **Made CSV Format B the canonical/recommended format** (Step 1 reordered — Format B first and marked RECOMMENDED/CANONICAL; Format A demoted to a clearly-labelled legacy subsection). Added a prominent "INVENTORY completed but 0 tables" callout explaining that `catalog`/`schema` rows expand live against the source metastore and resolve to 0 when `source_catalog` isn't visible from the target workspace (cross-links §3.1 + the `effective_input_path` driver-log check). Unrelated but shipped together: fixed `notebooks/create_jobs_notebook.py` so the **chunk-worker instance pool** is now an exposed widget (`worker_instance_pool_id`, widget 11) instead of silently defaulting to an internal test pool — every pool/compute value entered in the create-jobs notebook now becomes the created job's parameter default. |
| 1.6 | 2026-09-17 | Data Platform Engineering | **Fixed silent cross-batch history loss in `migration_control`.** Previously, `migration_control`'s identity was keyed only on `(source_catalog, source_schema, source_table)` — so onboarding the SAME source table under a NEW `batch_id` (e.g. re-running the same CSV in the evening after a morning run) would find and silently overwrite the earlier batch's row (`orchestrator/inventory_manager.py::_get_existing()`/`_upsert()`/`_mark_permanent_failure()`/`_mark_skipped_target_missing()`), losing that batch's `batch_id`, timestamps, and row counts. The identity key is now `(source_catalog, source_schema, source_table, batch_id)` — every `batch_id` gets its OWN row per source table, full historical lineage across batches is preserved directly in `migration_control`, and re-running the SAME `batch_id` remains idempotent (updates its own row, no duplicates). Also fixed a related bug where `_mark_permanent_failure()` never stamped `batch_id` on `FAILED_PERMANENT` rows at all. `force_reonboard` semantics narrowed accordingly — see the updated Step 5 note. Verified with a dedicated multi-batch test: the same 2 source tables run through **5 different batches** (3 standalone-job batches, 1 same-batch idempotent re-run, 1 `force_reonboard` same-batch reset, and 1 full chained `06_full_migration_workflow` run with the governance gate ON) each produced its own independent, correctly-tracked row — zero cross-batch bleed, confirmed via `migration_control`, `migration_attempts`, and `migration_validation_history`. Full report: [`Test_Report_MultiBatch_History.md`](Test_Report_MultiBatch_History.md). |
| 1.5 | 2026-09-16 | Data Platform Engineering | Added the **`require_target_precreated` governance gate** (`orchestrator/inventory_manager.py::_target_exists()` / `_mark_skipped_target_missing()`) — an opt-in job/bundle parameter that makes INVENTORY skip (not auto-create) any table whose target doesn't already exist, marking it `status=SKIPPED`/`error_code=TARGET_NOT_PRECREATED` so DEEP_CLONE/RETRY (which only select `status='QUEUED'`) can never touch it. Recovery path: pre-create the target out-of-band, then re-run INVENTORY with `force_reonboard=true` to flip it back to `QUEUED`. Documented in Step 1 ("Optional — Governance gate") and Step 2 (`databricks.yml` wiring). Fixed two bugs found during testing: `_mark_skipped_target_missing()`'s MERGE wasn't refreshing `target_catalog`/`target_schema`/`target_table` or resetting `validation_status`/row-counts/timestamps on re-skip, leaving misleading stale data on SKIPPED rows. Verified with a 6-case test matrix (unit test + standalone INVENTORY/DEEP_CLONE + recovery + full end-to-end `06_full_migration_workflow` run) — full results in [`Test_Report_Target_Precreated_Gate.md`](Test_Report_Target_Precreated_Gate.md). |
| 1.4 | 2026-09-10 | Data Platform Engineering | **Removed all external secrets.** Deleted the `deepclone-migration` Databricks Secret scope and every `client_id`/`client_secret`/`{{secrets/...}}` reference from `orchestrator/sql_client.py`, `orchestrator/api_client.py`, `orchestrator/config.py`, `notebooks/setup_control_tables.py`, and every `resources/*.yml` job cluster / `worker_cluster_json` spec. `SqlClient`/`ApiClient` now authenticate natively via `databricks.sdk.core.Config()` (Databricks unified/runtime auth — automatic inside any job/notebook, nothing to provision or rotate). The only workspace-connection values left are the plain, non-secret `target_warehouse_id` / `source_warehouse_id` bundle variables (SQL warehouse IDs, not credentials) — wired as job parameters on every job (`00`–`06`). Also fixed a latent bug this change surfaced: `03_deep_clone_job.yml`/`05_retry_job.yml` hard-coded/omitted `clone_type`, which only "worked" before because of leftover non-blank dummy secret values satisfying the old (weak) validation check — both now read `clone_type` from `${var.clone_type}` like every other job. Deleted the fully-superseded, secret-dependent `scripts/deploy_to_workspace.py` and `tests/generate_report.py` (pre-bundle, already marked stale/unused). Verified end-to-end **with the secret scope deleted**: ran INVENTORY → DEEP_CLONE → VALIDATE against a fresh batch (`no-secrets-test-*`, 7 tables) on `ril_bulk_csvtest` → `ril_tgt_02` — all 7 `VALIDATED` with matching row counts, zero auth errors. |
| 1.3 | 2026-09-09 | Data Platform Engineering | Added the **global exclusion list** feature (`exclusion_csv_path`, `orchestrator/exclusion_manager.py`) — a separate CSV of `catalog`/`schema`/`table` exclusion rules, applied at INVENTORY on top of any `input_type`, independent of Format B's per-row `exclude_schemas`/`exclude_tables`. Excluded tables never reach `migration_control`; they're recorded to the new `migration_exclusion_log` audit table instead. Documented in Step 1 ("Optional — Global exclusion list"), Step 2 (`databricks.yml` wiring), Step 5/6/10 (verification + sign-off queries), §17 (cheat-sheet query), and a new Gotcha G8. Also noted the current scale-test default `csv_path` → `configs/csv_scale_ril_bulk_02_full.csv` (whole-catalog `ril_bulk_02` → `ril_tgt_02`, 130 tables, verified with 8 parallel chunk clusters) in Step 1's tip. Verified end-to-end: INVENTORY with `exclusion_csv_path` set to exclude the whole `iot` schema (26 tables) + `finance.dim_finance_01` (1 table) from the 130-table scale CSV onboarded exactly 103 and logged exactly 27 exclusions, zero leakage into `migration_control`. |
| 1.2 | 2026-09-09 | Data Platform Engineering | Added §3.1 — mandatory pre-flight check that `source_catalog`/`source_schema`/`source_table` are already Delta Shared and mounted (or same-metastore visible) from the **target** workspace before Step 1, with the source-side (`CREATE SHARE`/`ADD SCHEMA`/`CREATE RECIPIENT`/`GRANT`) and target-side (`CREATE CATALOG ... USING SHARE`) setup commands for a true cross-metastore migration, plus a verification query (`SHOW SCHEMAS`/`SHOW TABLES`/`DESCRIBE DETAIL`). Cross-referenced from G2 as the #1 real-world cause of "0 tables resolved". |
| 1.1 | 2026-09-09 | Data Platform Engineering | Added CSV **Format B** (row-level `catalog`/`schema`/`table` selection with `exclude_schemas`/`exclude_tables`, `InputResolver._expand_mapping_entry()`), documented `batch_id` auto-generation + cross-task propagation fix, `force_reonboard` parameter, and the `run_id`→`batch_id` run-summary rescoping fix (G2/G6/G7). Format B verified end-to-end on a new `ril_bulk_csvtest` test catalog (schemas `finance`/`hr`, 10 tables) → `ril_tgt_02`, all 3 row types + both exclusion columns in one file, batch `batch-csvformat-test-01`. |
| 1.0 | 2026-09-09 | Data Platform Engineering | Initial CSV-run SOP, with screenshots from the verified end-to-end run on `ril_bulk_02` → `ril_tgt_02` (`clone_type=delta_share`) |

---

*For questions or exceptions to this SOP, contact the Data Platform Engineering team via your internal ticketing system.*
