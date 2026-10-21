# Regression Report — Predictive Optimization Inventory Gate

**Live end-to-end + unit regression — Deep Clone Cross-Region Migration Orchestrator**

> Workspace: **vk-west** (`adb-7405612780454429.9.azuredatabricks.net`) • Warehouse `345e7fb63d50405e` • Control tables `po_ctl_t2.migration_meta` • Oct 21, 2026

---

> **Result: PASS.** All 3 live scenarios behaved exactly as designed, and the full unit suite is green
> (**70 passed**, incl. 10 PO-specific tests). The gate was exercised against **real** orchestrator modules
> (`InputResolver`, `PredictiveOptimizationChecker`, `InventoryManager`, `plan_chunks`, Deep Clone,
> row-count validation) hitting the live Unity Catalog REST API.

## 1. Test fixtures (created live in vk-west)

| Object | PO setting | Tables (rows) |
|---|---|---|
| `po_e2e_cat_a` (catalog) | DISABLE | — |
| ↳ `sa_ok` | DISABLE (inherits) | `t_a1`=5, `t_a2`=7 |
| ↳ `sa_on` | **ENABLE (schema)** | `t_sch1`=2, `t_sch2`=3 |
| ↳ `sa_mixed` | DISABLE | `t_m_ok`=4 (DISABLE), `t_m_on`=4 (**ENABLE table**) |
| `po_e2e_cat_b` (catalog) | **ENABLE (catalog)** | — |
| ↳ `sb` | ENABLE (inherits) | `t_b1`=6, `t_b2`=6 |
| `po_e2e_cat_c` (catalog) | DISABLE | — |
| ↳ `sc1` | DISABLE (inherits) | `t_c1a`=8, `t_c1b`=9 |
| `po_ctl_t2.po_e2e_clean` | DISABLE | `t_single`=10 |
| `po_e2e_tgt` (catalog) | target | Deep Clone destination |

## 2. Scenario 1 — all selections qualify for failure

CSV: table row (`sa_mixed.t_m_on`, table-level ENABLE), schema row (`sa_on`, schema-level ENABLE),
catalog row (`po_e2e_cat_b`, catalog-level ENABLE). **Expected:** 5 resolved, all blocked, 0 queued, step fails.

| Source table | Detected level | Outcome |
|---|---|---|
| `sa_mixed.t_m_on` | TABLE | 🔴 PO_BLOCKED |
| `sa_on.t_sch1` | SCHEMA (sa_on) | 🔴 PO_BLOCKED |
| `sa_on.t_sch2` | SCHEMA (sa_on) | 🔴 PO_BLOCKED |
| `sb.t_b1` | CATALOG (po_e2e_cat_b) | 🔴 PO_BLOCKED |
| `sb.t_b2` | CATALOG (po_e2e_cat_b) | 🔴 PO_BLOCKED |

**Observed:** `total=5 inserted(QUEUED)=0 po_blocked=5 failed=0`; inventory step fails. **PASS** —
inheritance level correctly identified as TABLE / SCHEMA / CATALOG in each case.

## 3. Scenario 2 — mixed (qualifying + non-qualifying)

CSV: schema `sa_mixed` (one clean, one table-ENABLE), schema `sa_on` (ENABLE),
catalog `po_e2e_cat_b` (ENABLE), table `po_e2e_clean.t_single` (clean).
**Expected:** clean tables queued, offenders blocked, step still fails.

| Source table | Outcome |
|---|---|
| `sa_mixed.t_m_ok` | 🔵 QUEUED |
| `po_e2e_clean.t_single` | 🔵 QUEUED |
| `sa_mixed.t_m_on` | 🔴 PO_BLOCKED (TABLE) |
| `sa_on.t_sch1` / `sa_on.t_sch2` | 🔴 PO_BLOCKED (SCHEMA) |
| `sb.t_b1` / `sb.t_b2` | 🔴 PO_BLOCKED (CATALOG) |

**Observed:** `total=7 inserted(QUEUED)=2 po_blocked=5 failed=0`; step fails after recording. **PASS** —
confirms the chosen "queue the clean, block offenders, then fail" policy.

## 4. Scenario 3 — all pass → full Deep Clone + validate

CSV: catalog `po_e2e_cat_c`, schema `sa_ok`, table `po_e2e_clean.t_single` (all PO-disabled).
**Expected:** all queued, chunked, Deep Cloned, row counts validated, process completes.

| Source → Target | src rows | tgt rows | Validation |
|---|---|---|---|
| `sc1.t_c1a` → `po_e2e_tgt.sc1.t_c1a` | 8 | 8 | ✅ MATCH |
| `sc1.t_c1b` → `po_e2e_tgt.sc1.t_c1b` | 9 | 9 | ✅ MATCH |
| `sa_ok.t_a1` → `po_e2e_tgt.sa_ok.t_a1` | 5 | 5 | ✅ MATCH |
| `sa_ok.t_a2` → `po_e2e_tgt.sa_ok.t_a2` | 7 | 7 | ✅ MATCH |
| `po_e2e_clean.t_single` → `po_e2e_tgt.po_e2e_clean.t_single` | 10 | 10 | ✅ MATCH |

**Observed:** `total=5 inserted(QUEUED)=5 po_blocked=0` → planned **1 chunk (5 tables)** →
`CREATE OR REPLACE TABLE … DEEP CLONE …` executed → **5/5 row-count matches**, all marked
`COMPLETED` / `VALIDATED`. **PASS** — clean selections flow end-to-end unchanged.

## 5. Unit regression suite

```
$ python3 -m pytest tests/ -q
......................................................................  [100%]
70 passed, 1 warning in 1.93s
```

- **10** PO-specific tests — flag parsing (ENABLE / DISABLE / INHERIT / missing / API-error fail-closed),
  parallel `check_selections`, and `InventoryManager` integration.
- **60** pre-existing orchestrator tests — all still green (no regressions).
- Excluded: 3 modules that require the Databricks `dbutils` runtime and 1 notebook with a
  pre-existing Python-3.9 f-string syntax quirk — unrelated to this change.

## 6. Coverage summary

| Capability | Verified by | Result |
|---|---|---|
| Table-level PO detected | S1/S2 (`t_m_on`) | ✅ PASS |
| Schema-level inheritance detected | S1/S2 (`sa_on`) | ✅ PASS |
| Catalog-level inheritance detected | S1/S2 (`po_e2e_cat_b`) | ✅ PASS |
| Clean tables still queue | S2/S3 | ✅ PASS |
| Offenders recorded as `PO_BLOCKED` + error_code | S1/S2 control-table rows | ✅ PASS |
| Step fails when any offender present | S1/S2 | ✅ PASS |
| Parallelism preserved (no sequential pass) | 4-thread inventory worker | ✅ PASS |
| CSV-driven across catalog/schema/table rows | all 3 scenarios | ✅ PASS |
| Deep Clone + row-count validation on clean set | S3 | ✅ PASS |
| Fail-closed on unreadable PO state | unit test | ✅ PASS |

---

*Regression evidence captured from live runs against vk-west on Oct 21, 2026. Fixtures are isolated under
`po_e2e_*` catalogs and `po_ctl_t2` and can be dropped after review.*
