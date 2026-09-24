# Databricks notebook source
# MAGIC %md
# MAGIC # Chunk Worker Notebook
# MAGIC Executes all tables assigned to a single **chunk** in parallel threads.
# MAGIC
# MAGIC ## Design
# MAGIC - One cluster job is created per chunk (by `BatchChunkScheduler`)
# MAGIC - This cluster has `min_executors_per_chunk` Spark worker nodes (≥ 8)
# MAGIC - Tables within the chunk run via `ThreadPoolExecutor(max_workers=parallel_threads)`
# MAGIC - Each thread executes `spark.sql("CREATE OR REPLACE TABLE ... DEEP CLONE ...")`
# MAGIC - State updates are atomic (WHERE migration_id = '...' AND status = '...')
# MAGIC
# MAGIC ## Widgets
# MAGIC | Widget | Description |
# MAGIC |---|---|
# MAGIC | `batch_id` | Batch isolation key |
# MAGIC | `chunk_id` | Integer chunk index within the batch |
# MAGIC | `meta_catalog` | Control table catalog |
# MAGIC | `meta_schema` | Control table schema |
# MAGIC | `parallel_threads` | Threads per chunk (default 4) |

# COMMAND ----------

try:
    dbutils.widgets.text("batch_id",        "")
    dbutils.widgets.text("chunk_id",        "0")
    dbutils.widgets.text("meta_catalog",    "hive_metastore")
    dbutils.widgets.text("meta_schema",     "migration_meta")
    dbutils.widgets.text("parallel_threads","4")
except Exception:
    pass

# COMMAND ----------

import sys, os, time, logging, uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Path setup: try DBFS first (DAB deployment), then workspace path
for _p in [
    "/dbfs/deepclone_orchestrator",
    os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else "",
    "/Workspace/Users/vivek.ravichandiran@databricks.com/DeepcloneCrossRegion",
]:
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt= "%H:%M:%S",
)
log = logging.getLogger("chunk_worker")

# COMMAND ----------

# ── Read parameters ────────────────────────────────────────────────────────────
def _wg(name, default=""):
    try:    return dbutils.widgets.get(name) or default
    except: return os.environ.get(name, default)

BATCH_ID        = _wg("batch_id")
CHUNK_ID        = int(_wg("chunk_id", "0"))
META_CATALOG    = _wg("meta_catalog", "hive_metastore")
META_SCHEMA     = _wg("meta_schema",  "migration_meta")
PARALLEL_THREADS= int(_wg("parallel_threads", "4"))

if not BATCH_ID:
    raise ValueError("batch_id widget is required")
if CHUNK_ID == 0:
    raise ValueError("chunk_id must be a positive integer")

CTRL_TABLE     = f"{META_CATALOG}.{META_SCHEMA}.migration_control"
ATTEMPTS_TABLE = f"{META_CATALOG}.{META_SCHEMA}.migration_attempts"

log.info("="*60)
log.info("Chunk Worker | batch=%s  chunk=%d  threads=%d", BATCH_ID, CHUNK_ID, PARALLEL_THREADS)
log.info("Control tables: %s", CTRL_TABLE)
log.info("="*60)

# COMMAND ----------

# ── Fetch all tables for this chunk ───────────────────────────────────────────
safe_bid = BATCH_ID.replace("'", "\\'")
records_df = spark.sql(f"""
    SELECT *
    FROM {CTRL_TABLE}
    WHERE batch_id = '{safe_bid}'
      AND chunk_id  = {CHUNK_ID}
      AND status IN ('QUEUED', 'ASSIGNED', 'IN_PROGRESS')
""")

records = [row.asDict() for row in records_df.collect()]

log.info("Chunk %d: %d tables to process", CHUNK_ID, len(records))
if not records:
    log.warning("No QUEUED/ASSIGNED records for batch=%s chunk=%d — exiting", BATCH_ID, CHUNK_ID)
    dbutils.notebook.exit(f"SKIPPED:batch={BATCH_ID},chunk={CHUNK_ID},no_records")

# ── Mark all as ASSIGNED (batch) ───────────────────────────────────────────────
now_ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")

spark.sql(f"""
    UPDATE {CTRL_TABLE}
    SET status              = 'ASSIGNED',
        assigned_cluster_id = '{cluster_id}',
        updated_at          = TIMESTAMP '{now_ts}'
    WHERE batch_id = '{safe_bid}'
      AND chunk_id  = {CHUNK_ID}
      AND status    = 'QUEUED'
""")
log.info("Marked %d records ASSIGNED on cluster %s", len(records), cluster_id)

# COMMAND ----------

# ── Per-table clone function (runs in each thread) ────────────────────────────
def clone_table(rec: dict) -> dict:
    """
    Execute a single Deep Clone. Returns a result dict with success/error info.
    Each thread uses its own Spark session context (safe in Databricks).
    """
    mid        = rec["migration_id"]
    run_id     = rec.get("run_id", "")
    clone_type = rec.get("clone_type", "direct_adls")
    attempt    = int(rec.get("attempt_number") or 0) + 1
    max_att    = int(rec.get("max_attempts") or 3)

    src_cat  = rec["source_catalog"]
    src_sch  = rec["source_schema"]
    src_tbl  = rec["source_table"]
    tgt_cat  = rec["target_catalog"]
    tgt_sch  = rec["target_schema"]
    tgt_tbl  = rec["target_table"]
    src_path = rec.get("source_path") or ""

    src_fqn = f"`{src_cat}`.`{src_sch}`.`{src_tbl}`"
    tgt_fqn = f"`{tgt_cat}`.`{tgt_sch}`.`{tgt_tbl}`"

    log.info("[Thread] START %s → %s (attempt %d)", f"{src_cat}.{src_sch}.{src_tbl}", tgt_fqn, attempt)

    # Build clone SQL
    # TGT_EXT_LOC: optional env var pointing to the target workspace external
    # location root (e.g. abfss://unity-catalog-storage@acct.dfs.core.windows.net/ws_id).
    # When set, direct_adls clones include an explicit LOCATION on the target ADLS
    # to avoid LOCATION_OVERLAP when the source path is registered as an external
    # location in the shared Unity Catalog metastore.
    _tgt_ext_loc = os.environ.get("TGT_EXT_LOC", "").rstrip("/")

    if clone_type == "delta_share":
        # Delta Share / UC FQN mode
        clone_sql = f"CREATE OR REPLACE TABLE {tgt_fqn} DEEP CLONE {src_fqn}"
    else:
        # Direct ADLS path mode
        if src_path and src_path.startswith("abfss://"):
            if _tgt_ext_loc:
                # Explicit target LOCATION → avoids LOCATION_OVERLAP with shared
                # metastore external locations on the source workspace's storage.
                tgt_path = f"{_tgt_ext_loc}/migration_data/{tgt_sch}/{tgt_tbl}"
                clone_sql = (
                    f"CREATE OR REPLACE TABLE {tgt_fqn} "
                    f"DEEP CLONE delta.`{src_path}` "
                    f"LOCATION '{tgt_path}'"
                )
            else:
                clone_sql = f"CREATE OR REPLACE TABLE {tgt_fqn} DEEP CLONE delta.`{src_path}`"
        else:
            # Same-metastore fallback (no ADLS path available)
            clone_sql = f"CREATE OR REPLACE TABLE {tgt_fqn} DEEP CLONE {src_fqn}"

    started_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()

    # Mark IN_PROGRESS
    try:
        spark.sql(f"""
            UPDATE {CTRL_TABLE}
            SET status         = 'IN_PROGRESS',
                attempt_number = {attempt},
                started_at     = TIMESTAMP '{started_at}',
                updated_at     = TIMESTAMP '{started_at}'
            WHERE migration_id = '{mid}'
              AND status IN ('ASSIGNED', 'QUEUED', 'IN_PROGRESS')
        """)
    except Exception as e:
        log.warning("[Thread] Could not mark IN_PROGRESS for %s: %s", mid[:8], e)

    # Ensure target schema
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{tgt_cat}`.`{tgt_sch}`")
    except Exception as e:
        log.warning("[Thread] Could not create schema %s.%s: %s", tgt_cat, tgt_sch, e)

    # ── Capture the ACTUAL source version being cloned (at CLONE time) ────────
    # DEEP CLONE reads the source's LIVE/current version, which may be NEWER
    # than the version recorded at INVENTORY time — the source table can receive
    # writes during the (potentially long) QUEUED gap between inventory and this
    # clone. We read it here as a fallback and, on success below, prefer the
    # exact sourceVersion the CLONE operation itself recorded. This value is
    # persisted back to migration_control.source_version on COMPLETED so VALIDATE
    # counts the source AS OF the version that was truly cloned (not the stale
    # inventory version, which caused false row-count mismatches). Read against
    # the SAME source reference the clone uses.
    if clone_type != "delta_share" and src_path and src_path.startswith("abfss://"):
        _src_hist_ref = f"delta.`{src_path}`"
    else:
        _src_hist_ref = src_fqn
    cloned_source_version = None
    try:
        _sv_rows = spark.sql(f"DESCRIBE HISTORY {_src_hist_ref} LIMIT 1").collect()
        if _sv_rows:
            cloned_source_version = _sv_rows[0].asDict().get("version")
    except Exception as e:
        log.warning("[Thread] Could not read pre-clone source version for %s: %s", mid[:8], e)

    # Execute clone
    success = False
    error_code = None
    error_msg  = None
    target_num_files  = None
    target_version    = None
    target_size_bytes = 0

    try:
        log.info("[Thread] Executing: %s", clone_sql.strip())
        spark.sql(clone_sql)
        success    = True
        duration_s = int(time.time() - t0)
        log.info("[Thread] DONE %s.%s.%s in %ds", src_cat, src_sch, src_tbl, duration_s)
    except Exception as e:
        duration_s = int(time.time() - t0)
        err_str = str(e)
        error_msg = err_str[:500]
        if   "TABLE_OR_VIEW_NOT_FOUND" in err_str: error_code = "TABLE_OR_VIEW_NOT_FOUND"
        elif "LOCATION_OVERLAP"        in err_str: error_code = "LOCATION_OVERLAP"
        elif "PERMISSION_DENIED"       in err_str or "PermissionDenied" in err_str: error_code = "PERMISSION_DENIED"
        elif "AnalysisException"       in err_str: error_code = "ANALYSIS_EXCEPTION"
        else:                                       error_code = "CLONE_FAILED"
        log.error("[Thread] FAILED %s.%s.%s [%s]: %s", src_cat, src_sch, src_tbl, error_code, error_msg[:150])

    completed_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Capture post-clone metrics
    if success:
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {tgt_fqn}").collect()[0].asDict()
            target_num_files  = detail.get("numFiles")
            target_size_bytes = int(detail.get("sizeInBytes") or 0)
            hist = spark.sql(f"DESCRIBE HISTORY {tgt_fqn} LIMIT 1").collect()
            if hist:
                h0 = hist[0].asDict()
                target_version = h0.get("version")
                # Prefer the EXACT source version the CLONE operation recorded
                # (race-free) over the pre-clone DESCRIBE HISTORY read above. A
                # DEEP CLONE commit exposes it in operationParameters.sourceVersion.
                op_params = h0.get("operationParameters") or {}
                sv = op_params.get("sourceVersion") if hasattr(op_params, "get") else None
                if sv is not None:
                    try:
                        cloned_source_version = int(sv)
                    except (TypeError, ValueError):
                        pass
        except Exception as e:
            log.warning("[Thread] Could not fetch post-clone metrics: %s", e)

    # Write migration_control update
    PERM_ERRORS = {"TABLE_OR_VIEW_NOT_FOUND", "LOCATION_OVERLAP", "PERMISSION_DENIED", "ANALYSIS_EXCEPTION"}
    if success:
        tnf = target_num_files if target_num_files is not None else "NULL"
        tv  = target_version   if target_version   is not None else "NULL"
        # Overwrite the stale inventory-time source_version with the version
        # actually cloned. Only emit the column when we captured a value, so a
        # capture failure never clobbers the existing inventory value with NULL.
        sv_set = (
            f"source_version = {int(cloned_source_version)},\n                    "
            if cloned_source_version is not None else ""
        )
        try:
            spark.sql(f"""
                UPDATE {CTRL_TABLE}
                SET status           = 'COMPLETED',
                    completed_at     = TIMESTAMP '{completed_at}',
                    duration_seconds = {duration_s},
                    target_num_files = {tnf},
                    target_version   = {tv},
                    {sv_set}error_code       = NULL,
                    error_message    = NULL,
                    updated_at       = TIMESTAMP '{completed_at}'
                WHERE migration_id = '{mid}'
                  AND status       = 'IN_PROGRESS'
            """)
        except Exception as e:
            log.error("[Thread] Could not mark COMPLETED for %s: %s", mid[:8], e)
    else:
        is_perm    = error_code in PERM_ERRORS
        next_status = "FAILED_PERMANENT" if is_perm else (
            "RETRY_PENDING" if attempt < max_att else "FAILED"
        )
        err_esc  = (error_msg  or "").replace("'", "\\'")[:500]
        err_code = (error_code or "CLONE_FAILED").replace("'", "\\'")[:50]
        try:
            spark.sql(f"""
                UPDATE {CTRL_TABLE}
                SET status        = '{next_status}',
                    failed_at     = TIMESTAMP '{completed_at}',
                    duration_seconds = {duration_s},
                    error_code    = '{err_code}',
                    error_message = '{err_esc}',
                    updated_at    = TIMESTAMP '{completed_at}'
                WHERE migration_id = '{mid}'
                  AND status       = 'IN_PROGRESS'
            """)
        except Exception as e:
            log.error("[Thread] Could not mark %s for %s: %s", next_status, mid[:8], e)

    # Write migration_attempts (immutable history)
    worker_id     = str(uuid.uuid4())[:8]
    src_size      = int(rec.get("size_in_bytes") or 0)
    err_code_sql  = f"'{error_code}'" if error_code else "NULL"
    err_msg_sql   = f"'{(error_msg or '').replace(chr(39), chr(39)*2)[:500]}'" if error_msg else "NULL"
    try:
        spark.sql(f"""
            INSERT INTO {ATTEMPTS_TABLE} VALUES (
              '{run_id}', '{mid}', {attempt},
              '{cluster_id}', '{worker_id}',
              TIMESTAMP '{started_at}',
              TIMESTAMP '{completed_at}',
              '{'SUCCESS' if success else 'FAILED'}',
              {err_code_sql},
              {err_msg_sql},
              {duration_s},
              {src_size},
              {target_size_bytes},
              TIMESTAMP '{completed_at}'
            )
        """)
    except Exception as e:
        log.warning("[Thread] Could not write attempt record for %s: %s", mid[:8], e)

    return {
        "migration_id": mid,
        "success":      success,
        "duration_s":   duration_s,
        "error_code":   error_code,
    }

# COMMAND ----------

# ── Parallel execution with ThreadPoolExecutor ────────────────────────────────
log.info("Starting parallel clone — %d tables, %d threads", len(records), PARALLEL_THREADS)

results    = []
ok_count   = 0
fail_count = 0

with ThreadPoolExecutor(max_workers=PARALLEL_THREADS, thread_name_prefix="dc") as executor:
    futures = {executor.submit(clone_table, rec): rec for rec in records}
    for future in as_completed(futures):
        try:
            res = future.result()
            results.append(res)
            if res["success"]:
                ok_count += 1
            else:
                fail_count += 1
        except Exception as exc:
            rec = futures[future]
            log.error("Unexpected exception for %s: %s", rec.get("migration_id","?")[:8], exc)
            fail_count += 1

# COMMAND ----------

# ── Summary ───────────────────────────────────────────────────────────────────
log.info("="*60)
log.info("Chunk %d — batch=%s — COMPLETE", CHUNK_ID, BATCH_ID)
log.info("  Tables processed : %d", len(records))
log.info("  Succeeded        : %d", ok_count)
log.info("  Failed           : %d", fail_count)
log.info("="*60)

for r in results:
    icon = "✓" if r["success"] else "✗"
    log.info("  %s %s [%ds]%s", icon, r["migration_id"][:8], r["duration_s"],
             f" [{r['error_code']}]" if not r["success"] else "")

exit_msg = f"batch={BATCH_ID},chunk={CHUNK_ID},ok={ok_count},fail={fail_count}"
if fail_count > 0:
    raise Exception(f"PARTIAL_FAILURE:{exit_msg}")

dbutils.notebook.exit(f"SUCCESS:{exit_msg}")
