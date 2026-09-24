#!/usr/bin/env python3
"""
build_migration_dashboard.py — Generate the Delta DeepClone migration telemetry
AI/BI (Lakeview) dashboard as a serialized `.lvdash.json`, plus a ready-to-post
create-request body.

Design: docs/AIBI_Dashboard_Design.md
Data:   ril_migration_catalog.migration_meta.{migration_control,
        migration_attempts, migration_validation_history, migration_exclusion_log}

Usage:
    python3 scripts/build_migration_dashboard.py
Outputs:
    dashboards/migration_telemetry.lvdash.json      (serialized dashboard)
    /tmp/create_dashboard_request.json              (Lakeview create body)
"""
from __future__ import annotations
import json
import os
import uuid

CATALOG = "ril_migration_catalog"
SCHEMA = "migration_meta"
CTRL = f"{CATALOG}.{SCHEMA}.migration_control"
ATT = f"{CATALOG}.{SCHEMA}.migration_attempts"
VH = f"{CATALOG}.{SCHEMA}.migration_validation_history"
EXCL = f"{CATALOG}.{SCHEMA}.migration_exclusion_log"

WAREHOUSE_ID = "5fe1692f119e2528"
PARENT_PATH = "/Workspace/Users/99ba7259-12df-4ab6-a625-547950b4a26c"
DISPLAY_NAME = "Delta DeepClone - Migration Telemetry (Ops)"

DONE = "('COMPLETED','VALIDATED')"
FAILED = "('FAILED','FAILED_PERMANENT','VALIDATION_FAILED')"
ACTIVE = "('ASSIGNED','IN_PROGRESS')"


# ── datasets ──────────────────────────────────────────────────────────────────
def ds(name, sql):
    return {"name": name, "displayName": name,
            "queryLines": [ln + "\n" for ln in sql.strip().splitlines()]}


DATASETS = [
    ds("ds_control", f"""
        SELECT *,
          CASE WHEN source_row_count IS NULL OR target_row_count IS NULL THEN 'NOT CHECKED'
               WHEN source_row_count = target_row_count THEN 'MATCH'
               ELSE 'MISMATCH' END AS row_count_status
        FROM {CTRL}
    """),
    ds("ds_kpis", f"""
        SELECT
          COUNT(*) AS total_tables,
          SUM(CASE WHEN status IN {DONE}   THEN 1 ELSE 0 END) AS done_tables,
          SUM(CASE WHEN status IN {ACTIVE} THEN 1 ELSE 0 END) AS active_tables,
          SUM(CASE WHEN status = 'QUEUED'  THEN 1 ELSE 0 END) AS queued_tables,
          SUM(CASE WHEN status IN {FAILED} THEN 1 ELSE 0 END) AS failed_tables,
          ROUND(100.0 * SUM(CASE WHEN status IN {DONE} THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*),0), 1) AS pct_complete,
          ROUND(COALESCE(SUM(size_gb),0),2) AS total_gb,
          ROUND(COALESCE(SUM(CASE WHEN status IN {DONE} THEN size_gb ELSE 0 END),0),2) AS gb_done
        FROM {CTRL}
    """),
    ds("ds_cumulative", f"""
        WITH h AS (
          SELECT DATE_TRUNC('HOUR', completed_at) AS hour, COUNT(*) AS n
          FROM {CTRL}
          WHERE status IN {DONE} AND completed_at IS NOT NULL
          GROUP BY 1)
        SELECT hour, SUM(n) OVER (ORDER BY hour) AS cumulative_done
        FROM h ORDER BY hour
    """),
    ds("ds_throughput", f"""
        SELECT DATE_TRUNC('HOUR', completed_at) AS hour,
               COUNT(*) AS tables_done,
               ROUND(SUM(size_gb),2) AS gb_done,
               ROUND(AVG(duration_seconds),1) AS avg_duration_s
        FROM {CTRL}
        WHERE status IN {DONE} AND completed_at IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """),
    ds("ds_failcodes", f"""
        SELECT COALESCE(error_code,'(none)') AS error_code, COUNT(*) AS n
        FROM {CTRL} WHERE status IN {FAILED}
        GROUP BY 1 ORDER BY n DESC
    """),
    ds("ds_failures", f"""
        SELECT source_catalog, source_schema, source_table, status, error_code,
               error_message, attempt_number, max_attempts, batch_id, updated_at
        FROM {CTRL} WHERE status IN {FAILED}
        ORDER BY updated_at DESC LIMIT 500
    """),
    ds("ds_attempts_daily", f"""
        SELECT DATE(created_at) AS day, COALESCE(status,'(unknown)') AS status, COUNT(*) AS n
        FROM {ATT} GROUP BY 1,2 ORDER BY 1
    """),
    ds("ds_valhist_daily", f"""
        SELECT DATE(validated_at) AS day, COALESCE(status,'(unknown)') AS status, COUNT(*) AS n
        FROM {VH} GROUP BY 1,2 ORDER BY 1
    """),
    ds("ds_mismatch", f"""
        SELECT source_schema, source_table, target_schema, target_table,
               source_version, target_version, source_row_count, target_row_count,
               validation_status, updated_at
        FROM {CTRL}
        WHERE source_row_count IS NOT NULL AND target_row_count IS NOT NULL
          AND source_row_count <> target_row_count
        ORDER BY updated_at DESC LIMIT 500
    """),
    ds("ds_longest", f"""
        SELECT source_schema, source_table, target_schema, target_table,
               ROUND(size_gb,2) AS size_gb, duration_seconds, batch_id, status
        FROM {CTRL} WHERE duration_seconds IS NOT NULL
        ORDER BY duration_seconds DESC LIMIT 50
    """),
    ds("ds_chunks", f"""
        SELECT batch_id, chunk_id, COUNT(*) AS tables, ROUND(SUM(size_gb),2) AS gb,
               SUM(CASE WHEN status IN {DONE} THEN 1 ELSE 0 END) AS done
        FROM {CTRL} WHERE chunk_id IS NOT NULL
        GROUP BY batch_id, chunk_id ORDER BY batch_id, chunk_id
    """),
    ds("ds_excl", f"""
        SELECT run_id, batch_id, source_catalog, source_schema, source_table,
               exclusion_type, exclusion_rule, excluded_at
        FROM {EXCL} ORDER BY excluded_at DESC LIMIT 1000
    """),
]


# ── widget builders ─────────────────────────────────────────────────────────
def _wid():
    return uuid.uuid4().hex[:8]


def _canon(expr):
    """Canonical AI/BI field name for an expression.

    The Lakeview visualization editor auto-names query fields after their
    expression in lowercase with backticks/whitespace stripped (e.g.
    ``COUNT(`migration_id`)`` -> ``count(migration_id)``, ```status``` ->
    ``status``). The encoding ``fieldName`` must match this exact form or the
    widget renders "Select fields to visualize". We therefore always derive the
    field ``name`` (and matching ``fieldName``) from the expression itself.
    """
    return expr.replace("`", "").replace(" ", "").lower()


def _field(name, expr):
    return {"name": name, "expression": expr}


def _fld(expr):
    """Field whose name is the canonical form of its expression."""
    return {"name": _canon(expr), "expression": expr}


def _layout(widget, x, y, w, h):
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


def counter(dataset, col, title, x, y, w=1, h=3):
    qn = "main_query"
    expr = f"SUM(`{col}`)"
    fn = _canon(expr)
    widget = {
        "name": _wid(),
        "queries": [{"name": qn, "query": {
            "datasetName": dataset,
            "fields": [_fld(expr)],
            "disaggregated": False}}],
        "spec": {
            "version": 2, "widgetType": "counter",
            "encodings": {"value": {"fieldName": fn, "displayName": title}},
            "frame": {"title": title, "showTitle": True}},
    }
    return _layout(widget, x, y, w, h)


def bar(dataset, dim_expr, dim_name, measure_expr, measure_name, title,
        x, y, w, h, color_name=None, color_expr=None, dim_title=None,
        measure_title=None):
    qn = "main_query"
    dim_fn, meas_fn = _canon(dim_expr), _canon(measure_expr)
    fields = [_fld(dim_expr), _fld(measure_expr)]
    enc = {
        "x": {"fieldName": dim_fn, "scale": {"type": "categorical"},
              "displayName": dim_title or dim_name},
        "y": {"fieldName": meas_fn, "scale": {"type": "quantitative"},
              "displayName": measure_title or measure_name},
    }
    if color_name and color_expr:
        color_fn = _canon(color_expr)
        fields.append(_fld(color_expr))
        enc["color"] = {"fieldName": color_fn, "scale": {"type": "categorical"},
                        "displayName": color_name}
    widget = {
        "name": _wid(),
        "queries": [{"name": qn, "query": {
            "datasetName": dataset, "fields": fields, "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "bar", "encodings": enc,
                 "frame": {"title": title, "showTitle": True}},
    }
    return _layout(widget, x, y, w, h)


def line(dataset, x_expr, x_name, y_expr, y_name, title, x, y, w, h,
         color_name=None, color_expr=None, x_title=None, y_title=None,
         temporal=True):
    qn = "main_query"
    x_fn, y_fn = _canon(x_expr), _canon(y_expr)
    fields = [_fld(x_expr), _fld(y_expr)]
    enc = {
        "x": {"fieldName": x_fn,
              "scale": {"type": "temporal" if temporal else "categorical"},
              "displayName": x_title or x_name},
        "y": {"fieldName": y_fn, "scale": {"type": "quantitative"},
              "displayName": y_title or y_name},
    }
    if color_name and color_expr:
        color_fn = _canon(color_expr)
        fields.append(_fld(color_expr))
        enc["color"] = {"fieldName": color_fn, "scale": {"type": "categorical"},
                        "displayName": color_name}
    widget = {
        "name": _wid(),
        "queries": [{"name": qn, "query": {
            "datasetName": dataset, "fields": fields, "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "line", "encodings": enc,
                 "frame": {"title": title, "showTitle": True}},
    }
    return _layout(widget, x, y, w, h)


def pie(dataset, dim_expr, dim_name, measure_expr, measure_name, title, x, y, w, h):
    qn = "main_query"
    dim_fn, meas_fn = _canon(dim_expr), _canon(measure_expr)
    fields = [_fld(dim_expr), _fld(measure_expr)]
    enc = {
        "angle": {"fieldName": meas_fn, "scale": {"type": "quantitative"},
                  "displayName": measure_name},
        "color": {"fieldName": dim_fn, "scale": {"type": "categorical"},
                  "displayName": dim_name},
    }
    widget = {
        "name": _wid(),
        "queries": [{"name": qn, "query": {
            "datasetName": dataset, "fields": fields, "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "pie", "encodings": enc,
                 "frame": {"title": title, "showTitle": True}},
    }
    return _layout(widget, x, y, w, h)


def table(dataset, cols, title, x, y, w, h):
    """cols: list of (col_name, display)."""
    qn = "main_query"
    fields = [_fld(f"`{c}`") for c, _ in cols]
    columns = [{"fieldName": _canon(f"`{c}`"), "displayName": d} for c, d in cols]
    widget = {
        "name": _wid(),
        "queries": [{"name": qn, "query": {
            "datasetName": dataset, "fields": fields, "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "table",
                 "encodings": {"columns": columns},
                 "frame": {"title": title, "showTitle": True}},
    }
    return _layout(widget, x, y, w, h)


def flt_multi(dataset, col, title, x, y, w=2, h=2):
    qn = f"q_{_wid()}"
    widget = {
        "name": _wid(),
        "queries": [{"name": qn, "query": {
            "datasetName": dataset,
            "fields": [_field(col, f"`{col}`"),
                       _field(f"{col}_associativity",
                              "COUNT_IF(`associative_filter_predicate_group`)")],
            "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "filter-multi-select",
                 "encodings": {"fields": [{"fieldName": col, "displayName": title,
                                           "queryName": qn}]},
                 "frame": {"title": title, "showTitle": True}},
    }
    return _layout(widget, x, y, w, h)


# ── pages ─────────────────────────────────────────────────────────────────────
def page(name, display, layout):
    return {"name": name, "displayName": display, "layout": layout}


CNT = "COUNT(`migration_id`)"
GB = "SUM(`size_gb`)"

overview = [
    flt_multi("ds_control", "batch_id", "Batch", 0, 0, 2, 2),
    flt_multi("ds_control", "status", "Status", 2, 0, 2, 2),
    flt_multi("ds_control", "clone_type", "Clone type", 4, 0, 2, 2),
    counter("ds_kpis", "total_tables", "Total tables", 0, 2),
    counter("ds_kpis", "done_tables", "Done", 1, 2),
    counter("ds_kpis", "pct_complete", "% Complete", 2, 2),
    counter("ds_kpis", "active_tables", "Active", 3, 2),
    counter("ds_kpis", "queued_tables", "Queued", 4, 2),
    counter("ds_kpis", "failed_tables", "Failed", 5, 2),
    counter("ds_kpis", "total_gb", "Total GB onboarded", 0, 5, 3),
    counter("ds_kpis", "gb_done", "GB migrated", 3, 5, 3),
    bar("ds_control", "`status`", "status", CNT, "tables",
        "Status distribution", 0, 8, 3, 7, measure_title="Tables"),
    bar("ds_control", "`status`", "status", GB, "gb",
        "GB by status", 3, 8, 3, 7, measure_title="GB"),
    bar("ds_control", "`batch_id`", "batch_id", CNT, "tables",
        "Tables by batch (stacked by status)", 0, 15, 4, 7,
        color_name="status", color_expr="`status`", measure_title="Tables"),
    pie("ds_control", "`clone_type`", "clone_type", CNT, "tables",
        "Clone type split", 4, 15, 2, 7),
]

throughput = [
    line("ds_cumulative", "`hour`", "hour", "SUM(`cumulative_done`)", "cumulative_done",
         "Cumulative tables completed", 0, 0, 6, 7, y_title="Tables (running total)"),
    bar("ds_throughput", "`hour`", "hour", "SUM(`tables_done`)", "tables_done",
        "Tables completed per hour", 0, 7, 3, 6),
    bar("ds_throughput", "`hour`", "hour", "SUM(`gb_done`)", "gb_done",
        "GB migrated per hour", 3, 7, 3, 6),
    bar("ds_control", "`workload_class`", "workload_class",
        "AVG(`duration_seconds`)", "avg_duration_s",
        "Avg clone duration by workload class", 0, 13, 3, 6,
        measure_title="Avg seconds"),
    table("ds_longest", [
        ("source_schema", "Src schema"), ("source_table", "Src table"),
        ("size_gb", "GB"), ("duration_seconds", "Duration (s)"),
        ("batch_id", "Batch"), ("status", "Status")],
        "Longest-running clones (top 50)", 3, 13, 3, 8),
]

failures = [
    bar("ds_failcodes", "`error_code`", "error_code", "SUM(`n`)", "n",
        "Failures by error code", 0, 0, 3, 6, measure_title="Count"),
    bar("ds_control", "`attempt_number`", "attempt_number", CNT, "tables",
        "Attempts distribution", 3, 0, 3, 6, measure_title="Tables"),
    bar("ds_attempts_daily", "`day`", "day", "SUM(`n`)", "n",
        "Retry outcomes over time", 0, 6, 6, 6,
        color_name="status", color_expr="`status`", measure_title="Attempts"),
    table("ds_failures", [
        ("source_schema", "Src schema"), ("source_table", "Src table"),
        ("status", "Status"), ("error_code", "Error code"),
        ("error_message", "Error message"), ("attempt_number", "Attempt"),
        ("max_attempts", "Max"), ("batch_id", "Batch"), ("updated_at", "Updated")],
        "Failure detail", 0, 12, 6, 8),
]

validation = [
    pie("ds_control", "`validation_status`", "validation_status", CNT, "tables",
        "Validation status", 0, 0, 2, 6),
    bar("ds_control", "`row_count_status`", "row_count_status", CNT, "tables",
        "Row-count check outcome", 2, 0, 4, 6, measure_title="Tables"),
    line("ds_valhist_daily", "`day`", "day", "SUM(`n`)", "n",
         "Validation attempts over time", 0, 6, 6, 6,
         color_name="status", color_expr="`status`", y_title="Attempts"),
    table("ds_mismatch", [
        ("source_schema", "Src schema"), ("source_table", "Src table"),
        ("source_version", "Src ver"), ("target_version", "Tgt ver"),
        ("source_row_count", "Src rows"), ("target_row_count", "Tgt rows"),
        ("validation_status", "Status"), ("updated_at", "Updated")],
        "Row-count mismatches", 0, 12, 6, 8),
]

inventory = [
    bar("ds_control", "`workload_class`", "workload_class", CNT, "tables",
        "Tables by workload class", 0, 0, 3, 6, measure_title="Tables"),
    bar("ds_control", "`workload_class`", "workload_class", GB, "gb",
        "GB by workload class", 3, 0, 3, 6, measure_title="GB"),
    table("ds_chunks", [
        ("batch_id", "Batch"), ("chunk_id", "Chunk"), ("tables", "Tables"),
        ("gb", "GB"), ("done", "Done")],
        "Chunk fan-out per batch", 0, 6, 6, 7),
    bar("ds_excl", "`exclusion_type`", "exclusion_type", "COUNT(`source_table`)", "n",
        "Exclusions by type", 0, 13, 2, 6, measure_title="Count"),
    table("ds_excl", [
        ("source_catalog", "Catalog"), ("source_schema", "Schema"),
        ("source_table", "Table"), ("exclusion_type", "Type"),
        ("exclusion_rule", "Rule"), ("excluded_at", "Excluded at")],
        "Excluded tables", 2, 13, 4, 7),
]

DASHBOARD = {
    "datasets": DATASETS,
    "pages": [
        page("overview", "Overview", overview),
        page("throughput", "Progress & Throughput", throughput),
        page("failures", "Failures & Retries", failures),
        page("validation", "Validation", validation),
        page("inventory", "Inventory, Chunks & Exclusions", inventory),
    ],
}


def main():
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(repo, "dashboards")
    os.makedirs(out_dir, exist_ok=True)
    lvdash_path = os.path.join(out_dir, "migration_telemetry.lvdash.json")
    serialized = json.dumps(DASHBOARD, indent=2)
    with open(lvdash_path, "w") as f:
        f.write(serialized)

    req = {
        "display_name": DISPLAY_NAME,
        "warehouse_id": WAREHOUSE_ID,
        "parent_path": PARENT_PATH,
        "serialized_dashboard": serialized,
    }
    with open("/tmp/create_dashboard_request.json", "w") as f:
        json.dump(req, f)

    n_widgets = sum(len(p["layout"]) for p in DASHBOARD["pages"])
    print(f"Wrote {lvdash_path}")
    print(f"Datasets: {len(DATASETS)} | Pages: {len(DASHBOARD['pages'])} | Widgets: {n_widgets}")
    print("Create request: /tmp/create_dashboard_request.json")


if __name__ == "__main__":
    main()
