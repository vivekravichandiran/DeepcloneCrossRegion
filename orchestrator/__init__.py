"""
Delta Table Migration Orchestrator
===================================
A modular, restartable, auditable framework for migrating Delta tables
between Azure Databricks workspaces using DEEP CLONE.

Design document: Delta_Table_Migration_Orchestrator_Design_Document.pdf

Modules
-------
models            — Enums, dataclasses, state machine
config            — Configuration loading (defaults / YAML / env)
sql_client        — SQL Statement Execution API with transparent token refresh
api_client        — Databricks REST API (clusters, jobs, Unity Catalog)
parameter_parser  — Job widget / parameter parsing and precedence
input_resolver    — Normalise JOB / YAML / CSV inputs to a unified selection
table_discovery   — UC Tables API enumeration with full pagination
inventory_manager — DESCRIBE DETAIL + idempotent control-table upsert
workload_classifier — Size → class → weight mapping
cluster_pool      — In-memory capacity accounting across the logical cluster pool
scheduler         — QUEUED → ASSIGNED, work-conserving, atomic transitions
audit_manager     — State machine transitions + migration_attempts writes
clone_worker      — Submit worker notebook to target cluster, poll result
validator         — Post-clone source/target comparison
retry_manager     — Requeue eligible FAILED / VALIDATION_FAILED records
"""

__version__ = "1.9.0"
