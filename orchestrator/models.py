"""
models.py — Core data models, enums, and state machine definitions.

State machine
─────────────
DISCOVERED → ONBOARDED → WAITING_FOR_LOAD → QUEUED → ASSIGNED
→ IN_PROGRESS → COMPLETED | FAILED → RETRY_PENDING → QUEUED

Validation transitions
──────────────────────
COMPLETED → VALIDATED | VALIDATION_FAILED → RETRY_PENDING
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List
from datetime import datetime


# ── Enumerations ──────────────────────────────────────────────────────────────

class OperatingMode(str, Enum):
    INVENTORY  = "INVENTORY"    # Discover, classify, onboard
    DEEP_CLONE = "DEEP_CLONE"   # Execute clone operations
    VALIDATE   = "VALIDATE"     # Post-clone verification
    RETRY      = "RETRY"        # Requeue eligible failures
    DRY_RUN    = "DRY_RUN"      # Show plan without executing


class CloneType(str, Enum):
    DELTA_SHARE = "delta_share"    # DEEP CLONE via shared catalog FQN
    DIRECT_ADLS = "direct_adls"   # DEEP CLONE via abfss:// path


class SelectionType(str, Enum):
    CATALOG = "catalog"
    SCHEMA  = "schema"
    TABLE   = "table"


class InputType(str, Enum):
    JOB  = "JOB"    # Databricks job widget parameters
    YAML = "YAML"   # YAML configuration file
    CSV  = "CSV"    # Explicit source→target CSV mapping


class WorkloadClass(str, Enum):
    SMALL   = "SMALL"    # < 10 GB  → weight 1
    MEDIUM  = "MEDIUM"   # 10–100 GB → weight 3
    LARGE   = "LARGE"    # 100 GB–1 TB → weight 8
    XLARGE  = "XLARGE"   # > 1 TB  → weight 20
    UNKNOWN = "UNKNOWN"  # size never measured (skip_describe_detail=true) → weight 0


class MigrationStatus(str, Enum):
    # Control-plane states
    DISCOVERED        = "DISCOVERED"
    ONBOARDED         = "ONBOARDED"
    WAITING_FOR_LOAD  = "WAITING_FOR_LOAD"
    QUEUED            = "QUEUED"
    # Execution states
    ASSIGNED          = "ASSIGNED"
    IN_PROGRESS       = "IN_PROGRESS"
    # Terminal / hold states
    COMPLETED         = "COMPLETED"
    FAILED            = "FAILED"
    FAILED_PERMANENT  = "FAILED_PERMANENT"   # exceeded max_attempts or unrecoverable
    RETRY_PENDING     = "RETRY_PENDING"
    SKIPPED           = "SKIPPED"            # non-Delta / view, excluded by pattern
    # Validation states
    VALIDATED         = "VALIDATED"
    VALIDATION_FAILED = "VALIDATION_FAILED"


class ChunkStatus(str, Enum):
    """Lifecycle of a single chunk (group of tables sharing one cluster job)."""
    PENDING    = "PENDING"     # Assigned by batch planner, not yet dispatched
    RUNNING    = "RUNNING"     # Cluster job submitted, tables copying
    COMPLETED  = "COMPLETED"   # All tables in chunk finished (success or final failure)
    FAILED     = "FAILED"      # Chunk cluster job itself crashed


class ValidationStatus(str, Enum):
    PENDING         = "PENDING"
    VALIDATED       = "VALIDATED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    SKIPPED         = "SKIPPED"


class AttemptStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED  = "FAILED"


# ── Allowed state transitions ─────────────────────────────────────────────────

VALID_TRANSITIONS: dict[MigrationStatus, set[MigrationStatus]] = {
    MigrationStatus.DISCOVERED:       {MigrationStatus.ONBOARDED, MigrationStatus.FAILED_PERMANENT, MigrationStatus.SKIPPED},
    MigrationStatus.ONBOARDED:        {MigrationStatus.WAITING_FOR_LOAD},
    MigrationStatus.WAITING_FOR_LOAD: {MigrationStatus.QUEUED},
    MigrationStatus.QUEUED:           {MigrationStatus.ASSIGNED, MigrationStatus.SKIPPED},
    MigrationStatus.ASSIGNED:         {MigrationStatus.IN_PROGRESS, MigrationStatus.QUEUED},  # QUEUED on cluster eviction
    MigrationStatus.IN_PROGRESS:      {MigrationStatus.COMPLETED, MigrationStatus.FAILED},
    MigrationStatus.COMPLETED:        {MigrationStatus.VALIDATED, MigrationStatus.VALIDATION_FAILED},
    MigrationStatus.VALIDATED:        set(),                                   # terminal
    MigrationStatus.FAILED:           {MigrationStatus.RETRY_PENDING, MigrationStatus.FAILED_PERMANENT},
    MigrationStatus.RETRY_PENDING:    {MigrationStatus.QUEUED},
    MigrationStatus.VALIDATION_FAILED:{MigrationStatus.RETRY_PENDING, MigrationStatus.FAILED_PERMANENT},
    # FAILED_PERMANENT is terminal by DEFAULT (see TERMINAL_STATES below) — the
    # normal RETRY path never leaves it. The only edges out exist for the
    # OPT-IN retry_permanent=true reset (AuditManager.reset_permanent_failures),
    # which re-drives permanently-failed rows back into the retry pipeline.
    MigrationStatus.FAILED_PERMANENT: {MigrationStatus.RETRY_PENDING, MigrationStatus.QUEUED},
    MigrationStatus.SKIPPED:          set(),                                   # terminal
}

TERMINAL_STATES = {
    MigrationStatus.VALIDATED,
    MigrationStatus.FAILED_PERMANENT,
    MigrationStatus.SKIPPED,
}

RETRYABLE_STATES = {
    MigrationStatus.FAILED,
    MigrationStatus.RETRY_PENDING,
    MigrationStatus.VALIDATION_FAILED,
}


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class TableSelection:
    """Normalised single source→target table mapping produced by InputResolver."""
    source_catalog: str
    source_schema:  str
    source_table:   str
    target_catalog: str
    target_schema:  str
    target_table:   str

    @property
    def source_fqn(self) -> str:
        return f"{self.source_catalog}.{self.source_schema}.{self.source_table}"

    @property
    def target_fqn(self) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"


@dataclass
class TableInventory:
    """
    Metadata captured during INVENTORY phase (DESCRIBE DETAIL output).

    All fields except is_delta are Optional and default to None — when
    OrchestratorConfig.skip_describe_detail=True, DESCRIBE DETAIL is never
    executed and a bare TableInventory() (all None) is used instead, so
    migration_control ends up with NULL size/file/version metadata for that
    table rather than misleading zeros.
    """
    source_path:    Optional[str] = None   # abfss:// location
    size_in_bytes:  Optional[int] = None
    size_gb:        Optional[float] = None
    num_files:      Optional[int] = None
    format:         Optional[str] = "DELTA"   # Confirm this is a Delta table
    source_version: Optional[int] = None
    created_at:     Optional[str] = None
    last_modified:  Optional[str] = None
    is_delta:       bool = True


@dataclass
class ChunkAssignment:
    """
    One logical chunk produced by the batch planner.
    A chunk = a group of tables that will share ONE ephemeral cluster job.
    """
    chunk_id:      int
    batch_id:      str
    migration_ids: List[str]           = field(default_factory=list)
    total_gb:      float               = 0.0
    status:        str                 = ChunkStatus.PENDING.value

    @property
    def size(self) -> int:
        return len(self.migration_ids)


@dataclass
class MigrationRecord:
    """
    Represents one row in migration_control.
    Source of truth for a single table's migration lifecycle.

    Batch/chunk model (Section 8 redesign):
      batch_id  — user-supplied isolation key (e.g. "vivek-q3-migration").
                  Multiple parties can run independent batches simultaneously.
      chunk_id  — set by BatchPlanner. Tables with the same chunk_id share
                  one ephemeral cluster job. Grouping is by workload size
                  (greedy bin-packing) so large tables get their own chunk.
    """
    migration_id:      str
    run_id:            str
    clone_type:        str
    source_workspace:  str
    target_workspace:  str

    # Source + target identity
    source_catalog:    str
    source_schema:     str
    source_table:      str
    target_catalog:    str
    target_schema:     str
    target_table:      str

    # Batch + chunk assignment (set during INVENTORY → PLAN)
    batch_id:          str   = ""    # isolation key per migration owner
    chunk_id:          int   = 0     # which chunk within the batch (0 = unassigned)

    # Inventory metadata. size_in_bytes/size_gb/workload_class/workload_weight
    # are all None when skip_describe_detail=True was used at onboarding time
    # (see OrchestratorConfig.skip_describe_detail) — persisted as SQL NULL,
    # not 0/"SMALL", so it's unambiguous in migration_control that these were
    # never measured rather than measured-and-zero.
    source_path:       Optional[str] = None
    size_in_bytes:     Optional[int]   = None
    size_gb:           Optional[float] = None
    workload_class:    Optional[str]   = WorkloadClass.SMALL.value
    workload_weight:   Optional[int]   = 1

    # Scheduling
    assigned_cluster_id: Optional[str] = None

    # State
    status:            str = MigrationStatus.DISCOVERED.value
    attempt_number:    int = 0
    max_attempts:      int = 3

    # Timestamps
    discovered_at:     Optional[str] = None
    onboarded_at:      Optional[str] = None
    queued_at:         Optional[str] = None
    started_at:        Optional[str] = None
    completed_at:      Optional[str] = None
    failed_at:         Optional[str] = None
    duration_seconds:  Optional[int] = None

    # Error details
    error_code:        Optional[str] = None
    error_message:     Optional[str] = None

    # Validation metrics
    source_num_files:  Optional[int] = None
    target_num_files:  Optional[int] = None
    source_version:    Optional[int] = None
    target_version:    Optional[int] = None
    validation_status: Optional[str] = None
    validation_message: Optional[str] = None

    # Audit
    created_at:        Optional[str] = None
    updated_at:        Optional[str] = None

    @property
    def source_fqn(self) -> str:
        return f"{self.source_catalog}.{self.source_schema}.{self.source_table}"

    @property
    def target_fqn(self) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

    def can_transition_to(self, next_status: MigrationStatus) -> bool:
        current = MigrationStatus(self.status)
        return next_status in VALID_TRANSITIONS.get(current, set())


@dataclass
class MigrationAttempt:
    """One row in migration_attempts — preserves full execution history."""
    run_id:           str
    migration_id:     str
    attempt_number:   int
    cluster_id:       str
    worker_id:        str
    started_at:       str
    completed_at:     Optional[str]  = None
    status:           str            = AttemptStatus.FAILED.value
    error_code:       Optional[str]  = None
    error_message:    Optional[str]  = None
    duration_seconds: Optional[int]  = None
    source_size_bytes: int           = 0
    target_size_bytes: int           = 0
    created_at:       Optional[str]  = None


@dataclass
class ClusterCapacity:
    """Runtime capacity state for one cluster in the pool."""
    cluster_id:         str
    total_units:        int
    used_units:         int = 0
    available:          bool = True   # False when cluster is down/unavailable
    in_flight_tables:   List[str] = field(default_factory=list)  # migration_ids

    @property
    def free_units(self) -> int:
        return max(0, self.total_units - self.used_units)

    def can_fit(self, weight: int) -> bool:
        return self.available and self.free_units >= weight


@dataclass
class ValidationResult:
    """Result of one validation check."""
    check_name:     str
    passed:         bool
    message:        str
    source_value:   Optional[str] = None
    target_value:   Optional[str] = None

    def to_line(self) -> str:
        icon = "✓" if self.passed else "✗"
        return f"{icon} {self.check_name}: {self.message}"


@dataclass
class RunSummary:
    """Aggregated metrics emitted at the end of any operating mode."""
    mode:          str
    run_id:        str
    started_at:    str
    ended_at:      str
    clone_type:    str = ""
    input_type:    str = ""

    # Row-count validation aggregates (only meaningful when mode=VALIDATE and
    # row_count_validation was enabled; all remain 0 otherwise).
    row_count_checked:    int = 0
    row_count_matched:    int = 0
    row_count_mismatched: int = 0

    total:         int = 0
    onboarded:     int = 0
    queued:        int = 0
    completed:     int = 0
    failed:        int = 0
    skipped:       int = 0
    validated:     int = 0
    val_failed:    int = 0
    retried:       int = 0

    total_bytes:   int = 0
    bytes_done:    int = 0

    errors: List[str] = field(default_factory=list)

    def print_report(self):
        print(f"\n{'═'*68}")
        print(f"  Migration Orchestrator — Run Summary")
        print(f"  Mode     : {self.mode}")
        print(f"  Run ID   : {self.run_id}")
        print(f"  Clone type (effective): {self.clone_type}   "
              f"Input type (effective): {self.input_type}")
        print(f"  Duration : {self.started_at} → {self.ended_at}")
        print(f"{'─'*68}")
        print(f"  Total discovered : {self.total:>6}")
        print(f"  Onboarded        : {self.onboarded:>6}")
        print(f"  Queued           : {self.queued:>6}")
        print(f"  Completed        : {self.completed:>6}")
        print(f"  Failed           : {self.failed:>6}")
        print(f"  Skipped          : {self.skipped:>6}")
        print(f"  Validated        : {self.validated:>6}")
        print(f"  Validation failed: {self.val_failed:>6}")
        print(f"  Retried          : {self.retried:>6}")
        if self.row_count_checked:
            print(f"  Row count checked: {self.row_count_checked:>6}  "
                  f"(matched={self.row_count_matched}, mismatched={self.row_count_mismatched})")
        print(f"  Data migrated    : {self.bytes_done / (1024**3):>8.3f} GB"
              f" / {self.total_bytes / (1024**3):.3f} GB")
        if self.errors:
            print(f"{'─'*68}")
            print(f"  Errors ({len(self.errors)}):")
            for e in self.errors[:10]:
                print(f"    • {e[:90]}")
        print(f"{'═'*68}\n")
