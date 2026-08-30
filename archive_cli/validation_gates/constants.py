"""Validation ladder gate constants and exit codes."""

from __future__ import annotations

GATE_FRAMEWORK_STATE = "validation_gates_complete"
GATE_FRAMEWORK_COMPLETION_STATE = "validation_gate_framework_complete"
SECTION_F_COMPLETION_STATE = "section_f_observability_v3_gate_complete"

VALIDATION_GATE_LOG_ROOT = "validation-gates"

PRODUCTION_INSTANCE_ROLE = "production"

EXIT_SUCCESS = 0
EXIT_RUNTIME_FAILURE = 1
EXIT_VALIDATION_FAILED = 2
EXIT_REFUSED = 3
EXIT_BLOCKED = 4

GATE_SYNTHETIC_FIXTURES = "synthetic_fixtures"
GATE_SMALL_SLICE = "small_slice"
GATE_LARGER_SLICE = "larger_slice"
GATE_LOCAL_SEED_DRY_RUN = "local_seed_dry_run"
GATE_LOCAL_SEED_STAGING_APPLY = "local_seed_staging_apply"
GATE_PRODUCTION_DRY_RUN = "production_dry_run"
GATE_PRODUCTION_REVIEWED_APPLY = "production_reviewed_apply"
GATE_PRODUCTION_SOAK = "production_soak"

LADDER_GATES: tuple[str, ...] = (
    GATE_SYNTHETIC_FIXTURES,
    GATE_SMALL_SLICE,
    GATE_LARGER_SLICE,
    GATE_LOCAL_SEED_DRY_RUN,
    GATE_LOCAL_SEED_STAGING_APPLY,
    GATE_PRODUCTION_DRY_RUN,
    GATE_PRODUCTION_REVIEWED_APPLY,
    GATE_PRODUCTION_SOAK,
)

GATES_REQUIRED_BEFORE_PRODUCTION_APPLY: tuple[str, ...] = (
    GATE_SYNTHETIC_FIXTURES,
    GATE_SMALL_SLICE,
    GATE_LARGER_SLICE,
    GATE_LOCAL_SEED_DRY_RUN,
    GATE_LOCAL_SEED_STAGING_APPLY,
    GATE_PRODUCTION_DRY_RUN,
)

GATE_RUN_STATUS_PENDING = "pending"
GATE_RUN_STATUS_RUNNING = "running"
GATE_RUN_STATUS_PASSED = "passed"
GATE_RUN_STATUS_FAILED = "failed"
GATE_RUN_STATUS_BLOCKED = "blocked"
GATE_RUN_STATUS_REFUSED = "refused"

META_LAST_GATE_RUN_ID = "validation_gate_last_run_id"
META_LAST_GATE_NAME = "validation_gate_last_name"

EXPENSIVE_WORK_FLAGS: tuple[str, ...] = (
    "full_reclassification",
    "full_embedding_regeneration",
    "all_linker_rerun",
)
