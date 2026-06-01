"""Validation ladder gate registry, guards, and readiness."""

from __future__ import annotations

from .constants import (
    EXIT_BLOCKED,
    EXIT_REFUSED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_VALIDATION_FAILED,
    GATE_FRAMEWORK_COMPLETION_STATE,
    GATE_FRAMEWORK_STATE,
    GATE_LARGER_SLICE,
    GATE_LOCAL_SEED_DRY_RUN,
    GATE_LOCAL_SEED_STAGING_APPLY,
    GATE_PRODUCTION_DRY_RUN,
    GATE_PRODUCTION_REVIEWED_APPLY,
    GATE_PRODUCTION_SOAK,
    GATE_SMALL_SLICE,
    GATE_SYNTHETIC_FIXTURES,
    GATES_REQUIRED_BEFORE_PRODUCTION_APPLY,
    LADDER_GATES,
    PRODUCTION_INSTANCE_ROLE,
)
from .guards import (
    GateRefusalError,
    guard_expensive_work_opt_in,
    guard_prior_gate_evidence,
    guard_production_apply,
)
from .instance_identity import derive_archive_instance, is_production_instance, resolve_instance_role
from .readiness import ReadinessResult, evaluate_readiness
from .report import GateRunReport, gate_artifact_dir, write_gate_report

__all__ = [
    "EXIT_BLOCKED",
    "EXIT_REFUSED",
    "EXIT_RUNTIME_FAILURE",
    "EXIT_SUCCESS",
    "EXIT_VALIDATION_FAILED",
    "GATE_FRAMEWORK_COMPLETION_STATE",
    "GATE_FRAMEWORK_STATE",
    "GATE_LARGER_SLICE",
    "GATE_LOCAL_SEED_DRY_RUN",
    "GATE_LOCAL_SEED_STAGING_APPLY",
    "GATE_PRODUCTION_DRY_RUN",
    "GATE_PRODUCTION_REVIEWED_APPLY",
    "GATE_PRODUCTION_SOAK",
    "GATE_SMALL_SLICE",
    "GATE_SYNTHETIC_FIXTURES",
    "GATES_REQUIRED_BEFORE_PRODUCTION_APPLY",
    "GateRefusalError",
    "GateRunReport",
    "LADDER_GATES",
    "PRODUCTION_INSTANCE_ROLE",
    "ReadinessResult",
    "derive_archive_instance",
    "evaluate_readiness",
    "gate_artifact_dir",
    "guard_expensive_work_opt_in",
    "guard_prior_gate_evidence",
    "guard_production_apply",
    "is_production_instance",
    "resolve_instance_role",
    "write_gate_report",
]
