"""Reusable validation gate safety guards with standard exit code 3."""

from __future__ import annotations

from typing import Iterable

from ..errors import PpaError
from .constants import (
    EXIT_REFUSED,
    EXPENSIVE_WORK_FLAGS,
    GATE_PRODUCTION_DRY_RUN,
    GATE_RUN_STATUS_PASSED,
    GATES_REQUIRED_BEFORE_PRODUCTION_APPLY,
    PRODUCTION_INSTANCE_ROLE,
)
from .gate_registry import GateRegistry, GateRunRecord
from .instance_identity import is_production_instance


class GateRefusalError(PpaError):
    """Unsafe operation refused by validation gate guardrail."""

    exit_code = EXIT_REFUSED

    def __init__(self, message: str, *, reason: str = "unsafe_operation_refused"):
        super().__init__(message)
        self.reason = reason


def refuse(message: str, *, reason: str = "unsafe_operation_refused") -> None:
    raise GateRefusalError(message, reason=reason)


def guard_prior_gate_evidence(
    registry: GateRegistry,
    *,
    archive_instance: str,
    required_gates: Iterable[str] = GATES_REQUIRED_BEFORE_PRODUCTION_APPLY,
) -> None:
    missing = [
        gate for gate in required_gates if not registry.has_passed_gate(gate=gate, archive_instance=archive_instance)
    ]
    if missing:
        refuse(
            "Missing required validation gate evidence: " + ", ".join(missing),
            reason="missing_gate_evidence",
        )


def guard_reviewed_decision_run(
    registry: GateRegistry,
    decision_run_id: str,
    *,
    expected_gate: str = GATE_PRODUCTION_DRY_RUN,
    archive_instance: str | None = None,
) -> GateRunRecord:
    if not decision_run_id.strip():
        refuse("decision_run_id is required", reason="missing_decision_run_id")
    record = registry.get_run(decision_run_id.strip())
    if record is None:
        refuse(f"Unknown decision_run_id: {decision_run_id}", reason="unknown_decision_run_id")
    if record.gate != expected_gate:
        refuse(
            f"decision_run_id {decision_run_id} is gate {record.gate}, expected {expected_gate}",
            reason="wrong_gate_for_decision_run",
        )
    if archive_instance is not None and record.archive_instance != archive_instance:
        refuse(
            f"decision_run_id {decision_run_id} belongs to {record.archive_instance}, not {archive_instance}",
            reason="wrong_archive_instance",
        )
    if record.status != GATE_RUN_STATUS_PASSED:
        refuse(
            f"decision_run_id {decision_run_id} has status {record.status}, expected passed",
            reason="decision_run_not_passed",
        )
    if not record.reviewed or not record.approved:
        refuse(
            f"decision_run_id {decision_run_id} is not reviewed and approved",
            reason="decision_run_not_reviewed",
        )
    return record


def guard_production_apply(
    registry: GateRegistry,
    *,
    decision_run_id: str,
    archive_instance: str,
    confirm_production: bool,
    instance_role: str | None = None,
) -> GateRunRecord:
    if not confirm_production:
        refuse(
            "Production apply requires explicit confirmation (--confirm-production)",
            reason="missing_production_confirmation",
        )
    if not is_production_instance(archive_instance, instance_role=instance_role):
        refuse(
            "Production apply refused for non-production archive_instance "
            f"(expected role {PRODUCTION_INSTANCE_ROLE!r} via PPA_ARCHIVE_INSTANCE_ROLE or label prefix)",
            reason="not_production_instance",
        )
    guard_prior_gate_evidence(
        registry,
        archive_instance=archive_instance,
        required_gates=GATES_REQUIRED_BEFORE_PRODUCTION_APPLY,
    )
    return guard_reviewed_decision_run(
        registry,
        decision_run_id,
        expected_gate=GATE_PRODUCTION_DRY_RUN,
        archive_instance=archive_instance,
    )


def guard_expensive_work_opt_in(flag_name: str, enabled: bool) -> None:
    if flag_name not in EXPENSIVE_WORK_FLAGS:
        refuse(f"Unknown expensive-work flag: {flag_name}", reason="unknown_expensive_flag")
    if not enabled:
        refuse(
            f"{flag_name} requires an explicit opt-in flag",
            reason=f"missing_{flag_name}_opt_in",
        )
