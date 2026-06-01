"""Fail-closed readiness evaluation from validation gate evidence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .constants import (
    GATE_PRODUCTION_DRY_RUN,
    GATE_PRODUCTION_REVIEWED_APPLY,
    GATE_PRODUCTION_SOAK,
    GATES_REQUIRED_BEFORE_PRODUCTION_APPLY,
    LADDER_GATES,
)
from .gate_registry import GateRegistry


@dataclass
class ReadinessResult:
    ready: bool
    archive_instance: str
    passed_gates: list[str] = field(default_factory=list)
    missing_gates: list[str] = field(default_factory=list)
    blocking_reasons: list[str] = field(default_factory=list)
    latest_runs: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "archive_instance": self.archive_instance,
            "passed_gates": self.passed_gates,
            "missing_gates": self.missing_gates,
            "blocking_reasons": self.blocking_reasons,
            "latest_runs": self.latest_runs,
        }


def evaluate_readiness(
    registry: GateRegistry,
    *,
    archive_instance: str,
    require_production_soak: bool = False,
) -> ReadinessResult:
    required = list(GATES_REQUIRED_BEFORE_PRODUCTION_APPLY)
    if require_production_soak:
        required.extend([GATE_PRODUCTION_REVIEWED_APPLY, GATE_PRODUCTION_SOAK])

    passed: list[str] = []
    missing: list[str] = []
    blocking: list[str] = []
    latest: dict[str, Any] = {}

    for gate in LADDER_GATES:
        record = registry.latest_passed(gate=gate, archive_instance=archive_instance)
        if record is not None:
            passed.append(gate)
            latest[gate] = record.to_dict()
        elif gate in required:
            missing.append(gate)

    dry_run = registry.latest_passed(gate=GATE_PRODUCTION_DRY_RUN, archive_instance=archive_instance)
    if dry_run is None and GATE_PRODUCTION_DRY_RUN in required:
        blocking.append("production_dry_run_not_passed")
    elif dry_run is not None and (not dry_run.reviewed or not dry_run.approved):
        blocking.append("production_dry_run_not_reviewed_or_approved")

    ready = not missing and not blocking
    return ReadinessResult(
        ready=ready,
        archive_instance=archive_instance,
        passed_gates=passed,
        missing_gates=missing,
        blocking_reasons=blocking,
        latest_runs=latest,
    )
