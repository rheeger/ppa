"""Apply/rollback safety guards for corpus hygiene."""

from __future__ import annotations

from pathlib import Path

from archive_cli.validation_gates.constants import (
    GATE_LOCAL_SEED_DRY_RUN,
    GATE_RUN_STATUS_PASSED,
    GATE_SMALL_SLICE,
    GATE_SYNTHETIC_FIXTURES,
)
from archive_cli.validation_gates.gate_registry import GateRegistry, GateRunRecord
from archive_cli.validation_gates.guards import guard_production_apply, refuse
from archive_cli.validation_gates.instance_identity import is_production_instance

from .constants import SECTION_B_CENSUS_ARTIFACT_GATE
from .decision_io import decisions_artifact_path

ALLOWED_DRY_RUN_GATES = frozenset(
    {
        GATE_SYNTHETIC_FIXTURES,
        GATE_SMALL_SLICE,
        GATE_LOCAL_SEED_DRY_RUN,
        SECTION_B_CENSUS_ARTIFACT_GATE,
    }
)


def guard_corpus_hygiene_apply(
    registry: GateRegistry,
    *,
    decision_run_id: str,
    archive_instance: str,
    repo_root: Path,
    confirm_production: bool = False,
    instance_role: str | None = None,
) -> tuple[GateRunRecord, Path]:
    """Refuse apply when decision evidence or staging guards fail."""

    if not decision_run_id.strip():
        refuse("decision_run_id is required for corpus-hygiene apply", reason="missing_decision_run_id")

    if is_production_instance(archive_instance, instance_role=instance_role):
        guard_production_apply(
            registry,
            decision_run_id=decision_run_id,
            archive_instance=archive_instance,
            confirm_production=confirm_production,
            instance_role=instance_role,
        )
        record = registry.get_run(decision_run_id)
        assert record is not None
        decisions_path = decisions_artifact_path(repo_root, decision_run_id)
        if not decisions_path.is_file():
            refuse(
                f"Missing decisions artifact for {decision_run_id}: {decisions_path}",
                reason="missing_decisions_artifact",
            )
        return record, decisions_path

    record = registry.get_run(decision_run_id.strip())
    if record is None:
        refuse(f"Unknown decision_run_id: {decision_run_id}", reason="unknown_decision_run_id")
    if record.archive_instance != archive_instance:
        refuse(
            f"decision_run_id {decision_run_id} belongs to {record.archive_instance}, not {archive_instance}",
            reason="wrong_archive_instance",
        )
    if record.status != GATE_RUN_STATUS_PASSED:
        refuse(
            f"decision_run_id {decision_run_id} has status {record.status}, expected passed",
            reason="decision_run_not_passed",
        )
    if record.gate not in ALLOWED_DRY_RUN_GATES:
        refuse(
            f"decision_run_id {decision_run_id} gate {record.gate} is not an allowed dry-run gate",
            reason="wrong_gate_for_apply",
        )

    decisions_path = decisions_artifact_path(repo_root, decision_run_id)
    if not decisions_path.is_file():
        refuse(
            f"Missing decisions artifact for {decision_run_id}: {decisions_path}",
            reason="missing_decisions_artifact",
        )
    return record, decisions_path


def guard_corpus_hygiene_rollback(
    registry: GateRegistry,
    *,
    decision_run_id: str,
    archive_instance: str,
    instance_role: str | None = None,
) -> GateRunRecord:
    if is_production_instance(archive_instance, instance_role=instance_role):
        refuse(
            "Production rollback requires explicit operator workflow — use staging/slice first",
            reason="production_rollback_refused",
        )
    record = registry.get_run(decision_run_id.strip())
    if record is None:
        refuse(f"Unknown decision_run_id: {decision_run_id}", reason="unknown_decision_run_id")
    if record.archive_instance != archive_instance:
        refuse(
            f"decision_run_id {decision_run_id} belongs to {record.archive_instance}, not {archive_instance}",
            reason="wrong_archive_instance",
        )
    return record
