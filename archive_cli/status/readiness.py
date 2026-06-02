"""Fail-closed v3 readiness evaluation aggregating Sections B, D, E, and G evidence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from archive_cli.corpus_hygiene.constants import SECTION_B_APPLY_ARTIFACT_GATE
from archive_cli.ppa_engine import ppa_engine
from archive_cli.validation_gates.constants import (
    GATE_PRODUCTION_DRY_RUN,
    GATE_PRODUCTION_REVIEWED_APPLY,
    GATE_PRODUCTION_SOAK,
    GATE_RUN_STATUS_PASSED,
)
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_cli.validation_gates.readiness import ReadinessResult, evaluate_readiness
from archive_sync.source_updaters.constants import (
    STALENESS_BLOCKED,
    STALENESS_FAILED,
    STALENESS_NEVER_SYNCED,
)

from .corpus_summary import query_corpus_summary, rollback_decision_run_ids
from .suppression_visibility import SuppressionVisibilityResult, evaluate_suppression_visibility


@dataclass
class V3ReadinessResult:
    ready: bool
    archive_instance: str
    passed_checks: list[str] = field(default_factory=list)
    failed_checks: list[str] = field(default_factory=list)
    blocking_reasons: list[str] = field(default_factory=list)
    gate_readiness: ReadinessResult | None = None
    suppression_visibility: SuppressionVisibilityResult | None = None
    engine_mode: str = ""
    rollback_decision_run_ids: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "archive_instance": self.archive_instance,
            "passed_checks": list(self.passed_checks),
            "failed_checks": list(self.failed_checks),
            "blocking_reasons": list(self.blocking_reasons),
            "gate_readiness": self.gate_readiness.to_dict() if self.gate_readiness else None,
            "suppression_visibility": (
                self.suppression_visibility.to_dict() if self.suppression_visibility else None
            ),
            "engine_mode": self.engine_mode,
            "rollback_decision_run_ids": list(self.rollback_decision_run_ids),
            "details": dict(self.details),
        }


def _source_health_ok(sources_payload: dict[str, Any]) -> tuple[bool, list[str]]:
    failures: list[str] = []
    for entry in sources_payload.get("sources") or []:
        state = entry.get("state") or {}
        source_key = str(state.get("source_key") or entry.get("declaration", {}).get("source_key") or "")
        staleness = str(state.get("staleness_state") or STALENESS_NEVER_SYNCED)
        if not source_key:
            continue
        if staleness in (STALENESS_FAILED, STALENESS_BLOCKED):
            failures.append(f"source:{source_key}:{staleness}")
        if staleness == STALENESS_NEVER_SYNCED and bool(state.get("enabled", True)):
            failures.append(f"source:{source_key}:never_synced")
    return not failures, failures


def _processor_health_ok(processors_payload: dict[str, Any]) -> tuple[bool, list[str]]:
    failures: list[str] = []
    totals = processors_payload.get("totals") or {}
    if int(totals.get("failed") or 0) > 0:
        failures.append("processor_failures_present")
    for entry in processors_payload.get("processors") or []:
        key = str(entry.get("processor_key") or "")
        if not key:
            continue
        if int(entry.get("failed_count") or 0) > 0:
            failures.append(f"processor:{key}:failed")
    return not failures, failures


def _engine_mode_consistent(
    gate_readiness: ReadinessResult,
    *,
    current_engine: str,
) -> tuple[bool, list[str]]:
    modes = {
        str((run or {}).get("engine_mode") or "").strip().lower()
        for run in (gate_readiness.latest_runs or {}).values()
        if (run or {}).get("engine_mode")
    }
    modes.discard("")
    modes.discard("parity")
    if not modes:
        return False, ["engine_mode_evidence_missing"]
    if len(modes) > 1:
        return False, [f"engine_mode_divergence:{','.join(sorted(modes))}"]
    recorded = next(iter(modes))
    if current_engine not in (recorded, "parity") and recorded not in (current_engine, "parity"):
        return False, [f"engine_mode_mismatch:recorded={recorded},current={current_engine}"]
    return True, []


def evaluate_v3_readiness(
    *,
    registry: GateRegistry,
    conn: Any,
    schema: str,
    archive_instance: str,
    sources_payload: dict[str, Any],
    processors_payload: dict[str, Any],
    require_production_soak: bool = True,
) -> V3ReadinessResult:
    """Evaluate v3 readiness fail-closed from durable gate/source/processor/corpus evidence."""

    current_engine = ppa_engine()
    gate_readiness = evaluate_readiness(
        registry,
        archive_instance=archive_instance,
        require_production_soak=require_production_soak,
    )
    suppression = evaluate_suppression_visibility(conn, schema)
    corpus = query_corpus_summary(conn, schema)
    rollback_ids = rollback_decision_run_ids(conn, schema)

    passed: list[str] = []
    failed: list[str] = []
    blocking: list[str] = list(gate_readiness.blocking_reasons)

    if gate_readiness.ready:
        passed.append("validation_gates")
    else:
        failed.append("validation_gates")
        blocking.extend(gate_readiness.missing_gates)
        if gate_readiness.blocking_reasons:
            blocking.extend(gate_readiness.blocking_reasons)

    apply_record = registry.latest_passed(
        gate=GATE_PRODUCTION_REVIEWED_APPLY,
        archive_instance=archive_instance,
    )
    if apply_record is not None and apply_record.status == GATE_RUN_STATUS_PASSED:
        passed.append("corpus_cleanup_apply")
    else:
        failed.append("corpus_cleanup_apply")
        blocking.append("section_b_cleanup_apply_evidence_missing")

    dry_run = registry.latest_passed(gate=GATE_PRODUCTION_DRY_RUN, archive_instance=archive_instance)
    if dry_run is not None and dry_run.reviewed and dry_run.approved:
        passed.append("corpus_cleanup_reviewed")
    else:
        failed.append("corpus_cleanup_reviewed")
        blocking.append("section_b_cleanup_review_missing")

    if corpus.get("table_exists"):
        passed.append("corpus_state_recorded")
    else:
        failed.append("corpus_state_recorded")
        blocking.append("section_b_corpus_state_missing")

    if suppression.ok:
        passed.append("suppression_visibility")
    else:
        failed.append("suppression_visibility")
        if suppression.retrieval_violations:
            blocking.append("suppressed_email_visible_in_default_retrieval")
        if suppression.enrichment_queue_violations:
            blocking.append("suppressed_email_in_enrichment_queue")
        if suppression.link_job_violations:
            blocking.append("suppressed_email_in_link_candidates")

    sources_ok, source_failures = _source_health_ok(sources_payload)
    if sources_ok:
        passed.append("source_freshness")
    else:
        failed.append("source_freshness")
        blocking.extend(source_failures)

    processors_ok, processor_failures = _processor_health_ok(processors_payload)
    if processors_ok:
        passed.append("processor_health")
    else:
        failed.append("processor_health")
        blocking.extend(processor_failures)

    engine_ok, engine_failures = _engine_mode_consistent(gate_readiness, current_engine=current_engine)
    if engine_ok:
        passed.append("engine_mode")
    else:
        failed.append("engine_mode")
        blocking.extend(engine_failures)

    rollback_run_id = apply_record.run_id if apply_record is not None else (rollback_ids[0] if rollback_ids else "")
    if rollback_run_id and (rollback_ids or corpus.get("last_apply_decision_run_id")):
        passed.append("rollback_available")
    else:
        failed.append("rollback_available")
        blocking.append("cleanup_rollback_state_unavailable")

    soak = registry.latest_passed(gate=GATE_PRODUCTION_SOAK, archive_instance=archive_instance)
    if require_production_soak:
        if soak is not None:
            passed.append("production_soak")
        else:
            failed.append("production_soak")
            blocking.append("production_soak_evidence_missing")

    ready = not failed
    return V3ReadinessResult(
        ready=ready,
        archive_instance=archive_instance,
        passed_checks=passed,
        failed_checks=failed,
        blocking_reasons=sorted(set(blocking)),
        gate_readiness=gate_readiness,
        suppression_visibility=suppression,
        engine_mode=current_engine,
        rollback_decision_run_ids=rollback_ids,
        details={
            "corpus_summary": corpus,
            "section_b_apply_gate": SECTION_B_APPLY_ARTIFACT_GATE,
            "latest_apply_gate_run": apply_record.to_dict() if apply_record else None,
            "latest_soak_gate_run": soak.to_dict() if soak else None,
        },
    )
