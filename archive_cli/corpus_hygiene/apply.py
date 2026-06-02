"""Email corpus hygiene apply orchestration."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from archive_cli.ppa_engine import ppa_engine
from archive_cli.validation_gates.constants import GATE_LOCAL_SEED_STAGING_APPLY, GATE_RUN_STATUS_PASSED
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_cli.validation_gates.report import GateRunReport, write_gate_report
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

from .constants import SECTION_B_APPLY_ARTIFACT_GATE, SECTION_B_APPLY_COMPLETION_STATE
from .decision_io import load_decision_records_jsonl, validate_decision_records
from .decisions import EmailCorpusDecisionRecord
from .report import render_apply_summary
from .state_store import ApplyCounts, apply_decision_records


@dataclass
class ApplyResult:
    decision_run_id: str
    archive_instance: str
    vault_path: str
    index_schema: str
    engine_mode: str
    counts: ApplyCounts
    records: list[EmailCorpusDecisionRecord] = field(default_factory=list)
    total_elapsed_ms: int = 0
    artifact_paths: dict[str, str] = field(default_factory=dict)
    rollback_path: str = ""


def run_email_corpus_apply(
    conn: Any,
    schema: str,
    records: list[EmailCorpusDecisionRecord],
    *,
    decision_run_id: str,
    archive_instance: str,
    vault_path: str,
    engine_mode: str | None = None,
    repo_root: Path | None = None,
    registry: GateRegistry | None = None,
) -> ApplyResult:
    validate_decision_records(records, decision_run_id=decision_run_id)
    t0 = time.perf_counter()
    counts = apply_decision_records(conn, schema, records, decision_run_id=decision_run_id)
    elapsed = int((time.perf_counter() - t0) * 1000)
    result = ApplyResult(
        decision_run_id=decision_run_id,
        archive_instance=archive_instance,
        vault_path=vault_path,
        index_schema=schema,
        engine_mode=engine_mode or ppa_engine(),
        counts=counts,
        records=records,
        total_elapsed_ms=elapsed,
    )
    if repo_root is not None:
        result.artifact_paths = write_apply_artifacts(repo_root, result)
        rollback_payload = {
            "decision_run_id": decision_run_id,
            "archive_instance": archive_instance,
            "card_uids": sorted(
                {
                    uid
                    for rec in records
                    for uid in (rec.thread_uid, *rec.message_uids, *rec.attachment_uids)
                    if uid
                }
            ),
        }
        rollback_path = Path(result.artifact_paths["report"]).parent / "rollback.json"
        rollback_path.write_text(json.dumps(rollback_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        result.rollback_path = str(rollback_path)
        result.artifact_paths["rollback"] = str(rollback_path)

    if registry is not None and result.artifact_paths:
        apply_run = registry.create_run(
            gate=GATE_LOCAL_SEED_STAGING_APPLY,
            archive_instance=archive_instance,
            vault_path=vault_path,
            index_schema=schema,
            engine_mode=result.engine_mode,
            policy_version=EMAIL_PROMOTION_POLICY_VERSION,
            input_hash=decision_run_id,
        )
        registry.complete_run(
            apply_run.run_id,
            status=GATE_RUN_STATUS_PASSED,
            report_path=result.artifact_paths.get("report", ""),
            summary_path=result.artifact_paths.get("summary", ""),
            applied=True,
        )
    return result


def write_apply_artifacts(repo_root: Path, result: ApplyResult) -> dict[str, str]:
    run_id = f"{result.decision_run_id}-apply"
    report = GateRunReport(
        run_id=run_id,
        gate=SECTION_B_APPLY_ARTIFACT_GATE,
        ladder_gate="Local seed staging apply",
        archive_instance=result.archive_instance,
        vault_path=result.vault_path,
        index_schema=result.index_schema,
        engine_mode=result.engine_mode,
        policy_version=EMAIL_PROMOTION_POLICY_VERSION,
        decision_run_id=result.decision_run_id,
        overall_status="passed",
        total_elapsed_ms=result.total_elapsed_ms,
        corpus_counts=result.counts.by_corpus_state or {},
        next_recommended_gate="production_dry_run",
        completion_state=SECTION_B_APPLY_COMPLETION_STATE,
    )
    report.details = {
        "threads_applied": result.counts.threads_applied,
        "cards_updated": result.counts.cards_updated,
        "safety": {
            "production_mutation": False,
            "vault_markdown_deleted": False,
            "rollback_available": True,
        },
    }
    paths = write_gate_report(repo_root, report)
    summary_path = Path(paths["summary"])
    summary_path.write_text(render_apply_summary(result, report), encoding="utf-8")
    paths["summary"] = str(summary_path)
    return paths


def apply_from_decisions_path(
    conn: Any,
    schema: str,
    decisions_path: Path,
    **kwargs: Any,
) -> ApplyResult:
    records = load_decision_records_jsonl(decisions_path)
    decision_run_id = kwargs.pop("decision_run_id", records[0].decision_run_id if records else "")
    return run_email_corpus_apply(conn, schema, records, decision_run_id=decision_run_id, **kwargs)
