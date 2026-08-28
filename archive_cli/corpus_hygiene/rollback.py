"""Email corpus hygiene rollback orchestration."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from archive_cli.ppa_engine import ppa_engine
from archive_cli.validation_gates.report import GateRunReport, write_gate_report
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

from .constants import SECTION_B_APPLY_COMPLETION_STATE, SECTION_B_ROLLBACK_ARTIFACT_GATE
from .report import render_rollback_summary
from .state_store import RollbackCounts, rollback_decision_run


@dataclass
class RollbackResult:
    decision_run_id: str
    archive_instance: str
    vault_path: str
    index_schema: str
    engine_mode: str
    counts: RollbackCounts
    total_elapsed_ms: int = 0
    artifact_paths: dict[str, str] = field(default_factory=dict)
    vault_markdown_deleted: bool = False


def run_email_corpus_rollback(
    conn: Any,
    schema: str,
    *,
    decision_run_id: str,
    archive_instance: str,
    vault_path: str,
    engine_mode: str | None = None,
    repo_root: Path | None = None,
) -> RollbackResult:
    t0 = time.perf_counter()
    counts = rollback_decision_run(conn, schema, decision_run_id)
    if vault_path:
        from .apply import restore_rollback_kit

        counts.kit_files_restored = restore_rollback_kit(Path(vault_path), decision_run_id)
    elapsed = int((time.perf_counter() - t0) * 1000)
    result = RollbackResult(
        decision_run_id=decision_run_id,
        archive_instance=archive_instance,
        vault_path=vault_path,
        index_schema=schema,
        engine_mode=engine_mode or ppa_engine(),
        counts=counts,
        total_elapsed_ms=elapsed,
        vault_markdown_deleted=counts.kit_files_restored > 0,
    )
    if repo_root is not None:
        result.artifact_paths = write_rollback_artifacts(repo_root, result)
    return result


def write_rollback_artifacts(repo_root: Path, result: RollbackResult) -> dict[str, str]:
    run_id = f"{result.decision_run_id}-rollback"
    report = GateRunReport(
        run_id=run_id,
        gate=SECTION_B_ROLLBACK_ARTIFACT_GATE,
        ladder_gate="Local seed staging apply",
        archive_instance=result.archive_instance,
        vault_path=result.vault_path,
        index_schema=result.index_schema,
        engine_mode=result.engine_mode,
        policy_version=EMAIL_PROMOTION_POLICY_VERSION,
        decision_run_id=result.decision_run_id,
        overall_status="passed",
        total_elapsed_ms=result.total_elapsed_ms,
        next_recommended_gate="local_seed_dry_run",
        completion_state=SECTION_B_APPLY_COMPLETION_STATE,
    )
    report.details = {
        "cards_restored": result.counts.cards_restored,
        "threads_restored": result.counts.threads_restored,
        "kit_files_restored": result.counts.kit_files_restored,
        "safety": {
            "llm_calls": False,
            "vault_markdown_deleted": bool(result.vault_markdown_deleted),
        },
    }
    paths = write_gate_report(repo_root, report)
    summary_path = Path(paths["summary"])
    summary_path.write_text(render_rollback_summary(result, report), encoding="utf-8")
    paths["summary"] = str(summary_path)
    return paths
