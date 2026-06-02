"""Section C dry-run report artifacts (Section G shape)."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from archive_cli.validation_gates.constants import VALIDATION_GATE_LOG_ROOT
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

from .constants import SECTION_C_ARTIFACT_GATE, SECTION_C_COMPLETION_STATE
from .metrics import GmailPromotionBatchMetrics


@dataclass
class GmailPromotionRunReport:
    run_id: str
    gate: str = SECTION_C_ARTIFACT_GATE
    archive_instance: str = "fixture:gmail-promotion"
    vault_path: str = ""
    engine_mode: str = "python"
    policy_version: str = EMAIL_PROMOTION_POLICY_VERSION
    overall_status: str = "passed"
    total_elapsed_ms: int = 0
    classification_source_counts: dict[str, int] = field(default_factory=dict)
    new_llm_call_count: int = 0
    promotion_metrics: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    next_recommended_gate: str = "small_slice"
    report_path: str = ""
    summary_path: str = ""
    completion_state: str = SECTION_C_COMPLETION_STATE

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "gate": self.gate,
            "archive_instance": self.archive_instance,
            "vault_path": self.vault_path,
            "engine_mode": self.engine_mode,
            "policy_version": self.policy_version,
            "overall_status": self.overall_status,
            "total_elapsed_ms": self.total_elapsed_ms,
            "classification_source_counts": self.classification_source_counts,
            "new_llm_call_count": self.new_llm_call_count,
            "promotion_metrics": self.promotion_metrics,
            "warnings": self.warnings,
            "errors": self.errors,
            "next_recommended_gate": self.next_recommended_gate,
            "report_path": self.report_path,
            "summary_path": self.summary_path,
            "completion_state": self.completion_state,
        }


def write_promotion_report(
    repo_root: Path,
    report: GmailPromotionRunReport,
    *,
    metrics: GmailPromotionBatchMetrics,
    classification_source_counts: dict[str, int],
    new_llm_call_count: int,
) -> GmailPromotionRunReport:
    t0 = time.perf_counter()
    run_dir = (
        repo_root
        / "logs"
        / VALIDATION_GATE_LOG_ROOT
        / f"gate-{report.gate}"
        / report.run_id
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    report.report_path = str(run_dir / "report.json")
    report.summary_path = str(run_dir / "summary.md")
    report.classification_source_counts = dict(classification_source_counts)
    report.new_llm_call_count = new_llm_call_count
    report.promotion_metrics = metrics.to_dict()
    report.total_elapsed_ms = int((time.perf_counter() - t0) * 1000)

    Path(report.report_path).write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    summary_lines = [
        f"# Gmail sync promotion dry-run ({report.run_id})",
        "",
        f"- Status: {report.overall_status}",
        f"- Engine: {report.engine_mode}",
        f"- Policy: {report.policy_version}",
        "",
        "## Promotion counts",
        "",
    ]
    for key, value in sorted(report.promotion_metrics.items()):
        summary_lines.append(f"- {key}: {value}")
    summary_lines.extend(
        [
            "",
            "## Classification reuse",
            "",
        ]
    )
    for src, count in sorted(report.classification_source_counts.items()):
        summary_lines.append(f"- {src}: {count}")
    summary_lines.append(f"- new_llm: {report.new_llm_call_count}")
    summary_lines.append(f"\nNext gate: {report.next_recommended_gate}\n")
    Path(report.summary_path).write_text("\n".join(summary_lines), encoding="utf-8")
    return report
