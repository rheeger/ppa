"""Shared validation gate report schema (DeployResult-style)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from .constants import (
    GATE_FRAMEWORK_COMPLETION_STATE,
    GATE_FRAMEWORK_STATE,
    VALIDATION_GATE_LOG_ROOT,
)

ReportStatus = Literal["pending", "running", "passed", "failed", "blocked", "refused"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class GateReportPhase:
    name: str
    status: ReportStatus = "pending"
    elapsed_ms: int = 0
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class GateRunReport:
    run_id: str
    gate: str
    ladder_gate: str
    archive_instance: str
    vault_path: str
    index_schema: str
    engine_mode: str
    policy_version: str = ""
    decision_run_id: str = ""
    overall_status: ReportStatus = "pending"
    total_elapsed_ms: int = 0
    phases: list[GateReportPhase] = field(default_factory=list)
    classification_source_counts: dict[str, int] = field(default_factory=dict)
    new_llm_call_count: int = 0
    corpus_counts: dict[str, int] = field(default_factory=dict)
    dirty_processor_count: int = 0
    embedding_affected_count: int = 0
    linker_affected_count: int = 0
    throughput_by_phase: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    next_recommended_gate: str = ""
    report_path: str = ""
    summary_path: str = ""
    samples_path: str = ""
    errors_path: str = ""
    rollback_path: str = ""
    created_at: str = field(default_factory=_utc_now_iso)
    completion_state: str = GATE_FRAMEWORK_COMPLETION_STATE
    gate_framework_state: str = GATE_FRAMEWORK_STATE
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "gate": self.gate,
            "ladder_gate": self.ladder_gate,
            "archive_instance": self.archive_instance,
            "vault_path": self.vault_path,
            "index_schema": self.index_schema,
            "engine_mode": self.engine_mode,
            "policy_version": self.policy_version,
            "decision_run_id": self.decision_run_id or self.run_id,
            "overall_status": self.overall_status,
            "total_elapsed_ms": self.total_elapsed_ms,
            "phases": [p.to_dict() for p in self.phases],
            "classification_source_counts": self.classification_source_counts,
            "new_llm_call_count": self.new_llm_call_count,
            "corpus_counts": self.corpus_counts,
            "dirty_processor_count": self.dirty_processor_count,
            "embedding_affected_count": self.embedding_affected_count,
            "linker_affected_count": self.linker_affected_count,
            "throughput_by_phase": self.throughput_by_phase,
            "warnings": self.warnings,
            "errors": self.errors,
            "next_recommended_gate": self.next_recommended_gate,
            "artifact_paths": {
                "report": self.report_path,
                "summary": self.summary_path,
                "samples": self.samples_path,
                "errors": self.errors_path,
                "rollback": self.rollback_path,
            },
            "created_at": self.created_at,
            "completion_state": self.completion_state,
            "gate_framework_state": self.gate_framework_state,
            "details": self.details,
        }


def gate_artifact_dir(repo_root: str | Path, *, gate: str, run_id: str) -> Path:
    return Path(repo_root) / "logs" / VALIDATION_GATE_LOG_ROOT / f"gate-{gate}" / run_id


def render_summary(report: GateRunReport) -> str:
    lines = [
        f"# validation gate report — {report.gate}",
        "",
        f"- run_id: `{report.run_id}`",
        f"- archive_instance: `{report.archive_instance}`",
        f"- engine_mode: `{report.engine_mode}`",
        f"- overall_status: `{report.overall_status}`",
        f"- next_recommended_gate: `{report.next_recommended_gate or '(none)'}`",
        "",
    ]
    if report.warnings:
        lines.append("## Warnings")
        lines.extend(f"- {w}" for w in report.warnings)
        lines.append("")
    if report.errors:
        lines.append("## Errors")
        lines.extend(f"- {e}" for e in report.errors)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_gate_report(
    repo_root: str | Path,
    report: GateRunReport,
    *,
    write_samples: list[dict[str, Any]] | None = None,
    write_errors: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    artifact_dir = gate_artifact_dir(repo_root, gate=report.gate, run_id=report.run_id)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    report_path = artifact_dir / "report.json"
    summary_path = artifact_dir / "summary.md"
    report.report_path = str(report_path)
    report.summary_path = str(summary_path)
    report_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_summary(report), encoding="utf-8")
    paths = {"report": str(report_path), "summary": str(summary_path)}
    if write_samples is not None:
        samples_path = artifact_dir / "samples.jsonl"
        with samples_path.open("w", encoding="utf-8") as handle:
            for row in write_samples:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
        report.samples_path = str(samples_path)
        paths["samples"] = str(samples_path)
    if write_errors is not None:
        errors_path = artifact_dir / "errors.jsonl"
        with errors_path.open("w", encoding="utf-8") as handle:
            for row in write_errors:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
        report.errors_path = str(errors_path)
        paths["errors"] = str(errors_path)
    report_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return paths
