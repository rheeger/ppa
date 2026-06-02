"""Artifact writers for processor runs."""

from __future__ import annotations

import json
from pathlib import Path

from .batch import ProcessorRunReport
from .constants import PROCESSOR_LOG_ROOT, SECTION_E_COMPLETION_STATE


def processor_artifact_dir(repo_root: Path, processor_key: str, run_id: str) -> Path:
    safe_key = processor_key.replace(":", "_")
    return repo_root / "logs" / PROCESSOR_LOG_ROOT / f"processor-{safe_key}" / run_id


def write_processor_report(repo_root: Path, report: ProcessorRunReport) -> dict[str, str]:
    run_id = report.run_id or "unknown"
    base = processor_artifact_dir(repo_root, report.processor_key, run_id)
    base.mkdir(parents=True, exist_ok=True)
    report_path = base / "report.json"
    summary_path = base / "summary.md"
    payload = report.to_dict()
    payload["completion_state"] = SECTION_E_COMPLETION_STATE
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(_summary_md(report), encoding="utf-8")
    paths = {"report": str(report_path), "summary": str(summary_path)}
    report.artifact_paths = paths
    return paths


def _summary_md(report: ProcessorRunReport) -> str:
    lines = [
        f"# Processor run — {report.processor_key}",
        "",
        f"- **status**: {report.status}",
        f"- **run_id**: {report.run_id}",
        f"- **processor_version**: {report.processor_version}",
        f"- **archive_instance**: {report.archive_instance}",
        f"- **engine_mode**: {report.engine_mode or 'n/a'}",
        f"- **ladder_gate**: {report.ladder_gate}",
        "",
        "## Counts",
        f"- input_count: {report.input_count}",
        f"- dirty_count: {report.dirty_count}",
        f"- stale_count: {report.stale_count}",
        f"- skipped_count: {report.skipped_count}",
        f"- output_count: {report.output_count}",
        "",
    ]
    if report.stale_reasons:
        lines.extend(["## Stale reasons", *[f"- {k}: {v}" for k, v in sorted(report.stale_reasons.items())], ""])
    if report.skip_reasons:
        lines.extend(["## Skip reasons", *[f"- {k}: {v}" for k, v in sorted(report.skip_reasons.items())], ""])
    if report.errors:
        lines.extend(["## Errors", *[f"- {e}" for e in report.errors], ""])
    if report.warnings:
        lines.extend(["## Warnings", *[f"- {w}" for w in report.warnings], ""])
    return "\n".join(lines)
