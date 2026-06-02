"""Artifact writers for source updater runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .batch import SourceUpdaterRunReport
from .constants import SECTION_D_COMPLETION_STATE, SOURCE_UPDATER_LOG_ROOT


def write_source_updater_report(repo_root: Path, report: SourceUpdaterRunReport) -> dict[str, str]:
    run_id = report.run_id or "unknown"
    gate = report.ladder_gate or "synthetic_fixtures"
    base = repo_root / "logs" / SOURCE_UPDATER_LOG_ROOT / f"source-{report.source_key.replace(':', '_')}" / run_id
    base.mkdir(parents=True, exist_ok=True)
    report_path = base / "report.json"
    summary_path = base / "summary.md"
    payload = report.to_dict()
    payload["completion_state"] = SECTION_D_COMPLETION_STATE
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(_summary_md(report), encoding="utf-8")
    paths = {"report": str(report_path), "summary": str(summary_path)}
    report.artifact_paths = paths
    return paths


def _summary_md(report: SourceUpdaterRunReport) -> str:
    b = report.batch
    lines = [
        f"# Source updater run — {report.source_key}",
        "",
        f"- **status**: {report.status}",
        f"- **run_id**: {report.run_id}",
        f"- **archive_instance**: {report.archive_instance}",
        f"- **engine_mode**: {report.engine_mode or 'n/a'}",
        f"- **ladder_gate**: {report.ladder_gate}",
        "",
        "## Counts",
        f"- observed: {b.observed}",
        f"- unchanged: {b.unchanged}",
        f"- promoted: {b.promoted}",
        f"- suppressed: {b.suppressed}",
        f"- quarantined: {b.quarantined}",
        f"- updated: {b.updated}",
        f"- deleted/tombstoned: {b.deleted_or_tombstoned}",
        f"- dirty_card_uids: {b.dirty_card_uids_count}",
        "",
    ]
    if report.errors:
        lines.extend(["## Errors", *[f"- {e}" for e in report.errors], ""])
    if report.warnings:
        lines.extend(["## Warnings", *[f"- {w}" for w in report.warnings], ""])
    return "\n".join(lines)
