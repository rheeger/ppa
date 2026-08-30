"""Append-only maintenance report writer hook for Section F (non-mutating)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class MaintenanceStatusReport:
    run_id: str
    archive_instance: str
    vault_path: str
    index_schema: str
    engine_mode: str
    started_at: str = ""
    completed_at: str = ""
    overall_status: str = "partial"
    source_summaries: list[dict[str, Any]] = field(default_factory=list)
    corpus_summary: dict[str, Any] = field(default_factory=dict)
    processor_summaries: list[dict[str, Any]] = field(default_factory=list)
    embedding_summary: dict[str, Any] = field(default_factory=dict)
    linker_summary: dict[str, Any] = field(default_factory=dict)
    readiness: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[dict[str, Any]] = field(default_factory=list)
    next_action: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}


def maintenance_report_from_status(payload: dict[str, Any], *, run_id: str) -> MaintenanceStatusReport:
    """Build a maintenance report shape from an existing Section F status payload."""

    archive = payload.get("archive") or {}
    return MaintenanceStatusReport(
        run_id=run_id,
        archive_instance=str(archive.get("instance") or ""),
        vault_path=str(archive.get("vault_path") or ""),
        index_schema=str(archive.get("schema") or ""),
        engine_mode=str(archive.get("engine_mode") or ""),
        started_at=_utc_now_iso(),
        completed_at=_utc_now_iso(),
        overall_status=str(archive.get("status") or "partial"),
        source_summaries=list(payload.get("sources") or []),
        corpus_summary=dict(payload.get("corpus") or {}),
        processor_summaries=list(payload.get("processors") or []),
        embedding_summary=dict(payload.get("embeddings") or {}),
        linker_summary=dict(payload.get("linkers") or {}),
        readiness=dict(payload.get("v3_readiness") or {}),
        errors=list(payload.get("errors") or []),
        warnings=list(payload.get("warnings") or []),
        next_action=_next_action_from_payload(payload),
    )


def _next_action_from_payload(payload: dict[str, Any]) -> str:
    v3 = payload.get("v3_readiness") or {}
    if v3.get("ready"):
        return "continue normal maintenance cycles"
    blocking = v3.get("blocking_reasons") or []
    if blocking:
        return f"resolve: {blocking[0]}"
    failed = v3.get("failed_checks") or []
    if failed:
        return f"resolve: {failed[0]}"
    errors = payload.get("errors") or []
    if errors:
        return str(errors[0].get("message") or errors[0].get("reason") or "review errors")
    return "review status warnings"


def write_maintenance_status_report(
    repo_root: Path,
    report: MaintenanceStatusReport,
    *,
    timestamp: datetime | None = None,
) -> dict[str, str]:
    """Write append-only JSON maintenance report under logs/maintenance/."""

    ts = timestamp or datetime.now(timezone.utc)
    stamp = ts.strftime("%Y%m%d-%H%M%S")
    rel_dir = Path("logs") / "maintenance"
    out_dir = repo_root / rel_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"maintain-{stamp}-{report.run_id}.json"
    path = out_dir / filename
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {"report": str(path), "relative_report": str(rel_dir / filename)}


def attach_status_report_hook(
    maintain_report: dict[str, Any],
    status_payload: dict[str, Any],
    *,
    repo_root: Path,
    run_id: str,
) -> dict[str, Any]:
    """Non-mutating hook: add Section F report paths to an existing maintain report dict."""

    report = maintenance_report_from_status(status_payload, run_id=run_id)
    paths = write_maintenance_status_report(repo_root, report)
    maintain_report = dict(maintain_report)
    maintain_report["section_f_status_report"] = paths["report"]
    maintain_report["section_f_next_action"] = report.next_action
    return maintain_report
