"""Processor plan summaries and run reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class ProcessorPlanItem:
    processor_key: str
    input_uid: str
    stale: bool = False
    skipped: bool = False
    skip_reason: str = ""
    stale_reasons: list[str] = field(default_factory=list)
    current_input_hash: str = ""
    output_identity: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "processor_key": self.processor_key,
            "input_uid": self.input_uid,
            "stale": self.stale,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "stale_reasons": list(self.stale_reasons),
            "current_input_hash": self.current_input_hash,
            "output_identity": self.output_identity,
        }


@dataclass
class ProcessorPlanSummary:
    input_count: int = 0
    dirty_count: int = 0
    stale_count: int = 0
    skipped_count: int = 0
    pending_count: int = 0
    items: list[ProcessorPlanItem] = field(default_factory=list)
    skip_reasons: dict[str, int] = field(default_factory=dict)
    stale_reasons: dict[str, int] = field(default_factory=dict)
    processors_triggered: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_count": self.input_count,
            "dirty_count": self.dirty_count,
            "stale_count": self.stale_count,
            "skipped_count": self.skipped_count,
            "pending_count": self.pending_count,
            "processors_triggered": list(self.processors_triggered),
            "skip_reasons": dict(self.skip_reasons),
            "stale_reasons": dict(self.stale_reasons),
            "items": [item.to_dict() for item in self.items],
        }


@dataclass
class ProcessorRunReport:
    run_id: str
    processor_key: str
    processor_version: str
    archive_instance: str = ""
    status: str = "success"
    input_count: int = 0
    dirty_count: int = 0
    stale_count: int = 0
    skipped_count: int = 0
    output_count: int = 0
    skip_reasons: dict[str, int] = field(default_factory=dict)
    stale_reasons: dict[str, int] = field(default_factory=dict)
    plan: ProcessorPlanSummary = field(default_factory=ProcessorPlanSummary)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    started_at: str = field(default_factory=_utc_now_iso)
    completed_at: str = ""
    artifact_paths: dict[str, str] = field(default_factory=dict)
    engine_mode: str = ""
    ladder_gate: str = ""
    decision_run_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "processor_key": self.processor_key,
            "processor_version": self.processor_version,
            "archive_instance": self.archive_instance,
            "status": self.status,
            "input_count": self.input_count,
            "dirty_count": self.dirty_count,
            "stale_count": self.stale_count,
            "skipped_count": self.skipped_count,
            "output_count": self.output_count,
            "skip_reasons": dict(self.skip_reasons),
            "stale_reasons": dict(self.stale_reasons),
            "plan": self.plan.to_dict(),
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "started_at": self.started_at,
            "completed_at": self.completed_at or _utc_now_iso(),
            "artifact_paths": dict(self.artifact_paths),
            "engine_mode": self.engine_mode,
            "ladder_gate": self.ladder_gate,
            "decision_run_id": self.decision_run_id,
        }
