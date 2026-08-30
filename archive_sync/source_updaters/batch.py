"""Committed batch summaries and cursor commit safety."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class SourceUpdaterBatchSummary:
    observed: int = 0
    unchanged: int = 0
    promoted: int = 0
    suppressed: int = 0
    quarantined: int = 0
    updated: int = 0
    deleted_or_tombstoned: int = 0
    dirty_card_uids: list[str] = field(default_factory=list)

    @property
    def dirty_card_uids_count(self) -> int:
        return len(self.dirty_card_uids)

    def to_dict(self) -> dict[str, Any]:
        return {
            "observed": self.observed,
            "unchanged": self.unchanged,
            "promoted": self.promoted,
            "suppressed": self.suppressed,
            "quarantined": self.quarantined,
            "updated": self.updated,
            "deleted_or_tombstoned": self.deleted_or_tombstoned,
            "dirty_card_uids_count": self.dirty_card_uids_count,
            "dirty_card_uids": list(self.dirty_card_uids),
        }


@dataclass
class SourceUpdaterRunReport:
    run_id: str
    source_key: str
    source_type: str
    archive_instance: str = ""
    status: str = "success"
    cursor_before: dict[str, Any] = field(default_factory=dict)
    cursor_after: dict[str, Any] = field(default_factory=dict)
    batch: SourceUpdaterBatchSummary = field(default_factory=SourceUpdaterBatchSummary)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    started_at: str = field(default_factory=_utc_now_iso)
    completed_at: str = ""
    artifact_paths: dict[str, str] = field(default_factory=dict)
    engine_mode: str = ""
    ladder_gate: str = ""
    decision_run_id: str = ""
    adapter_version: str = ""
    policy_version: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source_key": self.source_key,
            "source_type": self.source_type,
            "archive_instance": self.archive_instance,
            "status": self.status,
            "cursor_before": dict(self.cursor_before),
            "cursor_after": dict(self.cursor_after),
            **self.batch.to_dict(),
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "started_at": self.started_at,
            "completed_at": self.completed_at or _utc_now_iso(),
            "artifact_paths": dict(self.artifact_paths),
            "engine_mode": self.engine_mode,
            "ladder_gate": self.ladder_gate,
            "decision_run_id": self.decision_run_id,
            "adapter_version": self.adapter_version,
            "policy_version": self.policy_version,
        }


def batch_summary_from_skip_details(
    skip_details: dict[str, int] | None,
    *,
    observed: int = 0,
    updated: int = 0,
    unchanged: int = 0,
    dirty_card_uids: list[str] | None = None,
) -> SourceUpdaterBatchSummary:
    """Map adapter skip_details / promotion metrics into batch counts."""

    details = dict(skip_details or {})
    promoted = int(details.get("promotion_promoted", 0) or 0)
    suppressed = int(details.get("promotion_suppressed", 0) or 0)
    quarantined = int(details.get("promotion_quarantined", 0) or 0)
    promo_observed = int(details.get("promotion_observed", 0) or 0)
    if observed <= 0 and promo_observed > 0:
        observed = promo_observed
    if observed <= 0:
        observed = promoted + suppressed + quarantined + updated + unchanged
    if unchanged <= 0:
        unchanged = sum(
            int(details.get(k, 0) or 0)
            for k in (
                "skipped_unchanged_threads",
                "skipped_unchanged_messages",
                "skipped_unchanged_attachments",
            )
        )
    deleted = int(details.get("deleted_or_tombstoned", 0) or details.get("tombstoned", 0) or 0)
    return SourceUpdaterBatchSummary(
        observed=observed,
        unchanged=unchanged,
        promoted=promoted,
        suppressed=suppressed,
        quarantined=quarantined,
        updated=updated,
        deleted_or_tombstoned=deleted,
        dirty_card_uids=list(dirty_card_uids or []),
    )


def commit_cursor_after_persisted(
    *,
    side_effects_persisted: bool,
    cursor_before: dict[str, Any],
    cursor_patch: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return cursor_after only when batch side effects were persisted (Section D safety)."""

    if not side_effects_persisted:
        return dict(cursor_before)
    if not cursor_patch:
        return dict(cursor_before)
    merged = dict(cursor_before)
    merged.update(cursor_patch)
    return merged


def cursor_patch_may_commit(
    *,
    side_effects_persisted: bool,
    batch_errors: list[str] | None = None,
) -> bool:
    """Mirror BaseAdapter rule: no cursor patch when unpersisted work or batch errors."""

    if not side_effects_persisted:
        return False
    if batch_errors:
        return False
    return True
