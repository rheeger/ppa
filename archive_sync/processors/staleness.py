"""Staleness evaluation for processor inputs and outputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .constants import (
    CORPUS_ACTIVE,
    CORPUS_QUARANTINE,
    CORPUS_SUPPRESSED,
    SKIP_NOT_APPLICABLE,
    SKIP_QUARANTINE,
    SKIP_SUPPRESSED,
    SKIP_UPSTREAM,
    STALE_CORPUS_STATE,
    STALE_DIRTY_INPUT,
    STALE_FAILED_OUTPUT,
    STALE_INPUT_HASH,
    STALE_MISSING_OUTPUT,
    STALE_PROCESSOR_VERSION,
    STALE_UPSTREAM,
)
from .declarations import ProcessorDeclaration


@dataclass
class ProcessorInputSnapshot:
    input_uid: str
    card_type: str
    corpus_state: str = CORPUS_ACTIVE
    processor_decision: str = ""
    field_values: dict[str, Any] = field(default_factory=dict)
    source_dirty: bool = False
    upstream_complete: bool = True
    recorded_input_hash: str = ""
    recorded_processor_version: str = ""
    recorded_corpus_state: str = ""
    output_exists: bool = False
    output_failed: bool = False
    upstream_output_hash: str = ""
    recorded_upstream_output_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_uid": self.input_uid,
            "card_type": self.card_type,
            "corpus_state": self.corpus_state,
            "processor_decision": self.processor_decision,
            "field_values": dict(self.field_values),
            "source_dirty": self.source_dirty,
            "upstream_complete": self.upstream_complete,
            "recorded_input_hash": self.recorded_input_hash,
            "recorded_processor_version": self.recorded_processor_version,
            "recorded_corpus_state": self.recorded_corpus_state,
            "output_exists": self.output_exists,
            "output_failed": self.output_failed,
            "upstream_output_hash": self.upstream_output_hash,
            "recorded_upstream_output_hash": self.recorded_upstream_output_hash,
        }


@dataclass
class StalenessEvaluation:
    processor_key: str
    input_uid: str
    stale: bool = False
    skipped: bool = False
    skip_reason: str = ""
    stale_reasons: list[str] = field(default_factory=list)
    current_input_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "processor_key": self.processor_key,
            "input_uid": self.input_uid,
            "stale": self.stale,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "stale_reasons": list(self.stale_reasons),
            "current_input_hash": self.current_input_hash,
        }


def input_matches_filters(snapshot: ProcessorInputSnapshot, decl: ProcessorDeclaration) -> bool:
    if snapshot.card_type not in decl.input_card_types:
        return False
    for key, expected in decl.input_filters.items():
        actual = ""
        if key == "corpus_decision":
            actual = snapshot.corpus_state
        elif key == "processor_decision":
            actual = snapshot.processor_decision
        else:
            actual = str(snapshot.field_values.get(key, ""))
        if actual != expected:
            return False
    return True


def should_skip_for_corpus_state(snapshot: ProcessorInputSnapshot, decl: ProcessorDeclaration) -> str | None:
    if not decl.active_only:
        return None
    if snapshot.corpus_state == CORPUS_SUPPRESSED:
        return SKIP_SUPPRESSED
    if snapshot.corpus_state == CORPUS_QUARANTINE:
        return SKIP_QUARANTINE
    return None


def evaluate_staleness(
    decl: ProcessorDeclaration,
    snapshot: ProcessorInputSnapshot,
    *,
    current_input_hash: str,
) -> StalenessEvaluation:
    """Evaluate whether a processor should run for one input (no execution)."""

    result = StalenessEvaluation(
        processor_key=decl.processor_key,
        input_uid=snapshot.input_uid,
        current_input_hash=current_input_hash,
    )
    if snapshot.card_type not in decl.input_card_types:
        result.skipped = True
        result.skip_reason = SKIP_NOT_APPLICABLE
        return result

    skip = should_skip_for_corpus_state(snapshot, decl)
    if skip:
        result.skipped = True
        result.skip_reason = skip
        return result

    if not input_matches_filters(snapshot, decl):
        result.skipped = True
        result.skip_reason = SKIP_NOT_APPLICABLE
        return result

    if decl.depends_on and not snapshot.upstream_complete:
        result.skipped = True
        result.skip_reason = SKIP_UPSTREAM
        return result

    reasons: list[str] = []
    if snapshot.source_dirty:
        reasons.append(STALE_DIRTY_INPUT)
    if snapshot.recorded_input_hash and snapshot.recorded_input_hash != current_input_hash:
        reasons.append(STALE_INPUT_HASH)
    if snapshot.recorded_processor_version and snapshot.recorded_processor_version != decl.processor_version:
        reasons.append(STALE_PROCESSOR_VERSION)
    if snapshot.recorded_corpus_state and snapshot.recorded_corpus_state != snapshot.corpus_state:
        reasons.append(STALE_CORPUS_STATE)
    if (
        snapshot.upstream_output_hash
        and snapshot.recorded_upstream_output_hash
        and snapshot.upstream_output_hash != snapshot.recorded_upstream_output_hash
    ):
        reasons.append(STALE_UPSTREAM)
    if not snapshot.output_exists and not snapshot.output_failed:
        reasons.append(STALE_MISSING_OUTPUT)
    if snapshot.output_failed:
        reasons.append(STALE_FAILED_OUTPUT)

    result.stale_reasons = reasons
    result.stale = bool(reasons)
    return result
