"""JSONL staging records for Phase 2.875 enrichment (entity mentions + match candidates)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class EntityMention:
    source_card_uid: str
    source_card_type: str
    workflow: str
    entity_type: str  # person | place | organization
    raw_text: str
    context: dict[str, Any]
    confidence: float
    run_id: str

    def to_json_line(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)


@dataclass
class MatchCandidate:
    source_card_uid: str
    source_card_type: str
    workflow: str
    target_card_type: str
    match_signals: dict[str, Any]
    field_to_write: str
    confidence: float
    run_id: str

    def to_json_line(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)
