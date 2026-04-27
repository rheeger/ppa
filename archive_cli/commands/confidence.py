"""Confidence signaling and gap detection for retrieval tools.

Every retrieval tool response includes a 'confidence' field (high/medium/low).
When results are sparse, a gap entry is logged to the retrieval_gaps table
(created by Phase 1e) for the maintenance cycle to act on.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

log = logging.getLogger("ppa.confidence")


class ConfidenceLevel(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(frozen=True)
class GapEntry:
    query_text: str
    gap_type: str
    detail: str = ""
    card_uid: str = ""


def compute_confidence(
    *,
    result_count: int,
    exact_match: bool = False,
    query_text: str = "",
) -> ConfidenceLevel:
    """Compute confidence level per current Phase 8 rules."""
    del query_text  # reserved for future signals
    if exact_match or result_count > 10:
        return ConfidenceLevel.HIGH
    if 3 <= result_count <= 10:
        return ConfidenceLevel.MEDIUM
    return ConfidenceLevel.LOW


def detect_gaps(
    *,
    query_text: str,
    result_count: int,
    expected_types: list[str] | None = None,
    actual_types: list[str] | None = None,
    card_uid: str = "",
) -> list[GapEntry]:
    """Return gap entries for sparse/missing results."""
    gaps: list[GapEntry] = []
    if result_count == 0:
        gaps.append(GapEntry(query_text=query_text, gap_type="no_results"))
    elif result_count < 3:
        gaps.append(
            GapEntry(
                query_text=query_text,
                gap_type="sparse_results",
                detail=f"only {result_count} results",
            )
        )
    if expected_types and actual_types is not None:
        missing = set(expected_types) - set(actual_types)
        if missing:
            gaps.append(
                GapEntry(
                    query_text=query_text,
                    gap_type="type_mismatch",
                    detail=f"missing types: {sorted(missing)}",
                    card_uid=card_uid,
                )
            )
    return gaps


def log_gaps(
    gaps: list[GapEntry],
    *,
    index: Any,
    logger: logging.Logger,
) -> None:
    """Write gap entries to retrieval_gaps table. Skip silently if table missing."""
    del logger
    if not gaps:
        return
    index.log_retrieval_gaps(
        [
            {
                "query_text": g.query_text,
                "gap_type": g.gap_type,
                "detail": g.detail,
                "card_uid": g.card_uid,
            }
            for g in gaps
        ]
    )
