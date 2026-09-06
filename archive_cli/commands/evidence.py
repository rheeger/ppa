"""Compact chronological evidence listing."""

from __future__ import annotations

import logging
from typing import Any

from archive_cli.card_traversal import attach_context_labels, clamp_evidence_limit, narrative_outline
from archive_cli.index_config import (
    get_context_following,
    get_context_max_tokens_per_hit,
    get_context_max_tokens_total,
    get_context_preceding,
)

from ..store import DefaultArchiveStore
from .confidence import compute_confidence, detect_gaps, log_gaps


def evidence(
    *,
    query: str = "",
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    start_date: str = "",
    end_date: str = "",
    limit: int = 12,
    narrative: bool = False,
    expand_context: bool = False,
    context_units: list[Any] | None = None,
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Compact dated hits via ``store.evidence``; optional narrative outline.

    ``expand_context`` labels adjacent units when callers already have ordered
    neighbor units. Parser / MCP registration waits for P10-D.
    """
    cap = clamp_evidence_limit(limit)
    logger.info(
        "evidence_start query=%r type=%r people=%r start=%r end=%r limit=%s narrative=%s",
        query,
        type_filter,
        people_filter,
        start_date,
        end_date,
        cap,
        narrative,
    )
    result = store.evidence(
        query=query,
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
        start_date=start_date,
        end_date=end_date,
        limit=cap,
    )
    hits = list(result.get("hits") or [])
    qtext = query.strip() or f"evidence:{type_filter}:{people_filter}:{start_date}..{end_date}"
    result["confidence"] = compute_confidence(result_count=len(hits), query_text=qtext).value
    if expand_context:
        from archive_engine.context import expand_ranked_hits

        units = list(context_units or [])
        if units:
            expanded = expand_ranked_hits(
                hits,
                units,
                preceding=get_context_preceding(),
                following=get_context_following(),
                max_tokens_per_hit=get_context_max_tokens_per_hit(),
                max_tokens_total=get_context_max_tokens_total(),
            )
            hits = [attach_context_labels(hit, page) for hit, page in zip(hits, expanded, strict=False)]
            result["hits"] = hits
            result["expanded"] = [page.to_payload() for page in expanded]
        else:
            result["context_status"] = "no_units"
    if narrative:
        result["narrative"] = narrative_outline(hits)
    logger.info("evidence_done result_count=%s narrative=%s expand_context=%s", len(hits), narrative, expand_context)
    gaps = detect_gaps(query_text=qtext, result_count=len(hits))
    if gaps:
        try:
            log_gaps(gaps, index=store.index, logger=logger)
        except Exception:
            logger.warning("gap_logging_failed", exc_info=True)
    return result
