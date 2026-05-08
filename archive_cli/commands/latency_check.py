"""Query latency checks for Phase 9 deployment."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

LATENCY_TARGETS_MS = {
    "fts_search": 5000,
    "temporal_neighbors": 2000,
    "hybrid_search": 5000,
    "type_filter_query": 5000,
}


@dataclass(frozen=True)
class LatencyResult:
    query_type: str
    target_ms: int
    actual_ms: int
    passed: bool
    query_text: str
    iterations: int = 1

    def to_dict(self) -> dict[str, object]:
        return dict(self.__dict__)


def run_latency_check(index: Any, iterations: int = 3) -> list[LatencyResult]:
    iterations = max(iterations, 1)
    return [
        _measure(index, "fts_search", iterations, _query_fts),
        _measure(index, "temporal_neighbors", iterations, _query_temporal),
        _measure(index, "hybrid_search", iterations, _query_hybrid),
        _measure(index, "type_filter_query", iterations, _query_type_filter),
    ]


def _measure(index: Any, query_type: str, iterations: int, query_fn: Callable[[Any], str]) -> LatencyResult:
    timings: list[int] = []
    query_text = ""
    for _ in range(iterations):
        t0 = time.monotonic()
        query_text = query_fn(index)
        timings.append(int((time.monotonic() - t0) * 1000))
    actual_ms = sorted(timings)[len(timings) // 2]
    target_ms = LATENCY_TARGETS_MS[query_type]
    return LatencyResult(
        query_type=query_type,
        target_ms=target_ms,
        actual_ms=actual_ms,
        passed=actual_ms <= target_ms,
        query_text=query_text,
        iterations=iterations,
    )


def _query_fts(index: Any) -> str:
    query = "test"
    index.search(query, limit=10)
    return query


def _query_temporal(index: Any) -> str:
    timestamp = "2025-06-15T12:00:00Z"
    index.temporal_neighbors(timestamp, limit=10)
    return f"temporal_neighbors({timestamp})"


def _query_hybrid(index: Any) -> str:
    query = "test"
    from ..index_config import (get_default_embedding_model,
                                get_default_embedding_version,
                                get_vector_dimension)

    index.hybrid_search(
        query=query,
        query_vector=[0.0] * get_vector_dimension(),
        embedding_model=get_default_embedding_model(),
        embedding_version=get_default_embedding_version(),
        limit=10,
    )
    return query


def _query_type_filter(index: Any) -> str:
    index.query_cards(type_filter="email_message", limit=10)
    return "query type=email_message"
