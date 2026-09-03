"""Phase timers for query-path bottleneck reports.

Clocks are store/index method time, not MCP transport. Lexical and vector
branches time themselves from job start so they do not include waiting on
the sibling future.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any

logger = logging.getLogger("ppa.query_timing")


@dataclass
class QueryPhaseTimes:
    connect_ms: float = 0.0
    embed_ms: float = 0.0
    embed_cache_hit: bool = False
    lexical_sql_ms: float = 0.0
    vector_sql_ms: float = 0.0
    graph_sql_ms: float = 0.0
    fusion_ms: float = 0.0
    total_ms: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        extra = payload.pop("extra", {}) or {}
        payload.update(extra)
        return payload


def add_ms(times: QueryPhaseTimes, field_name: str, started: float) -> None:
    elapsed = (time.monotonic() - started) * 1000.0
    setattr(times, field_name, round(getattr(times, field_name) + elapsed, 3))


def log_phase_times(op: str, times: QueryPhaseTimes) -> None:
    logger.info(
        "query_phase op=%s connect_ms=%.1f embed_ms=%.1f embed_cache_hit=%s "
        "lexical_sql_ms=%.1f vector_sql_ms=%.1f graph_sql_ms=%.1f fusion_ms=%.1f total_ms=%.1f",
        op,
        times.connect_ms,
        times.embed_ms,
        times.embed_cache_hit,
        times.lexical_sql_ms,
        times.vector_sql_ms,
        times.graph_sql_ms,
        times.fusion_ms,
        times.total_ms,
    )
