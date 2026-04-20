"""Quality score distribution report by card type."""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger("ppa.quality_report")


def quality_report(*, dsn: str | None = None, schema: str | None = None) -> dict[str, Any]:
    """Aggregate quality_score distribution by card type."""
    import psycopg

    from archive_cli.index_config import get_index_dsn, get_index_schema

    dsn = (dsn or get_index_dsn()).strip()
    schema = (schema or get_index_schema()).strip()
    if not dsn:
        raise RuntimeError("PPA_INDEX_DSN is required for quality-report")

    with psycopg.connect(dsn) as conn:
        score_rows = conn.execute(
            f"""
            SELECT type, COUNT(*) AS count,
                ROUND(AVG(quality_score)::numeric, 3) AS avg_score,
                ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY quality_score))::numeric, 3) AS median,
                ROUND((PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY quality_score))::numeric, 3) AS p10,
                ROUND((PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY quality_score))::numeric, 3) AS p90
            FROM {schema}.cards GROUP BY type ORDER BY type
            """
        ).fetchall()

        flag_rows = conn.execute(
            f"""
            SELECT type, flag, COUNT(*) AS cnt
            FROM {schema}.cards, unnest(quality_flags) AS flag
            GROUP BY type, flag ORDER BY type, cnt DESC
            """
        ).fetchall()

    flags_by_type: dict[str, list[dict[str, Any]]] = {}
    for r in flag_rows:
        t = r[0]
        if t not in flags_by_type:
            flags_by_type[t] = []
        if len(flags_by_type[t]) < 5:
            flags_by_type[t].append({"flag": r[1], "count": int(r[2])})

    types: list[dict[str, Any]] = []
    total_count = 0
    total_score_sum = 0.0
    for r in score_rows:
        card_type = r[0]
        entry = {
            "type": card_type,
            "count": int(r[1]),
            "avg_score": float(r[2]),
            "median": float(r[3]),
            "p10": float(r[4]),
            "p90": float(r[5]),
            "common_flags": flags_by_type.get(card_type, []),
        }
        total_count += entry["count"]
        total_score_sum += entry["avg_score"] * entry["count"]
        types.append(entry)

    return {
        "types": types,
        "summary": {
            "total_cards": total_count,
            "overall_avg": round(total_score_sum / total_count, 3) if total_count else 0,
        },
    }
