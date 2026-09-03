"""Oracle-only EXPLAIN capture for Phase 1 bottleneck reports.

Production query tools never call this. QueryMixin / warehouse SQL stays as the
test oracle; EXPLAIN documents why that path is not a serving fallback.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("ppa.query_explain")

EXPLAIN_PREFIX = "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) "


def explain_sql(conn: Any, sql: str, params: tuple[Any, ...] | None = None) -> dict[str, Any]:
    """Run EXPLAIN ANALYZE JSON. Captures canceled/timeout plans as errors."""
    try:
        cur = conn.execute(EXPLAIN_PREFIX + sql, params or ())
        row = cur.fetchone()
        if row is None:
            return {"ok": False, "error": "empty_explain"}
        payload = row[0] if not isinstance(row, dict) else next(iter(row.values()))
        if isinstance(payload, str):
            payload = json.loads(payload)
        return {"ok": True, "plan": payload}
    except Exception as exc:
        logger.info("query_explain canceled_or_failed error=%s", exc)
        return {"ok": False, "error": str(exc), "canceled": "timeout" in str(exc).lower()}
