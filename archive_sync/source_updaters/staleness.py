"""Staleness state derivation for source updaters."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .constants import (
    FRESHNESS_WINDOW_DAYS,
    RUN_STATUS_BLOCKED,
    RUN_STATUS_FAILED,
    STALENESS_BLOCKED,
    STALENESS_FAILED,
    STALENESS_FRESH,
    STALENESS_NEVER_SYNCED,
    STALENESS_STALE,
)


def _parse_ts(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def compute_staleness_state(
    *,
    last_success_at: Any = None,
    last_attempt_at: Any = None,
    last_error: str = "",
    last_run_status: str = "",
    enabled: bool = True,
    freshness_days: int = FRESHNESS_WINDOW_DAYS,
) -> str:
    if not enabled:
        return STALENESS_BLOCKED
    err = (last_error or "").strip().lower()
    if last_run_status == RUN_STATUS_BLOCKED or "blocked" in err or "auth" in err or "permission" in err:
        return STALENESS_BLOCKED
    success_ts = _parse_ts(last_success_at)
    attempt_ts = _parse_ts(last_attempt_at)
    if success_ts is None:
        if last_run_status == RUN_STATUS_FAILED:
            return STALENESS_FAILED
        if err:
            return STALENESS_BLOCKED if "blocked" in err else STALENESS_FAILED
        return STALENESS_NEVER_SYNCED
    if last_run_status == RUN_STATUS_FAILED and attempt_ts and (success_ts is None or attempt_ts >= success_ts):
        return STALENESS_FAILED
    if err and attempt_ts and success_ts and attempt_ts >= success_ts:
        return STALENESS_BLOCKED if "blocked" in err else STALENESS_FAILED
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, freshness_days))
    if success_ts >= cutoff:
        return STALENESS_FRESH
    return STALENESS_STALE
