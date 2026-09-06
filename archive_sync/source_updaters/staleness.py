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


BURST_FRESHNESS_UNKNOWN = "unknown"
BURST_FRESHNESS_INVALIDATED = "invalidated"
COVERAGE_BOUNDED = "bounded"
COVERAGE_UNKNOWN = "unknown"
COVERAGE_COMPLETE = "complete"


def burst_freshness_state(*, resolver_present: bool, keys_invalidated: int = 0) -> str:
    """Unknown is honest when P01 burst resolution is not attached."""

    if resolver_present and keys_invalidated > 0:
        return BURST_FRESHNESS_INVALIDATED
    return BURST_FRESHNESS_UNKNOWN


def source_coverage_state(*, complete: bool = False, bounded: bool = False) -> str:
    if complete:
        return COVERAGE_COMPLETE
    if bounded:
        return COVERAGE_BOUNDED
    return COVERAGE_UNKNOWN


def connector_freshness_report(
    *,
    last_success_at: Any = None,
    last_attempt_at: Any = None,
    last_error: str = "",
    last_run_status: str = "",
    enabled: bool = True,
    resolver_present: bool = False,
    keys_invalidated: int = 0,
    bounded: bool = True,
    cursor_status: str = "active",
) -> dict[str, str]:
    return {
        "staleness_state": compute_staleness_state(
            last_success_at=last_success_at,
            last_attempt_at=last_attempt_at,
            last_error=last_error,
            last_run_status=last_run_status,
            enabled=enabled,
        ),
        "burst_freshness": burst_freshness_state(
            resolver_present=resolver_present,
            keys_invalidated=keys_invalidated,
        ),
        "coverage": source_coverage_state(bounded=bounded),
        "cursor_status": cursor_status,
    }
