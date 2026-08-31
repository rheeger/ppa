"""Classify Gmail HTTP / gws errors. Do not treat every 403 as quota."""

from __future__ import annotations

import json
import random
import re
from typing import Literal

GmailErrorKind = Literal["rate_limit", "daily_quota", "permission", "transient", "other"]

RATE_LIMIT_REASONS = frozenset({"rateLimitExceeded", "userRateLimitExceeded"})
DAILY_QUOTA_REASONS = frozenset({"dailyLimitExceeded"})
PERMISSION_REASONS = frozenset({"forbidden", "insufficientPermissions"})
TRANSIENT_STATUS = frozenset({429, 500, 502, 503, 504})

_REASON_RE = re.compile(r'"reason":\s*"([^"]+)"')
_STATUS_RE = re.compile(r'"status":\s*"([^"]+)"')
_CODE_RE = re.compile(r'"code":\s*(\d+)')


class GmailPermissionDenied(RuntimeError):
    """Hard 403: this attachment/message is not readable with the current token."""

    def __init__(self, message: str, *, reason: str = "forbidden") -> None:
        super().__init__(message)
        self.reason = reason


class GmailDailyQuotaExceeded(RuntimeError):
    """Gmail daily quota is exhausted; stop fetching for this run."""

    def __init__(self, message: str, *, reason: str = "dailyLimitExceeded") -> None:
        super().__init__(message)
        self.reason = reason


def _parse_error_payload(message: str) -> dict:
    text = (message or "").strip()
    start = text.find("{")
    if start < 0:
        return {}
    try:
        payload = json.loads(text[start:])
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def gmail_error_reason(message: str) -> str:
    payload = _parse_error_payload(message)
    err = payload.get("error")
    if isinstance(err, dict):
        errors = err.get("errors")
        if isinstance(errors, list) and errors:
            first = errors[0] if isinstance(errors[0], dict) else {}
            reason = str(first.get("reason") or "").strip()
            if reason:
                return reason
        reason = str(err.get("reason") or "").strip()
        if reason:
            return reason
        status = str(err.get("status") or "").strip()
        if status == "PERMISSION_DENIED":
            return "forbidden"
        if status == "RESOURCE_EXHAUSTED":
            return "rateLimitExceeded"
    match = _REASON_RE.search(message or "")
    if match:
        return match.group(1)
    status_match = _STATUS_RE.search(message or "")
    if status_match and status_match.group(1) == "PERMISSION_DENIED":
        return "forbidden"
    return ""


def gmail_error_status_code(message: str, status: int | None = None) -> int | None:
    if status is not None:
        return status
    payload = _parse_error_payload(message)
    err = payload.get("error")
    if isinstance(err, dict) and err.get("code") is not None:
        try:
            return int(err["code"])
        except (TypeError, ValueError):
            pass
    match = _CODE_RE.search(message or "")
    if match:
        return int(match.group(1))
    return None


def classify_gmail_error(message: str, status: int | None = None) -> GmailErrorKind:
    """Distinguish rate-limit 403/429 from permission 403 and daily quota."""

    text = message or ""
    reason = gmail_error_reason(text)
    code = gmail_error_status_code(text, status)
    if reason in DAILY_QUOTA_REASONS or "dailyLimitExceeded" in text:
        return "daily_quota"
    if code == 429 or reason in RATE_LIMIT_REASONS:
        return "rate_limit"
    if reason in PERMISSION_REASONS or "PERMISSION_DENIED" in text:
        return "permission"
    if code in TRANSIENT_STATUS:
        return "transient"
    return "other"


def raise_classified_gmail_error(message: str, *, status: int | None = None) -> None:
    kind = classify_gmail_error(message, status)
    reason = gmail_error_reason(message) or kind
    if kind == "daily_quota":
        raise GmailDailyQuotaExceeded(message, reason=reason)
    if kind == "permission":
        raise GmailPermissionDenied(message, reason=reason)
    raise RuntimeError(message)


def retry_after_seconds(retry_after_header: str | None, attempt: int) -> float:
    raw = (retry_after_header or "").strip()
    if raw:
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
    return min(90.0, 5.0 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.5)
