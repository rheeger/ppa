"""Gmail 403 classification. No live Gmail."""

from __future__ import annotations

from archive_sync.adapters.gmail_http_errors import (
    GmailDailyQuotaExceeded,
    GmailPermissionDenied,
    classify_gmail_error,
    gmail_error_reason,
    raise_classified_gmail_error,
)

PERMISSION_BODY = """{
  "error": {
    "code": 403,
    "message": "Permission denied",
    "errors": [
      {
        "message": "Permission denied",
        "domain": "global",
        "reason": "forbidden"
      }
    ],
    "status": "PERMISSION_DENIED"
  }
}"""

RATE_LIMIT_BODY = '{"error":{"code":403,"message":"Quota exceeded","errors":[{"reason":"rateLimitExceeded"}]}}'
USER_RATE_BODY = '{"error":{"code":403,"errors":[{"reason":"userRateLimitExceeded"}]}}'
DAILY_BODY = '{"error":{"code":403,"errors":[{"reason":"dailyLimitExceeded"}]}}'


def test_classifies_log_permission_denied_as_permission() -> None:
    assert gmail_error_reason(PERMISSION_BODY) == "forbidden"
    assert classify_gmail_error(PERMISSION_BODY, 403) == "permission"


def test_classifies_rate_limit_reasons() -> None:
    assert classify_gmail_error(RATE_LIMIT_BODY, 403) == "rate_limit"
    assert classify_gmail_error(USER_RATE_BODY, 403) == "rate_limit"
    assert classify_gmail_error("rate limited", 429) == "rate_limit"


def test_classifies_daily_quota() -> None:
    assert classify_gmail_error(DAILY_BODY, 403) == "daily_quota"


def test_raise_typed_exceptions() -> None:
    try:
        raise_classified_gmail_error(PERMISSION_BODY, status=403)
        raise AssertionError("expected permission")
    except GmailPermissionDenied as exc:
        assert exc.reason == "forbidden"
    try:
        raise_classified_gmail_error(DAILY_BODY, status=403)
        raise AssertionError("expected daily")
    except GmailDailyQuotaExceeded:
        pass
