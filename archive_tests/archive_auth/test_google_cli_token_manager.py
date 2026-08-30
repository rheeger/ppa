"""Calendar token-profile and scoped-refresh mint tests (no live OAuth)."""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from archive_auth.token_manager import (
    CALENDAR_READONLY_SCOPES,
    CALENDAR_SCOPES,
    mint_access_token,
    resolve_scopes,
    _should_retry_mint_without_scopes,
)


def test_calendar_service_profile_is_full_calendar_not_readonly() -> None:
    scopes = resolve_scopes(services=["calendar"])
    assert scopes == list(CALENDAR_SCOPES)
    assert "https://www.googleapis.com/auth/calendar" in scopes
    assert CALENDAR_READONLY_SCOPES[0] not in scopes


def test_explicit_readonly_scopes_still_resolve_when_requested() -> None:
    assert resolve_scopes(scopes=CALENDAR_READONLY_SCOPES) == list(CALENDAR_READONLY_SCOPES)


def test_scope_mint_retry_gate() -> None:
    assert _should_retry_mint_without_scopes(
        had_scopes=True,
        status=400,
        body='{"error":"invalid_scope"}',
    )
    assert _should_retry_mint_without_scopes(had_scopes=True, status=400, body="")
    assert not _should_retry_mint_without_scopes(
        had_scopes=True,
        status=400,
        body='{"error":"invalid_grant"}',
    )
    assert not _should_retry_mint_without_scopes(had_scopes=False, status=400, body='{"error":"invalid_scope"}')
    assert not _should_retry_mint_without_scopes(had_scopes=True, status=401, body='{"error":"invalid_scope"}')


def test_mint_retries_without_scopes_on_invalid_scope(monkeypatch: Any) -> None:
    calls: list[dict[str, list[str]]] = []

    class _FakeResp:
        def __init__(self, payload: dict[str, Any]) -> None:
            self._payload = json.dumps(payload).encode("utf-8")

        def read(self) -> bytes:
            return self._payload

        def __enter__(self) -> _FakeResp:
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    def fake_urlopen(req: urllib.request.Request, timeout: int = 30) -> _FakeResp:
        parsed = urllib.parse.parse_qs(req.data.decode("utf-8") if req.data else "")
        calls.append(parsed)
        if "scope" in parsed:
            body = io.BytesIO(b'{"error":"invalid_scope","error_description":"Bad scope"}')
            raise urllib.error.HTTPError(
                req.full_url,
                400,
                "Bad Request",
                hdrs=None,  # type: ignore[arg-type]
                fp=body,
            )
        return _FakeResp({"access_token": "granted-token", "expires_in": 3600})

    monkeypatch.setattr("archive_auth.token_manager.urllib.request.urlopen", fake_urlopen)
    payload = mint_access_token(
        refresh_token="refresh-fixture",
        client_config={
            "client_id": "cid",
            "client_secret": "csecret",
            "token_uri": "https://oauth2.googleapis.com/token",
        },
        scopes=CALENDAR_READONLY_SCOPES,
    )
    assert payload["access_token"] == "granted-token"
    assert len(calls) == 2
    assert calls[0]["scope"] == ["https://www.googleapis.com/auth/calendar.readonly"]
    assert "scope" not in calls[1]


def test_mint_does_not_retry_invalid_grant(monkeypatch: Any) -> None:
    def fake_urlopen(req: urllib.request.Request, timeout: int = 30) -> None:
        body = io.BytesIO(b'{"error":"invalid_grant","error_description":"Token expired"}')
        raise urllib.error.HTTPError(
            req.full_url,
            400,
            "Bad Request",
            hdrs=None,  # type: ignore[arg-type]
            fp=body,
        )

    monkeypatch.setattr("archive_auth.token_manager.urllib.request.urlopen", fake_urlopen)
    try:
        mint_access_token(
            refresh_token="refresh-fixture",
            client_config={
                "client_id": "cid",
                "client_secret": "csecret",
                "token_uri": "https://oauth2.googleapis.com/token",
            },
            scopes=CALENDAR_SCOPES,
        )
    except RuntimeError as exc:
        assert "Token refresh failed" in str(exc)
        assert "invalid_grant" in str(exc).lower() or "expired" in str(exc).lower()
    else:
        raise AssertionError("expected Token refresh failed")
