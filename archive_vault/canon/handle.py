"""Social-handle canon — URL-aware when a provider or URL is present."""

from __future__ import annotations

import re

_WS = re.compile(r"\s+")
_LINKEDIN = re.compile(r"(?:https?://)?(?:[\w]+\.)?linkedin\.com/in/([^/?#]+)", re.I)
_GITHUB = re.compile(r"(?:https?://)?(?:www\.)?github\.com/([^/?#]+)", re.I)
_TWITTER = re.compile(r"(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/([^/?#]+)", re.I)
_INSTAGRAM = re.compile(r"(?:https?://)?(?:www\.)?instagram\.com/([^/?#]+)", re.I)
_TELEGRAM = re.compile(r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/([^/?#]+)", re.I)


def _bare(value: str) -> str:
    return _WS.sub(" ", value.strip()).removeprefix("@").strip("/").lower()


def canonical(value: str | None, provider: str | None = None) -> str:
    """Lowercase handle, no leading ``@``. Extract from a profile URL when possible."""

    raw = _WS.sub(" ", str(value or "").strip())
    if not raw:
        return ""
    lowered = raw.lower()
    provider = (provider or "").strip().lower()
    patterns = []
    if provider == "github" or "github.com" in lowered:
        patterns.append(_GITHUB)
    if provider == "linkedin" or "linkedin.com" in lowered:
        patterns.append(_LINKEDIN)
    if provider in {"twitter", "x"} or "twitter.com" in lowered or "x.com/" in lowered:
        patterns.append(_TWITTER)
    if provider == "instagram" or "instagram.com" in lowered:
        patterns.append(_INSTAGRAM)
    if provider == "telegram" or "t.me/" in lowered or "telegram.me" in lowered:
        patterns.append(_TELEGRAM)
    if not patterns:
        patterns = [_LINKEDIN, _GITHUB, _TWITTER, _INSTAGRAM, _TELEGRAM]
    for pattern in patterns:
        match = pattern.search(raw)
        if match:
            return _bare(match.group(1))
    return _bare(raw)


def linkedin_fields(handle: str, url: str) -> tuple[str, str]:
    """Return ``(handle, url)`` with handle extracted from either field."""

    handle_value = canonical(handle, provider="linkedin")
    url_value = _WS.sub(" ", str(url or "").strip())
    match = _LINKEDIN.search(url_value or handle or "")
    if match:
        handle_value = _bare(match.group(1))
    if handle_value and not url_value:
        url_value = f"https://www.linkedin.com/in/{handle_value}"
    return handle_value, url_value
