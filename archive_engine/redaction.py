"""Redact credentials, DSNs, and raw card bodies from diagnostics.

Safe identifiers are hashes — never tokens, DSNs, or source text. This module
does not encrypt anything.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

REDACTED = "[REDACTED]"

_BEARER = re.compile(r"(?i)(\bbearer\s+)([A-Za-z0-9._\-+=/]{8,})")
_SK = re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_\-]{10,}")
_GOOGLE = re.compile(r"\bAIza[0-9A-Za-z_\-]{20,}")
_PG = re.compile(r"(?i)(postgres(?:ql)?://)([^:/@]+):([^@/]+)@")
_QUERY_KEY = re.compile(r"(?i)([?&](?:key|api_key|token|access_token|secret)=)([^&\s\"']+)")
_LABELED = re.compile(
    r"(?i)\b(api[_-]?key|authorization|access[_-]?token|refresh[_-]?token|password|secret|dsn|op_service_account_token)\s*[:=]\s*([^\s,;\"']+)"
)
_SENTRY = re.compile(r"https://[0-9A-Za-z._\-]+@o\d+\.ingest\.[^\s\"']+")
_FRONTMATTER = re.compile(r"(?s)---\r?\n(?:[a-z_][a-z0-9_]*:.*\r?\n)+---")
_CARD_DUMP = re.compile(r"(?s)---\r?\n(?:[a-z_][a-z0-9_]*:.*\r?\n)+---\r?\n.*")
_BODY_ASSIGN = re.compile(r"(?i)((?:card_|raw_|message_)?body\s*[:=]\s*)(.{16,})")


def redact_text(value: str) -> str:
    """Return *value* with tokens, DSNs, and raw bodies replaced."""

    if not value:
        return value
    text = _BEARER.sub(rf"\1{REDACTED}", value)
    text = _SK.sub(REDACTED, text)
    text = _GOOGLE.sub(REDACTED, text)
    text = _PG.sub(rf"\1{REDACTED}@", text)
    text = _QUERY_KEY.sub(rf"\1{REDACTED}", text)
    text = _LABELED.sub(lambda m: f"{m.group(1)}={REDACTED}", text)
    text = _SENTRY.sub(f"https://{REDACTED}@ingest.invalid", text)
    text = _CARD_DUMP.sub("---\n[REDACTED_CARD]\n---", text)
    text = _FRONTMATTER.sub("---\n[REDACTED_FRONTMATTER]\n---", text)
    text = _BODY_ASSIGN.sub(rf"\1{REDACTED}", text)
    return text


def redact_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, bytes):
        try:
            return redact_text(value.decode("utf-8"))
        except UnicodeDecodeError:
            return REDACTED
    return value


def safe_diagnostic_id(*parts: object) -> str:
    """Stable hashed identifier for logs — not reversible to the input."""

    material = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return hashlib.sha256(material).hexdigest()[:16]


class RedactingFormatter(logging.Formatter):
    """Formatter that never emits tokens, DSNs, or raw card bodies."""

    def format(self, record: logging.LogRecord) -> str:
        formatted = super().format(record)
        return redact_text(formatted)


LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s %(message)s"


def redacting_formatter() -> RedactingFormatter:
    return RedactingFormatter(LOG_FORMAT)
