"""P05-C: tokens, DSNs, and raw bodies never appear in ppa logs."""

from __future__ import annotations

import logging
from io import StringIO
from pathlib import Path

import pytest

from archive_cli.log import configure_logging, ensure_redacting_handlers
from archive_engine.redaction import REDACTED, redact_text, redacting_formatter, safe_diagnostic_id

SYN_DSN = "postgresql://ppa_user:s3cretPass@localhost:5432/ppa"
SYN_BEARER = "Bearer sk-test-synthetic-p05c-not-real"
SYN_KEY = "sk-test-synthetic-p05c-not-real"
SYN_GEMINI = "AIzaSyFakeSyntheticKey0000000000000"
SYN_SENTRY = "https://abc123def456@o1.ingest.sentry.io/123"
SYN_BODY = """---
uid: hfa-synthetic-p05c0001
type: email_message
source: gmail
---
secret medical note body must not be logged
"""


@pytest.fixture(autouse=True)
def _clear_egress_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)


def test_redact_text_covers_tokens_and_dsn() -> None:
    blob = f"dsn={SYN_DSN} auth={SYN_BEARER} key={SYN_KEY} g={SYN_GEMINI} {SYN_SENTRY} {SYN_BODY}"
    out = redact_text(blob)
    assert SYN_DSN not in out
    assert "s3cretPass" not in out
    assert SYN_KEY not in out
    assert SYN_GEMINI not in out
    assert "abc123def456" not in out
    assert "secret medical note body" not in out
    assert REDACTED in out


def test_query_key_redacted() -> None:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/x:generateContent?key={SYN_GEMINI}"
    out = redact_text(f"_post_json failed url={url}")
    assert SYN_GEMINI not in out
    assert "key=" in out


def test_safe_diagnostic_id_is_stable_and_not_raw() -> None:
    a = safe_diagnostic_id("alice", "gmail")
    b = safe_diagnostic_id("alice", "gmail")
    c = safe_diagnostic_id("bob", "medical")
    assert a == b
    assert a != c
    assert "alice" not in a
    assert len(a) == 16


def test_formatter_redacts_args() -> None:
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(redacting_formatter())
    log = logging.getLogger("ppa.test_sensitive")
    log.handlers[:] = [handler]
    log.propagate = False
    log.setLevel(logging.ERROR)
    log.error("connect failed dsn=%s token=%s body=%s", SYN_DSN, SYN_BEARER, SYN_BODY)
    text = stream.getvalue()
    assert SYN_DSN not in text
    assert "s3cretPass" not in text
    assert SYN_KEY not in text
    assert "secret medical note body" not in text
    assert REDACTED in text


def test_configure_logging_installs_redacting_formatter() -> None:
    configure_logging()
    ensure_redacting_handlers()
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(redacting_formatter())
    root = logging.getLogger("ppa")
    root.addHandler(handler)
    try:
        logging.getLogger("ppa.gap").warning("PPA_TEST_PG_DSN=%s", SYN_DSN)
        text = stream.getvalue()
        assert "s3cretPass" not in text
        assert REDACTED in text
    finally:
        root.removeHandler(handler)


def test_data_boundaries_does_not_claim_ppa_encryption() -> None:
    path = Path(__file__).resolve().parents[1] / "archive_docs" / "DATA_BOUNDARIES.md"
    text = path.read_text(encoding="utf-8")
    assert "PPA does not encrypt" in text
    assert "not implemented" in text.lower() or "not engine guarantees" in text
    assert "LUKS" in text
    # Mentioned only as something PPA does not implement.
    lowered = text.lower()
    assert "ppa encrypts" not in lowered
    assert "encrypted at rest by ppa" not in lowered
