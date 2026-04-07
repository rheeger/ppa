"""Tests for archive_sync.extractors.preprocessing."""

from __future__ import annotations

from archive_sync.extractors.preprocessing import clean_email_body


def test_plaintext_passthrough():
    body = "Hello\n\nOrder from Joe\n\nTotal: $10\n"
    assert clean_email_body(body) == body.strip()


def test_zero_width_chars_stripped():
    body = "Hi\u200bthere\uFEFF"
    assert clean_email_body(body) == "Hithere"


def test_provenance_comments_preserved_in_html():
    raw = """<html><body>
<!-- provenance: keep me -->
<p>Shipped</p>
<!-- remove me -->
</body></html>"""
    out = clean_email_body(raw)
    assert "provenance: keep me" in out
    assert "remove me" not in out


def test_html_email_converted_to_text():
    raw = """<html><body>
<style>.x{display:none}</style>
<p>Tracking: <b>1Z999AA10123456784</b></p>
</body></html>"""
    out = clean_email_body(raw)
    assert "1Z999AA10123456784" in out.replace(" ", "")


def test_tracking_pixels_removed():
    raw = """<html><body>
<p>Hello</p>
<img src="https://x.com/p.gif" width="1" height="1" alt="" />
<p>End</p>
</body></html>"""
    out = clean_email_body(raw)
    assert "p.gif" not in out
