"""Email body preprocessing: HTML -> clean text for extractor parsing."""

from __future__ import annotations

import re

import html2text

_h2t = html2text.HTML2Text()
_h2t.ignore_links = False
_h2t.ignore_images = True
_h2t.body_width = 0  # no wrapping
_h2t.ignore_tables = False

_CSS_BLOCK = re.compile(r"<style[^>]*>.*?</style>", re.S | re.I)
_SCRIPT_BLOCK = re.compile(r"<script[^>]*>.*?</script>", re.S | re.I)
_TRACKING_PIXEL = re.compile(r"<img[^>]+(?:width|height)\s*=\s*[\"']?1[\"']?[^>]*>", re.I)
_ZERO_WIDTH = re.compile(r"[\u200b\u200c\u200d\ufeff\u034f]+")
_MULTI_BLANK = re.compile(r"\n{3,}")
_PROVENANCE_COMMENT = re.compile(r"<!--\s*provenance.*?-->", re.S | re.I)


def _strip_non_provenance_comments(html: str) -> str:
    """Remove HTML comments except blocks that mention provenance (vault metadata)."""

    def repl(m: re.Match[str]) -> str:
        block = m.group(0)
        if "provenance" in block.lower():
            return block
        return ""

    return re.sub(r"<!--.*?-->", repl, html, flags=re.S)


def clean_email_body(raw_body: str) -> str:
    """Convert raw email body (HTML or plaintext) to clean text.

    Preserves provenance comments. Strips CSS, scripts, tracking pixels,
    zero-width chars. Runs html2text for structural conversion.
    """
    if "<" not in raw_body or "<html" not in raw_body.lower()[:500]:
        # Likely already plaintext -- just clean up zero-width chars
        text = _ZERO_WIDTH.sub("", raw_body)
        return _MULTI_BLANK.sub("\n\n", text).strip()

    provenance_blocks = _PROVENANCE_COMMENT.findall(raw_body)
    text = _CSS_BLOCK.sub("", raw_body)
    text = _SCRIPT_BLOCK.sub("", text)
    text = _strip_non_provenance_comments(text)
    text = _TRACKING_PIXEL.sub("", text)
    text = _h2t.handle(text)
    text = _ZERO_WIDTH.sub("", text)
    text = _MULTI_BLANK.sub("\n\n", text)
    text = text.strip()
    if provenance_blocks:
        text = "\n".join(provenance_blocks) + "\n\n" + text
    return text.strip()
