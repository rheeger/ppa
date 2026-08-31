"""Hosted OCR opt-in for firecrawl-anydoc (NeedsOcr → Firecrawl Parse).

anydoc converts locally by default. Scanned/image-only PDFs raise NeedsOcrError
unless ``ocr="hosted"``, which uploads the whole file to Firecrawl Parse.

We only enable hosted OCR when a key is present (env ``FIRECRAWL_API_KEY`` or
``~/.ppa/firecrawl_key.txt``). Keyless hosted OCR is intentionally not used.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Literal

log = logging.getLogger("ppa.anydoc_ocr")

OcrMode = Literal["hosted", "reject"]

_REJECT_LOGGED = False


def firecrawl_key_path() -> Path:
    return Path.home() / ".ppa" / "firecrawl_key.txt"


def load_firecrawl_api_key() -> str:
    """Return the Firecrawl key from env, else ``~/.ppa/firecrawl_key.txt``.

    When the file is used, copy it into ``FIRECRAWL_API_KEY`` so worker
    processes and anydoc's own fallback see the same value. Never logs the key.
    """

    existing = (os.environ.get("FIRECRAWL_API_KEY") or "").strip()
    if existing:
        return existing
    try:
        key = firecrawl_key_path().read_text(encoding="utf-8").strip()
    except OSError:
        return ""
    if key:
        os.environ["FIRECRAWL_API_KEY"] = key
    return key


def anydoc_ocr_mode() -> OcrMode:
    """``hosted`` when a key is present, otherwise ``reject`` (log once)."""

    global _REJECT_LOGGED
    if load_firecrawl_api_key():
        return "hosted"
    if not _REJECT_LOGGED:
        _REJECT_LOGGED = True
        log.warning(
            "anydoc hosted OCR disabled: set FIRECRAWL_API_KEY or write %s "
            "(scanned PDFs stay NeedsOcr / reject)",
            firecrawl_key_path(),
        )
    return "reject"


def anydoc_ocr_kwargs() -> dict[str, str]:
    """Keyword args for ``anydoc.to_markdown`` / ``to_markdown_bytes``."""

    key = load_firecrawl_api_key()
    if key:
        return {"ocr": "hosted", "api_key": key}
    _ = anydoc_ocr_mode()  # log-once reject
    return {"ocr": "reject"}


def reset_ocr_reject_log() -> None:
    """Test helper: allow the missing-key warning to fire again."""

    global _REJECT_LOGGED
    _REJECT_LOGGED = False
