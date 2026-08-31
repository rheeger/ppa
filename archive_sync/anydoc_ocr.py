"""Local-first anydoc OCR. Hosted Firecrawl only after ``NeedsOcr``.

anydoc converts locally by default. Scanned/image-only PDFs raise
``NeedsOcrError``. Hosted OCR (``ocr="hosted"``) uploads the whole file to
Firecrawl Parse and is credit-based.

Policy for every call site:

1. Always call anydoc with ``ocr="reject"`` first.
2. Retry with hosted only when that raises ``NeedsOcr``, a Firecrawl key is
   present (``FIRECRAWL_API_KEY`` or ``~/.ppa/firecrawl_key.txt``), and the
   caller did not pass ``allow_hosted=False``.
3. Never pass ``ocr="hosted"`` as the first attempt.

``anydoc_ocr_kwargs()`` always returns reject kwargs so leftover call sites
cannot skip the local attempt.
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


def hosted_ocr_available() -> bool:
    return bool(load_firecrawl_api_key())


def anydoc_ocr_mode() -> OcrMode:
    """First-attempt mode. Always ``reject`` — hosted is a NeedsOcr retry only."""

    return "reject"


def anydoc_ocr_kwargs() -> dict[str, str]:
    """Keyword args for the **first** anydoc call. Always local-only."""

    return {"ocr": "reject"}


def anydoc_hosted_ocr_kwargs() -> dict[str, str] | None:
    """Hosted retry kwargs, or ``None`` when no key is configured."""

    global _REJECT_LOGGED
    key = load_firecrawl_api_key()
    if key:
        return {"ocr": "hosted", "api_key": key}
    if not _REJECT_LOGGED:
        _REJECT_LOGGED = True
        log.warning(
            "anydoc hosted OCR disabled: set FIRECRAWL_API_KEY or write %s "
            "(scanned PDFs stay NeedsOcr / reject)",
            firecrawl_key_path(),
        )
    return None


def is_needs_ocr(exc: BaseException) -> bool:
    return type(exc).__name__.endswith("NeedsOcrError")


def to_markdown_local_first(
    path: str | Path,
    *,
    allow_hosted: bool = True,
    data: bytes | None = None,
) -> tuple[str, str]:
    """Convert via anydoc: local ``ocr=reject`` first, hosted only on NeedsOcr.

    Returns ``(markdown, text_source)`` where ``text_source`` is ``anydoc`` or
    ``anydoc_hosted``. Re-raises the local error when hosted is unavailable,
    disallowed, or also fails.
    """

    import anydoc

    path_str = str(path)
    local_exc: BaseException | None = None
    try:
        if data is not None:
            raw = anydoc.to_markdown_bytes(data, ocr="reject")
        else:
            raw = anydoc.to_markdown(path_str, ocr="reject")
        text = str(raw or "").strip()
        return text, "anydoc"
    except Exception as exc:
        if not is_needs_ocr(exc):
            raise
        local_exc = exc

    if not allow_hosted:
        assert local_exc is not None
        raise local_exc

    hosted = anydoc_hosted_ocr_kwargs()
    if hosted is None:
        assert local_exc is not None
        raise local_exc

    log.info("anydoc NeedsOcr; hosted retry path=%s", path_str)
    try:
        if data is not None:
            raw = anydoc.to_markdown_bytes(data, ocr="hosted", api_key=hosted["api_key"])
        else:
            raw = anydoc.to_markdown(path_str, **hosted)
    except Exception:
        assert local_exc is not None
        raise local_exc
    return str(raw or "").strip(), "anydoc_hosted"


def reset_ocr_reject_log() -> None:
    """Test helper: allow the missing-key warning to fire again."""

    global _REJECT_LOGGED
    _REJECT_LOGGED = False
