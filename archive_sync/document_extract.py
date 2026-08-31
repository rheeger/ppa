"""Shared document/attachment extract: path or bytes → markdown + status.

Used by file-library ingest, Gmail attachment apply, ``extract-document-text``,
and ``extract-attachment-text``. One OCR stack (``archive_sync.anydoc_ocr``):

- local anydoc ``ocr="reject"`` first
- Firecrawl hosted only after ``NeedsOcr`` when a key exists and hosted is
  not skipped (tiny inline images, suppressed/marketing)

Skip rules are shared: 200MB, ``~$`` lockfiles, audio/video/zip; 60k trim;
incremental skip on ``extracted_text_sha`` of **source bytes**.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

from archive_sync.anydoc_ocr import is_needs_ocr, to_markdown_local_first
from archive_sync.extract_cache import CachedExtract, get_extract_cache

log = logging.getLogger("ppa.document_extract")

MAX_FILE_BYTES = 200 * 1024 * 1024
MAX_EXTRACT_CHARS = 60_000
TINY_IMAGE_BYTES = 50 * 1024

STATUS_EXTRACTED = "content_extracted"
STATUS_TOO_LARGE = "skipped_too_large"
STATUS_NON_DOC = "skipped_non_doc"
STATUS_MISSING = "skipped_missing"
STATUS_LOCKFILE = "skipped_lockfile"
STATUS_TINY_IMAGE = "skipped_tiny_image"
STATUS_SUPPRESSED = "skipped_suppressed"
STATUS_FAILED = "failed"
STATUS_NEEDS_OCR = "needs_ocr"

DONE_TEXT_SOURCES = frozenset({"anydoc", "anydoc_hosted", "html2text", "markitdown"})
DONE_SKIP_STATUSES = frozenset(
    {
        STATUS_TOO_LARGE,
        STATUS_NON_DOC,
        STATUS_LOCKFILE,
        STATUS_TINY_IMAGE,
        STATUS_SUPPRESSED,
    }
)
SUPPRESSED_CLASSIFICATIONS = frozenset(
    {
        "marketing",
        "automated_notification",
        "automated",
        "noise",
        "suppressed",
    }
)
TINY_IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"})
CONVERTIBLE_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".doc",
        ".docx",
        ".rtf",
        ".ppt",
        ".pptx",
        ".xls",
        ".xlsx",
        ".csv",
        ".html",
        ".htm",
        ".txt",
        ".md",
        ".png",
        ".jpg",
        ".jpeg",
        ".tif",
        ".tiff",
        ".webp",
        ".gif",
        ".bmp",
    }
)
ANYDOC_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".doc",
        ".docx",
        ".rtf",
        ".ppt",
        ".pptx",
        ".xls",
        ".xlsx",
        ".csv",
        ".png",
        ".jpg",
        ".jpeg",
        ".tif",
        ".tiff",
        ".webp",
        ".gif",
        ".bmp",
    }
)
SKIP_EXTENSIONS = frozenset(
    {
        ".mp3",
        ".m4a",
        ".wav",
        ".aac",
        ".flac",
        ".ogg",
        ".wma",
        ".opus",
        ".mp4",
        ".mov",
        ".avi",
        ".mkv",
        ".webm",
        ".m4v",
        ".wmv",
        ".zip",
        ".tar",
        ".gz",
        ".tgz",
        ".rar",
        ".7z",
        ".bz2",
        ".exe",
        ".dmg",
        ".pkg",
        ".iso",
        ".ics",
        ".vcf",
        ".dat",
        ".eml",
        ".msg",
        ".key",
        ".pages",
        ".numbers",
        ".psd",
        ".ai",
        ".eps",
        ".svg",
        ".heic",
    }
)

# Types we will actually convert. Anything else is a quiet skip.
EXTRACTABLE_EXTENSIONS = ANYDOC_EXTENSIONS | {".html", ".htm", ".txt", ".md"}
PLAIN_TEXT_EXTENSIONS = frozenset({".txt", ".md"})


class UnsupportedExtract(Exception):
    """File type is outside the anydoc / html / plain extract set."""


def is_extractable(filename: str, mime_type: str = "") -> bool:
    """True only for types we convert. Nameless Gmail tokens are not."""

    raw = (filename or "").strip()
    if not raw or raw.startswith("ANGjd"):
        return False
    if is_skippable_non_doc(raw, mime_type):
        return False
    suffix = Path(safe_filename(raw)).suffix.lower()
    if not suffix:
        suffix = Path(raw).suffix.lower()
    return suffix in EXTRACTABLE_EXTENSIONS


SKIP_MIME_PREFIXES = ("audio/", "video/")
SKIP_MIME_TYPES = frozenset(
    {
        "application/zip",
        "application/x-tar",
        "application/x-7z-compressed",
        "application/x-rar-compressed",
        "application/gzip",
        "application/x-gzip",
        "application/octet-stream",
        "application/ms-tnef",
        "text/calendar",
        "application/ics",
    }
)


@dataclass
class ExtractResult:
    status: str
    text: str = ""
    text_source: str = ""
    extracted_text_sha: str = ""
    filename: str = ""
    reason: str = ""


def bytes_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def trim_text(text: str, *, limit: int = MAX_EXTRACT_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "\n\n[truncated]"


def safe_filename(filename: str) -> str:
    name = Path(filename or "").name.strip()
    if not name or name in {".", ".."}:
        return "attachment"
    if len(name) > 120:
        suffix = Path(name).suffix.lower()[:12]
        digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
        return f"att-{digest}{suffix}"
    return name


def is_lockfile(filename: str) -> bool:
    return safe_filename(filename).startswith("~$")


def is_skippable_non_doc(filename: str, mime_type: str) -> bool:
    if is_lockfile(filename):
        return True
    suffix = Path(safe_filename(filename)).suffix.lower()
    mime = (mime_type or "").strip().lower()
    if suffix in SKIP_EXTENSIONS:
        return True
    if any(mime.startswith(prefix) for prefix in SKIP_MIME_PREFIXES):
        return True
    if mime in SKIP_MIME_TYPES and suffix not in CONVERTIBLE_EXTENSIONS:
        return True
    if suffix and suffix not in CONVERTIBLE_EXTENSIONS and not mime.startswith("image/"):
        if mime.startswith("application/") and suffix not in CONVERTIBLE_EXTENSIONS:
            return suffix not in {".pdf"}
        if mime.startswith("text/") and suffix not in CONVERTIBLE_EXTENSIONS:
            return suffix not in {".txt", ".md", ".html", ".htm", ".csv"}
    return False


def is_tiny_image(filename: str, size_bytes: int, mime_type: str = "") -> bool:
    suffix = Path(safe_filename(filename)).suffix.lower()
    mime = (mime_type or "").strip().lower()
    if size_bytes <= 0 or size_bytes > TINY_IMAGE_BYTES:
        return False
    if suffix in TINY_IMAGE_EXTENSIONS:
        return True
    return mime.startswith("image/") and suffix not in {".tif", ".tiff", ".pdf"}


def is_suppressed_classification(value: str) -> bool:
    return str(value or "").strip().lower() in SUPPRESSED_CLASSIFICATIONS


def _reuse_if_unchanged(
    *,
    sha: str,
    existing_sha: str,
    existing_status: str,
    existing_text: str,
    existing_text_source: str,
    filename: str,
) -> ExtractResult | None:
    if (
        existing_sha
        and existing_sha == sha
        and existing_status == STATUS_EXTRACTED
        and existing_text.strip()
    ):
        return ExtractResult(
            status=STATUS_EXTRACTED,
            text=existing_text,
            text_source=existing_text_source,
            extracted_text_sha=sha,
            filename=filename,
            reason="unchanged",
        )
    if existing_sha and existing_sha == sha and existing_status in DONE_SKIP_STATUSES:
        return ExtractResult(
            status=existing_status,
            extracted_text_sha=sha,
            filename=filename,
            reason="unchanged",
        )
    return None


def convert_document_to_markdown(
    source_path: Path,
    *,
    allow_hosted: bool = True,
    data: bytes | None = None,
) -> tuple[str, str]:
    """Return ``(markdown, text_source)``. Local anydoc first; hosted only on NeedsOcr.

    Unsupported types raise ``UnsupportedExtract`` — callers skip quietly.
    MarkItDown is not used as a fallback (it floods WARNING on unknown types).
    """

    suffix = source_path.suffix.lower()
    if suffix in ANYDOC_EXTENSIONS:
        try:
            text, source = to_markdown_local_first(
                source_path, allow_hosted=allow_hosted, data=data
            )
            if text:
                return text, source
        except Exception as exc:
            if is_needs_ocr(exc):
                raise
            name = type(exc).__name__
            if name in {"UnsupportedError", "EncryptedError"} or "not supported" in str(exc).lower():
                raise UnsupportedExtract(str(exc)) from exc
            log.debug("anydoc convert failed path=%s err=%s", source_path, exc)
            raise
    if suffix in {".html", ".htm"}:
        import html2text

        converter = html2text.HTML2Text()
        converter.ignore_links = False
        converter.ignore_images = True
        converter.body_width = 0
        converter.ignore_tables = False
        raw = (
            data.decode("utf-8", errors="ignore")
            if data is not None
            else source_path.read_text(encoding="utf-8", errors="ignore")
        )
        text = converter.handle(raw).strip()
        if text:
            return text, "html2text"
        raise UnsupportedExtract("empty html2text output")
    if suffix in PLAIN_TEXT_EXTENSIONS:
        raw = (
            data.decode("utf-8", errors="ignore")
            if data is not None
            else source_path.read_text(encoding="utf-8", errors="ignore")
        )
        text = raw.strip()
        if text:
            return text, "plain"
        raise UnsupportedExtract("empty plain text")
    raise UnsupportedExtract(f"unsupported suffix {suffix or '(none)'}")


def extract_from_path(
    path: Path,
    *,
    filename: str = "",
    mime_type: str = "",
    is_inline: bool = False,
    skip_hosted: bool = False,
    skip_extract: bool = False,
    existing_sha: str = "",
    existing_status: str = "",
    existing_text: str = "",
    existing_text_source: str = "",
) -> ExtractResult:
    name = safe_filename(filename or path.name)
    if skip_extract:
        return ExtractResult(status=STATUS_SUPPRESSED, filename=name, reason="suppressed")
    if is_lockfile(name):
        return ExtractResult(status=STATUS_LOCKFILE, filename=name, reason="lockfile")
    if not is_extractable(filename or path.name, mime_type):
        return ExtractResult(status=STATUS_NON_DOC, filename=name, reason="unsupported")
    try:
        size = path.stat().st_size
    except OSError:
        return ExtractResult(status=STATUS_MISSING, filename=name, reason="source_missing")
    if size > MAX_FILE_BYTES:
        return ExtractResult(status=STATUS_TOO_LARGE, filename=name, reason="too_large")
    try:
        data = path.read_bytes()
    except OSError:
        return ExtractResult(status=STATUS_MISSING, filename=name, reason="source_missing")
    return extract_from_bytes(
        data,
        filename=name,
        mime_type=mime_type,
        is_inline=is_inline,
        skip_hosted=skip_hosted,
        skip_extract=False,
        existing_sha=existing_sha,
        existing_status=existing_status,
        existing_text=existing_text,
        existing_text_source=existing_text_source,
        source_path=path,
    )


def extract_from_bytes(
    data: bytes,
    *,
    filename: str,
    mime_type: str = "",
    is_inline: bool = False,
    skip_hosted: bool = False,
    skip_extract: bool = False,
    existing_sha: str = "",
    existing_status: str = "",
    existing_text: str = "",
    existing_text_source: str = "",
    source_path: Path | None = None,
) -> ExtractResult:
    name = safe_filename(filename)
    if skip_extract:
        return ExtractResult(status=STATUS_SUPPRESSED, filename=name, reason="suppressed")
    if is_lockfile(name):
        return ExtractResult(status=STATUS_LOCKFILE, filename=name, reason="lockfile")
    if is_skippable_non_doc(name, mime_type) or not is_extractable(filename, mime_type):
        return ExtractResult(status=STATUS_NON_DOC, filename=name, reason="unsupported")
    if len(data) > MAX_FILE_BYTES:
        return ExtractResult(
            status=STATUS_TOO_LARGE,
            filename=name,
            extracted_text_sha=bytes_sha256(data),
            reason="too_large",
        )
    sha = bytes_sha256(data)
    reused = _reuse_if_unchanged(
        sha=sha,
        existing_sha=existing_sha,
        existing_status=existing_status,
        existing_text=existing_text,
        existing_text_source=existing_text_source,
        filename=name,
    )
    if reused is not None:
        return reused

    def _from_cached(cached: CachedExtract) -> ExtractResult:
        return ExtractResult(
            status=STATUS_EXTRACTED,
            text=cached.markdown,
            text_source=cached.text_source,
            extracted_text_sha=sha,
            filename=name,
            reason="hash_reuse",
        )

    cached = get_extract_cache().get(sha)
    if cached is not None and cached.markdown.strip():
        log.debug(
            "document extract hash-reuse sha=%s source=%s filename=%s",
            sha[:12],
            cached.text_source,
            name,
        )
        return _from_cached(cached)

    tiny = is_tiny_image(name, len(data), mime_type)
    if tiny and is_inline:
        return ExtractResult(
            status=STATUS_TINY_IMAGE,
            filename=name,
            extracted_text_sha=sha,
            reason="tiny_inline_image",
        )
    allow_hosted = not skip_hosted and not tiny
    path = source_path if source_path is not None else Path(name)
    with get_extract_cache().inflight(sha) as inflight_hit:
        if inflight_hit is not None and inflight_hit.markdown.strip():
            return _from_cached(inflight_hit)
        try:
            text, text_source = convert_document_to_markdown(
                path, allow_hosted=allow_hosted, data=data if source_path is None else None
            )
        except Exception as exc:
            if is_needs_ocr(exc):
                return ExtractResult(
                    status=STATUS_NEEDS_OCR,
                    filename=name,
                    extracted_text_sha=sha,
                    reason="needs_ocr",
                )
            if isinstance(exc, UnsupportedExtract) or type(exc).__name__ in {
                "UnsupportedError",
                "EncryptedError",
            }:
                log.debug("document extract skip filename=%s err=%s", name, exc)
                return ExtractResult(
                    status=STATUS_NON_DOC,
                    filename=name,
                    extracted_text_sha=sha,
                    reason="unsupported",
                )
            log.warning("document extract failed filename=%s err=%s", name, exc)
            return ExtractResult(
                status=STATUS_FAILED, filename=name, extracted_text_sha=sha, reason=str(exc)
            )
        text = trim_text(str(text or "").strip())
        if not text:
            status = STATUS_NEEDS_OCR if tiny else STATUS_FAILED
            return ExtractResult(
                status=status,
                filename=name,
                extracted_text_sha=sha,
                reason="empty_output",
            )
        get_extract_cache().put(sha, text, text_source, STATUS_EXTRACTED)
        return ExtractResult(
            status=STATUS_EXTRACTED,
            text=text,
            text_source=text_source,
            extracted_text_sha=sha,
            filename=name,
        )
