"""Filename + wikilink list on email message cards. Never OCR markdown."""

from __future__ import annotations

import re

from archive_sync.document_extract import safe_filename

ATTACHMENTS_DIR = "Attachments"
ATTACHMENTS_SECTION_HEADING = "## Attachments"
ATTACHMENTS_LIST_SENTINEL = "<!-- ppa-attachment-list -->"
# Legacy OCR dump written by 535679b — strip on sight, never re-emit.
ATTACHMENTS_SECTION_SENTINEL = "<!-- ppa-attachment-text -->"
_OCR_DUMP_RE = re.compile(
    rf"\n*{re.escape(ATTACHMENTS_SECTION_SENTINEL)}\n{re.escape(ATTACHMENTS_SECTION_HEADING)}\n.*\Z",
    re.DOTALL,
)
_LIST_RE = re.compile(
    rf"\n*{re.escape(ATTACHMENTS_LIST_SENTINEL)}\n{re.escape(ATTACHMENTS_SECTION_HEADING)}\n.*\Z",
    re.DOTALL,
)


def strip_ocr_dump_section(body: str) -> str:
    """Remove a legacy OCR dump from an email message body."""

    return _OCR_DUMP_RE.sub("", str(body or "")).rstrip()


def strip_attachments_section(body: str) -> str:
    text = strip_ocr_dump_section(body)
    return _LIST_RE.sub("", text).rstrip()


def extract_attachments_section(body: str) -> str:
    text = strip_ocr_dump_section(body)
    match = _LIST_RE.search(text)
    if not match:
        return ""
    return match.group(0).strip()


def render_attachment_list(items: list[tuple[str, str]]) -> str:
    """Filename + wikilink list only — never extracted markdown."""

    lines: list[str] = []
    seen: set[str] = set()
    for uid, filename in items:
        slug = str(uid or "").strip().strip("[]")
        name = safe_filename(filename)
        if not slug or slug in seen:
            continue
        seen.add(slug)
        lines.append(f"- [[{slug}]] {name}")
    if not lines:
        return ""
    return f"{ATTACHMENTS_LIST_SENTINEL}\n{ATTACHMENTS_SECTION_HEADING}\n\n" + "\n".join(lines)


def merge_message_body(raw_body: str, section: str) -> str:
    stripped = strip_attachments_section(raw_body)
    section = (section or "").strip()
    if not section:
        return stripped
    if stripped:
        return f"{stripped}\n\n{section}"
    return section


def preserve_message_attachments_section(incoming_body: str, existing_body: str) -> str:
    """Keep a filename list; drop any legacy OCR dump."""

    incoming = strip_ocr_dump_section(incoming_body)
    existing = strip_ocr_dump_section(existing_body)
    incoming_section = extract_attachments_section(incoming)
    if incoming_section:
        return merge_message_body(strip_attachments_section(incoming), incoming_section)
    existing_section = extract_attachments_section(existing)
    if existing_section:
        return merge_message_body(strip_attachments_section(incoming), existing_section)
    return incoming
