"""Compact evidence listing: dated hits, stack pointers, optional narrative.

``archive_evidence`` / ``ppa evidence`` return uid/date/type/title/why plus
parent/attachment/duplicate UIDs — not card bodies. Other tools (search,
hybrid, query, read) stay available for wide scans and full reads.
"""

from __future__ import annotations

import re
from typing import Any

from archive_cli.index_config import _activity_date, _format_activity_at

DEFAULT_EVIDENCE_LIMIT = 12
TITLE_CHARS = 80
SUPPORT_CHARS = 120

# Frontmatter / edge field names that are stack pointers, not prose.
_ATTACHMENT_FIELDS = frozenset({"attachments"})
_DUPLICATE_FIELDS = frozenset({"duplicates"})
_PARENT_FIELDS = frozenset({"message", "thread", "source_email", "parent"})

_WIKILINK_RE = re.compile(r"\[\[([^\]|#]+)(?:[|#][^\]]*)?\]\]")
_UID_RE = re.compile(r"^hfa-[a-z0-9-]+$", re.IGNORECASE)

# Keys that must never appear on a compact hit (full extracts).
_BODY_KEYS = frozenset(
    {
        "content",
        "body",
        "preview",
        "extracted_text",
        "ocr",
        "search_text",
        "text",
        "markdown",
    }
)


def clamp_evidence_limit(limit: int | None) -> int:
    """Floor invalid/empty limits at 1. No upper cap — raise ``limit`` to go wide."""
    raw = DEFAULT_EVIDENCE_LIMIT if limit is None else int(limit)
    return max(raw, 1)


def uid_from_ref(value: Any) -> str:
    """Normalize a wikilink, slug, or bare UID to a card UID when possible."""
    text = str(value or "").strip().strip("[]").split("|", 1)[0].split("#", 1)[0].strip()
    if not text:
        return ""
    if _UID_RE.match(text):
        return text
    # Wikilink target that is already a UID-shaped slug.
    if text.startswith("hfa-"):
        return text
    return ""


def uids_from_frontmatter_list(value: Any) -> list[str]:
    """Collect UIDs from a frontmatter list of wikilinks or bare UIDs."""
    if value is None:
        return []
    items = value if isinstance(value, (list, tuple)) else [value]
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        raw = str(item or "")
        found = [uid_from_ref(m.group(1)) for m in _WIKILINK_RE.finditer(raw)]
        if not found:
            found = [uid_from_ref(raw)]
        for uid in found:
            if uid and uid not in seen:
                seen.add(uid)
                out.append(uid)
    return out


def stack_pointers_from_frontmatter(frontmatter: dict[str, Any]) -> dict[str, Any]:
    """Attachment / duplicate / parent UIDs from one card's frontmatter (links only)."""
    attachment_uids = uids_from_frontmatter_list(frontmatter.get("attachments"))
    duplicate_uids = uids_from_frontmatter_list(frontmatter.get("duplicates"))
    parent_uid = ""
    for key in ("message", "thread", "source_email", "parent"):
        parent_uid = uid_from_ref(frontmatter.get(key, ""))
        if parent_uid:
            break
    return {
        "attachment_uids": attachment_uids,
        "duplicate_uids": duplicate_uids,
        "parent_uid": parent_uid,
    }


def empty_pointers() -> dict[str, Any]:
    return {"attachment_uids": [], "duplicate_uids": [], "parent_uid": ""}


def merge_pointer_maps(base: dict[str, dict[str, Any]], extra: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {uid: {**empty_pointers(), **dict(ptr)} for uid, ptr in base.items()}
    for uid, ptr in extra.items():
        slot = out.setdefault(uid, empty_pointers())
        for key in ("attachment_uids", "duplicate_uids"):
            seen = set(slot.get(key) or [])
            merged = list(slot.get(key) or [])
            for item in ptr.get(key) or []:
                text = str(item or "").strip()
                if text and text not in seen:
                    seen.add(text)
                    merged.append(text)
            slot[key] = merged
        if not slot.get("parent_uid") and ptr.get("parent_uid"):
            slot["parent_uid"] = str(ptr["parent_uid"])
    return out


def apply_edge_to_pointers(
    pointers: dict[str, dict[str, Any]],
    *,
    source_uid: str,
    target_uid: str,
    field_name: str,
) -> None:
    """Fold one edges-table row into a uid → pointers map."""
    src = str(source_uid or "").strip()
    tgt = str(target_uid or "").strip()
    field = str(field_name or "").strip().lower()
    if not src or not tgt:
        return
    slot = pointers.setdefault(src, empty_pointers())
    if field in _ATTACHMENT_FIELDS:
        if tgt not in slot["attachment_uids"]:
            slot["attachment_uids"].append(tgt)
        child = pointers.setdefault(tgt, empty_pointers())
        if not child.get("parent_uid"):
            child["parent_uid"] = src
    elif field in _DUPLICATE_FIELDS:
        if tgt not in slot["duplicate_uids"]:
            slot["duplicate_uids"].append(tgt)
        other = pointers.setdefault(tgt, empty_pointers())
        if src not in other["duplicate_uids"]:
            other["duplicate_uids"].append(src)
    elif field in _PARENT_FIELDS:
        if not slot.get("parent_uid"):
            slot["parent_uid"] = tgt


def _one_line(text: Any, limit: int) -> str:
    cleaned = " ".join(str(text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 1)].rstrip() + "…"


def support_for_row(row: dict[str, Any], *, question: str = "") -> str:
    """One-line why this hit is listed — never a body extract."""
    bits: list[str] = []
    matched = str(row.get("matched_by") or "").strip()
    if matched:
        bits.append(matched)
    if row.get("exact_match"):
        bits.append("exact")
    card_type = str(row.get("type") or "").strip()
    if card_type:
        bits.append(card_type)
    q = question.strip()
    if q:
        bits.append(f"matches {q!r}")
    elif not bits:
        bits.append("filter")
    return _one_line("; ".join(bits), SUPPORT_CHARS)


def row_uid(row: dict[str, Any]) -> str:
    for key in ("card_uid", "uid"):
        text = str(row.get(key) or "").strip()
        if text:
            return text
    return ""


def row_date(row: dict[str, Any]) -> str:
    return _activity_date(row.get("activity_at") or row.get("created") or row.get("date"))


def compact_hit(
    row: dict[str, Any],
    *,
    pointers: dict[str, Any] | None = None,
    question: str = "",
) -> dict[str, Any]:
    """Build a body-free evidence hit from a search/query/timeline row."""
    uid = row_uid(row)
    date = row_date(row)
    recency = _format_activity_at(row.get("activity_at") or row.get("created") or "")
    ptr = pointers or empty_pointers()
    hit = {
        "uid": uid,
        "date": date,
        "type": str(row.get("type") or "").strip(),
        "title": _one_line(row.get("summary") or row.get("title") or row.get("rel_path") or uid, TITLE_CHARS),
        "support": support_for_row(row, question=question),
        "recency": recency or date,
        "rel_path": str(row.get("rel_path") or "").strip(),
        "parent_uid": str(ptr.get("parent_uid") or "").strip(),
        "attachment_uids": list(ptr.get("attachment_uids") or []),
        "duplicate_uids": list(ptr.get("duplicate_uids") or []),
        "corpus_state": str(row.get("corpus_state") or "active").strip() or "active",
    }
    return hit


def compact_hits(
    rows: list[dict[str, Any]],
    *,
    pointers_by_uid: dict[str, dict[str, Any]] | None = None,
    question: str = "",
    chronological: bool = True,
) -> list[dict[str, Any]]:
    """Compact retrieve rows; drop bodies; sort dated (chrono) or recency-first."""
    ptrs = pointers_by_uid or {}
    hits = [compact_hit(row, pointers=ptrs.get(row_uid(row), empty_pointers()), question=question) for row in rows]
    if chronological:
        hits.sort(key=lambda h: (h["date"] == "", h["date"], h.get("rel_path") or ""))
    else:
        hits.sort(key=lambda h: (h["recency"] == "", h["recency"], h.get("rel_path") or ""), reverse=True)
    return hits


def assert_compact_payload(payload: dict[str, Any]) -> None:
    """Raise AssertionError if a compact payload smuggles a full extract."""
    hits = payload.get("hits") if isinstance(payload, dict) else None
    if not isinstance(hits, list):
        raise AssertionError("compact payload missing hits")
    for hit in hits:
        if not isinstance(hit, dict):
            raise AssertionError("hit is not an object")
        overlap = _BODY_KEYS.intersection(hit)
        if overlap:
            raise AssertionError(f"compact hit leaked body keys: {sorted(overlap)}")
        for key in ("title", "support"):
            text = str(hit.get(key) or "")
            if len(text) > SUPPORT_CHARS + 20:
                raise AssertionError(f"{key} too long for compact listing")


def narrative_outline(hits: list[dict[str, Any]]) -> str:
    """Short dated outline — one bullet per event, UIDs only, no extracts."""
    if not hits:
        return "No dated evidence."
    lines: list[str] = []
    for hit in hits:
        date = hit.get("date") or "undated"
        title = hit.get("title") or hit.get("uid") or "card"
        uid = hit.get("uid") or ""
        card_type = hit.get("type") or "card"
        cite = f" ({uid})" if uid else ""
        extra: list[str] = []
        parent = hit.get("parent_uid") or ""
        if parent:
            extra.append(f"parent={parent}")
        atts = hit.get("attachment_uids") or []
        if atts:
            extra.append(f"attachments={','.join(str(a) for a in atts)}")
        dups = hit.get("duplicate_uids") or []
        if dups:
            extra.append(f"duplicates={','.join(str(d) for d in dups)}")
        suffix = f" [{'; '.join(extra)}]" if extra else ""
        lines.append(f"- {date} — {card_type}: {title}{cite}{suffix}")
    return "\n".join(lines)
