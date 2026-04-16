"""Workflow F — document card enrichment (Phase 2.875)."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from archive_sync.llm_enrichment.staging_types import EntityMention

PROMPT_FILE = Path(__file__).resolve().parent.parent / "prompts" / "enrich_document.txt"
ENRICH_DOCUMENT_PROMPT_VERSION = "v3"

_DATE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_BODY_EXCERPT = 4000


def load_system_prompt() -> str:
    return PROMPT_FILE.read_text(encoding="utf-8")


def gate_document(_card_data: dict[str, Any]) -> bool:
    """No gating — process every document card (uid / vault-percent filters still apply in the runner)."""

    return True


def prefilter_document(_card_data: dict[str, Any]) -> tuple[bool, str]:
    """No prefilter — spreadsheets and tabular files are summarized too if the model can."""

    return True, "ok"


def should_skip_populated_document(_fm: dict[str, Any]) -> bool:
    """Never skip on frontmatter population.

    Ingestion often stores a long body-prefix as ``description``; that is not a real summary.
    We always run the LLM so ``summary`` and ``description`` can be replaced.
    """

    return False


def document_content_hash(fm: dict[str, Any], body: str) -> str:
    excerpt = body[:_BODY_EXCERPT] if body else ""
    payload = {
        "filename": str(fm.get("filename") or ""),
        "document_type": str(fm.get("document_type") or ""),
        "extension": str(fm.get("extension") or ""),
        "title": str(fm.get("title") or ""),
        "file_modified_at": str(fm.get("file_modified_at") or ""),
        "body_excerpt": excerpt,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def render_user_message(fm: dict[str, Any], body: str) -> str:
    excerpt = body[:_BODY_EXCERPT] if body else ""
    return (
        f"Filename: {fm.get('filename') or ''}\n"
        f"Type: {fm.get('document_type') or ''} ({fm.get('extension') or ''})\n"
        f"Current title: {fm.get('title') or ''}\n"
        f"File date: {fm.get('file_modified_at') or ''}\n"
        f"\n--- DOCUMENT TEXT (first {_BODY_EXCERPT} chars) ---\n"
        f"{excerpt}"
    )


def _norm_ws(s: str) -> str:
    return " ".join(s.split())


def _compact_summary_line(text: str, max_len: int = 200) -> str:
    """One line for BaseCard.summary (search snippets, card lists)."""

    t = _norm_ws(text).strip()
    if not t:
        return ""
    if len(t) <= max_len:
        return t
    cut = t[: max_len - 1].rsplit(" ", 1)[0]
    return (cut or t[: max_len]).rstrip(",;:") + "…"


def _title_needs_cleanup(fm: dict[str, Any]) -> bool:
    flags = fm.get("quality_flags") or []
    if not isinstance(flags, list):
        flags = []
    if "title_from_filename" in {str(x) for x in flags}:
        return True
    title = str(fm.get("title") or "")
    if "{\\" in title or title.strip().startswith("{\\rtf"):
        return True
    fn = str(fm.get("filename") or "")
    if fn and title.strip() == fn.strip():
        return True
    return False


def parse_document_response(
    data: dict[str, Any],
    *,
    fm: dict[str, Any],
    body: str = "",
    source_uid: str,
    run_id: str,
) -> tuple[dict[str, Any], list[EntityMention]]:
    """Map LLM JSON to vault field updates and staged entity rows (only when allowed by card state)."""

    field_updates: dict[str, Any] = {}

    raw_title = str(data.get("title") or "").strip()
    if raw_title and _title_needs_cleanup(fm):
        field_updates["title"] = raw_title
        flags = fm.get("quality_flags") or []
        if isinstance(flags, list) and flags:
            new_flags = [str(x) for x in flags if str(x) != "title_from_filename"]
            if new_flags != [str(x) for x in flags]:
                field_updates["quality_flags"] = new_flags

    raw_desc = str(data.get("description") or "").strip()
    if raw_desc:
        field_updates["description"] = raw_desc

    raw_date = data.get("document_date")
    if raw_date is not None:
        ds = str(raw_date).strip()
        existing_dd = str(fm.get("document_date") or "").strip()
        if ds and _DATE_ISO.match(ds) and not existing_dd:
            field_updates["document_date"] = ds

    raw_summary = str(data.get("summary") or "").strip()
    if raw_summary:
        field_updates["summary"] = raw_summary
    elif "description" in field_updates:
        field_updates["summary"] = _compact_summary_line(field_updates["description"])
    elif "title" in field_updates:
        field_updates["summary"] = _compact_summary_line(field_updates["title"])

    entities: list[EntityMention] = []
    raw_mentions = data.get("entity_mentions") or []
    if isinstance(raw_mentions, list):
        for m in raw_mentions:
            if not isinstance(m, dict):
                continue
            et = str(m.get("type") or "").strip().lower()
            if et == "org":
                et = "organization"
            if et not in ("person", "place", "organization"):
                continue
            name = str(m.get("name") or "").strip()
            if not name:
                continue
            ctx = m.get("context")
            if not isinstance(ctx, dict):
                ctx = {}
            conf = m.get("confidence")
            try:
                c = float(conf) if conf is not None else 0.75
            except (TypeError, ValueError):
                c = 0.75
            entities.append(
                EntityMention(
                    source_card_uid=source_uid,
                    source_card_type="document",
                    workflow="document_enrichment",
                    entity_type=et,
                    raw_text=name,
                    context=ctx,
                    confidence=max(0.0, min(1.0, c)),
                    run_id=run_id,
                )
            )

    return field_updates, entities


def response_to_cache_payload(parsed: dict[str, Any] | None) -> dict[str, Any]:
    return parsed if isinstance(parsed, dict) else {"_error": "invalid_json", "raw": str(parsed)}
