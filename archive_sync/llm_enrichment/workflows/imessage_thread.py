"""Workflow B — iMessage / Beeper thread enrichment (Phase 2.875)."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from archive_sync.llm_enrichment.staging_types import EntityMention
from archive_sync.llm_enrichment.threads import (MessageStubIndex,
                                                 ThreadMessage,
                                                 render_imessage_chunk_for_llm)
from archive_sync.llm_enrichment.workflows.email_thread import \
    strip_thread_summary_boilerplate

PROMPT_FILE = Path(__file__).resolve().parent.parent / "prompts" / "enrich_imessage_thread.txt"
ENRICH_IMESSAGE_THREAD_PROMPT_VERSION = "v2"

CHUNK_SIZE = 800
CHUNK_OVERLAP = 50


def load_system_prompt() -> str:
    return PROMPT_FILE.read_text(encoding="utf-8")


def gate_imessage_thread_card(card: dict[str, Any]) -> bool:
    """Threads with at least one message."""
    return int(card.get("message_count") or 0) > 0


def prefilter_imessage_thread(message_stubs: list[dict[str, Any]]) -> tuple[bool, str]:
    """Require at least one outbound message from the archive owner."""

    if not message_stubs:
        return False, "no_message_stubs"
    if not any(bool(m.get("is_from_me")) for m in message_stubs):
        return False, "no_from_me"
    return True, "from_me"


def load_message_stub_frontmatters_for_thread(
    thread_card: dict[str, Any],
    *,
    msg_index: MessageStubIndex,
) -> list[dict[str, Any]]:
    """Resolve message wikilinks → frontmatters via bulk in-memory index (no per-link SQL)."""

    from hfa.thread_hash import slug_from_wikilink

    raw_links = thread_card.get("messages") or []
    if not isinstance(raw_links, list):
        return []
    out: list[dict[str, Any]] = []
    for wikilink in raw_links:
        slug = slug_from_wikilink(str(wikilink))
        if not slug:
            continue
        rel = msg_index.resolve_slug(slug)
        if not rel:
            continue
        fm = msg_index.frontmatter(rel)
        if fm is None:
            continue
        out.append(fm)
    return out


def thread_display_label(
    thread_card: dict[str, Any],
    *,
    card_type: str,
    handle_names: dict[str, str] | None = None,
) -> str:
    _names = handle_names or {}

    if card_type == "beeper_thread":
        title = str(thread_card.get("thread_title") or "").strip()
        if title:
            return title
        names = thread_card.get("counterpart_names") or []
        if isinstance(names, list) and names:
            return ", ".join(str(x) for x in names[:4] if x)
        ids = thread_card.get("counterpart_identifiers") or []
        if isinstance(ids, list) and ids:
            resolved = [_names.get(str(x).strip()) or str(x) for x in ids[:4] if x]
            return ", ".join(resolved)
        return str(thread_card.get("beeper_room_id") or "chat").strip() or "chat"

    dn = str(thread_card.get("display_name") or "").strip()
    if dn and not dn.startswith("chat"):
        return dn

    handles = thread_card.get("participant_handles") or []
    if isinstance(handles, list) and handles:
        resolved = [_names.get(str(h).strip()) or str(h) for h in handles[:4] if h]
        label = ", ".join(resolved)
        if label:
            return label

    cid = str(thread_card.get("chat_identifier") or "").strip()
    if cid and not cid.startswith("chat"):
        return cid

    return str(thread_card.get("imessage_chat_id") or "chat").strip() or "chat"


def thread_context_header(thread_card: dict[str, Any], *, card_type: str) -> str:
    lines: list[str] = []
    if card_type == "beeper_thread":
        proto = str(thread_card.get("protocol") or "").strip()
        bridge = str(thread_card.get("bridge_name") or "").strip()
        if proto:
            lines.append(f"Protocol: {proto}")
        if bridge:
            lines.append(f"Bridge: {bridge}")
        cn = thread_card.get("counterpart_names") or []
        if isinstance(cn, list) and cn:
            lines.append("Counterparts: " + ", ".join(str(x) for x in cn[:10] if x))
    else:
        svc = str(thread_card.get("service") or "").strip()
        if svc:
            lines.append(f"Service: {svc}")
        if bool(thread_card.get("is_group")):
            lines.append("Group chat: yes")
    return "\n".join(lines)


def dedupe_conversations(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for c in items:
        dr = c.get("date_range")
        if not isinstance(dr, list):
            dr = []
        ds = str(dr[0]) if len(dr) > 0 else ""
        de = str(dr[1]) if len(dr) > 1 else ""
        sm = str(c.get("summary") or "").strip()
        key = (ds, de, sm[:240].lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out


def compose_thread_summary(
    display_label: str,
    conversations: list[dict[str, Any]],
    *,
    max_conversations: int = 10,
) -> str:
    if not conversations:
        return ""

    def first_date(c: dict[str, Any]) -> str:
        dr = c.get("date_range")
        if isinstance(dr, list) and dr:
            return str(dr[0])
        return ""

    sorted_conv = sorted(conversations, key=first_date, reverse=True)
    total = len(conversations)
    all_first: list[str] = []
    for c in conversations:
        d = first_date(c)
        if d:
            all_first.append(d)
    first_date_s = min(all_first) if all_first else ""

    recent = sorted_conv[:max_conversations]
    since = f" since {first_date_s}" if first_date_s else ""
    lines = [
        f"Active conversation with {display_label} ({total} meaningful conversation(s){since}).",
        "",
        "Recent:",
        "",
    ]
    for c in recent:
        dr = c.get("date_range")
        if not isinstance(dr, list):
            dr = []
        ds = str(dr[0]) if len(dr) > 0 else "?"
        de = str(dr[1]) if len(dr) > 1 else ds
        sm = str(c.get("summary") or "").strip()
        lines.append(f"- {sm} ({ds}–{de})")
    return "\n".join(lines).strip()


def parse_chunk_conversations(data: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    raw = data.get("conversations")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for c in raw:
        if isinstance(c, dict) and str(c.get("summary") or "").strip():
            out.append(c)
    return out


def chunk_cache_segment_hash(
    messages: list[ThreadMessage],
    display_label: str,
    context_header: str,
) -> str:
    text = render_imessage_chunk_for_llm(
        messages,
        display_label=display_label,
        context_header=context_header,
    )
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_outputs_from_conversations(
    conversations: list[dict[str, Any]],
    *,
    display_label: str,
    source_uid: str,
    source_card_type: str,
    run_id: str,
) -> tuple[dict[str, Any], list[EntityMention]]:
    raw_summary = compose_thread_summary(display_label, conversations)
    summary = strip_thread_summary_boilerplate(raw_summary)
    field_updates: dict[str, Any] = {}
    if summary:
        field_updates["thread_summary"] = summary

    entities: list[EntityMention] = []
    wf = (
        "beeper_thread_enrichment"
        if source_card_type == "beeper_thread"
        else "imessage_thread_enrichment"
    )
    for c in conversations:
        ctx_base: dict[str, Any] = {
            "conversation_summary": str(c.get("summary") or "").strip(),
            "topics": c.get("topics") if isinstance(c.get("topics"), list) else [],
            "date_range": c.get("date_range"),
        }
        for name in c.get("people_mentioned") or []:
            if not isinstance(name, str):
                continue
            n = name.strip()
            if not n:
                continue
            entities.append(
                EntityMention(
                    source_card_uid=source_uid,
                    source_card_type=source_card_type,
                    workflow=wf,
                    entity_type="person",
                    raw_text=n,
                    context=ctx_base,
                    confidence=0.7,
                    run_id=run_id,
                )
            )
        for place in c.get("places_mentioned") or []:
            if not isinstance(place, str):
                continue
            p = place.strip()
            if not p:
                continue
            entities.append(
                EntityMention(
                    source_card_uid=source_uid,
                    source_card_type=source_card_type,
                    workflow=wf,
                    entity_type="place",
                    raw_text=p,
                    context=ctx_base,
                    confidence=0.7,
                    run_id=run_id,
                )
            )

    return field_updates, entities


def response_to_cache_payload(parsed: dict[str, Any] | None) -> dict[str, Any]:
    return parsed if isinstance(parsed, dict) else {"_error": "invalid_json", "raw": str(parsed)}
