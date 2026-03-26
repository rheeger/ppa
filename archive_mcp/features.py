"""Shared canonical feature extraction helpers for ppa."""

from __future__ import annotations

import json
from typing import Any

from .contracts import ArchiveContext

CHUNKABLE_TEXT_FIELDS = ("summary", "subject", "snippet", "description", "thread_summary", "title")
EXTERNAL_ID_FIELDS = {
    "source_id": "canonical",
    "gmail_thread_id": "gmail",
    "gmail_message_id": "gmail",
    "gmail_history_id": "gmail",
    "message_id_header": "email",
    "attachment_id": "attachment",
    "content_id": "attachment",
    "calendar_id": "calendar",
    "event_id": "calendar",
    "event_etag": "calendar",
    "ical_uid": "calendar",
    "invite_ical_uid": "calendar",
    "invite_event_id_hint": "calendar",
    "event_id_hint": "calendar",
    "imessage_chat_id": "imessage",
    "imessage_message_id": "imessage",
    "beeper_room_id": "beeper",
    "beeper_event_id": "beeper",
    "photos_asset_id": "photos",
    "otter_meeting_id": "otter",
    "otter_conversation_id": "otter",
    "repository_id": "github_repo",
    "repository_name_with_owner": "github_repo",
    "commit_sha": "github_commit",
    "github_thread_id": "github_thread",
    "github_message_id": "github_message",
    "number": "github_thread_number",
    "review_commit_sha": "github_commit",
    "original_commit_sha": "github_commit",
    "encounter_source_id": "medical_encounter",
}
EXTERNAL_ID_LIST_FIELDS = {
    "invite_ical_uids": "calendar",
    "invite_event_id_hints": "calendar",
    "parent_shas": "github_commit",
    "associated_pr_numbers": "github_thread_number",
    "associated_pr_urls": "github_pr",
}
TIMELINE_FIELDS = (
    "created",
    "updated",
    "created_at",
    "updated_at",
    "sent_at",
    "start_at",
    "end_at",
    "first_message_at",
    "last_message_at",
    "captured_at",
    "occurred_at",
    "recorded_at",
    "committed_at",
)
RELATIONSHIP_FIELDS = (
    "people",
    "orgs",
    "reports_to",
    "messages",
    "attachments",
    "calendar_events",
    "meeting_transcripts",
    "source_messages",
    "source_threads",
    "thread",
    "message",
    "repository",
)


def iter_string_values(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        rows: list[str] = []
        for item in value:
            rows.extend(iter_string_values(item))
        return rows
    return []


def primary_person(value: Any) -> str:
    people = iter_string_values(value)
    return people[0] if people else ""


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key).replace("\x00", ""): _sanitize_json_value(item) for key, item in value.items()}
    return value


def json_text(value: Any) -> str:
    if isinstance(value, (dict, list)):
        payload = _sanitize_json_value(value)
    elif value in (None, ""):
        payload = {} if not isinstance(value, list) else []
    else:
        payload = _sanitize_json_value(value)
    return json.dumps(payload, sort_keys=True)


def card_activity_at(frontmatter: dict[str, Any]) -> str:
    for field_name in (
        "last_message_at",
        "sent_at",
        "start_at",
        "captured_at",
        "committed_at",
        "occurred_at",
        "updated",
        "created",
        "first_message_at",
    ):
        value = str(frontmatter.get(field_name, "") or "").strip()
        if value:
            return value
    return ""


def iter_external_ids(frontmatter: dict[str, Any]) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for field_name, provider in EXTERNAL_ID_FIELDS.items():
        value = frontmatter.get(field_name, "")
        if isinstance(value, str) and value.strip():
            rows.append((field_name, provider, value.strip()))
    for field_name, provider in EXTERNAL_ID_LIST_FIELDS.items():
        for item in iter_string_values(frontmatter.get(field_name, [])):
            rows.append((field_name, provider, item))
    return rows


def external_ids_by_provider(frontmatter: dict[str, Any]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for _field_name, provider, external_id in iter_external_ids(frontmatter):
        grouped.setdefault(provider, [])
        if external_id not in grouped[provider]:
            grouped[provider].append(external_id)
    return grouped


def relationship_payload(frontmatter: dict[str, Any]) -> dict[str, list[str]]:
    payload: dict[str, list[str]] = {}
    for field_name in RELATIONSHIP_FIELDS:
        values = iter_string_values(frontmatter.get(field_name, []))
        if values:
            payload[field_name] = values
    return payload


def build_context_json(
    *,
    card_type: str,
    summary: str = "",
    source_labels: tuple[str, ...] | list[str] = (),
    people: tuple[str, ...] | list[str] = (),
    orgs: tuple[str, ...] | list[str] = (),
    time_span: tuple[str, ...] | list[str] = (),
    provenance_bias: float = 0.0,
    typed_projection_names: tuple[str, ...] | list[str] = (),
) -> dict[str, Any]:
    """Structured context for rerankers, explain payloads, and agents."""
    return {
        "card_type": card_type,
        "summary": summary,
        "source_labels": list(source_labels),
        "people": list(people),
        "orgs": list(orgs),
        "time_span": list(time_span),
        "provenance_bias": provenance_bias,
        "typed_projection_names": list(typed_projection_names),
    }


def build_context_text(ctx: dict[str, Any] | ArchiveContext) -> str:
    """Single line-oriented string for embedding prefixing and reranker input."""
    if isinstance(ctx, ArchiveContext):
        payload = context_payload_from_archive_context(ctx)
    else:
        payload = ctx
    lines: list[str] = []
    ct = str(payload.get("card_type", "") or "").strip()
    if ct:
        lines.append(f"type: {ct}")
    summary = str(payload.get("summary", "") or "").strip()
    if summary:
        lines.append(f"summary: {summary[:400]}")
    sources = payload.get("source_labels") or []
    if sources:
        lines.append(f"sources: {', '.join(str(s) for s in sources[:12])}")
    people = payload.get("people") or []
    if people:
        lines.append(f"people: {', '.join(str(p) for p in people[:12])}")
    orgs = payload.get("orgs") or []
    if orgs:
        lines.append(f"orgs: {', '.join(str(o) for o in orgs[:12])}")
    span = payload.get("time_span") or []
    if span:
        lines.append(f"time: {' — '.join(str(t) for t in span if t)[:120]}")
    tpn = payload.get("typed_projection_names") or []
    if tpn:
        lines.append(f"projections: {', '.join(str(t) for t in tpn[:8])}")
    pb = payload.get("provenance_bias", 0.0)
    if pb:
        lines.append(f"provenance_bias: {pb}")
    return "\n".join(lines)


def context_payload_from_archive_context(ctx: ArchiveContext) -> dict[str, Any]:
    return {
        "card_type": ctx.card_type,
        "summary": "",
        "source_labels": list(ctx.source_labels),
        "people": list(ctx.people),
        "orgs": list(ctx.orgs),
        "time_span": list(ctx.time_span),
        "provenance_bias": ctx.provenance_bias,
        "typed_projection_names": list(ctx.typed_projection_names),
    }


def _split_pipe_agg(value: Any) -> tuple[str, ...]:
    if not value:
        return ()
    if isinstance(value, str):
        return tuple(p.strip() for p in value.split("|") if p.strip())
    return ()


def build_context_prefix_for_embed_row(row: dict[str, Any]) -> str:
    """Prefix chunk text at embed time using joined card metadata (index_store batch rows)."""
    ctx = build_context_json(
        card_type=str(row.get("ctype", "") or row.get("type", "") or ""),
        summary=str(row.get("summary", "") or ""),
        source_labels=_split_pipe_agg(row.get("sources_agg")),
        people=_split_pipe_agg(row.get("people_agg")),
        orgs=_split_pipe_agg(row.get("orgs_agg")),
        time_span=(str(row.get("activity_at", "") or "").strip(),) if row.get("activity_at") else (),
        provenance_bias=0.0,
    )
    text = build_context_text(ctx)
    return f"{text}\n---\n" if text else ""


def archive_context(
    *,
    card_type: str,
    frontmatter: dict[str, Any],
    provenance_bias: float = 0.0,
    graph_neighbor_types: tuple[str, ...] = (),
    typed_projection_names: tuple[str, ...] = (),
) -> ArchiveContext:
    time_span = tuple(
        value
        for value in (
            str(frontmatter.get("start_at", "") or ""),
            str(frontmatter.get("end_at", "") or ""),
            str(frontmatter.get("sent_at", "") or ""),
            str(frontmatter.get("captured_at", "") or ""),
            str(frontmatter.get("occurred_at", "") or ""),
        )
        if value
    )
    return ArchiveContext(
        card_type=card_type,
        source_labels=tuple(iter_string_values(frontmatter.get("source", []))),
        people=tuple(iter_string_values(frontmatter.get("people", []))),
        orgs=tuple(iter_string_values(frontmatter.get("orgs", []))),
        time_span=time_span,
        provenance_bias=provenance_bias,
        graph_neighbor_types=graph_neighbor_types,
        typed_projection_names=typed_projection_names,
    )
