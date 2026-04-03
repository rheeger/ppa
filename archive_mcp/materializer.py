"""Row materialization, edge building, and person lookup for the derived index."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from hfa.schema import BaseCard
from hfa.vault import extract_wikilinks, read_note_file

from .card_registry import REGISTRATION_BY_CARD_TYPE
from .chunking import render_chunks_for_card
from .features import (TIMELINE_FIELDS, card_activity_at, card_activity_end_at,
                       iter_external_ids, parse_timestamp_to_utc)
from .projections.base import ProjectionRowBuffer, build_projection_row
from .projections.registry import projection_for_card_type
from .scanner import CanonicalRow

EXTERNAL_ID_TARGET_PREFIX = "external-id://"


def _normalize_slug(value: str) -> str:
    return value.replace(" ", "-").lower().strip()


def _normalize_exact_text(value: str) -> str:
    return _clean_text(value).lower()


def _clean_text(value: str) -> str:
    sanitized = str(value).replace("\x00", "")
    return re.sub(r"\s+", " ", sanitized.strip())


def _iter_string_values(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        cleaned = value.replace("\x00", "").strip()
        if cleaned:
            yield cleaned
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_string_values(item)
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from _iter_string_values(item)


def _coerce_string_list(value: Any) -> list[str]:
    return list(_iter_string_values(value))


def _dedupe_row_key(row: tuple[Any, ...]) -> tuple[Any, ...]:
    """Build a hashable key; lists (e.g. TEXT[] cells) are normalized to tuple."""
    key: list[Any] = []
    for cell in row:
        if isinstance(cell, list):
            key.append(tuple(cell))
        else:
            key.append(cell)
    return tuple(key)


def _dedupe_rows(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    seen: set[tuple[Any, ...]] = set()
    out: list[tuple[Any, ...]] = []
    for row in rows:
        k = _dedupe_row_key(row)
        if k in seen:
            continue
        seen.add(k)
        out.append(row)
    return out


def _build_search_text(frontmatter: dict[str, Any], body: str) -> str:
    parts: list[str] = []
    for key, value in frontmatter.items():
        if key in {"uid"}:
            continue
        for text in _iter_string_values(value):
            parts.append(_clean_text(text))
    body_cleaned = body.replace("\x00", "").strip()
    if body_cleaned:
        parts.append(body_cleaned)
    return "\n".join(parts)


def _content_hash(frontmatter: dict[str, Any], body: str) -> str:
    sanitized_frontmatter = json.loads(json.dumps(frontmatter, sort_keys=True, default=str).replace("\\u0000", ""))
    payload = json.dumps(sanitized_frontmatter, sort_keys=True, default=str) + "\n" + body.replace("\x00", "")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _chunk_key(card_uid: str, chunk_type: str, chunk_index: int, content_hash: str) -> str:
    payload = f"{card_uid}:{chunk_type}:{chunk_index}:{content_hash}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _synthetic_external_id_path(provider: str, external_id: str) -> str:
    return f"{EXTERNAL_ID_TARGET_PREFIX}{provider}/{external_id}"


def _slug_from_wikilink(value: str) -> str:
    cleaned = value.strip()
    if cleaned.startswith("[[") and cleaned.endswith("]]"):
        return cleaned[2:-2].split("|", 1)[0].strip()
    return cleaned


def _resolve_slug(slug_map: dict[str, str], slug: str) -> str | None:
    cleaned = slug.strip()
    if not cleaned:
        return None
    return slug_map.get(cleaned) or slug_map.get(_normalize_slug(cleaned))


def _wikilinks_from_frontmatter(frontmatter: dict[str, Any]) -> list[tuple[str, str]]:
    matches: list[tuple[str, str]] = []
    for field_name, value in frontmatter.items():
        for text in _iter_string_values(value):
            if text.startswith("[[") and text.endswith("]]"):
                slug = _slug_from_wikilink(text)
                if slug:
                    matches.append((field_name, slug))
    return matches


def _body_wikilinks(body: str) -> list[tuple[str, str]]:
    return [("body", slug.strip()) for slug in extract_wikilinks(body) if slug.strip()]


def _build_person_lookup(rows: list[CanonicalRow]) -> dict[str, str]:
    person_lookup: dict[str, str] = {}
    for row in rows:
        card = row.card
        if getattr(card, "type", "") != "person":
            continue
        rel_path = row.rel_path
        stem = Path(rel_path).stem
        for key in [stem, _normalize_slug(card.summary)]:
            if key:
                person_lookup[key] = rel_path
        for alias in getattr(card, "aliases", []):
            normalized = _normalize_slug(alias)
            if normalized:
                person_lookup[normalized] = rel_path
        for email in getattr(card, "emails", []):
            normalized = _normalize_exact_text(email)
            if normalized:
                person_lookup[normalized] = rel_path
        for handle_field in ("linkedin", "github", "twitter", "instagram", "telegram", "discord"):
            normalized = _normalize_exact_text(str(getattr(card, handle_field, "") or ""))
            if normalized:
                person_lookup[normalized] = rel_path
    return person_lookup


def _resolve_person_reference(person_lookup: dict[str, str], value: str) -> str | None:
    slug = _slug_from_wikilink(value)
    normalized_slug = _normalize_slug(slug)
    normalized_value = _normalize_exact_text(value)
    return person_lookup.get(normalized_slug) or person_lookup.get(normalized_value)


def _append_edge(
    edges: list[dict[str, str]],
    seen: set[tuple[str, str, str, str]],
    *,
    source_uid: str,
    source_path: str,
    target_slug: str,
    target_path: str,
    target_uid: str,
    target_kind: str,
    edge_type: str,
    field_name: str,
) -> None:
    key = (source_uid, target_path, edge_type, field_name)
    if key in seen:
        return
    seen.add(key)
    edges.append(
        {
            "source_uid": source_uid,
            "source_path": source_path,
            "target_slug": target_slug,
            "target_path": target_path,
            "target_uid": target_uid,
            "target_kind": target_kind,
            "edge_type": edge_type,
            "field_name": field_name,
        }
    )


def _build_edges(
    *,
    rel_path: str,
    frontmatter: dict[str, Any],
    card: BaseCard,
    body: str,
    slug_map: dict[str, str],
    path_to_uid: dict[str, str],
    person_lookup: dict[str, str],
) -> list[dict[str, str]]:
    edges: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()

    def append_card_edge(field_name: str, slug_or_ref: str, edge_type: str) -> None:
        target_path = _resolve_slug(slug_map, _slug_from_wikilink(slug_or_ref))
        if target_path is None:
            return
        _append_edge(
            edges,
            seen,
            source_uid=card.uid,
            source_path=rel_path,
            target_slug=_slug_from_wikilink(slug_or_ref),
            target_path=target_path,
            target_uid=path_to_uid.get(target_path, ""),
            target_kind="card",
            edge_type=edge_type,
            field_name=field_name,
        )

    def append_person_edge(field_name: str, ref: str, edge_type: str) -> None:
        target_path = _resolve_person_reference(person_lookup, ref)
        if target_path is None:
            return
        _append_edge(
            edges,
            seen,
            source_uid=card.uid,
            source_path=rel_path,
            target_slug=_slug_from_wikilink(ref),
            target_path=target_path,
            target_uid=path_to_uid.get(target_path, ""),
            target_kind="card",
            edge_type=edge_type,
            field_name=field_name,
        )

    # --- universal edges (all card types) ---

    for field_name, slug in _wikilinks_from_frontmatter(frontmatter) + _body_wikilinks(body):
        append_card_edge(field_name, slug, "wikilink")

    for field_name, provider, external_id in iter_external_ids(frontmatter):
        _append_edge(
            edges,
            seen,
            source_uid=card.uid,
            source_path=rel_path,
            target_slug=external_id,
            target_path=_synthetic_external_id_path(provider, external_id),
            target_uid="",
            target_kind="external_id",
            edge_type="entity_has_external_id",
            field_name=field_name,
        )

    registration = REGISTRATION_BY_CARD_TYPE.get(card.type)
    person_edge_type = registration.person_edge_type if registration else "mentions_person"

    for person in getattr(card, "people", []):
        append_person_edge("people", person, person_edge_type)

    # --- card-type-specific edges (declarative) ---

    if registration:
        for rule in registration.edge_rules:
            if rule.multi:
                values: list[str] = []
                for sf in rule.source_fields:
                    raw = frontmatter.get(sf, [])
                    values.extend(_coerce_string_list(raw) if isinstance(raw, (list, tuple)) else [str(raw)])
            else:
                val = str(frontmatter.get(rule.source_fields[0], "")).strip()
                values = [val] if val else []

            for v in values:
                if not v.strip():
                    continue
                if rule.target == "card":
                    append_card_edge(rule.field_name, v, rule.edge_type)
                else:
                    append_person_edge(rule.field_name, v, rule.edge_type)

    return edges


def _compute_quality_score(
    card_type: str,
    frontmatter: dict[str, Any],
    *,
    body: str,
    summary: str,
) -> tuple[float, list[str]]:
    registration = REGISTRATION_BY_CARD_TYPE.get(card_type)
    critical = registration.quality_critical_fields if registration else ()
    flags: list[str] = []
    if not critical:
        score = 0.5
    else:
        filled = 0
        for field in critical:
            value = frontmatter.get(field)
            empty = value is None or value == "" or value == [] or value == {}
            if empty:
                flags.append(f"missing:{field}")
            else:
                filled += 1
        score = filled / len(critical) if critical else 0.5
    body_stripped = body.strip()
    if len(body_stripped) > 80:
        score = min(1.0, score + 0.08)
    elif len(body_stripped) > 20:
        score = min(1.0, score + 0.04)
    if summary.strip():
        score = min(1.0, score + 0.04)
    return round(float(score), 4), flags


def _materialize_row(
    row: CanonicalRow,
    *,
    vault_root: str,
    slug_map: dict[str, str],
    path_to_uid: dict[str, str],
    person_lookup: dict[str, str],
    batch_id: str = "",
) -> ProjectionRowBuffer:
    from .index_config import CHUNK_SCHEMA_VERSION

    frontmatter = dict(row.frontmatter)
    card = row.card
    rel_path = row.rel_path
    body = read_note_file(Path(vault_root) / rel_path, vault_root=vault_root).body
    search_text = _build_search_text(frontmatter, body)
    content_hash_val = _content_hash(frontmatter, body)
    activity_raw = card_activity_at(frontmatter)
    activity_at = parse_timestamp_to_utc(activity_raw)
    activity_end_raw = card_activity_end_at(card.type, frontmatter)
    activity_end_at = parse_timestamp_to_utc(activity_end_raw)
    timeline_values = {field: str(frontmatter.get(field, "") or "") for field in TIMELINE_FIELDS}
    quality_score, quality_flag_list = _compute_quality_score(
        card.type, frontmatter, body=body, summary=card.summary
    )
    quality_flags_for_row: list[str] = list(quality_flag_list)
    source_adapter = str(card.source[0]).strip() if card.source else ""

    batch = ProjectionRowBuffer()
    batch.add(
        "cards",
        (
            card.uid,
            rel_path,
            Path(rel_path).stem,
            card.type,
            card.summary,
            str(frontmatter.get("source_id", "") or ""),
            timeline_values["created"],
            timeline_values["updated"],
            activity_at,
            activity_end_at,
            timeline_values["sent_at"],
            timeline_values["start_at"],
            timeline_values["first_message_at"],
            timeline_values["last_message_at"],
            quality_score,
            quality_flags_for_row,
            0,
            "none",
            None,
            content_hash_val,
            search_text,
        ),
    )
    batch.ingestion_log_rows.append((card.uid, "created", source_adapter, batch_id))
    for source in card.source:
        batch.add("card_sources", (card.uid, source))
    for person in getattr(card, "people", []):
        ps = str(person).strip()
        resolved_path = _resolve_person_reference(person_lookup, ps)
        if resolved_path and resolved_path in path_to_uid:
            person_key = str(path_to_uid[resolved_path])
        else:
            person_key = ps
        batch.add("card_people", (card.uid, person_key))
    for org in getattr(card, "orgs", []):
        batch.add("card_orgs", (card.uid, org))
    typed_projection = projection_for_card_type(card.type)
    if typed_projection is not None:
        typed_row, _canonical_ready, _migration_notes = build_projection_row(
            typed_projection,
            card=card,
            rel_path=rel_path,
            frontmatter=frontmatter,
        )
        batch.add(typed_projection.table_name, typed_row)
    for field_name, provider, external_id in iter_external_ids(frontmatter):
        batch.add("external_ids", (card.uid, field_name, provider, external_id))
    for edge in _build_edges(
        rel_path=rel_path,
        frontmatter=frontmatter,
        card=card,
        body=body,
        slug_map=slug_map,
        path_to_uid=path_to_uid,
        person_lookup=person_lookup,
    ):
        batch.add(
            "edges",
            (
                edge["source_uid"],
                edge["source_path"],
                edge["target_uid"],
                edge["target_slug"],
                edge["target_path"],
                edge["target_kind"],
                edge["edge_type"],
                edge["field_name"],
            ),
        )
    for chunk in render_chunks_for_card(frontmatter, body):
        ck = _chunk_key(
            card.uid,
            str(chunk["chunk_type"]),
            int(chunk["chunk_index"]),
            str(chunk["content_hash"]),
        )
        batch.add(
            "chunks",
            (
                ck,
                card.uid,
                rel_path,
                str(chunk["chunk_type"]),
                int(chunk["chunk_index"]),
                CHUNK_SCHEMA_VERSION,
                json.dumps(chunk["source_fields"]),
                str(chunk["content"]),
                str(chunk["content_hash"]),
                int(chunk["token_count"]),
            ),
        )
    for table_name, rows_list in list(batch.rows_by_table.items()):
        batch.rows_by_table[table_name] = _dedupe_rows(rows_list)
    return batch


def _materialize_row_batch(
    rows: list[CanonicalRow],
    *,
    vault_root: str,
    slug_map: dict[str, str],
    path_to_uid: dict[str, str],
    person_lookup: dict[str, str],
    batch_id: str = "",
) -> ProjectionRowBuffer:
    batch = ProjectionRowBuffer()
    for row in rows:
        batch.extend(
            _materialize_row(
                row,
                vault_root=vault_root,
                slug_map=slug_map,
                path_to_uid=path_to_uid,
                person_lookup=person_lookup,
                batch_id=batch_id,
            )
        )
    return batch
