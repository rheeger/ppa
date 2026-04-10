"""Workflow C — finance card enrichment (Phase 2.875)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from archive_sync.llm_enrichment.staging_types import (EntityMention,
                                                       MatchCandidate)

PROMPT_FILE = Path(__file__).resolve().parent.parent / "prompts" / "enrich_finance.txt"
ENRICH_FINANCE_PROMPT_VERSION = "v4"

_COUNTERPARTY_TYPES = frozenset(
    {
        "person",
        "merchant",
        "subscription",
        "transfer",
        "government",
        "employer",
    }
)


def load_system_prompt() -> str:
    return PROMPT_FILE.read_text(encoding="utf-8")


def gate_finance_card(card_data: dict[str, Any]) -> bool:
    """Pass cards with a counterparty and non-trivial amount."""

    counterparty = str(card_data.get("counterparty") or "").strip()
    try:
        amount = float(card_data.get("amount") or 0)
    except (TypeError, ValueError):
        amount = 0.0
    return bool(counterparty) and abs(amount) >= 1.0


def prefilter_finance(_card_data: dict[str, Any]) -> tuple[bool, str]:
    """No deterministic skip — gated finance cards are verified spending data; the LLM decides relevance."""

    return True, "ok"


def finance_content_hash(fm: dict[str, Any]) -> str:
    """Stable hash for inference cache keys (transaction identity)."""

    payload = {
        "counterparty": str(fm.get("counterparty") or ""),
        "amount": fm.get("amount"),
        "currency": str(fm.get("currency") or ""),
        "category": str(fm.get("category") or ""),
        "parent_category": str(fm.get("parent_category") or ""),
        "transaction_type": str(fm.get("transaction_type") or ""),
        "note": str(fm.get("note") or ""),
        "created": str(fm.get("created") or ""),
        "account": str(fm.get("account") or ""),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def render_user_message(fm: dict[str, Any]) -> str:
    """Plaintext context for the LLM (no vault body for finance cards)."""

    tags = fm.get("provider_tags") or []
    tag_list = tags if isinstance(tags, list) else []
    base_tags = fm.get("tags") or []
    if isinstance(base_tags, list):
        tag_list = list(tag_list) + [str(t) for t in base_tags if t]
    lines = [
        f"Transaction: {fm.get('counterparty') or ''}",
        f"Amount: {fm.get('amount')} {fm.get('currency') or 'USD'}",
        f"Date (created): {fm.get('created') or ''}",
        f"Category: {fm.get('category') or ''}",
        f"Parent category: {fm.get('parent_category') or ''}",
        f"Account: {fm.get('account') or ''}",
        f"Type: {fm.get('transaction_type') or ''}",
        f"Status: {fm.get('transaction_status') or ''}",
        f"Note: {fm.get('note') or ''}",
        f"Recurring label: {fm.get('recurring_label') or ''}",
        f"Tags: {', '.join(str(t) for t in tag_list) if tag_list else '(none)'}",
    ]
    return "\n".join(lines)


def has_counterparty_type_tag(fm: dict[str, Any]) -> bool:
    for t in fm.get("provider_tags") or []:
        if str(t).startswith("counterparty_type:"):
            return True
    return False


def merge_counterparty_type_into_provider_tags(
    existing: list[str] | None,
    counterparty_type: str,
) -> list[str]:
    prefix = "counterparty_type:"
    cleaned = [str(t) for t in (existing or []) if not str(t).startswith(prefix)]
    ct = str(counterparty_type).strip().lower()
    if ct and ct in _COUNTERPARTY_TYPES:
        cleaned.append(f"{prefix}{ct}")
    return cleaned


def parse_finance_response(
    data: dict[str, Any],
    *,
    source_uid: str,
    run_id: str,
    existing_provider_tags: list[str] | None,
) -> tuple[dict[str, Any], list[EntityMention], list[MatchCandidate]]:
    """Map LLM JSON to field updates + staging rows."""

    raw_type = str(data.get("counterparty_type") or "").strip().lower()
    field_updates: dict[str, Any] = {}
    if raw_type and raw_type in _COUNTERPARTY_TYPES:
        merged = merge_counterparty_type_into_provider_tags(existing_provider_tags, raw_type)
        if merged != list(existing_provider_tags or []):
            field_updates["provider_tags"] = merged

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
                # LLM often omits type; infer from counterparty_type so staging is not empty.
                if raw_type == "person":
                    et = "person"
                else:
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
                    source_card_type="finance",
                    workflow="finance_enrichment",
                    entity_type=et,
                    raw_text=name,
                    context=ctx,
                    confidence=max(0.0, min(1.0, c)),
                    run_id=run_id,
                )
            )

    matches: list[MatchCandidate] = []
    em = data.get("email_match")
    if isinstance(em, dict) and em:
        matches.append(
            MatchCandidate(
                source_card_uid=source_uid,
                source_card_type="finance",
                workflow="finance_enrichment",
                target_card_type="email_message",
                match_signals={
                    "counterparty_keywords": em.get("counterparty_keywords") or [],
                    "amount": em.get("amount"),
                    "date_range": em.get("date_range") or [],
                },
                field_to_write="source_email",
                confidence=0.7,
                run_id=run_id,
            )
        )

    return field_updates, entities, matches


def response_to_cache_payload(parsed: dict[str, Any] | None) -> dict[str, Any]:
    return parsed if isinstance(parsed, dict) else {"_error": "invalid_json", "raw": str(parsed)}
