"""Stage 2 — extract typed cards from a hydrated thread."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archive_sync.extractors.field_validation import validate_provenance_round_trip
from archive_sync.llm_enrichment.cache import InferenceCache, build_inference_cache_key
from archive_sync.llm_enrichment.schema_gen import card_type_to_llm_json_schema, combined_schema_version
from archive_sync.llm_enrichment.threads import ThreadDocument, render_thread_for_extraction
from archive_vault.llm_provider import OllamaProvider
from archive_vault.schema import BaseCard, validate_card_permissive

logger = logging.getLogger("ppa.llm_enrichment.extract")

EXTRACT_PROMPT_VERSION = "extract-v1"
_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


@dataclass
class ExtractedCard:
    card_type: str
    data: dict[str, Any]
    validated: BaseCard | None
    round_trip_warnings: list[str]


@dataclass
class ExtractResult:
    cards: list[ExtractedCard]
    reasoning: str
    raw: dict[str, Any]
    cache_hit: bool = False


def _read_text(name: str) -> str:
    return (_PROMPTS_DIR / name).read_text(encoding="utf-8")


def _flatten_thread_for_roundtrip(thread: ThreadDocument) -> str:
    return "\n".join(m.body for m in thread.messages)


def _coerce_card_dict(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    out = dict(item)
    if "type" not in out and "_type" in out:
        out["type"] = out.pop("_type")
    return out


def extract_cards_for_thread(
    provider: OllamaProvider,
    thread: ThreadDocument,
    card_types: list[str],
    *,
    model: str | None = None,
    temperature: float = 0.0,
    seed: int = 42,
    cache: InferenceCache | None = None,
    run_id: str = "",
) -> ExtractResult:
    """Run extraction for one thread; validates Pydantic + round-trip warnings."""

    if not card_types:
        return ExtractResult(cards=[], reasoning="", raw={})

    use_model = model or provider.model
    rendered = render_thread_for_extraction(thread)
    source_blob = _flatten_thread_for_roundtrip(thread)
    schema_v = combined_schema_version(card_types)
    content_hash = thread.content_hash

    cache_key: str | None = None
    if cache is not None:
        cache_key = build_inference_cache_key(
            content_hash=content_hash,
            model_id=use_model,
            prompt_version=EXTRACT_PROMPT_VERSION,
            schema_version=schema_v,
            temperature=temperature,
            seed=seed,
        )
        hit = cache.get(cache_key)
        if hit is not None:
            return _extract_result_from_cache_payload(hit, thread, source_blob, cache_hit=True)

    system = _read_text("extract_system.txt")
    schema_blocks: list[str] = []
    for ct in sorted(set(card_types)):
        sch = card_type_to_llm_json_schema(ct)
        schema_blocks.append(f"### JSON Schema for `{ct}`\n```json\n{json.dumps(sch, indent=2)}\n```")

    user = (
        f"Thread subject: {thread.subject}\n"
        f"Date range: {thread.date_range[0]} — {thread.date_range[1]}\n"
        f"Messages: {thread.message_count}\n"
        f"Target card type(s): {', '.join(card_types)}\n\n"
        + "\n\n".join(schema_blocks)
        + "\n\n--- THREAD BODY ---\n"
        + rendered
        + "\n--- END THREAD ---\n\n"
        "Return JSON: {\"cards\": [{\"type\": \"meal_order\", ...}, ...], \"reasoning\": \"...\"}. "
        "Each card MUST include a string \"type\" field matching one of the target types."
    )

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]
    r = provider.chat_json(messages, model=use_model, temperature=temperature, seed=seed)
    parsed = r.parsed_json if isinstance(r.parsed_json, dict) else {}

    if not r.content.strip():
        logger.warning("extract: empty LLM response (HTTP failure or timeout) thread=%s", thread.thread_id)
        parsed = {"cards": [], "reasoning": "_llm_error"}

    out_payload = {
        **parsed,
        "_tokens": {"prompt": r.prompt_tokens, "completion": r.completion_tokens},
        "_latency_ms": r.latency_ms,
    }
    if cache is not None and cache_key is not None:
        cache.put(
            cache_key,
            stage="extract",
            model_id=use_model,
            prompt_version=EXTRACT_PROMPT_VERSION,
            content_hash=content_hash,
            response=out_payload,
            tokens=(r.prompt_tokens, r.completion_tokens),
            latency_ms=r.latency_ms,
            run_id=run_id,
        )

    return _build_extract_result(parsed, thread, source_blob, cache_hit=False)


def _build_extract_result(
    parsed: dict[str, Any],
    thread: ThreadDocument,
    source_blob: str,
    *,
    cache_hit: bool,
) -> ExtractResult:
    reasoning = str(parsed.get("reasoning") or "").strip()
    raw_cards = parsed.get("cards")
    if not isinstance(raw_cards, list):
        raw_cards = []
    extracted: list[ExtractedCard] = []
    for item in raw_cards:
        cd = _coerce_card_dict(item)
        if not cd:
            continue
        ct = str(cd.get("type") or "").strip()
        if not ct:
            continue
        val: BaseCard | None = None
        try:
            val = validate_card_permissive(cd)
        except Exception as exc:  # noqa: BLE001 — surface as unvalidated
            logger.debug("pydantic validation failed for %s: %s", ct, exc)
        data = cd
        warns = validate_provenance_round_trip(data, source_blob, ct) if val else []
        extracted.append(
            ExtractedCard(card_type=ct, data=data, validated=val, round_trip_warnings=warns)
        )
    return ExtractResult(cards=extracted, reasoning=reasoning, raw=parsed, cache_hit=cache_hit)


def _extract_result_from_cache_payload(
    payload: dict[str, Any],
    thread: ThreadDocument,
    source_blob: str,
    *,
    cache_hit: bool,
) -> ExtractResult:
    clean = {k: v for k, v in payload.items() if not str(k).startswith("_")}
    return _build_extract_result(clean, thread, source_blob, cache_hit=cache_hit)
