"""Stage 1 — classify email threads and route to card types."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archive_sync.llm_enrichment.cache import (InferenceCache,
                                               build_inference_cache_key)
from archive_sync.llm_enrichment.schema_gen import all_extractable_card_types
from hfa.llm_provider import OllamaProvider

logger = logging.getLogger("ppa.llm_enrichment.triage")

TRIAGE_PROMPT_VERSION = "triage-v1"
_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"

# Conservative skip (plan): marketing / automated / noise / P2P v1
SKIP_CLASSIFICATIONS: frozenset[str] = frozenset(
    {
        "marketing",
        "automated_notification",
        "noise",
        "person_to_person",
    }
)


@dataclass
class TriageResult:
    classification: str
    card_types: list[str]
    confidence: float
    reasoning: str
    skip: bool
    raw: dict[str, Any]
    cache_hit: bool = False


def _read_text(name: str) -> str:
    path = _PROMPTS_DIR / name
    return path.read_text(encoding="utf-8")


def _triage_content_hash(triage_input: str) -> str:
    return hashlib.sha256(triage_input.encode("utf-8")).hexdigest()


def _normalize_card_types(raw: list[Any]) -> list[str]:
    allowed = frozenset(all_extractable_card_types())
    out: list[str] = []
    for x in raw or []:
        t = str(x).strip()
        if t in allowed and t not in out:
            out.append(t)
    return out


def triage_thread(
    provider: OllamaProvider,
    triage_input: str,
    *,
    model: str | None = None,
    temperature: float = 0.0,
    seed: int = 42,
    cache: InferenceCache | None = None,
    run_id: str = "",
) -> TriageResult:
    """Classify a thread from :func:`render_thread_for_triage` text (no body reads)."""

    use_model = model or provider.model
    content_hash = _triage_content_hash(triage_input)
    schema_v = "triage-output-v1"
    cache_key: str | None = None
    if cache is not None:
        cache_key = build_inference_cache_key(
            content_hash=content_hash,
            model_id=use_model,
            prompt_version=TRIAGE_PROMPT_VERSION,
            schema_version=schema_v,
            temperature=temperature,
            seed=seed,
        )
        hit = cache.get(cache_key)
        if hit is not None:
            return _result_from_raw(hit, skip_computed=hit.get("_skip"), cache_hit=True)

    system = _read_text("triage_system.txt")
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": triage_input},
    ]
    r = provider.chat_json(messages, model=use_model, temperature=temperature, seed=seed)
    parsed = r.parsed_json or {}
    if not isinstance(parsed, dict):
        parsed = {}

    if not r.content.strip():
        logger.warning("triage: empty LLM response (HTTP failure or timeout)")
        parsed = {"classification": "noise", "confidence": 0.0, "card_types": [], "reasoning": "_llm_error"}

    skip = _compute_skip(parsed)
    out_raw = {**parsed, "_skip": skip}
    if cache is not None and cache_key is not None:
        cache.put(
            cache_key,
            stage="triage",
            model_id=use_model,
            prompt_version=TRIAGE_PROMPT_VERSION,
            content_hash=content_hash,
            response=out_raw,
            tokens=(r.prompt_tokens, r.completion_tokens),
            latency_ms=r.latency_ms,
            run_id=run_id,
        )

    return _result_from_raw(out_raw, skip_computed=skip, cache_hit=False)


def _compute_skip(parsed: dict[str, Any]) -> bool:
    classification = str(parsed.get("classification") or "").strip()
    conf_raw = parsed.get("confidence", 0.0)
    try:
        confidence = float(conf_raw)
    except (TypeError, ValueError):
        confidence = 0.0
    card_types = _normalize_card_types(list(parsed.get("card_types") or []))

    if confidence < 0.5:
        return True
    if classification in SKIP_CLASSIFICATIONS:
        return True
    if not card_types:
        return True
    return False


def _result_from_raw(
    parsed: dict[str, Any],
    *,
    skip_computed: bool | None,
    cache_hit: bool,
) -> TriageResult:
    classification = str(parsed.get("classification") or "").strip() or "noise"
    try:
        confidence = float(parsed.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0
    card_types = _normalize_card_types(list(parsed.get("card_types") or []))
    reasoning = str(parsed.get("reasoning") or "").strip()
    skip = skip_computed if skip_computed is not None else _compute_skip(parsed)
    raw = {k: v for k, v in parsed.items() if not str(k).startswith("_")}
    return TriageResult(
        classification=classification,
        card_types=card_types,
        confidence=confidence,
        reasoning=reasoning,
        skip=skip,
        raw=raw,
        cache_hit=cache_hit,
    )
