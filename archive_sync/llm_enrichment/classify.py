"""Stage 1 — lightweight email thread classification (cheap, every thread).

Categorizes threads before expensive extraction. Only ``transactional`` threads
proceed to Stage 2 extraction. ~100 tokens per call vs ~1,700 for extraction.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Union

from archive_sync.llm_enrichment.cache import InferenceCache, build_inference_cache_key
from archive_vault.llm_provider import GeminiProvider, OllamaProvider

logger = logging.getLogger("ppa.llm_enrichment.classify")

CLASSIFY_PROMPT_VERSION = "classify-v2"
_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"

TRANSACTIONAL_CATEGORIES: frozenset[str] = frozenset({"transactional"})

SKIP_CATEGORIES: frozenset[str] = frozenset({
    "personal",
    "marketing",
    "automated",
    "noise",
})


@dataclass
class ClassifyResult:
    category: str
    confidence: float
    card_types: list[str]
    is_transactional: bool
    cache_hit: bool = False
    raw: dict[str, Any] | None = None


def _read_text(name: str) -> str:
    return (_PROMPTS_DIR / name).read_text(encoding="utf-8")


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def render_classify_input(
    subject: str,
    from_email: str,
    snippet: str,
    message_count: int,
) -> str:
    """Minimal thread representation for classification — no full bodies."""

    parts = [f"Subject: {subject}"]
    if from_email:
        parts.append(f"From: {from_email}")
    parts.append(f"Messages: {message_count}")
    if snippet:
        parts.append(f"Preview: {snippet[:300]}")
    return "\n".join(parts)


def classify_thread(
    provider: Union[OllamaProvider, GeminiProvider],
    classify_input: str,
    *,
    model: str | None = None,
    temperature: float = 0.0,
    seed: int = 42,
    cache: InferenceCache | None = None,
    run_id: str = "",
) -> ClassifyResult:
    """Classify a thread with a lightweight LLM call (~100 tokens)."""

    use_model = model or provider.model
    ch = _content_hash(classify_input)
    schema_v = "classify-output-v1"

    cache_key: str | None = None
    if cache is not None:
        cache_key = build_inference_cache_key(
            content_hash=ch,
            model_id=use_model,
            prompt_version=CLASSIFY_PROMPT_VERSION,
            schema_version=schema_v,
            temperature=temperature,
            seed=seed,
        )
        hit = cache.get(cache_key)
        if hit is not None:
            return _result_from_raw(hit, cache_hit=True)

    system = _read_text("classify_system.txt")
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": classify_input},
    ]
    r = provider.chat_json(
        messages,
        model=use_model,
        temperature=temperature,
        seed=seed,
        max_tokens=256,
    )

    parsed: dict[str, Any] | list[Any] | None = r.parsed_json
    if not isinstance(parsed, dict):
        parsed = {}
    if not parsed and (r.content or "").strip():
        from archive_vault.llm_provider import _parse_json_from_model_text

        loose = _parse_json_from_model_text((r.content or "").strip())
        if isinstance(loose, dict):
            parsed = loose

    if not (r.content or "").strip():
        parsed = {"category": "noise", "confidence": 0.0}

    if cache is not None and cache_key is not None:
        cache.put(
            cache_key,
            stage="classify",
            model_id=use_model,
            prompt_version=CLASSIFY_PROMPT_VERSION,
            content_hash=ch,
            response=parsed,
            tokens=(r.prompt_tokens, r.completion_tokens),
            latency_ms=r.latency_ms,
            run_id=run_id,
        )

    return _result_from_raw(parsed, cache_hit=False)


_VALID_CARD_TYPES = frozenset({
    "meal_order", "grocery_order", "purchase", "ride", "flight",
    "accommodation", "car_rental", "shipment", "subscription",
    "event_ticket", "payroll",
})


def _normalize_card_types(raw_types: list[Any] | None) -> list[str]:
    if not raw_types:
        return []
    return [str(t).strip() for t in raw_types if str(t).strip() in _VALID_CARD_TYPES]


def _normalize_category_label(raw: str) -> str:
    """Map common model variants to canonical labels."""

    c = (raw or "").strip().lower()
    if not c:
        return "noise"
    if c in frozenset({"transaction", "txn", "commerce", "commercial"}):
        return "transactional"
    if c.startswith("transaction") and c != "transactional":
        return "transactional"
    return c


def _result_from_raw(parsed: dict[str, Any], *, cache_hit: bool) -> ClassifyResult:
    if parsed.get("is_transactional") is True:
        parsed = {**parsed, "category": "transactional"}

    category = _normalize_category_label(str(parsed.get("category") or ""))
    conf_present = "confidence" in parsed
    try:
        confidence = float(parsed.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0

    if category == "transactional" and not conf_present:
        confidence = max(confidence, 0.85)

    card_types = _normalize_card_types(parsed.get("card_types"))
    is_tx = category in TRANSACTIONAL_CATEGORIES and confidence >= 0.2
    return ClassifyResult(
        category=category,
        confidence=confidence,
        card_types=card_types,
        is_transactional=is_tx,
        cache_hit=cache_hit,
        raw=parsed,
    )
