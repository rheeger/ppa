"""Tests for Stage 1 — lightweight classify (mocked LLM)."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from archive_sync.llm_enrichment.cache import InferenceCache
from archive_sync.llm_enrichment.classify import (ClassifyResult,
                                                  _result_from_raw,
                                                  classify_thread,
                                                  render_classify_input)
from hfa.llm_provider import LLMResponse


def _mock_provider(category: str, confidence: float, card_types: list[str] | None = None) -> MagicMock:
    prov = MagicMock()
    prov.model = "test-model"
    payload = {"category": category, "confidence": confidence}
    if card_types:
        payload["card_types"] = card_types
    prov.chat_json.return_value = LLMResponse(
        content=json.dumps(payload),
        parsed_json=payload,
        model="test-model",
        prompt_tokens=30,
        completion_tokens=10,
        latency_ms=50.0,
    )
    return prov


# ---------------------------------------------------------------------------
# render_classify_input
# ---------------------------------------------------------------------------

def test_render_classify_input_includes_subject() -> None:
    text = render_classify_input("Order Confirmation", "shop@amazon.com", "Your order has shipped", 1)
    assert "Subject: Order Confirmation" in text
    assert "From: shop@amazon.com" in text
    assert "Messages: 1" in text
    assert "Preview: Your order has shipped" in text


def test_render_classify_input_truncates_long_snippet() -> None:
    long = "x" * 500
    text = render_classify_input("S", "a@b.com", long, 1)
    assert len(text) < 500


def test_render_classify_input_handles_empty() -> None:
    text = render_classify_input("", "", "", 0)
    assert "Subject:" in text
    assert "Messages: 0" in text


# ---------------------------------------------------------------------------
# _result_from_raw
# ---------------------------------------------------------------------------

def test_result_transactional_high_confidence() -> None:
    r = _result_from_raw({"category": "transactional", "confidence": 0.95}, cache_hit=False)
    assert r.category == "transactional"
    assert r.is_transactional is True
    assert r.confidence == 0.95


def test_result_transactional_low_confidence_skips() -> None:
    r = _result_from_raw({"category": "transactional", "confidence": 0.1}, cache_hit=False)
    assert r.is_transactional is False


def test_result_marketing() -> None:
    r = _result_from_raw({"category": "marketing", "confidence": 0.9}, cache_hit=False)
    assert r.is_transactional is False
    assert r.category == "marketing"


def test_result_noise_default_on_empty() -> None:
    r = _result_from_raw({}, cache_hit=False)
    assert r.category == "noise"
    assert r.is_transactional is False


def test_result_bad_confidence_type() -> None:
    r = _result_from_raw({"category": "transactional", "confidence": "high"}, cache_hit=False)
    assert r.confidence == 0.0
    assert r.is_transactional is False


def test_result_preserves_cache_hit() -> None:
    r = _result_from_raw({"category": "transactional", "confidence": 0.9}, cache_hit=True)
    assert r.cache_hit is True


# ---------------------------------------------------------------------------
# classify_thread (mocked LLM)
# ---------------------------------------------------------------------------

def test_classify_transactional() -> None:
    prov = _mock_provider("transactional", 0.95)
    r = classify_thread(prov, "Subject: Your Uber receipt")
    assert r.is_transactional is True
    assert r.category == "transactional"
    prov.chat_json.assert_called_once()


def test_classify_marketing_skipped() -> None:
    prov = _mock_provider("marketing", 0.9)
    r = classify_thread(prov, "Subject: 50% off this weekend!")
    assert r.is_transactional is False


def test_classify_noise_on_empty_response() -> None:
    prov = MagicMock()
    prov.model = "m"
    prov.chat_json.return_value = LLMResponse(
        content="", parsed_json=None, model="m",
        prompt_tokens=0, completion_tokens=0, latency_ms=1.0,
    )
    r = classify_thread(prov, "Subject: test")
    assert r.category == "noise"
    assert r.is_transactional is False


def test_classify_uses_max_tokens_96() -> None:
    prov = _mock_provider("transactional", 0.9)
    classify_thread(prov, "Subject: test")
    call_kwargs = prov.chat_json.call_args
    assert call_kwargs.kwargs.get("max_tokens") == 96 or call_kwargs[1].get("max_tokens") == 96


# ---------------------------------------------------------------------------
# Cache integration
# ---------------------------------------------------------------------------

def test_classify_cache_hit_skips_llm() -> None:
    prov = _mock_provider("transactional", 0.9)
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test_classify.db"
        cache = InferenceCache(db)
        r1 = classify_thread(prov, "Subject: receipt", cache=cache)
        assert r1.cache_hit is False
        assert prov.chat_json.call_count == 1

        r2 = classify_thread(prov, "Subject: receipt", cache=cache)
        assert r2.cache_hit is True
        assert r2.is_transactional is True
        assert prov.chat_json.call_count == 1
        cache.close()


def test_classify_returns_card_types() -> None:
    prov = _mock_provider("transactional", 0.9, card_types=["ride", "purchase"])
    r = classify_thread(prov, "Subject: Uber receipt")
    assert r.card_types == ["ride", "purchase"]
    assert r.is_transactional is True


def test_classify_filters_invalid_card_types() -> None:
    prov = _mock_provider("transactional", 0.9, card_types=["ride", "fake_type", "flight"])
    r = classify_thread(prov, "Subject: test")
    assert r.card_types == ["ride", "flight"]


def test_classify_empty_card_types_when_not_provided() -> None:
    prov = _mock_provider("transactional", 0.9)
    r = classify_thread(prov, "Subject: test")
    assert r.card_types == []


def test_classify_different_inputs_different_cache() -> None:
    prov = _mock_provider("transactional", 0.9)
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test_classify2.db"
        cache = InferenceCache(db)
        classify_thread(prov, "Subject: A", cache=cache)
        classify_thread(prov, "Subject: B", cache=cache)
        assert prov.chat_json.call_count == 2
        cache.close()
