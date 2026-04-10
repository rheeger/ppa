"""Tests for triage + extract (mocked LLM)."""

from __future__ import annotations

from unittest.mock import MagicMock

from archive_sync.llm_enrichment.extract import extract_cards_for_thread
from archive_sync.llm_enrichment.threads import ThreadDocument, ThreadMessage
from archive_sync.llm_enrichment.triage import triage_thread
from hfa.llm_provider import LLMResponse


def test_triage_skips_low_confidence() -> None:
    prov = MagicMock()
    prov.model = "m"
    prov.chat_json.return_value = LLMResponse(
        content="{}",
        parsed_json={
            "classification": "transactional_receipt",
            "card_types": ["meal_order"],
            "confidence": 0.1,
            "reasoning": "unsure",
        },
        model="m",
        prompt_tokens=1,
        completion_tokens=2,
        latency_ms=1.0,
    )
    r = triage_thread(prov, "dummy triage text", cache=None)
    assert r.skip is True


def test_triage_respects_marketing_skip() -> None:
    prov = MagicMock()
    prov.model = "m"
    prov.chat_json.return_value = LLMResponse(
        content="{}",
        parsed_json={
            "classification": "marketing",
            "card_types": ["meal_order"],
            "confidence": 1.0,
            "reasoning": "newsletter",
        },
        model="m",
        prompt_tokens=1,
        completion_tokens=2,
        latency_ms=1.0,
    )
    r = triage_thread(prov, "x", cache=None)
    assert r.skip is True


def test_extract_validates_round_trip() -> None:
    prov = MagicMock()
    prov.model = "m"
    prov.chat_json.return_value = LLMResponse(
        content="{}",
        parsed_json={
            "cards": [
                {
                    "type": "ride",
                    "uid": "hfa-test-ride-llmextract01",
                    "source": ["gmail"],
                    "source_id": "src1",
                    "created": "2025-01-01",
                    "updated": "2025-01-01",
                    "summary": "Uber trip",
                    "service": "Uber",
                    "pickup_location": "SomewhereNotInEmailBodyXXXXZZ",
                }
            ],
            "reasoning": "test",
        },
        model="m",
        prompt_tokens=1,
        completion_tokens=2,
        latency_ms=1.0,
    )
    doc = ThreadDocument(
        thread_id="t",
        messages=[
            ThreadMessage(
                uid="u1",
                rel_path="Email/x.md",
                from_email="a@b.com",
                from_name="A",
                sent_at="2025-01-01",
                subject="S",
                body="Uber trip receipt for $10",
                direction="inbound",
            )
        ],
        subject="S",
        participants=["a@b.com"],
        date_range=("2025-01-01", "2025-01-01"),
        message_count=1,
        total_chars=30,
        content_hash="c",
    )
    ex = extract_cards_for_thread(prov, doc, ["ride"], cache=None)
    assert len(ex.cards) == 1
    assert any("pickup_location" in w for w in ex.cards[0].round_trip_warnings)
