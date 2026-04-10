"""Tests for LLM enrichment batch runner helpers."""

from __future__ import annotations

from archive_sync.llm_enrichment.enrich_runner import derive_llm_card_uid


def test_derive_llm_card_uid_stable() -> None:
    a = derive_llm_card_uid("tid1", "ride", {"service": "Uber", "fare": 10.0})
    b = derive_llm_card_uid("tid1", "ride", {"service": "Uber", "fare": 10.0})
    assert a == b
    assert a.startswith("hfa-ride-")


def test_derive_llm_card_uid_differs_by_thread() -> None:
    a = derive_llm_card_uid("tid1", "ride", {"service": "Uber"})
    b = derive_llm_card_uid("tid2", "ride", {"service": "Uber"})
    assert a != b
