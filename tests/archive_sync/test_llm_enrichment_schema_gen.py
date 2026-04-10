"""Tests for archive_sync.llm_enrichment.schema_gen."""

from __future__ import annotations

from archive_sync.llm_enrichment.schema_gen import (
    LLM_OMIT_FIELDS, all_extractable_card_types, card_type_to_llm_json_schema,
    combined_schema_version, schema_version_for_card_type)


def test_all_extractable_includes_transaction_types() -> None:
    all_t = set(all_extractable_card_types())
    assert "meal_order" in all_t
    assert "ride" in all_t
    assert "email_message" not in all_t


def test_meal_order_schema_omits_system_fields() -> None:
    sch = card_type_to_llm_json_schema("meal_order")
    props = sch.get("properties") or {}
    for f in LLM_OMIT_FIELDS:
        assert f not in props, f"field {f!r} should be stripped"
    assert "restaurant" in props or "items" in props


def test_schema_version_stable() -> None:
    a = schema_version_for_card_type("ride")
    b = schema_version_for_card_type("ride")
    assert a == b
    assert len(a) == 64


def test_combined_schema_version_depends_on_types() -> None:
    x = combined_schema_version(["ride", "meal_order"])
    y = combined_schema_version(["ride"])
    assert x != y
