"""Phase 1: schema tests for new card types."""

from __future__ import annotations

from hfa.schema import CARD_TYPES, MealOrderCard


def test_meal_order_summary_fallback():
    c = MealOrderCard(
        uid="hfa-test-meal-1",
        type="meal_order",
        source=["x"],
        source_id="sid",
        created="2025-01-01",
        updated="2025-01-01",
        summary="",
        service="DD",
        restaurant="Joe's",
    )
    assert "DD" in c.summary and "Joe" in c.summary


def test_all_new_types_registered():
    for name in (
        "meal_order",
        "grocery_order",
        "ride",
        "flight",
        "accommodation",
        "car_rental",
        "purchase",
        "shipment",
        "subscription",
        "event_ticket",
        "payroll",
        "place",
        "organization",
        "knowledge",
        "observation",
    ):
        assert name in CARD_TYPES
