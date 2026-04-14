"""Phase 3: slice manifest includes derived-type count placeholders (min 0 until vault populated)."""

from __future__ import annotations

import json
from pathlib import Path

_MANIFEST = Path(__file__).resolve().parent / "slice_manifest.json"


def test_slice_manifest_includes_derived_type_minimums() -> None:
    data = json.loads(_MANIFEST.read_text(encoding="utf-8"))
    counts = data.get("card_counts_by_type") or {}
    for t in (
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
        assert t in counts
        assert counts[t] == 0
