"""Compute critical-field population rates from staged extracted cards."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from archive_vault.vault import read_note_frontmatter_file

# Card type -> (field_name, predicate). Predicate True => populated.
CRITICAL_FIELDS: dict[str, list[tuple[str, Any]]] = {
    "meal_order": [
        ("restaurant", lambda fm: _nonempty_str(fm.get("restaurant")) and _not_generic_meal_restaurant(fm)),
        ("items", lambda fm: isinstance(fm.get("items"), list) and len(fm.get("items") or []) > 0),
        ("total", lambda fm: _positive_float(fm.get("total"))),
    ],
    "ride": [
        ("pickup_location", lambda fm: _nonempty_str(fm.get("pickup_location"))),
        ("dropoff_location", lambda fm: _nonempty_str(fm.get("dropoff_location"))),
        ("fare", lambda fm: _positive_float(fm.get("fare"))),
    ],
    "flight": [
        ("origin_airport", lambda fm: _nonempty_str(fm.get("origin_airport"))),
        ("destination_airport", lambda fm: _nonempty_str(fm.get("destination_airport"))),
        ("confirmation_code", lambda fm: _valid_flight_pnr(fm.get("confirmation_code"))),
    ],
    "accommodation": [
        ("property_name", lambda fm: _nonempty_str(fm.get("property_name")) and _not_generic_airbnb(fm)),
        ("check_in", lambda fm: _nonempty_str(fm.get("check_in"))),
        ("check_out", lambda fm: _nonempty_str(fm.get("check_out"))),
        ("confirmation_code", lambda fm: _nonempty_str(fm.get("confirmation_code"))),
    ],
    "grocery_order": [
        ("store", lambda fm: _nonempty_str(fm.get("store")) and _not_generic_instacart_store(fm)),
        ("items", lambda fm: isinstance(fm.get("items"), list) and len(fm.get("items") or []) > 0),
        ("total", lambda fm: _positive_float(fm.get("total"))),
    ],
    "shipment": [
        ("tracking_number", lambda fm: _nonempty_str(fm.get("tracking_number"))),
        ("carrier", lambda fm: _nonempty_str(fm.get("carrier"))),
    ],
    "car_rental": [
        ("company", lambda fm: _nonempty_str(fm.get("company"))),
        ("confirmation_code", lambda fm: _nonempty_str(fm.get("confirmation_code"))),
        ("pickup_at", lambda fm: _nonempty_str(fm.get("pickup_at"))),
    ],
}


def _nonempty_str(v: Any) -> bool:
    return bool(str(v or "").strip())


def _positive_float(v: Any) -> bool:
    try:
        return float(v) > 0
    except (TypeError, ValueError):
        return False


def _not_generic_meal_restaurant(fm: dict[str, Any]) -> bool:
    r = str(fm.get("restaurant") or "").strip().lower()
    return r not in ("doordash order", "uber eats", "unknown restaurant")


def _not_generic_airbnb(fm: dict[str, Any]) -> bool:
    p = str(fm.get("property_name") or "").strip().lower()
    return p not in ("airbnb stay", "airbnb")


def _not_generic_instacart_store(fm: dict[str, Any]) -> bool:
    s = str(fm.get("store") or "").strip().lower()
    return s != "instacart"


def _valid_flight_pnr(v: Any) -> bool:
    code = str(v or "").strip().upper()
    if len(code) != 6 or not code.isalnum():
        return False
    if code == "NUMBER" or code == "TBDXXXX":
        return False
    return True


def compute_extraction_confidence(card_type: str, card_data: dict[str, Any]) -> float:
    """Fraction of critical fields populated (0.0–1.0). Unknown card types → 1.0."""
    specs = CRITICAL_FIELDS.get(card_type)
    if not specs:
        return 1.0
    total = len(specs)
    populated = 0
    for _field_name, predicate in specs:
        try:
            if predicate(card_data):
                populated += 1
        except Exception:
            pass
    return round(populated / total, 2) if total > 0 else 1.0


def compute_field_population(staging_root: Path) -> dict[str, dict[str, float]]:
    """Return { card_type: { field_name: fraction_populated } } for cards under staging_root."""
    by_type: dict[str, list[dict[str, Any]]] = {}
    if not staging_root.is_dir():
        return {}

    for path in staging_root.rglob("*.md"):
        if path.name.startswith("_"):
            continue
        try:
            rec = read_note_frontmatter_file(path)
        except OSError:
            continue
        fm = rec.frontmatter
        ct = str(fm.get("type") or "").strip()
        if not ct or ct == "email_message":
            continue
        by_type.setdefault(ct, []).append(fm)

    out: dict[str, dict[str, float]] = {}
    for ct, rows in by_type.items():
        specs = CRITICAL_FIELDS.get(ct)
        if not specs or not rows:
            continue
        field_rates: dict[str, float] = {}
        for field_name, pred in specs:
            ok = sum(1 for fm in rows if pred(fm))
            field_rates[field_name] = round(ok / len(rows), 4)
        out[ct] = field_rates
    return out
