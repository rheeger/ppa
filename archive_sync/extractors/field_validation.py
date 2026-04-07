"""Shared field validation — rejects garbage values at extraction time.

validate_field() is called by base.py _instantiate_card() for every non-system field
before card construction. Returns the cleaned value, or None to omit the field.
"""

from __future__ import annotations

import logging
import re
from typing import Any

_rt_log = logging.getLogger("ppa.extractor.roundtrip")

# System fields excluded from round-trip checks (must match base._SYSTEM_FIELDS)
SYSTEM_FIELDS: frozenset[str] = frozenset({
    "uid",
    "type",
    "source",
    "source_id",
    "created",
    "updated",
    "source_email",
    "summary",
    "aliases",
    "extraction_confidence",
})

# Top ~500 IATA airport codes. Inline frozenset — no external data dependency.
_IATA_CODES: frozenset[str] = frozenset({
    "ATL", "DFW", "DEN", "ORD", "LAX", "CLT", "MCO", "LAS", "PHX", "MIA",
    "SEA", "IAH", "JFK", "EWR", "SFO", "FLL", "MSP", "BOS", "DTW", "PHL",
    "LGA", "BWI", "SLC", "SAN", "DCA", "IAD", "TPA", "MDW", "HNL", "PDX",
    "STL", "BNA", "OAK", "AUS", "MCI", "RDU", "SMF", "SJC", "CLE", "SAT",
    "IND", "PIT", "CVG", "CMH", "MKE", "ONT", "OGG", "RSW", "BDL", "JAX",
    "ANC", "BUF", "ABQ", "OMA", "LIH", "RIC", "TUL", "OKC", "KOA", "GEG",
    "BOI", "TUS", "LGB", "ELP", "SNA", "BHM", "DSM", "CHS", "MSY", "PBI",
    "SDF", "MEM", "GRR", "DAY", "PVD", "ORF", "GSP", "ROC", "SYR", "RNO",
    "SAV", "HSV", "PWM", "ICT", "BTV", "MHT", "PSP", "FAT", "LIT", "GSO",
    "LHR", "CDG", "FRA", "AMS", "MAD", "BCN", "FCO", "MXP", "ZRH", "VIE",
    "MUC", "IST", "DXB", "DOH", "SIN", "HKG", "NRT", "HND", "ICN", "BKK",
    "SYD", "MEL", "AKL", "DEL", "BOM", "PEK", "PVG", "CAN", "TPE", "KUL",
    "MNL", "CGK", "GIG", "GRU", "EZE", "SCL", "BOG", "LIM", "MEX", "CUN",
    "YYZ", "YVR", "YUL", "YOW", "YEG", "YYC",
})


def is_valid_iata_airport(code: str) -> bool:
    """True if ``code`` is a 3-letter code in the allowlist (same as flight card validation)."""
    s = str(code or "").strip().upper()
    if len(s) != 3 or not s.isalpha():
        return False
    return s in _IATA_CODES


_ENGLISH_BLOCKLIST: frozenset[str] = frozenset({
    "THAT", "THIS", "THEN", "THEM", "THAN", "THE", "THERE", "THEIR", "THEY",
    "HAVE", "BEEN", "WERE", "WILL", "WITH", "YOUR", "FROM", "INTO", "JUST",
    "ONLY", "VERY", "ALSO", "SOME", "SUCH", "EACH", "MAKE", "LIKE", "LONG",
    "LOOK", "MANY", "MOST", "MUCH", "MUST", "NAME", "NEED", "NEXT", "COME",
    "COULD", "WOULD", "SHOULD", "WHERE", "WHICH", "WHILE", "ABOUT", "AFTER",
    "AGAIN", "BEING", "EVERY", "FIRST", "FOUND", "GREAT", "HOUSE",
    "LARGE", "MIGHT", "NEVER", "OTHER", "PLACE", "POINT", "RIGHT", "SHALL",
    "SINCE", "SMALL", "STILL", "THINK", "THREE", "UNDER", "UNTIL", "WORLD",
    "THESE", "THOSE",
    "CONFIRMATION", "VACATION", "PEACEFUL", "EXPECTED", "NUMBER", "XXXXXX",
    "CONFIRMED", "RESERVED", "UPCOMING", "RECEIPT", "DETAILS", "BOOKING",
})

_MONTH_SUBSTR = (
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    "january", "february", "march", "april", "may", "june", "july", "august",
    "september", "october", "november", "december",
)


def _has_date_like(s: str) -> bool:
    if re.search(r"\d", s):
        return True
    sl = s.lower()
    return any(m in sl for m in _MONTH_SUBSTR)


def validate_field(card_type: str, field_name: str, value: Any) -> Any:
    """Return cleaned value, or None if value is garbage."""
    if value is None:
        return None
    if field_name == "items" and isinstance(value, list):
        return _validate_items_list(value)
    key = f"{card_type}.{field_name}"
    validator = _VALIDATORS.get(key) or _VALIDATORS.get(f"*.{field_name}")
    if validator:
        return validator(value)
    return value


def _validate_items_list(value: list[Any]) -> list[dict[str, Any]] | None:
    out: list[dict[str, Any]] = []
    for it in value:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or "").strip()
        if not name:
            continue
        nl = name.lower()
        if len(name) > 200:
            continue
        if "http://" in nl or "https://" in nl or "<http" in nl:
            continue
        out.append(it)
    return out if out else None


def _validate_restaurant(value: Any) -> Any | None:
    s = str(value or "").strip()
    if not s:
        return None
    if len(s) > 120:
        return None
    sl = s.lower()
    if "http://" in sl or "https://" in sl or "<http" in sl:
        return None
    for noise in (
        ".eats_footer",
        "bank statement",
        "rate order",
        "learn more",
        "contact support",
        "warning",
        ".eats_footer_table",
    ):
        if noise in sl:
            return None
    return s


def _validate_grocery_store(value: Any) -> Any | None:
    s = _validate_restaurant(value)
    if s is None:
        return None
    sl = s.lower()
    if sl in ("instacart", "the instacart app", "your cart"):
        return None
    if "instacart" in sl and len(sl) < 20:
        return None
    return s


def _validate_iata(value: Any) -> Any | None:
    s = str(value or "").strip().upper()
    if len(s) != 3 or not s.isalpha():
        return None
    if s not in _IATA_CODES:
        return None
    return s


def _validate_flight_fare(value: Any) -> Any | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v <= 0 or v > 50_000:
        return None
    return v


def _validate_ride_fare(value: Any) -> Any | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v <= 0 or v > 500:
        return None
    return v


def _validate_meal_total(value: Any) -> Any | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v <= 0 or v > 2_000:
        return None
    return v


def _validate_flight_confirmation(value: Any) -> Any | None:
    s = str(value or "").strip().upper()
    if len(s) != 6 or not s.isalnum():
        return None
    if s in _ENGLISH_BLOCKLIST:
        return None
    return s


def _validate_generic_confirmation(value: Any) -> Any | None:
    s = str(value or "").strip().upper()
    if not s:
        return None
    if s in _ENGLISH_BLOCKLIST:
        return None
    return str(value or "").strip()


def _validate_property_name(value: Any) -> Any | None:
    s = str(value or "").strip()
    if not s:
        return None
    if len(s) > 200:
        return None
    sl = s.lower()
    if "http://" in sl or "https://" in sl:
        return None
    for noise in (
        "would absolutely",
        "left clean",
        "great condition",
        "highly recommend",
        "communicated their plans",
        "no carbon monoxide",
    ):
        if noise in sl:
            return None
    return s


def _validate_date_field(value: Any) -> Any | None:
    s = str(value or "").strip()
    if not s:
        return None
    sl = s.lower()
    if "flexible" in sl or "as soon as possible" in sl:
        return None
    if sl in ("checkout", "check-in", "check out", "check in"):
        return None
    if not _has_date_like(s):
        return None
    return s


def _validate_pickup_at(value: Any) -> Any | None:
    s = str(value or "").strip()
    if not s:
        return None
    if not _has_date_like(s):
        return None
    return s


def _validate_location(value: Any) -> Any | None:
    s = str(value or "").strip()
    if not s or len(s) < 4:
        return None
    if len(s) > 300:
        return None
    sl = s.lower().strip()
    if sl in ("location:", "location", "pickup", "dropoff"):
        return None
    if re.match(r"^location:\s*$", s, re.I):
        return None
    return s


def _validate_shipment_date(value: Any) -> Any | None:
    s = str(value or "").strip()
    if not s:
        return None
    sl = s.lower()
    for noise in ("based on the selected service", "click", "limitations and ex"):
        if noise in sl:
            return None
    return s


def _validate_flight_time(value: Any) -> Any | None:
    s = str(value or "").strip()
    if not s:
        return None
    if len(s) > 100:
        return None
    sl = s.strip()
    if sl.lower() in ("city and time", "cabin", "arrive"):
        return None
    return s


_VALIDATORS: dict[str, Any] = {
    "meal_order.restaurant": _validate_restaurant,
    "meal_order.total": _validate_meal_total,
    "grocery_order.store": _validate_grocery_store,
    "grocery_order.total": _validate_meal_total,
    "flight.origin_airport": _validate_iata,
    "flight.destination_airport": _validate_iata,
    "flight.fare_amount": _validate_flight_fare,
    "flight.confirmation_code": _validate_flight_confirmation,
    "flight.departure_at": _validate_flight_time,
    "flight.arrival_at": _validate_flight_time,
    "accommodation.property_name": _validate_property_name,
    "accommodation.confirmation_code": _validate_generic_confirmation,
    "accommodation.check_in": _validate_date_field,
    "accommodation.check_out": _validate_date_field,
    "car_rental.confirmation_code": _validate_generic_confirmation,
    "car_rental.pickup_at": _validate_pickup_at,
    "car_rental.pickup_location": _validate_location,
    "car_rental.dropoff_location": _validate_location,
    "ride.pickup_location": _validate_location,
    "ride.dropoff_location": _validate_location,
    "ride.fare": _validate_ride_fare,
    "shipment.delivered_at": _validate_shipment_date,
    "shipment.estimated_delivery": _validate_shipment_date,
}


def validate_provenance_round_trip(
    card_data: dict[str, Any],
    source_body: str,
    card_type: str,
) -> list[str]:
    """Check that deterministically-extracted field values appear in the source email.

    Returns warning messages for fields that fail round-trip. Does NOT reject the card.
    """
    warnings: list[str] = []
    source_lower = re.sub(r"\s+", " ", source_body.lower().strip())

    for field_name, value in card_data.items():
        if field_name.startswith("_") or field_name in SYSTEM_FIELDS:
            continue
        if value is None or value == "" or value == 0 or value == 0.0:
            continue
        if isinstance(value, (list, dict)):
            continue
        val_str = str(value).strip()
        if len(val_str) < 3:
            continue
        val_normalized = re.sub(r"\s+", " ", val_str.lower().strip())
        if val_normalized not in source_lower:
            warnings.append(f"{card_type}.{field_name}: '{val_str[:60]}' not found in source email")
            _rt_log.debug("round-trip fail: %s.%s = '%s'", card_type, field_name, val_str[:80])

    return warnings
