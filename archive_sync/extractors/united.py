"""United Airlines emails -> flight cards."""

from __future__ import annotations

import re
from typing import Any

from archive_sync.extractors.base import EmailExtractor, TemplateVersion
from archive_sync.extractors.field_validation import is_valid_iata_airport

_CONF = re.compile(
    r"(?:confirmation|record\s*locator)\s*(?:number|code)?\s*[:\s#]+([A-Z0-9]{6})\b",
    re.I,
)
_ROUTE = re.compile(r"\b([A-Z]{3})\s*(?:to|→|-|/)\s*([A-Z]{3})\b", re.IGNORECASE)
_BAD_CODES = frozenset({"NUMBER", "XXXXXX"})


def _best_airport_route(body: str) -> tuple[str, str]:
    """First XYZ–ABC pair where both codes are real IATA (avoids SUB→TAL template noise)."""
    # Calendar / plain-text itineraries (before generic X-Y patterns that can hit "SFO - San …")
    dep = re.search(r"(?i)depart\s*:\s*([A-Z]{3})\s*[-–]", body)
    arr = re.search(r"(?i)arriv(?:e|al)\s*:\s*([A-Z]{3})\s*[-–]", body)
    if dep and arr:
        o, d = dep.group(1).upper(), arr.group(1).upper()
        if is_valid_iata_airport(o) and is_valid_iata_airport(d):
            return o, d
    for m in _ROUTE.finditer(body):
        o, d = m.group(1).upper(), m.group(2).upper()
        if is_valid_iata_airport(o) and is_valid_iata_airport(d):
            return o, d
    # United purchase receipts: "City…(EWR)   City…(LAX)" without EWR-LAX or "to" between
    codes: list[str] = []
    for m in re.finditer(r"\(([A-Z]{3})\)", body):
        c = m.group(1).upper()
        if is_valid_iata_airport(c):
            codes.append(c)
        if len(codes) >= 2:
            return codes[0], codes[1]
    return "", ""


def _valid_confirmation_code(code: str) -> bool:
    u = code.upper()
    if len(u) != 6 or not u.isalnum():
        return False
    if u in _BAD_CODES:
        return False
    return True


class UnitedExtractor(EmailExtractor):
    sender_patterns = [r".*@united\.com$"]
    output_card_type = "flight"
    reject_subject_patterns = [
        r"(?i).*(?:mileageplus offer|bonus miles|sale|save \d|deal|promo|newsletter).*",
    ]
    receipt_indicators = [
        "confirmation",
        "itinerary",
        "departure",
        "flight",
        "united",
        "record locator",
        "e-ticket",
    ]

    def template_versions(self) -> list[TemplateVersion]:
        def era(fm: dict[str, Any], body: str) -> list[dict[str, Any]]:
            cm = _CONF.search(body)
            if not cm:
                return []
            code = cm.group(1).upper()
            if not _valid_confirmation_code(code):
                return []
            origin, dest = _best_airport_route(body)
            dep = ""
            arr = ""
            m = re.search(r"(?:depart|departure)\s*[:\s]+([^\n]+)", body, re.I)
            if m:
                dep = m.group(1).strip()[:80]
            m = re.search(r"(?:arriv(?:e|al))\s*[:\s]+([^\n]+)", body, re.I)
            if m:
                arr = m.group(1).strip()[:80]
            fare = 0.0
            m = re.search(r"(?:total|fare)\s*[:\s]+\$?\s*([\d,]+\.?\d*)", body, re.I)
            if m:
                try:
                    fare = float(m.group(1).replace(",", ""))
                except ValueError:
                    pass
            if not origin or not dest:
                return []
            row: dict[str, Any] = {
                "_discriminator": code,
                "airline": "United",
                "confirmation_code": code,
                "origin_airport": origin,
                "destination_airport": dest,
                "departure_at": dep,
                "arrival_at": arr,
                "fare_amount": fare,
                "booking_source": "United",
                "_body": f"# United {origin}→{dest} {code}\n",
            }
            return [row]

        return [TemplateVersion("default", ("2000-01-01", "2099-12-31"), era)]
