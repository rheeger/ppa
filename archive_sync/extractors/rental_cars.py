"""Car rental confirmations -> car_rental cards."""

from __future__ import annotations

import re
from typing import Any

from archive_sync.extractors.base import EmailExtractor, TemplateVersion
from archive_sync.extractors.field_validation import validate_field


def _company(from_email: str) -> str:
    fe = (from_email or "").lower()
    if "national" in fe or "nationalcar" in fe:
        return "National"
    if "hertz" in fe:
        return "Hertz"
    if "emerald" in fe:
        return "National"
    return ""


def _trim_location_noise(loc: str) -> str:
    """Strip marketing / cancel-your-reservation sentences merged into one html2text line."""
    s = (loc or "").strip()
    if not s:
        return ""
    lower = s.lower()
    for phrase in (
        "please click on the link",
        "click on the link below",
        "below to cancel",
        "to cancel your reservation",
        "if you need to modify",
    ):
        i = lower.find(phrase)
        if i != -1:
            s = s[:i].strip()
            lower = s.lower()
    return s.rstrip(",; ").strip()


class RentalCarsExtractor(EmailExtractor):
    sender_patterns = [
        r".*@nationalcar\.com$",
        r".*@hertz\.com$",
        r".*@emeraldclub\.com$",
    ]
    output_card_type = "car_rental"
    reject_subject_patterns = [
        r"(?i).*(?:special offer|save \d|promotion|newsletter).*",
    ]
    receipt_indicators = [
        "confirmation",
        "reservation",
        "pickup",
        "rental",
        "return",
    ]

    def template_versions(self) -> list[TemplateVersion]:
        def era(fm: dict[str, Any], body: str) -> list[dict[str, Any]]:
            m = re.search(r"(?i)confirmation\s+number\s+is\s*[:\s]+\s*([A-Z0-9]{4,14})\b", body)
            if not m:
                m = re.search(
                    r"(?:confirmation|reservation)\s*(?:number|#)?\s*:\s*([A-Z0-9]{4,12})",
                    body,
                    re.I,
                )
            if not m:
                m = re.search(r"\b([A-Z]{1,2}\d{6,10})\b", body)
            if not m:
                return []
            code = m.group(1).upper()
            if validate_field("car_rental", "confirmation_code", code) is None:
                return []
            company = _company(str(fm.get("from_email") or ""))
            if not company:
                return []
            pickup = ""
            dropoff = ""
            m = re.search(r"(?:pick\s*up|pickup)\s*(?:location|at)?\s*[:\s]+(.+?)(?:\n|$)", body, re.I)
            if m:
                pickup = _trim_location_noise(m.group(1).strip())
            if not pickup:
                m = re.search(r"(?is)\bat\s+(.+?)\s+Your confirmation number", body)
                if m:
                    pickup = _trim_location_noise(m.group(1).strip().rstrip(".").strip())
            m = re.search(r"(?:return|drop\s*off)\s*[:\s]+(.+?)(?:\n|$)", body, re.I)
            if m:
                dropoff = _trim_location_noise(m.group(1).strip())
            p_at = ""
            d_at = ""
            m = re.search(r"(?:pickup|pick\s*up)\s+(?:date|time)\s*[:\s]+([^\n]+)", body, re.I)
            if m:
                p_at = m.group(1).strip()[:80]
            if not p_at:
                m = re.search(
                    r"(?i)vehicle\s+on\s+([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})",
                    body,
                )
                if m:
                    p_at = m.group(1).strip()[:80]
            if not p_at:
                m = re.search(
                    r"(?i)PICK[- ]?UP\b.*?(\w+day,?\s+[A-Z][a-z]+\s+\d{1,2},?\s+\d{4})",
                    body,
                    re.S,
                )
                if m:
                    p_at = m.group(1).strip()[:80]
            if not p_at:
                m = re.search(
                    r"(?i)reserved.*?on\s+([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})",
                    body,
                )
                if m:
                    p_at = m.group(1).strip()[:80]
            m = re.search(r"(?:return)\s+(?:date|time)\s*[:\s]+([^\n]+)", body, re.I)
            if m:
                d_at = m.group(1).strip()[:80]
            if not d_at:
                m = re.search(
                    r"(?i)(?:RETURN|DROP[- ]?OFF)\b.*?(\w+day,?\s+[A-Z][a-z]+\s+\d{1,2},?\s+\d{4})",
                    body,
                    re.S,
                )
                if m:
                    d_at = m.group(1).strip()[:80]
            total = 0.0
            m = re.search(r"(?:total)\s*[:\s]+\$?\s*([\d,]+\.?\d*)", body, re.I)
            if m:
                try:
                    total = float(m.group(1).replace(",", ""))
                except ValueError:
                    pass
            row: dict[str, Any] = {
                "_discriminator": code,
                "company": company,
                "confirmation_code": code,
                "pickup_location": pickup,
                "dropoff_location": dropoff,
                "pickup_at": p_at,
                "dropoff_at": d_at,
                "total_cost": total,
                "_body": f"# {company} rental {code}\n",
            }
            return [row]

        return [TemplateVersion("default", ("2000-01-01", "2099-12-31"), era)]
