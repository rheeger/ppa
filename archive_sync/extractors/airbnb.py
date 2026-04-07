"""Airbnb reservation emails -> accommodation cards."""

from __future__ import annotations

import re
from typing import Any

from archive_sync.extractors.base import EmailExtractor, TemplateVersion

_CONF = re.compile(
    r"(?i)(?:confirmation|record\s*locator)\s*(?:code|number)?\s*[:\s#–—-]+\s*([A-Z0-9]{6,12})\b",
)


def _property_from_subject(subject: str) -> str:
    s = (subject or "").strip()
    m = re.search(r"(?i)Reservation\s+at\s+(.+?)\s+for\s+", s)
    if m:
        name = m.group(1).strip()
        if name and len(name) > 2:
            return name[:200]
    for sep in ("—", "–", "-"):
        if sep in s:
            tail = s.split(sep, 1)[1].strip()
            tl = tail.lower()
            if len(tail) > 2 and not tl.startswith("airbnb") and tl not in ("confirmed", "reminder", "itinerary"):
                return tail[:200]
    return ""


def _property_from_body(body: str) -> str:
    for pat in (
        re.compile(r"(?i)your\s+trip\s+to\s+(.+?)(?:\n|$)"),
        re.compile(r"(?i)your\s+(?:reservation|trip|stay)\s+(?:at|in)\s+(.+?)(?:\n|$)"),
        re.compile(r"(?i)(?:listing|property|home)\s*[:\s]+(.+?)(?:\n|$)"),
        re.compile(r"(?i)trip\s*[:\s]+(.+?)(?:\n|$)"),
    ):
        m = pat.search(body)
        if m:
            name = m.group(1).strip().split("\n")[0].strip()
            if name and name.lower() not in ("airbnb", "coming up", "confirmed"):
                return name
    return ""


class AirbnbExtractor(EmailExtractor):
    sender_patterns = [r".*@airbnb\.com$", r".*@guest\.airbnb\.com$"]
    output_card_type = "accommodation"
    reject_subject_patterns = [
        r"(?i).*(?:explore stays|plan your|inspiration|newsletter|%\s*off).*",
    ]
    receipt_indicators = [
        "reservation",
        "confirmation",
        "check-in",
        "check out",
        "checkout",
        "itinerary",
        "guest",
    ]

    def template_versions(self) -> list[TemplateVersion]:
        def era(fm: dict[str, Any], body: str) -> list[dict[str, Any]]:
            subject = str(fm.get("subject") or "")
            code = ""
            m = _CONF.search(body)
            if m:
                code = m.group(1).upper()
            if not code:
                m = re.search(r"(?im)reservation\s+code\s*\n\s*([A-Z0-9]{6,12})\b", body)
                if m:
                    code = m.group(1).upper()
            if not code:
                m = re.search(
                    r"(?:itinerary|receipt|change)\?[^&\s\n]*\bcode=([A-Z0-9]{6,12})\b",
                    body,
                    re.I,
                )
                if m:
                    code = m.group(1).upper()
            if not code:
                dm = re.search(r"\b(\d{8,12})\b", body)
                if dm:
                    code = dm.group(1)
            if not code:
                cm = re.search(r"\b([A-Z0-9]{8,12})\b", body)
                if cm and cm.group(1).upper() not in ("RESERVATION", "CONFIRMED"):
                    code = cm.group(1).upper()
            if not code:
                return []
            prop = _property_from_subject(subject) or _property_from_body(body)
            if not prop:
                return []
            check_in = ""
            check_out = ""
            m = re.search(r"check[-\s]?in\s*[:\s]+([^\n]+)", body, re.I)
            if m:
                check_in = m.group(1).strip()[:80]
            m = re.search(r"check[-\s]?out\s*[:\s]+([^\n]+)", body, re.I)
            if m:
                check_out = m.group(1).strip()[:80]
            # Subject: "for Jun 11 - 23, 2021" or "for July 26, 2020 - August 7, 2020"
            if not check_in or not check_out:
                m = re.search(
                    r"for\s+([A-Z][a-z]+\s+\d{1,2}(?:,\s*\d{4})?)\s*[-–]\s*([A-Z][a-z]+\s+\d{1,2},?\s*\d{4})",
                    subject,
                )
                if m:
                    check_in = check_in or m.group(1).strip()[:80]
                    check_out = check_out or m.group(2).strip()[:80]
            # URL params: check_in=2026-01-04&check_out=2026-03-14
            if not check_in:
                m = re.search(r"check_in=(\d{4}-\d{2}-\d{2})", body)
                if m:
                    check_in = m.group(1)
            if not check_out:
                m = re.search(r"check_out=(\d{4}-\d{2}-\d{2})", body)
                if m:
                    check_out = m.group(1)
            # Body: "Sunday Jul 26, 2020 - Friday Aug 07, 2020"
            if not check_in or not check_out:
                m = re.search(
                    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+"
                    r"([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})\s*[-–]\s*"
                    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+"
                    r"([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})",
                    body,
                )
                if m:
                    check_in = check_in or m.group(1).strip()[:80]
                    check_out = check_out or m.group(2).strip()[:80]
            addr = ""
            m = re.search(r"(?:address)\s*[:\s]+(.+?)(?:\n\n|\n[A-Z])", body, re.I | re.S)
            if m:
                addr = m.group(1).strip().split("\n")[0]
            total = 0.0
            m = re.search(r"(?is)amount\s*[\n\r]+\s*\$?\s*([\d,]+\.?\d*)", body)
            if m:
                try:
                    total = float(m.group(1).replace(",", ""))
                except ValueError:
                    pass
            if total <= 0:
                m = re.search(r"(?:total|amount)\s*[:\s]+\$?\s*([\d,]+\.?\d*)", body, re.I)
                if m:
                    try:
                        total = float(m.group(1).replace(",", ""))
                    except ValueError:
                        pass
            row: dict[str, Any] = {
                "_discriminator": code,
                "property_name": prop,
                "property_type": "short_term_rental",
                "address": addr,
                "check_in": check_in,
                "check_out": check_out,
                "confirmation_code": code,
                "total_cost": total,
                "booking_source": "Airbnb",
                "_body": f"# Airbnb {prop}\n",
            }
            return [row]

        return [TemplateVersion("default", ("2000-01-01", "2099-12-31"), era)]
