"""Carrier tracking emails -> shipment cards."""

from __future__ import annotations

import re
from typing import Any

from archive_sync.extractors.base import EmailExtractor, TemplateVersion

_UPS = re.compile(r"\b1Z[\w0-9]{16}\b", re.I)
_FEDEX = re.compile(r"\b(\d{12}|\d{14}|\d{20})\b")
_USPS = re.compile(r"\b(?:9\d{21}|\d{22}|\d{30})\b")
_AMZ_TRACK = re.compile(r"\b(TBA\d{10,})\b", re.I)
_RELATIVE_ED = re.compile(
    r"(?i)^(today|tomorrow|tonight|this\s+(?:week|month)|"
    r"monday|tuesday|wednesday|thursday|friday|saturday|sunday)\.?\s*$",
)


def _carrier_from_email(from_email: str) -> str:
    fe = (from_email or "").lower()
    if "ups" in fe:
        return "UPS"
    if "fedex" in fe:
        return "FedEx"
    if "usps" in fe:
        return "USPS"
    if "amazon" in fe:
        return "Amazon"
    return ""


def _tracking(body: str, from_email: str) -> tuple[str, str]:
    for pat, car in (
        (_UPS, "UPS"),
        (_USPS, "USPS"),
        (_AMZ_TRACK, "Amazon"),
        (_FEDEX, "FedEx"),
    ):
        m = pat.search(body)
        if m:
            return m.group(0), car
    return "", _carrier_from_email(from_email)


def _sanitize_estimated_delivery(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    if _RELATIVE_ED.match(v):
        return ""
    if re.match(r"(?i)^(today|tomorrow)\.?\s*$", v):
        return ""
    lower = v.lower()
    if any(
        p in lower
        for p in (
            "limitations and",
            "based on the selected service",
            "destination and ship date",
            "once we receive",
            "will arrive in",
        )
    ):
        return ""
    return v[:80]


class ShippingExtractor(EmailExtractor):
    sender_patterns = [
        r".*@ups\.com$",
        r".*@fedex\.com$",
        r".*@usps\.com$",
        r".*@amazon\.com$",
    ]
    subject_patterns = [
        r"(?i).*(?:ship|track|deliver|package|on\s+the\s+way|out\s+for\s+delivery).*",
    ]
    reject_subject_patterns = [
        r"(?i).*(?:prime day|recommended for you|your deals|subscribe|newsletter).*",
    ]
    receipt_indicators = [
        "track",
        "package",
        "delivery",
        "tracking",
        "ship",
        "on the way",
        "shipped",
    ]

    def matches(self, from_email: str, subject: str) -> bool:
        from_email = (from_email or "").strip().lower()
        subject = subject or ""
        if re.search(r"@amazon\.com$", from_email):
            if not re.search(r"(?:ship|track|deliver|package)", subject, re.I):
                return False
        return super().matches(from_email, subject)

    output_card_type = "shipment"

    def template_versions(self) -> list[TemplateVersion]:
        def era(fm: dict[str, Any], body: str) -> list[dict[str, Any]]:
            from_email = str(fm.get("from_email") or "")
            track, carrier = _tracking(body, from_email)
            if not track or not carrier:
                return []
            row: dict[str, Any] = {
                "_discriminator": track,
                "carrier": carrier,
                "tracking_number": track,
                "_body": f"# Shipment {carrier} {track}\n",
            }
            for label, pat in (
                ("shipped_at", re.compile(r"(?:shipped|ship(?:ped)?\s+on)\s*[:\s]+([^\n]+)", re.I)),
                (
                    "estimated_delivery",
                    re.compile(
                        r"(?:estimated\s+delivery|expected\s+delivery|arriv(?:es|ing))\s*[:\s]+([^\n]+)",
                        re.I,
                    ),
                ),
            ):
                m = pat.search(body)
                if m:
                    val = m.group(1).strip()
                    if label == "estimated_delivery":
                        val = _sanitize_estimated_delivery(val)
                    row[label] = val[:80] if val else ""
            d_at = re.search(r"(?is)(?:^|\n)\s*delivered\s*:\s*([^\n]+)", body)
            if not d_at:
                d_at = re.search(r"(?im)^\s*delivered\s*$\s*\n\s*([^\n]+)", body)
            if d_at:
                row["delivered_at"] = d_at.group(1).strip()[:80]
            return [row]

        return [TemplateVersion("default", ("2000-01-01", "2099-12-31"), era)]
