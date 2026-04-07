"""Lyft ride receipts -> ride cards."""

from __future__ import annotations

from typing import Any

from archive_sync.extractors.base import EmailExtractor, TemplateVersion
from archive_sync.extractors.ride_common import parse_ride_receipt_fields


def _render_ride_md(fields: dict[str, Any]) -> str:
    lines = ["# Lyft ride", ""]
    for k, label in (
        ("pickup_location", "Pickup"),
        ("dropoff_location", "Dropoff"),
        ("pickup_at", "Pickup at"),
        ("fare", "Fare"),
        ("tip", "Tip"),
        ("driver_name", "Driver"),
    ):
        if fields.get(k):
            lines.append(f"- **{label}**: {fields[k]}")
    return "\n".join(lines)


class LyftExtractor(EmailExtractor):
    sender_patterns = [r".*@lyft\.com$", r".*@lyftmail\.com$"]
    subject_patterns = [r".*(?:receipt|ride|trip|lyft).*"]
    output_card_type = "ride"
    reject_subject_patterns = [
        r"(?i).*(?:% off|promo|newsletter|refer).*",
    ]
    receipt_indicators = [
        "ride",
        "receipt",
        "pickup",
        "total",
        "lyft",
    ]

    def template_versions(self) -> list[TemplateVersion]:
        def era(fm: dict[str, Any], body: str) -> list[dict[str, Any]]:
            sent = str(fm.get("sent_at") or "")
            fields = parse_ride_receipt_fields(body, sent, skip_if_uber_eats=False)
            if not fields:
                return []
            pickup_at = fields["pickup_at"] or sent
            row: dict[str, Any] = {
                "_discriminator": pickup_at,
                "service": "Lyft",
                "ride_type": "car",
                "pickup_location": fields["pickup_location"],
                "dropoff_location": fields["dropoff_location"],
                "pickup_at": pickup_at,
                "dropoff_at": str(fields.get("dropoff_at") or ""),
                "fare": float(fields.get("fare", 0.0)),
                "tip": float(fields.get("tip", 0.0)),
                "distance_miles": float(fields.get("distance_miles", 0.0)),
                "duration_minutes": float(fields.get("duration_minutes", 0.0)),
                "driver_name": str(fields.get("driver_name", "")),
                "vehicle": str(fields.get("vehicle", "")),
                "_body": _render_ride_md({**fields, "pickup_at": pickup_at}),
            }
            return [row]

        return [TemplateVersion("default", ("2000-01-01", "2099-12-31"), era)]
