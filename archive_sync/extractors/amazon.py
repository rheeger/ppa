"""Amazon order / shipment confirmations -> purchase cards."""

from __future__ import annotations

import re
from typing import Any

from archive_sync.extractors.base import EmailExtractor, TemplateVersion

_ORDER = re.compile(r"(?:order|order\s+#)\s*[:\s#]*([\d-]+)", re.I)
# Subject lines often carry the id when the body is boilerplate.
_AMAZON_ORDER_ID = re.compile(r"\b(\d{3}-\d{7}-\d{7})\b")


def _parse_items(body: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for line in body.splitlines():
        line = line.strip()
        m = re.match(r"^[-*]\s+(.+?)\s+\$(\d+\.?\d*)$", line)
        if m:
            items.append({"name": m.group(1).strip(), "price": m.group(2)})
            continue
        m2 = re.match(r"^\s*(\d+)\s+(.+?)\s+\$(\d+\.?\d*)\s*$", line)
        if m2 and not re.match(r"^\d{3}-\d{7}-\d{7}", line):
            items.append({"name": m2.group(2).strip(), "price": m2.group(3)})
    return items


def _totals(body: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for key, pat in (
        ("subtotal", re.compile(r"subtotal\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("tax", re.compile(r"tax\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("shipping_cost", re.compile(r"(?:shipping)\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("total", re.compile(r"(?:order\s+total|grand\s+total|total)\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
    ):
        m = pat.search(body)
        if m:
            try:
                out[key] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    if "total" not in out:
        m = re.search(r"(?is)(?:order\s+total|grand\s+total|total)\s*[\n\r]+\s*\$?\s*([\d,]+\.?\d*)", body)
        if m:
            try:
                out["total"] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    return out


class AmazonExtractor(EmailExtractor):
    sender_patterns = [r".*@amazon\.com$"]
    subject_patterns = [r"(?:order|delivery).*(?:confirm|confirmation)"]
    reject_subject_patterns = [
        r"(?i).*(?:rate your|review your|survey|watch\s+list|deals?\s+we|recommended for you).*",
    ]
    receipt_indicators = [
        "order",
        "subtotal",
        "total",
        "ship",
        "delivery",
        "amazon",
    ]
    output_card_type = "purchase"

    def template_versions(self) -> list[TemplateVersion]:
        def era(fm: dict[str, Any], body: str) -> list[dict[str, Any]]:
            subj = str(fm.get("subject") or "")
            om = _ORDER.search(body) or _ORDER.search(subj)
            if not om:
                m = _AMAZON_ORDER_ID.search(subj) or _AMAZON_ORDER_ID.search(body)
                if not m:
                    return []
                order_number = m.group(1).strip()
            else:
                order_number = om.group(1).strip()
            items = _parse_items(body)
            totals = _totals(body)
            if not items and not totals:
                return []
            row: dict[str, Any] = {
                "_discriminator": order_number,
                "vendor": "Amazon",
                "order_number": order_number,
                "items": items,
                "subtotal": float(totals.get("subtotal", 0.0)),
                "total": float(totals.get("total", totals.get("subtotal", 0.0))),
                "tax": float(totals.get("tax", 0.0)),
                "shipping_cost": float(totals.get("shipping_cost", 0.0)),
                "_body": f"# Amazon order {order_number}\n",
            }
            addr = re.search(r"(?:ship\s+to|shipping\s+address)\s*[:\s]+(.+?)(?:\n\n|\Z)", body, re.I | re.S)
            if addr:
                row["shipping_address"] = addr.group(1).strip().split("\n")[0]
            return [row]

        return [
            TemplateVersion("recent", ("2018-01-01", "2099-12-31"), era),
            TemplateVersion("legacy", ("2000-01-01", "2017-12-31"), era),
        ]
