"""Instacart grocery receipts -> grocery_order."""

from __future__ import annotations

import re
from typing import Any

from archive_sync.extractors.base import EmailExtractor, TemplateVersion


def _store_key(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def _parse_totals(body: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for key, pat in (
        ("subtotal", re.compile(r"subtotal\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("total", re.compile(r"(?:^|\n)total\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("delivery_fee", re.compile(r"(?:delivery|service)\s*fee\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
    ):
        m = pat.search(body)
        if m:
            try:
                out[key] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    if "total" not in out:
        m = re.search(r"(?i)order\s+totals?\s*[:\s]+\$?\s*([\d,]+\.?\d*)", body)
        if m:
            try:
                out["total"] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    if "total" not in out:
        m = re.search(r"(?is)(?:^|\n)\s*total\s*[\n\r]+\s*\$?\s*([\d,]+\.?\d*)", body)
        if m:
            try:
                out["total"] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    return out


def _store_from_subject(subject: str) -> str:
    s = (subject or "").strip()
    for pat in (
        re.compile(
            r"(?i)your\s+order\s+from\s+(.+?)(?:\s+was\s+delivered|\s+is\s+|\s+has\s+been\s+|\s+—|\s*$)",
        ),
        re.compile(r"(?i)delivery\s+from\s+(.+?)(?:\s+is\s+|$)"),
    ):
        m = pat.search(s)
        if m:
            name = m.group(1).strip().split("—")[0].strip()
            sk = _store_key(name)
            if name and sk not in ("instacart", "the instacart app", "your cart") and len(name) < 120:
                return name
    return ""


def _store_from_body(body: str) -> str:
    m = re.search(r"(?i)your\s+order\s+from\s+(.+?)\s+was\s+delivered\b", body)
    if m:
        name = m.group(1).strip().split("\n")[0].strip()
        sk = _store_key(name)
        if sk and sk not in ("instacart", "the instacart app", "your cart"):
            return name
    m = re.search(r"(?im)^From\s*:\s*(.+)$", body)
    if m:
        name = m.group(1).strip().split("\n")[0].strip()
        sk = _store_key(name)
        if sk and sk not in ("instacart", "the instacart app", "your cart"):
            return name
    for pat in (
        re.compile(r"(?i)your\s+(?:order|delivery)\s+from\s+(.+?)(?:\n|$)"),
        re.compile(r"(?i)(?:^|\n)(?:store|from)\s*[:\s]+(.+?)(?:\n|$)"),
        re.compile(r"(?i)shopping\s+(?:at|from)\s+(.+?)(?:\n|$)"),
    ):
        m = pat.search(body)
        if m:
            name = m.group(1).strip().split("\n")[0].strip()
            sk = _store_key(name)
            if sk and sk not in ("instacart", "the instacart app", "your cart"):
                return name
    return ""


class InstacartExtractor(EmailExtractor):
    sender_patterns = [
        r".*@instacartemail\.com$",
        r".*@instacart\.com$",
    ]
    output_card_type = "grocery_order"
    reject_subject_patterns = [
        r"(?i).*(?:shop now|new items|% off|deal|promo|last chance).*",
    ]
    receipt_indicators = [
        "order",
        "receipt",
        "subtotal",
        "total",
        "delivered",
        "delivery",
    ]

    def template_versions(self) -> list[TemplateVersion]:
        def era(fm: dict[str, Any], body: str) -> list[dict[str, Any]]:
            store = _store_from_subject(str(fm.get("subject") or "")) or _store_from_body(body)
            if not store:
                return []
            totals = _parse_totals(body)
            total = float(totals.get("total", totals.get("subtotal", 0.0)))
            if total <= 0:
                return []
            row: dict[str, Any] = {
                "_discriminator": _store_key(store),
                "service": "Instacart",
                "store": store,
                "items": [],
                "subtotal": float(totals.get("subtotal", 0.0)),
                "total": total,
                "delivery_fee": float(totals.get("delivery_fee", 0.0)),
                "_body": f"# Instacart — {store}\n",
            }
            return [row]

        return [TemplateVersion("default", ("2000-01-01", "2099-12-31"), era)]
