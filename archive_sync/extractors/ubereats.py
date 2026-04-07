"""Uber Eats receipts -> meal_order."""

from __future__ import annotations

import re
from typing import Any

from archive_sync.extractors.base import EmailExtractor, TemplateVersion


def _normalize_restaurant_key(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def _parse_totals(body: str) -> dict[str, float]:
    out: dict[str, float] = {}
    body = _split_horizontal_runs(body)
    for key, pat in (
        ("subtotal", re.compile(r"subtotal\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("tax", re.compile(r"tax\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("total", re.compile(r"(?:^|\n)total\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
    ):
        m = pat.search(body)
        if m:
            try:
                out[key] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    if "total" not in out:
        m = re.search(r"(?:^|\n)total\s+charged\s+\$?\s*([\d,]+\.?\d*)", body, re.I)
        if m:
            try:
                out["total"] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    return out


def _trim_ue_horizontal_noise(name: str) -> str:
    """Uber Eats html2text often collapses receipt + map + footer into one line."""
    s = (name or "").strip()
    for sep in (
        "Picked up from",
        "Delivered to",
        "Delivered by",
        "Rate order",
        "Contact support",
        ".eats_footer",
        "Contact us",
    ):
        if sep in s:
            s = s.split(sep)[0].strip()
    s = re.split(r"\s*[—–-]\s*uber\s*eats\b", s, flags=re.I)[0].strip()
    return s


def _restaurant_from_subject(subject: str) -> str:
    s = (subject or "").strip()
    for pat in (
        re.compile(r"(?i)your\s+uber\s+eats\s+order\s+with\s+(.+)$"),
        re.compile(r"(?i)uber\s+eats\s+order\s*[:\s]+\s*(.+)$"),
    ):
        m = pat.search(s)
        if m:
            name = m.group(1).strip().split("\n")[0].strip()
            if name and len(name) < 120:
                return name
    return ""


def _restaurant_from_body(body: str) -> str:
    for pat in (
        re.compile(r"(?i)your\s+order\s+from\s+(.+?)(?:\n|$)"),
        re.compile(r"(?i)(?:^|\n)order\s+from\s+(.+?)(?:\n|$)"),
        re.compile(r"(?i)(?:from|order\s+from)\s*[:\s]+(.+?)(?:\n|$)"),
    ):
        m = pat.search(body)
        if m:
            name = m.group(1).strip().split("\n")[0].strip()
            name = _trim_ue_horizontal_noise(name)
            return name
    return ""


def _split_horizontal_runs(body: str) -> str:
    body = body.replace("\xa0", " ")
    body = re.sub(r" {3,}", "\n", body)
    return body


def _parse_items(body: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    body = _split_horizontal_runs(body)
    lines = body.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        i += 1
        if not line:
            continue
        m = re.match(r"^[-*]\s+(.+?)\s+x\s*(\d+)\s+\$?\s*([\d,]+\.?\d*)\s*$", line, re.I)
        if m:
            items.append(
                {"name": m.group(1).strip(), "quantity": int(m.group(2)), "price": m.group(3).strip()}
            )
            continue
        m2 = re.match(r"^[-*]\s+(.+?)\s+\$(\d+\.?\d*)$", line)
        if m2:
            items.append({"name": m2.group(1).strip(), "quantity": 1, "price": m2.group(2)})
            continue
        lower = line.lower()
        if "point per eligible" in lower or "diamond benefit" in lower:
            continue
        # Same-line: "1   ITEM NAME      $12.34"
        m3 = re.match(r"^\s*(\d+)\s+(.+?)\s+\$([\d,]+\.?\d*)\s*$", line)
        if m3:
            items.append(
                {
                    "name": m3.group(2).strip(),
                    "quantity": int(m3.group(1)),
                    "price": m3.group(3).strip(),
                }
            )
            continue
        # Stacked after horizontal split: qty alone → name → $price (each on own line)
        if re.match(r"^\d+$", line) and int(line) < 20:
            qty = int(line)
            if i < len(lines):
                name_line = lines[i].strip()
                i += 1
                if (
                    name_line
                    and not re.match(r"^[\$\d@{.<]", name_line)
                    and len(name_line) > 1
                    and len(name_line) < 120
                ):
                    price = ""
                    if i < len(lines):
                        price_line = lines[i].strip()
                        pm = re.match(r"^\$([\d,]+\.?\d*)$", price_line)
                        if pm:
                            price = pm.group(1)
                            i += 1
                    nl = name_line.lower()
                    if nl not in ("subtotal", "total", "tax", "tip", "delivery fee", "service fee"):
                        items.append({"name": name_line, "quantity": qty, "price": price})
    return items


class UberEatsExtractor(EmailExtractor):
    """Registered before UberRidesExtractor so Eats subjects win on uber.com."""

    sender_patterns = [r"^ubereats@uber\.com$", r".*@uber\.com$"]
    subject_patterns = [r".*Uber\s*Eats.*"]
    output_card_type = "meal_order"
    reject_subject_patterns = [
        r"(?i).*(?:% off|save \$|deal|promo|free delivery|last chance|reward).*",
    ]
    receipt_indicators = [
        "order from",
        "your order",
        "receipt",
        "subtotal",
        "total",
        "delivery",
    ]

    def matches(self, from_email: str, subject: str) -> bool:
        from_email = (from_email or "").strip().lower()
        subject = subject or ""
        if from_email == "ubereats@uber.com":
            return True
        if re.search(r"@uber\.com$", from_email) and re.search(r"uber\s*eats", subject, re.I):
            return True
        return False

    def template_versions(self) -> list[TemplateVersion]:
        def era(fm: dict[str, Any], body: str) -> list[dict[str, Any]]:
            subject = str(fm.get("subject") or "")
            restaurant = _restaurant_from_subject(subject) or _restaurant_from_body(body)
            if not restaurant:
                return []
            rk = _normalize_restaurant_key(restaurant)
            if rk in ("uber eats", "eats"):
                return []
            totals = _parse_totals(body)
            total = float(totals.get("total", totals.get("subtotal", 0.0)))
            if total <= 0:
                return []
            items = _parse_items(body)
            disc = _normalize_restaurant_key(restaurant)
            row: dict[str, Any] = {
                "_discriminator": disc,
                "service": "Uber Eats",
                "restaurant": restaurant,
                "items": items,
                "subtotal": float(totals.get("subtotal", 0.0)),
                "total": total,
                "tax": float(totals.get("tax", 0.0)),
                "mode": "delivery",
                "_body": f"# Uber Eats — {restaurant}\n",
            }
            return [row]

        return [TemplateVersion("default", ("2000-01-01", "2099-12-31"), era)]
