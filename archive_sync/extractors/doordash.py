"""DoorDash receipt email -> meal_order cards."""

from __future__ import annotations

import re
from typing import Any

from archive_sync.extractors.base import EmailExtractor, TemplateVersion

_MONEY = re.compile(r"\$\s*([\d,]+\.?\d*)")
_BAD_RESTAURANTS = frozenset({"doordash", "doordash order", "order", "unknown restaurant", ""})


def _money(line: str) -> float | None:
    m = _MONEY.search(line)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _normalize_restaurant_key(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def _split_horizontal_runs(body: str) -> str:
    """Break html2text single-line runs into separate lines for item parsing."""
    body = body.replace("\xa0", " ")
    body = re.sub(r" {3,}", "\n", body)
    return body


def _parse_items_block(body: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    body = _split_horizontal_runs(body)
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("|") and line.count("|") >= 3:
            cells = [c.strip() for c in line.strip("|").split("|")]
            cells = [c for c in cells if c and not re.match(r"^-{3,}$", c)]
            if len(cells) >= 2 and cells[0].lower() not in ("item", "name", "qty", "quantity", "price"):
                name = cells[0]
                qty = 1
                price = ""
                if len(cells) >= 3:
                    qcell = cells[1]
                    if qcell.isdigit():
                        qty = int(qcell)
                    price = cells[-1] if "$" in cells[-1] or re.search(r"\d+\.\d{2}", cells[-1]) else ""
                elif len(cells) == 2 and _money(cells[1]):
                    price = cells[1]
                if name and not name.startswith("---"):
                    items.append({"name": name, "quantity": qty, "price": price or ""})
            continue
        m = re.match(r"^[-*]\s+(.+?)\s+x\s*(\d+)\s+(.+)$", line, re.I)
        if m:
            items.append({"name": m.group(1).strip(), "quantity": int(m.group(2)), "price": m.group(3).strip()})
            continue
        m = re.match(r"^(.+?)\s+x\s*(\d+)\s+\$?\s*([\d,]+\.?\d*)\s*$", line, re.I)
        if m:
            items.append(
                {
                    "name": m.group(1).strip(),
                    "quantity": int(m.group(2)),
                    "price": m.group(3).strip(),
                }
            )
            continue
        m2 = re.match(r"^[-*]\s+(.+?)\s+\$(\d+\.?\d*)$", line)
        if m2:
            items.append({"name": m2.group(1).strip(), "quantity": 1, "price": m2.group(2)})
            continue
        # "1xItem Name (Category)" — older DoorDash receipts
        m3 = re.match(r"^(\d+)x(.+?)(?:\s+\$(\d+\.?\d*))?\s*$", line, re.I)
        if m3:
            name = m3.group(2).strip()
            if name and len(name) > 1 and name.lower() not in ("subtotal", "total", "tax", "tip"):
                items.append({
                    "name": name,
                    "quantity": int(m3.group(1)),
                    "price": m3.group(3) or "",
                })
    return items


def _parse_totals(body: str) -> dict[str, float]:
    out: dict[str, float] = {}
    body = _split_horizontal_runs(body)
    for key, pat in (
        ("subtotal", re.compile(r"(?<!\.)\bsubtotal\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("tax", re.compile(r"\btax\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("delivery_fee", re.compile(r"(?:delivery\s*fee|delivery)\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("tip", re.compile(r"\btip\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
        ("total", re.compile(r"(?:^|\n)\s*total\s*[:\s]+\$?\s*([\d,]+\.?\d*)", re.I)),
    ):
        m = pat.search(body)
        if m:
            try:
                out[key] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    if "total" not in out:
        m = re.search(r"(?:^|\n)\s*total\s+charged\s+\$?\s*([\d,]+\.?\d*)", body, re.I)
        if m:
            try:
                out["total"] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    # Compact post-2024 confirmations: "…Restaurant Name Total: $12.34" (single line, no Subtotal block)
    if "total" not in out:
        m = re.search(
            r"(?i)credits\s+(.+?)\s+Total\s*:\s*\$?\s*([\d,]+\.?\d*)",
            body,
        )
        if m:
            try:
                out["total"] = float(m.group(2).replace(",", ""))
            except ValueError:
                pass
    if "total" not in out:
        m = re.search(r"(?i)estimated\s+total\s*[\s\n]*\$?\s*([\d,]+\.?\d*)", body)
        if m:
            try:
                out["total"] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    if "total" not in out:
        m = re.search(r"(?i)(?:^|[\n\r])\s*Total\s*:\s*\$?\s*([\d,]+\.?\d*)", body)
        if m:
            try:
                out["total"] = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    return out


def _restaurant_from_subject(subject: str) -> str:
    s = (subject or "").strip()
    for pat in (
        re.compile(r"(?i)your\s+order\s+from\s+(.+)$"),
        re.compile(r"(?i)order\s+from\s+(.+)$"),
        # "Order Confirmation for Robbie from Burma Love" (common DoorDash subject)
        re.compile(r"(?i)order\s+confirmation\s+for\s+.+?\s+from\s+(.+)$"),
    ):
        m = pat.search(s)
        if m:
            name = m.group(1).strip()
            if name and len(name) < 120:
                return name.split("\n")[0].strip()
    return ""


def _restaurant_from_body(body: str) -> str:
    for pat in (
        re.compile(r"(?i)your\s+order\s+from\s+(.+?)(?:\n|$)"),
        re.compile(r"(?i)order\s+from\s+(.+?)(?:\n|$)"),
        re.compile(r"(?i)(?:^|\n)from\s+(.+?)(?:\n\n|\nSubtotal|\nTotal|\Z)"),
        re.compile(r"(?i)restaurant\s*[:\s]+(.+?)(?:\n|$)"),
    ):
        m = pat.search(body)
        if m:
            name = m.group(1).strip().split("\n")[0].strip()
            if name:
                # Stop at common footer / legal noise (Prop 65, URLs)
                lower = name.lower()
                if "http" in lower or "warning" in lower or "p65" in lower or "ca.gov" in lower:
                    continue
                return name
    return ""


def _restaurant_from_credits_line(body: str) -> str:
    """Compact confirmations: '…credits {Restaurant Name} Total: $…' with no order-from line."""
    m = re.search(r"(?i)credits\s+(.+?)\s+Total\s*:", body)
    if not m:
        return ""
    name = m.group(1).strip().split("\n")[0].strip()
    if not name or _normalize_restaurant_key(name) in _BAD_RESTAURANTS:
        return ""
    return name


def _render_meal_markdown(restaurant: str, items: list[dict[str, Any]], totals: dict[str, float]) -> str:
    lines = [f"# DoorDash — {restaurant or 'Order'}", ""]
    if items:
        lines.append("| Item | Qty | Price |")
        lines.append("| --- | --- | --- |")
        for it in items:
            lines.append(f"| {it.get('name', '')} | {it.get('quantity', 1)} | {it.get('price', '')} |")
        lines.append("")
    for k in ("subtotal", "delivery_fee", "tax", "tip", "total"):
        if k in totals:
            lines.append(f"- **{k.replace('_', ' ').title()}**: ${totals[k]:.2f}")
    return "\n".join(lines)


class DoordashExtractor(EmailExtractor):
    sender_patterns = [
        r".*@doordash\.com$",
        r".*@messages\.doordash\.com$",
    ]
    output_card_type = "meal_order"
    reject_subject_patterns = [
        r"(?i).*(?:off|save|deal|dashpass|credit|earn|refer|reward|promo|miss out|last chance|week \d).*",
    ]
    receipt_indicators = [
        "order from",
        "your order",
        "order confirmed",
        "receipt",
        "subtotal",
        "delivery fee",
        "total",
    ]

    def template_versions(self) -> list[TemplateVersion]:
        def era_plain(fm: dict[str, Any], body: str) -> list[dict[str, Any]]:
            subject = str(fm.get("subject") or "")
            restaurant = (
                _restaurant_from_subject(subject)
                or _restaurant_from_body(body)
                or _restaurant_from_credits_line(body)
            )
            rk = _normalize_restaurant_key(restaurant)
            if rk in _BAD_RESTAURANTS:
                restaurant = ""
            items = _parse_items_block(body)
            totals = _parse_totals(body)
            total = float(totals.get("total", totals.get("subtotal", 0.0)))
            if not restaurant and not items and total <= 0:
                return []
            if not restaurant or rk in _BAD_RESTAURANTS:
                return []
            if total <= 0 and not items:
                return []
            disc = _normalize_restaurant_key(restaurant)
            row: dict[str, Any] = {
                "_discriminator": disc,
                "service": "DoorDash",
                "restaurant": restaurant,
                "items": items,
                "subtotal": float(totals.get("subtotal", 0.0)),
                "total": total,
                "tip": float(totals.get("tip", 0.0)),
                "delivery_fee": float(totals.get("delivery_fee", 0.0)),
                "tax": float(totals.get("tax", 0.0)),
                "mode": "delivery",
            }
            addr_m = re.search(
                r"(?:deliver(?:y|ed)\s+to|address)\s*[:\s]+(.+?)(?:\n\n|\nTotal|\Z)",
                body,
                re.I | re.S,
            )
            if addr_m:
                row["delivery_address"] = addr_m.group(1).strip().split("\n")[0]
            row["_body"] = _render_meal_markdown(restaurant, items, totals)
            return [row]

        return [TemplateVersion("plain", ("2000-01-01", "2099-12-31"), era_plain)]
