"""Shared parsing for Uber/Lyft-style ride receipt bodies (post–clean_email_body)."""

from __future__ import annotations

import re
from typing import Any

_ISO = re.compile(r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)")
_TIME_ADDR = re.compile(r"^(\d{1,2}:\d{2}\s*(?:AM|PM))\s+(.+)$", re.I | re.M)


def _is_location_placeholder(loc: str) -> bool:
    t = (loc or "").strip().lower().rstrip(":")
    if len(t) < 2:
        return True
    return t in ("location", "pickup location", "dropoff location", "pickup", "dropoff")


def _resolve_pickup_after_location_label(body: str, current: str) -> str:
    """Early Uber templates put the address on the line after ``Pickup:`` / ``Location:``."""
    if not _is_location_placeholder(current):
        return current
    for pat in (
        re.compile(
            r"(?is)(?:pickup|picked\s+up)\s*:\s*(?:\n\s*Location\s*:\s*)?\s*\n\s*(.+?)(?=\n\s*(?:drop|dropoff|destination)|\n\nfare|\n\n|\Z)",
        ),
        re.compile(r"(?i)Pickup\s*:\s*Location\s*:\s*\n\s*(.+?)(?:\n\n|\n(?:drop|Drop)|\Z)"),
    ):
        m = pat.search(body)
        if m:
            addr = m.group(1).strip()
            if addr and not _is_location_placeholder(addr):
                return addr
    return current


def _resolve_dropoff_after_location_label(body: str, current: str) -> str:
    if not _is_location_placeholder(current):
        return current
    for pat in (
        re.compile(
            r"(?is)(?:drop-?off|destination)\s*:\s*(?:\n\s*Location\s*:\s*)?\s*\n\s*(.+?)(?=\n\s*(?:fare|total|tip|\d+\s*mi)|\n\n|\Z)",
        ),
        re.compile(r"(?i)Drop-?off\s*:\s*Location\s*:\s*\n\s*(.+?)(?:\n\n|\n(?:fare|Fare)|\Z)"),
    ):
        m = pat.search(body)
        if m:
            addr = m.group(1).strip()
            if addr and not _is_location_placeholder(addr):
                return addr
    return current


def _pickup_ts(body: str, sent_at: str) -> str:
    m = _ISO.search(body)
    if m:
        return m.group(1).replace(" ", "T")
    return (sent_at or "").strip()


def _time_prefixed_addresses(body: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for m in _TIME_ADDR.finditer(body):
        addr = m.group(2).strip()
        if len(addr) >= 8:
            pairs.append((m.group(1), addr))
    return pairs


_LYFT_STACKED = re.compile(
    r"(?is)(?:pickup|pick-up)\s+[^\n]+\n\s*([^\n]+?)\s*\n\s*drop-?off\s+[^\n]+\n\s*([^\n]+)",
)


def _lyft_stacked_addresses(body: str) -> tuple[str, str]:
    """Lyft receipts: time on Pickup/Drop-off line; street address on the following line."""
    m = _LYFT_STACKED.search(body)
    if not m:
        return "", ""
    pu, dr = m.group(1).strip(), m.group(2).strip()
    if len(pu) < 8 or len(dr) < 8:
        return "", ""
    if not re.search(r"\d", pu) or not re.search(r"\d", dr):
        return "", ""
    return pu, dr


def _lyft_total_before_paid(body: str) -> float:
    """Lyft omits 'Total:'; last dollar amount before 'You've already paid' is the charge."""
    low = body.lower()
    idx = low.find("you've already paid")
    if idx == -1:
        return 0.0
    window = body[max(0, idx - 1500) : idx]
    amounts: list[float] = []
    for m in re.finditer(r"\$(\d+\.\d{2})\b", window):
        try:
            amounts.append(float(m.group(1)))
        except ValueError:
            pass
    return max(amounts) if amounts else 0.0


def parse_ride_receipt_fields(
    body: str,
    sent_at: str,
    *,
    skip_if_uber_eats: bool = False,
) -> dict[str, Any] | None:
    """Return ride field dict or None if this does not look like a ride receipt."""
    if skip_if_uber_eats and "uber eats" in body.lower():
        return None

    pickup_location = ""
    dropoff_location = ""
    pairs = _time_prefixed_addresses(body)
    if len(pairs) >= 2:
        pickup_location = pairs[0][1]
        dropoff_location = pairs[1][1]
    else:
        m = re.search(r"(?:pickup|pick-up|picked\s+up)\s*[:\s]+(.+?)(?:\n|$)", body, re.I)
        if m:
            pickup_location = m.group(1).strip()
        m = re.search(r"(?:drop-?off|destination)\s*[:\s]+(.+?)(?:\n|$)", body, re.I)
        if m:
            dropoff_location = m.group(1).strip()

    pickup_location = _resolve_pickup_after_location_label(body, pickup_location)
    dropoff_location = _resolve_dropoff_after_location_label(body, dropoff_location)

    lyft_pu, lyft_do = _lyft_stacked_addresses(body)
    if lyft_pu and lyft_do:
        pickup_location, dropoff_location = lyft_pu, lyft_do

    fare = 0.0
    # Prefer receipt "Total" (amount charged) over line-item trip fare.
    for pat in (
        re.compile(r"(?:^|\n)total\s*[:\s]*\$?\s*([\d,]+\.?\d*)", re.I),
        re.compile(r"(?i)\btotal\s{1,12}\$?\s*([\d,]+\.?\d*)"),
        re.compile(r"total\s+\$(\d+\.?\d*)", re.I),
        re.compile(r"(?:trip\s+fare|ride\s+fare)\s*[:\s]*\$?\s*([\d,]+\.?\d*)", re.I),
    ):
        m = pat.search(body)
        if m:
            try:
                fare = float(m.group(1).replace(",", ""))
            except ValueError:
                pass
            if fare > 0:
                break

    tip = 0.0
    m = re.search(r"(?:tip)\s*[:\s]+\$?\s*([\d,]+\.?\d*)", body, re.I)
    if m:
        try:
            tip = float(m.group(1).replace(",", ""))
        except ValueError:
            pass

    distance_miles = 0.0
    m = re.search(r"([\d.]+)\s*(?:mi|miles)\b", body, re.I)
    if m:
        try:
            distance_miles = float(m.group(1))
        except ValueError:
            pass

    duration_minutes = 0.0
    m = re.search(r"(\d+)\s*min", body, re.I)
    if m:
        try:
            duration_minutes = float(m.group(1))
        except ValueError:
            pass

    driver_name = ""
    for pat in (
        re.compile(r"you\s+rode\s+with\s+(.+?)(?:\n|$)", re.I),
        re.compile(r"ridden\s+with\s+(.+?)(?:\n|$)", re.I),
        re.compile(r"driver\s*[:\s]+(.+?)(?:\n|$)", re.I),
    ):
        m = pat.search(body)
        if m:
            driver_name = m.group(1).strip()
            break

    vehicle = ""
    m = re.search(r"(uberxl|uberx|uber\s*black|lyft\s+\w+|comfort|green)\b", body, re.I)
    if m:
        vehicle = m.group(1).strip()

    ride_type = "car"
    m = re.search(r"\b(UberXL|UberX|Uber\s*Black|Lyft\s+\w+|Comfort)\b", body, re.I)
    if m:
        ride_type = re.sub(r"\s+", " ", m.group(1).strip())

    pickup_at = _pickup_ts(body, sent_at)
    if not pickup_at and (pickup_location or dropoff_location):
        pickup_at = sent_at[:10] if len(sent_at) >= 10 else ""

    if not pickup_location or not dropoff_location:
        return None

    if fare <= 0:
        m = re.search(r"total\s*[:\s]*\$?\s*([\d,]+\.\d{2})", body, re.I)
        if m:
            try:
                fare = float(m.group(1).replace(",", ""))
            except ValueError:
                pass

    if fare <= 0:
        m = re.search(r"(?:^|\n)fare\s*[:\s]+\$?\s*([\d,]+\.?\d*)", body, re.I)
        if m:
            try:
                fare = float(m.group(1).replace(",", ""))
            except ValueError:
                pass

    if fare <= 0:
        lyft_f = _lyft_total_before_paid(body)
        if lyft_f > 0:
            fare = lyft_f

    if fare <= 0:
        return None

    return {
        "pickup_location": pickup_location,
        "dropoff_location": dropoff_location,
        "pickup_at": pickup_at,
        "dropoff_at": "",
        "fare": fare,
        "tip": tip,
        "distance_miles": distance_miles,
        "duration_minutes": duration_minutes,
        "driver_name": driver_name,
        "vehicle": vehicle,
        "ride_type": ride_type,
    }
