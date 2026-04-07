"""Sender census — taxonomy of email types per sender domain (EDL Phase 1)."""

from __future__ import annotations

import logging
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from hfa.vault import iter_email_message_notes

_log = logging.getLogger("ppa.sender_census")

# Independent keyword hits (a single subject can match several).
_SUBJECT_KEYWORD_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("order / receipt", ("order", "receipt", "confirmation", "confirmed", "invoice")),
    ("shipping / tracking", ("shipped", "ship", "tracking", "track", "package", "delivery", "delivered", "on the way")),
    ("promo / deal", ("%", "off", "deal", "promo", "save $", "reward", "free")),
    ("account / security", ("password", "verify", "security", "login", "log in", "account")),
    ("travel / flight", ("flight", "itinerary", "boarding", "gate", "check-in", "airline")),
    ("ride / trip", ("uber", "lyft", "your ride", "trip with", "trip receipt")),
    ("survey / review", ("survey", "rate your", "review your", "feedback")),
)


def _domain_matches(from_email: str, domain: str) -> bool:
    from_email = (from_email or "").strip().lower()
    domain = domain.strip().lower().lstrip("@")
    if "@" not in from_email:
        return False
    host = from_email.rsplit("@", 1)[-1]
    return host == domain or host.endswith("." + domain)


def _parse_sent_ts(sent_at: str) -> float:
    s = (sent_at or "").strip()
    if not s:
        return 0.0
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return datetime.fromisoformat(s[:10]).timestamp()
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return 0.0


def _categorize_subject(subject: str) -> str:
    """Return primary bucket: receipt, delivery, promotion, account, travel_ride, other."""
    s = (subject or "").lower()
    if any(
        k in s
        for k in (
            "password",
            "account",
            "verify",
            "security",
            "payment method",
            "update your",
            "log in",
            "login",
        )
    ):
        return "account"
    if any(
        k in s
        for k in (
            "% off",
            "save $",
            "off your",
            "deal",
            "promo",
            "free delivery",
            "last chance",
            "reward",
            " credit",
            "dashpass exclusive",
            "exclusive offer",
        )
    ):
        return "promotion"
    # Travel / ride (before generic receipt — avoids classifying flight itineraries as "receipt" only)
    if any(
        k in s
        for k in (
            "boarding pass",
            "itinerary",
            "flight",
            "your trip",
            "gate ",
            "uber",
            "lyft",
            "your ride",
        )
    ):
        return "travel_ride"
    if any(
        k in s
        for k in (
            "on the way",
            "out for delivery",
            "delivered",
            "delivery update",
            "shipped",
            "arriving",
        )
    ):
        return "delivery"
    if any(
        k in s
        for k in (
            "order",
            "receipt",
            "confirmation",
            "your order from",
            "confirmed",
        )
    ):
        return "receipt"
    return "other"


def _extractable_hint(cat: str) -> str:
    if cat == "receipt":
        return "YES"
    if cat == "delivery":
        return "NO (often duplicate; no new totals)"
    if cat == "promotion":
        return "NO (marketing)"
    if cat == "account":
        return "NO (account mgmt)"
    if cat == "travel_ride":
        return "MAYBE (flight/ride — use dedicated extractor)"
    return "UNKNOWN"


def snapshot_subject_category(subject: str) -> str:
    """Rule-based primary bucket (same as census tables)."""
    return _categorize_subject(subject)


def snapshot_subject_shape(subject: str) -> str:
    """Normalized subject line for clustering (see census *subject shapes* section)."""
    return _subject_shape(subject)


def _subject_shape(subject: str) -> str:
    """Normalize subject for pattern clustering (digits / long numbers → placeholders)."""
    s = (subject or "").strip().lower()
    if not s:
        return "(empty subject)"
    s = re.sub(r"\b\d{1,2}:\d{2}\s*(?:am|pm)?\b", "[time]", s)
    s = re.sub(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", "[date]", s)
    s = re.sub(r"\b\d[\d,.\s-]{4,}\b", "[#]", s)
    s = re.sub(r"\b\d+\b", "#", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:160] if len(s) <= 160 else s[:157] + "..."


def _md_cell(s: str, max_len: int = 200) -> str:
    t = (s or "").replace("\n", " ").strip()
    if len(t) > max_len:
        t = t[: max_len - 1] + "…"
    return t.replace("|", "\\|")


def _even_sample(rows: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    if len(rows) <= n:
        return list(rows)
    rows = sorted(rows, key=lambda r: r["_ts"])
    if n <= 1:
        return [rows[0]]
    out: list[dict[str, Any]] = []
    for i in range(n):
        idx = round(i * (len(rows) - 1) / (n - 1))
        out.append(rows[idx])
    return out


def run_sender_census(
    *,
    vault_path: str,
    domain: str,
    sample_size: int = 100,
    out_path: str = "",
    detail_rows_per_category: int = 8,
    top_from_addresses: int = 0,
    top_exact_subjects: int = 0,
    top_subject_shapes: int = 0,
    include_keyword_hits: bool = True,
) -> str:
    """Scan vault for email_message from domain; write markdown taxonomy.

    The full domain match list is always scanned (one Email/ walk). *sample_size*
    controls the stratified pool used for example subjects; category *counts* are
    always totals. Optional snapshot sections activate when the corresponding
    *top_* limits are > 0.
    """
    all_rows: list[dict[str, Any]] = []
    scanned = 0
    for note in iter_email_message_notes(vault_path):
        scanned += 1
        if scanned % 25_000 == 0:
            _log.info("sender-census progress: read %s notes under Email/", f"{scanned:,}")
        fm = note.frontmatter
        if fm.get("type") != "email_message":
            continue
        from_email = str(fm.get("from_email") or "")
        if not _domain_matches(from_email, domain):
            continue
        sent_at = str(fm.get("sent_at") or "")
        uid = str(fm.get("uid") or "")
        subject = str(fm.get("subject") or "")
        from_name = str(fm.get("from_name") or "").strip()
        body = str(note.body or "")[:200]
        ts = _parse_sent_ts(sent_at)
        cat = _categorize_subject(subject)
        all_rows.append(
            {
                "uid": uid,
                "subject": subject,
                "sent_at": sent_at,
                "from_email": from_email,
                "from_name": from_name,
                "body_preview": body,
                "category": cat,
                "_ts": ts,
            }
        )

    total = len(all_rows)
    sample_n = min(sample_size, total) if total else 0
    sampled = _even_sample(all_rows, sample_n) if sample_n else []

    counts: dict[str, int] = {}
    for r in all_rows:
        c = r["category"]
        counts[c] = counts.get(c, 0) + 1

    by_cat_samples: dict[str, list[dict[str, Any]]] = {}
    for r in sampled:
        by_cat_samples.setdefault(r["category"], []).append(r)

    dates = [r["_ts"] for r in all_rows if r["_ts"] > 0]
    date_min = date_max = "—"
    if dates:
        date_min = datetime.fromtimestamp(min(dates)).strftime("%Y-%m-%d")
        date_max = datetime.fromtimestamp(max(dates)).strftime("%Y-%m-%d")

    # --- aggregate snapshots (full list) ---
    from_counter = Counter(r["from_email"] for r in all_rows if r.get("from_email"))
    subject_counter = Counter(r["subject"] for r in all_rows)
    shape_counter = Counter(_subject_shape(r["subject"]) for r in all_rows)

    kw_group_counts: dict[str, int] = {label: 0 for label, _ in _SUBJECT_KEYWORD_GROUPS}
    if include_keyword_hits and all_rows:
        for r in all_rows:
            subj = (r.get("subject") or "").lower()
            for label, kws in _SUBJECT_KEYWORD_GROUPS:
                if any(kw.lower() in subj for kw in kws):
                    kw_group_counts[label] += 1

    lines: list[str] = []
    lines.append(f"# Sender Census: {domain}")
    lines.append("")
    lines.append(f"**Total emails:** {total} (stratified sample pool: {sample_n})")
    lines.append(f"**Date range:** {date_min} to {date_max}")
    lines.append("")

    if include_keyword_hits and total:
        lines.append("## Subject keyword hits (non-exclusive)")
        lines.append("")
        lines.append(
            "Each row counts messages whose subject matches *any* listed token in that group "
            "(one email can increment multiple groups)."
        )
        lines.append("")
        lines.append("| Group | ~Hits |")
        lines.append("|-------|------|")
        for label, _ in _SUBJECT_KEYWORD_GROUPS:
            lines.append(f"| {label} | {kw_group_counts.get(label, 0)} |")
        lines.append("")

    if top_from_addresses > 0 and from_counter:
        lines.append(f"## Distinct From addresses (top {top_from_addresses} by volume)")
        lines.append("")
        lines.append("| Count | From email | Example display name |")
        lines.append("|------:|------------|------------------------|")
        for addr, cnt in from_counter.most_common(top_from_addresses):
            name_ex = ""
            for r in all_rows:
                if r.get("from_email") == addr and r.get("from_name"):
                    name_ex = r["from_name"][:80]
                    break
            lines.append(f"| {cnt} | `{_md_cell(addr, 80)}` | {_md_cell(name_ex, 80)} |")
        lines.append("")

    if top_exact_subjects > 0 and subject_counter:
        lines.append(f"## Most common exact subjects (top {top_exact_subjects})")
        lines.append("")
        lines.append("| Count | Subject |")
        lines.append("|------:|---------|")
        for subj, cnt in subject_counter.most_common(top_exact_subjects):
            lines.append(f"| {cnt} | {_md_cell(subj, 140)} |")
        lines.append("")

    if top_subject_shapes > 0 and shape_counter:
        lines.append(f"## Most common subject shapes (normalized; top {top_subject_shapes})")
        lines.append("")
        lines.append(
            "Digits collapsed to `#` / `[date]` / `[time]` to surface template patterns."
        )
        lines.append("")
        lines.append("| Count | Shape |")
        lines.append("|------:|-------|")
        for shape, cnt in shape_counter.most_common(top_subject_shapes):
            lines.append(f"| {cnt} | {_md_cell(shape, 160)} |")
        lines.append("")

    lines.append("## Primary category (rule-based)")
    lines.append("")
    lines.append("| Category | Subject pattern examples | Count | Extractable? |")
    lines.append("|----------|---------------------------|------:|-------------|")

    cat_order = ("receipt", "delivery", "travel_ride", "promotion", "account", "other")
    labels = {
        "receipt": "Receipt / order",
        "delivery": "Delivery / shipping",
        "travel_ride": "Travel / ride",
        "promotion": "Promotion",
        "account": "Account",
        "other": "Other",
    }
    for cat in cat_order:
        cnt = counts.get(cat, 0)
        ex = _extractable_hint(cat)
        examples = by_cat_samples.get(cat, [])[:3]
        ex_subj = ", ".join(f'"{e["subject"][:55]}"' for e in examples if e.get("subject"))
        if not ex_subj:
            ex_subj = "—"
        lines.append(f"| {labels[cat]} | {ex_subj} | {cnt} | {ex} |")

    lines.append("")
    lines.append("## Sample emails by category")
    lines.append("")
    for cat in cat_order:
        n = max(0, int(detail_rows_per_category))
        rows_c = by_cat_samples.get(cat, [])[:n]
        if not rows_c:
            continue
        lines.append(f"### {labels[cat]}")
        lines.append("")
        lines.append("| UID | From | Subject | Date |")
        lines.append("|-----|------|---------|------|")
        for e in rows_c:
            sd = (e.get("sent_at") or "")[:10] or "—"
            fe = _md_cell(e.get("from_email", ""), 56)
            lines.append(
                f"| {e.get('uid', '')} | `{fe}` | {_md_cell(e.get('subject', ''), 100)} | {sd} |"
            )
        lines.append("")

    text = "\n".join(lines).strip() + "\n"
    _log.info("sender-census domain=%s total=%s sample=%s", domain, total, sample_n)
    if out_path:
        Path(out_path).write_text(text, encoding="utf-8")
    return text
