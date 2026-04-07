#!/usr/bin/env python3
"""Markdown extraction-quality report from a staging directory (slice or full run).

Uses CRITICAL_FIELDS predicates from field_metrics plus light heuristics for
common failure modes. Run from repo `ppa/` with the project venv active:

  .venv/bin/python scripts/generate_extraction_quality_report.py \\
    --staging-dir _staging-5pct --label 5pct --out logs/extraction-quality-5pct.md
"""
from __future__ import annotations

import argparse
import json
import random
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from archive_mcp.commands.staging import (format_staging_report_markdown,
                                          staging_report)
from archive_sync.extractors.field_metrics import (CRITICAL_FIELDS,
                                                   compute_field_population)
from archive_sync.extractors.field_validation import (validate_field,
                                                      validate_provenance_round_trip)
from archive_sync.extractors.preprocessing import clean_email_body
from hfa.vault import read_note_by_uid, read_note_file

# Card family → extractors that typically emit it (for review planning).
TYPE_TO_EXTRACTORS: dict[str, list[str]] = {
    "meal_order": ["uber_eats", "doordash"],
    "ride": ["uber_rides", "lyft"],
    "flight": ["united"],
    "accommodation": ["airbnb"],
    "shipment": ["shipping"],
    "grocery_order": ["instacart"],
    "car_rental": ["rental_cars"],
    "purchase": ["amazon"],
}


def _strict_field_flags(ct: str, fm: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    specs = CRITICAL_FIELDS.get(ct, [])
    for field_name, pred in specs:
        try:
            if not pred(fm):
                flags.append(f"critical_fail:{field_name}")
        except Exception:
            flags.append(f"critical_err:{field_name}")
    return flags


def _heuristic_flags(ct: str, fm: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    if ct == "meal_order":
        r = str(fm.get("restaurant") or "")
        rl = r.lower()
        if "http://" in rl or "https://" in rl or "<http" in rl:
            flags.append("heuristic:url_in_restaurant")
        if "bank statement" in rl or ("learn more" in rl and len(r) > 60):
            flags.append("heuristic:footer_noise_restaurant")
        items = fm.get("items")
        if not isinstance(items, list) or len(items) == 0:
            flags.append("heuristic:empty_items")
    if ct == "ride":
        pl = str(fm.get("pickup_location") or "").strip()
        dl = str(fm.get("dropoff_location") or "").strip()
        if pl.lower() in ("location:", "pickup", "") or len(pl) < 4:
            flags.append("heuristic:weak_pickup")
        if dl.lower() in ("location:", "dropoff", "") or len(dl) < 4:
            flags.append("heuristic:weak_dropoff")
    if ct == "flight":
        for k in ("origin_airport", "destination_airport"):
            v = str(fm.get(k) or "").strip()
            if v and validate_field("flight", k, v) is None:
                flags.append(f"heuristic:non_iata_{k}")
    if ct == "accommodation":
        if not str(fm.get("check_in") or "").strip():
            flags.append("heuristic:missing_check_in")
        if not str(fm.get("check_out") or "").strip():
            flags.append("heuristic:missing_check_out")
    if ct == "grocery_order":
        items = fm.get("items")
        if not isinstance(items, list) or len(items) == 0:
            flags.append("heuristic:empty_items")
    return flags


def all_flags(ct: str, fm: dict[str, Any]) -> list[str]:
    return _strict_field_flags(ct, fm) + _heuristic_flags(ct, fm)


def _wikilink_uid(source_email: str) -> str:
    m = re.search(r"\[\[([^\]]+)\]\]", (source_email or "").strip())
    return m.group(1).strip() if m else ""


def _round_trip_flags(ct: str, fm: dict[str, Any], vault: Path) -> list[str]:
    uid = _wikilink_uid(str(fm.get("source_email") or ""))
    if not uid:
        return []
    got = read_note_by_uid(vault, uid)
    if not got:
        return []
    _rp, _efm, body, _prov = got
    clean = clean_email_body(body)
    warnings = validate_provenance_round_trip(dict(fm), clean, ct)
    return [f"heuristic:round_trip_fail:{w.split('.', 1)[1].split(':', 1)[0]}" for w in warnings if "." in w]


def _dedup_key(ct: str, fm: dict[str, Any]) -> tuple[Any, ...]:
    if ct == "meal_order":
        try:
            tot = float(fm.get("total") or 0)
        except (TypeError, ValueError):
            tot = 0.0
        return (
            ct,
            str(fm.get("restaurant") or "").strip().lower(),
            tot,
            str(fm.get("created") or "")[:10],
        )
    if ct == "ride":
        try:
            fare = float(fm.get("fare") or 0)
        except (TypeError, ValueError):
            fare = 0.0
        return (ct, str(fm.get("pickup_at") or ""), fare)
    if ct == "flight":
        return (ct, str(fm.get("confirmation_code") or "").strip().upper())
    if ct == "shipment":
        return (ct, str(fm.get("tracking_number") or "").strip())
    if ct == "accommodation":
        return (
            ct,
            str(fm.get("confirmation_code") or "").strip(),
            str(fm.get("check_in") or "")[:10],
        )
    if ct == "car_rental":
        return (ct, str(fm.get("confirmation_code") or "").strip())
    if ct == "grocery_order":
        try:
            tot = float(fm.get("total") or 0)
        except (TypeError, ValueError):
            tot = 0.0
        return (
            ct,
            str(fm.get("store") or "").strip().lower(),
            tot,
            str(fm.get("created") or "")[:10],
        )
    return (ct, str(fm.get("uid") or ""))


def _duplicate_uids(by_type: dict[str, list[tuple[Path, dict[str, Any]]]]) -> set[str]:
    suspicious: set[str] = set()
    for ct, rows in by_type.items():
        counts: Counter[tuple[Any, ...]] = Counter()
        for _path, fm in rows:
            counts[_dedup_key(ct, fm)] += 1
        for _path, fm in rows:
            uid = str(fm.get("uid") or "")
            if uid and counts[_dedup_key(ct, fm)] > 1:
                suspicious.add(uid)
    return suspicious


def _confidence_histogram(rows: list[tuple[Path, dict[str, Any]]]) -> str:
    buckets = {"high": 0, "mid": 0, "low": 0, "na": 0}
    for _p, fm in rows:
        c = fm.get("extraction_confidence")
        if c is None:
            buckets["na"] += 1
            continue
        try:
            v = float(c)
        except (TypeError, ValueError):
            buckets["na"] += 1
            continue
        if v >= 0.8:
            buckets["high"] += 1
        elif v >= 0.5:
            buckets["mid"] += 1
        else:
            buckets["low"] += 1
    n = len(rows) or 1
    return (
        f">=0.8: {buckets['high']} ({100 * buckets['high'] / n:.0f}%), "
        f"0.5–0.8: {buckets['mid']} ({100 * buckets['mid'] / n:.0f}%), "
        f"<0.5: {buckets['low']} ({100 * buckets['low'] / n:.0f}%), "
        f"n/a: {buckets['na']}"
    )


def load_staging_cards(root: Path) -> dict[str, list[tuple[Path, dict[str, Any]]]]:
    by_type: dict[str, list[tuple[Path, dict[str, Any]]]] = defaultdict(list)
    for path in sorted(root.rglob("*.md")):
        if path.name.startswith("_"):
            continue
        try:
            rec = read_note_file(path)
        except OSError:
            continue
        fm = rec.frontmatter
        ct = str(fm.get("type") or "").strip() or "unknown"
        if ct == "email_message":
            continue
        by_type[ct].append((path, fm))
    return by_type


def fm_dump(fm: dict[str, Any], *, max_len: int = 4000) -> str:
    lines: list[str] = []
    for k in sorted(fm.keys()):
        v = fm[k]
        s = json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v
        if len(s) > 200 and k not in ("summary",):
            s = s[:197] + "..."
        lines.append(f"{k}: {s}")
    text = "\n".join(lines)
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return text


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging-dir", required=True, type=Path)
    ap.add_argument("--label", required=True, help="Short name for title, e.g. 5pct")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--problem-samples",
        type=int,
        default=6,
        help="Max examples per type that have at least one flag",
    )
    ap.add_argument(
        "--clean-samples",
        type=int,
        default=4,
        help="Max examples per type with no flags (if available)",
    )
    ap.add_argument(
        "--vault",
        default="",
        type=Path,
        help="Optional vault for round-trip source checks (re-reads source_email)",
    )
    args = ap.parse_args()

    root = args.staging_dir.resolve()
    random.seed(args.seed)
    vault_opt: Path | None = None
    if str(args.vault or "").strip():
        vault_opt = Path(args.vault).resolve()

    metrics_path = root / "_metrics.json"
    metrics: dict[str, Any] = {}
    if metrics_path.is_file():
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    by_type = load_staging_cards(root)
    report = staging_report(str(root))
    fp = compute_field_population(root)
    table_md = format_staging_report_markdown(report, field_population=fp)

    lines: list[str] = []
    lines.append(f"# Extraction quality — `{args.label}` slice")
    lines.append("")
    lines.append(f"Staging directory: `{root}`")
    lines.append("")
    dup_uids = _duplicate_uids(by_type)

    if metrics:
        lines.append("## Runner metrics (from `_metrics.json`)")
        lines.append("")
        lines.append(f"- **Emails scanned:** {metrics.get('total_emails_scanned', '—')}")
        lines.append(f"- **Matched:** {metrics.get('matched_emails', '—')}")
        lines.append(f"- **Cards extracted:** {metrics.get('extracted_cards', '—')}")
        wc = metrics.get("wall_clock_seconds")
        if isinstance(wc, (int, float)):
            lines.append(f"- **Wall clock (s):** {wc:.1f}")
        pe = metrics.get("per_extractor") or {}
        if pe:
            lines.append("")
            lines.append("| Extractor | matched | extracted | errors | skipped | rejected |")
            lines.append("|-----------|--------:|----------:|-------:|--------:|---------:|")
            rej = metrics.get("rejected_emails") or {}
            for name in sorted(pe.keys()):
                row = pe[name]
                lines.append(
                    f"| {name} | {row.get('matched', 0)} | {row.get('extracted', 0)} | "
                    f"{row.get('errors', 0)} | {row.get('skipped', 0)} | {rej.get(name, 0)} |"
                )
        yb = metrics.get("yield_by_extractor") or {}
        if yb:
            lines.append("")
            lines.append("**Yield (extracted / matched)** — low values often mean parse/skip inside extractor.")
            for name in sorted(yb.keys()):
                lines.append(f"- `{name}`: {float(yb[name]):.3f}")
        lines.append("")

    lines.append("## Volume + field population (staging scan)")
    lines.append("")
    lines.append(table_md.strip())
    lines.append("")

    lines.append("## Per card type — flags, rates, and examples")
    lines.append("")
    lines.append(
        "Flags combine **critical_fail:** (strict predicate from `field_metrics.CRITICAL_FIELDS`) "
        "and **heuristic:** (common junk patterns). Use flagged rows first when upgrading extractors."
    )
    if vault_opt is not None:
        lines.append(f"Round-trip source checks use vault `{vault_opt}`.")
    lines.append("")

    for ct in sorted(by_type.keys()):
        rows = by_type[ct]
        lines.append(f"### `{ct}` ({len(rows)} cards)")
        ex = TYPE_TO_EXTRACTORS.get(ct)
        if ex:
            lines.append(f"*Typical extractors:* {', '.join(f'`{e}`' for e in ex)}")
        lines.append("")
        lines.append(f"- **Confidence distribution:** {_confidence_histogram(rows)}")
        lines.append("")

        flag_counter: Counter[str] = Counter()
        any_flag = 0
        per_path_flags: list[tuple[Path, dict[str, Any], list[str]]] = []
        for path, fm in rows:
            fl = all_flags(ct, fm)
            if vault_opt is not None and vault_opt.is_dir():
                fl.extend(_round_trip_flags(ct, fm, vault_opt))
            uid = str(fm.get("uid") or "")
            if uid and uid in dup_uids:
                fl.append("heuristic:duplicate_suspect")
            per_path_flags.append((path, fm, fl))
            if fl:
                any_flag += 1
            for f in fl:
                flag_counter[f] += 1

        if rows:
            lines.append(
                f"- **Share with ≥1 flag:** {any_flag}/{len(rows)} ({100.0 * any_flag / len(rows):.1f}%)"
            )
        if flag_counter:
            lines.append("- **Flag counts (cards can have multiple):**")
            for fname, cnt in flag_counter.most_common(25):
                lines.append(f"  - `{fname}`: {cnt}")
        lines.append("")

        flagged = [(p, fm, fl) for p, fm, fl in per_path_flags if fl]
        clean = [(p, fm, fl) for p, fm, fl in per_path_flags if not fl]

        def emit_sample(tag: str, bucket: list[tuple[Path, dict[str, Any], list[str]]], limit: int) -> None:
            if not bucket:
                return
            random.shuffle(bucket)
            lines.append(f"#### {tag}")
            lines.append("")
            for path, fm, fl in bucket[:limit]:
                try:
                    rel = path.relative_to(root)
                except ValueError:
                    rel = path
                lines.append(f"**File:** `{rel}`")
                if fl:
                    lines.append(f"**Flags:** {', '.join(f'`{x}`' for x in fl)}")
                lines.append("")
                lines.append("```yaml")
                lines.append(fm_dump(dict(fm)))
                lines.append("```")
                try:
                    rec = read_note_file(path)
                    body = (rec.body or "").strip()
                    body = body[:1800] + ("…\n" if len(body) > 1800 else "")
                    lines.append("")
                    lines.append("Body (truncated):")
                    lines.append("")
                    lines.append("```markdown")
                    lines.append(body)
                    lines.append("```")
                except OSError as exc:
                    lines.append(f"(body read error: {exc})")
                lines.append("")
                lines.append("---")
                lines.append("")

        emit_sample("Flagged examples (prioritize fixes)", flagged, args.problem_samples)
        emit_sample("Clean examples (quality bar)", clean, args.clean_samples)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
