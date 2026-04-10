#!/usr/bin/env python3
"""Markdown extraction-quality report from a staging directory (slice or full run).

Uses CRITICAL_FIELDS predicates from field_metrics plus light heuristics for
common failure modes. Run from repo `ppa/` with the project venv active:

  .venv/bin/python scripts/generate_extraction_quality_report.py \\
    --staging-dir _artifacts/_staging-5pct --label 5pct --out logs/extraction-quality-5pct.md
"""
from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from pathlib import Path
from typing import Any

from archive_mcp.commands.staging import (format_staging_report_markdown,
                                          staging_report)
from archive_sync.extractors.field_metrics import compute_field_population
from archive_sync.extractors.quality_flags import (all_flags, duplicate_uids,
                                                   email_uid_index,
                                                   load_staging_cards,
                                                   round_trip_flags,
                                                   source_uids_needed,
                                                   wikilink_uid)
from hfa.vault import read_note_file

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
        default=None,
        type=Path,
        help="Optional vault for round-trip source checks (re-reads source_email)",
    )
    args = ap.parse_args()

    root = args.staging_dir.resolve()
    random.seed(args.seed)
    # argparse type=Path with default="" became Path('.') on some Python versions,
    # which triggered a full-tree walk per card — only set when the user passes --vault.
    vault_opt: Path | None = None
    if args.vault is not None and str(args.vault).strip() not in ("", "."):
        vault_opt = Path(args.vault).resolve()
    else:
        vault_opt = None

    metrics_path = root / "_metrics.json"
    metrics: dict[str, Any] = {}
    if metrics_path.is_file():
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    by_type = load_staging_cards(root)
    uid_to_path: dict[str, Path] = {}
    if vault_opt is not None and vault_opt.is_dir():
        need = source_uids_needed(by_type)
        uid_to_path = email_uid_index(vault_opt, need)

    report = staging_report(str(root))
    fp = compute_field_population(root)
    table_md = format_staging_report_markdown(report, field_population=fp)

    lines: list[str] = []
    lines.append(f"# Extraction quality — `{args.label}` slice")
    lines.append("")
    lines.append(f"Staging directory: `{root}`")
    lines.append("")
    dup_uids = duplicate_uids(by_type)

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
            if vault_opt is not None and vault_opt.is_dir() and uid_to_path:
                fl.extend(round_trip_flags(ct, fm, vault_opt, uid_to_path))
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
