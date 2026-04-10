#!/usr/bin/env python3
"""Phase 2.75 Step 8b — build a human-review packet from ``enrichment_ground_truth*.json``.

Reads the ground truth file produced by ``build_enrichment_benchmark.py`` and writes a
markdown report: aggregate stats, stratified positive samples, negative samples, and
plan gate checklist (≥200 positives / ≥50 negatives when targeting full seed).

Run from ``ppa/``::

  .venv/bin/python scripts/prepare_step8b_human_review.py \\
    --ground-truth _artifacts/_benchmark/enrichment_ground_truth_10pct.json \\
    --out _artifacts/_benchmark/step8b_review_packet.md
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def _card_summary(card: dict[str, Any]) -> str:
    ct = str(card.get("type") or "?")
    bits: list[str] = []
    for key in (
        "restaurant",
        "store",
        "service",
        "vendor",
        "airline",
        "total",
        "fare",
        "confirmation_code",
        "tracking_number",
        "origin_airport",
        "destination_airport",
    ):
        if key in card and card[key] not in (None, "", []):
            bits.append(f"{key}={card[key]!r}")
    return f"`{ct}`" + (" — " + "; ".join(bits[:6]) if bits else "")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--ground-truth",
        type=Path,
        default=Path("_artifacts/_benchmark/enrichment_ground_truth_10pct.json"),
        help="Path to enrichment_ground_truth JSON",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("_artifacts/_benchmark/step8b_review_packet.md"),
        help="Markdown output path",
    )
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--positive-samples", type=int, default=28, help="Target ~20–30")
    ap.add_argument("--negative-samples", type=int, default=10)
    args = ap.parse_args()
    random.seed(args.seed)

    path = args.ground_truth
    if not path.is_file():
        raise SystemExit(f"missing ground truth file: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    meta = data.get("meta") or {}
    pos = data.get("benchmark_set") or []
    neg = data.get("negative_benchmark_set") or []

    by_type: Counter[str] = Counter()
    flat_pos: list[tuple[int, int, dict[str, Any]]] = []
    for bi, row in enumerate(pos):
        for ci, card in enumerate(row.get("expected_cards") or []):
            ct = str(card.get("type") or "unknown")
            by_type[ct] += 1
            flat_pos.append((bi, ci, card))

    lines: list[str] = []
    lines.append("# Step 8b — Ground truth human review packet")
    lines.append("")
    lines.append("Auto-generated for Phase 2.75. **You** decide APPROVE / FIX before Step 9.")
    lines.append("")
    lines.append("## Source")
    lines.append("")
    lines.append(f"- File: `{path}`")
    lines.append(f"- Staging: `{meta.get('staging_dir', '—')}`")
    lines.append(f"- Vault: `{meta.get('vault', '—')}`")
    lines.append("")
    lines.append("## Stats")
    lines.append("")
    lines.append(f"- **Positive threads:** {meta.get('positive_threads', len(pos))}")
    lines.append(f"- **Positive cards:** {meta.get('positive_cards', sum(by_type.values()))}")
    lines.append(f"- **Negative threads:** {meta.get('negative_threads', len(neg))}")
    lines.append("")
    lines.append("| Card type | Count |")
    lines.append("|-----------|------:|")
    for ct, n in sorted(by_type.items(), key=lambda x: (-x[1], x[0])):
        lines.append(f"| `{ct}` | {n} |")
    lines.append("")
    lines.append("### Plan gate (full-seed target)")
    lines.append("")
    pc = int(meta.get("positive_cards") or 0)
    nc = int(meta.get("negative_threads") or 0)
    lines.append(
        f"- Plan asks **≥200** positive cards and **≥50** negative threads on a full run. "
        f"This file: **{pc}** positives, **{nc}** negatives."
    )
    if pc < 200 or nc < 50:
        lines.append(
            "- **Slice / partial run:** OK to APPROVE for *pipeline smoke*; re-build from "
            "full `_artifacts/_staging/` + matching `PPA_PATH` (or larger slice) before trusting benchmark scale."
        )
    lines.append("")
    lines.append("## Positive samples (check field values vs email)")
    lines.append("")
    lines.append(
        "Confirm each row: subject plausibly matches card type and key fields look right for that email."
    )
    lines.append("")

    # Stratified sample: round-robin by type
    by_t: dict[str, list[tuple[int, int, dict[str, Any]]]] = defaultdict(list)
    for bi, ci, card in flat_pos:
        by_t[str(card.get("type") or "unknown")].append((bi, ci, card))
    types = sorted(by_t.keys())
    picked: list[tuple[Any, ...]] = []
    cap = args.positive_samples
    while len(picked) < cap and any(by_t[t] for t in types):
        for t in types:
            if len(picked) >= cap:
                break
            if not by_t[t]:
                continue
            idx = random.randrange(len(by_t[t]))
            picked.append(by_t[t].pop(idx))
    for i, (bi, ci, card) in enumerate(picked, start=1):
        row = pos[bi]
        emails = row.get("source_emails") or []
        subj = emails[0].get("subject", "—") if emails else "—"
        tid = row.get("source_thread_id", "—")
        lines.append(f"### {i}. `{card.get('type')}` — thread `{tid}`")
        lines.append("")
        lines.append(f"- **Subject (first msg):** {subj}")
        lines.append(f"- **Expected:** {_card_summary(card)}")
        lines.append("")

    lines.append("## Negative samples (should NOT yield transaction cards)")
    lines.append("")
    lines.append("Triage should skip / extract nothing. Confirm these look like marketing or non-transaction noise.")
    lines.append("")
    for i, row in enumerate((neg or [])[: args.negative_samples], start=1):
        emails = row.get("source_emails") or []
        subj = emails[0].get("subject", "—") if emails else "—"
        tid = row.get("source_thread_id", "—")
        lines.append(f"{i}. Thread `{tid}` — **{subj}**")
    lines.append("")
    lines.append("## Edge cases")
    lines.append("")
    lines.append(
        "- Regex baseline can be **partial** (e.g. total without restaurant). LLM may beat ground truth; "
        "that is OK — do not reject the packet for that alone if the regex card was weak."
    )
    lines.append("")
    lines.append("## Decision")
    lines.append("")
    lines.append("- [ ] **APPROVE** — proceed to Step 9 (benchmark harness)")
    lines.append("- [ ] **FIX** — note bad rows; re-run or hand-edit ground truth, then re-review")
    lines.append("")

    out = args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
