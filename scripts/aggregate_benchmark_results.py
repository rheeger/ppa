#!/usr/bin/env python3
"""Merge per-model ``scores.json`` files into one comparison table (Phase 2.75 Step 10).

Run from ``ppa/`` after ``run_enrichment_benchmark.py``::

  .venv/bin/python scripts/aggregate_benchmark_results.py --results-dir _artifacts/_benchmark/results

Writes ``<results-dir>/index.md`` with a sortable comparison of all completed runs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_scores(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", type=Path, default=Path("_artifacts/_benchmark/results"))
    args = ap.parse_args()

    root = args.results_dir.resolve()
    if not root.is_dir():
        raise SystemExit(f"results dir missing: {root}")

    rows: list[tuple[str, dict[str, Any]]] = []
    for scores_path in sorted(root.glob("*/scores.json")):
        data = _load_scores(scores_path)
        if not data:
            continue
        model = str(data.get("model") or scores_path.parent.name)
        rows.append((model, data))

    if not rows:
        raise SystemExit(f"no */scores.json under {root}")

    lines: list[str] = [
        "# Enrichment benchmark — comparison",
        "",
        f"Results directory: `{root}`",
        "",
        "| Model | Positives | Negatives | Wall (s) | thr/min | Triage FN (pos) | Neg FP rate | Schema OK cards | Mean field match |",
        "|-------|----------:|----------:|---------:|--------:|----------------:|------------:|----------------:|-----------------:|",
    ]
    for model, s in sorted(rows, key=lambda x: x[0]):
        th = s.get("threads") or {}
        tr = s.get("triage") or {}
        ex = s.get("extraction") or {}
        lines.append(
            "| `{model}` | {pos} | {neg} | {wall} | {tpm} | {tfn} | {nfpr} | {sv} | {fm} |".format(
                model=model,
                pos=th.get("positives", "—"),
                neg=th.get("negatives", "—"),
                wall=th.get("wall_clock_seconds", "—"),
                tpm=th.get("threads_per_minute", "—"),
                tfn=tr.get("positive_false_negative_count", "—"),
                nfpr=tr.get("negative_triage_false_positive_rate", "—"),
                sv=ex.get("schema_validated_cards", "—"),
                fm=ex.get("mean_field_match_on_matched_positives", "—"),
            )
        )

    lines += [
        "",
        "## How to use (Step 10)",
        "",
        "1. Pick **triage** candidate: high `Positives` recall (low triage FN), low `Neg FP rate`.",
        "2. Pick **extraction** candidate: high `Mean field match`, high `Schema OK cards`.",
        "3. Record choices in `docs/phase275-step10-model-selection.md` (copy to `_artifacts/_benchmark/model_selection.md` if you want a local scratch file).",
        "4. Proceed to **Step 10b** human approval before long pilot runs.",
        "",
    ]

    out = root / "index.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
