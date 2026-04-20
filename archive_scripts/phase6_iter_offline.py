"""Offline turn evaluator: re-score the precomputed cache with a different config.

Each turn is defined by `archive_scripts/phase6_turns.json` and writes its summary
to `_artifacts/_phase6-iterations/turn-{label}-summary.md`. No LLM calls, no DB
queries — pure formula re-evaluation. Runs in ~1 second per turn.

Configurable knobs:
- `k`: keep only the top-k targets per source (by embedding_similarity)
- `threshold`: drop candidates whose embedding_similarity is below this
- `weight_llm`: coefficient for `llm_score`
- `weight_emb`: coefficient for `embedding_score`
- `risk_below_emb`: penalty if embedding_score < this value
- `risk_amount`: penalty value
- `same_type_discount`: subtract this from final_confidence when source/target share type
- `bucket_diversity_discount`: subtract this when source/target share path bucket
- `auto_promote_floor`, `auto_review_floor`: bands

Usage:
    .venv/bin/python archive_scripts/phase6_iter_offline.py <turn_label>
    .venv/bin/python archive_scripts/phase6_iter_offline.py --all  (run every turn in turns.json)
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
ITER_DIR = REPO_ROOT / "_artifacts" / "_phase6-iterations"
DEFAULT_CACHE_PATH = ITER_DIR / "cache-1020.json"
TURNS_PATH = REPO_ROOT / "archive_scripts" / "phase6_turns.json"


def _bucket(rel_path: str) -> str:
    parts = rel_path.split("/")
    return "/".join(parts[:2]) if len(parts) >= 2 else parts[0]


def _band(c: float) -> str:
    if c >= 0.95:
        return "band_4_0.95+"
    if c >= 0.85:
        return "band_3_0.85_0.95"
    if c >= 0.70:
        return "band_2_0.70_0.85"
    if c >= 0.50:
        return "band_1_0.50_0.70"
    if c >= 0.30:
        return "band_a_0.30_0.50"
    return "band_0_below_0.30"


def evaluate_turn(label: str, cfg: dict[str, Any], cache: dict[str, Any]) -> dict[str, Any]:
    k = int(cfg.get("k", 20))
    threshold = float(cfg.get("threshold", 0.7))
    formula = str(cfg.get("formula", "weighted"))  # "weighted" | "multiply" | "min"
    w_llm = float(cfg.get("weight_llm", 0.60))
    w_emb = float(cfg.get("weight_emb", 0.40))
    risk_below_emb = float(cfg.get("risk_below_emb", 0.7))
    risk_amount = float(cfg.get("risk_amount", 0.20))
    same_type_discount = float(cfg.get("same_type_discount", 0.0))
    bucket_diversity_discount = float(cfg.get("bucket_diversity_discount", 0.0))
    require_llm_yes = bool(cfg.get("require_llm_yes", False))
    min_llm = float(cfg.get("min_llm", 0.0))
    min_emb = float(cfg.get("min_emb", 0.0))
    auto_promote_floor = float(cfg.get("auto_promote_floor", 0.95))
    auto_review_floor = float(cfg.get("auto_review_floor", 0.85))

    # Group cache entries by source to apply per-source top-k
    by_source: dict[str, list[dict[str, Any]]] = {}
    for entry in cache.values():
        by_source.setdefault(entry["source_uid"], []).append(entry)

    decisions: list[dict[str, Any]] = []
    for source_uid, entries in by_source.items():
        # Filter by threshold then take top-k by embedding_similarity
        kept = sorted(
            (e for e in entries if e["embedding_similarity"] >= threshold),
            key=lambda x: -x["embedding_similarity"],
        )[:k]
        for e in kept:
            emb = float(e["embedding_similarity"])
            llm = float(e["llm_score"])
            verdict = e.get("llm_verdict", "")
            if require_llm_yes and verdict != "YES":
                continue
            if llm < min_llm or emb < min_emb:
                continue
            risk = risk_amount if emb < risk_below_emb else 0.0
            penalty = 0.0
            if same_type_discount and e["source_type"] == e["target_type"]:
                penalty += same_type_discount
            if bucket_diversity_discount and _bucket(e["source_rel_path"]) == _bucket(e["target_rel_path"]):
                penalty += bucket_diversity_discount
            if formula == "multiply":
                base = llm * emb
            elif formula == "min":
                base = min(llm, emb)
            else:
                base = w_llm * llm + w_emb * emb
            final = max(0.0, min(1.0, base - risk - penalty))
            if final >= auto_promote_floor:
                decision = "auto_promote"
            elif final >= auto_review_floor:
                decision = "review"
            else:
                decision = "discard"
            decisions.append({
                **e,
                "final_confidence": round(final, 6),
                "decision": decision,
                "band": _band(final),
            })

    decisions.sort(key=lambda x: -x["final_confidence"])

    band_counts = Counter(d["band"] for d in decisions)
    surfaceable = sum(1 for d in decisions if d["final_confidence"] >= auto_review_floor)
    auto_promotable = sum(1 for d in decisions if d["final_confidence"] >= auto_promote_floor)
    cross_type = sum(1 for d in decisions if d["source_type"] != d["target_type"])
    band_order = ["band_4_0.95+", "band_3_0.85_0.95", "band_2_0.70_0.85",
                  "band_1_0.50_0.70", "band_a_0.30_0.50", "band_0_below_0.30"]

    summary = [
        f"# Iteration {label} — semantic linker offline re-eval",
        "",
        "## Config",
        "",
        "| knob | value |",
        "|---|---|",
        f"| k | {k} |",
        f"| threshold | {threshold} |",
        f"| formula | {formula} |",
        f"| weight_llm | {w_llm} |",
        f"| weight_emb | {w_emb} |",
        f"| min_llm | {min_llm} |",
        f"| min_emb | {min_emb} |",
        f"| require_llm_yes | {require_llm_yes} |",
        f"| risk_below_emb | {risk_below_emb} |",
        f"| risk_amount | {risk_amount} |",
        f"| same_type_discount | {same_type_discount} |",
        f"| bucket_diversity_discount | {bucket_diversity_discount} |",
        f"| auto_promote_floor | {auto_promote_floor} |",
        f"| auto_review_floor | {auto_review_floor} |",
        "",
        "## Headline",
        "",
        f"- candidates judged: **{len(decisions)}**",
        f"- surfaceable (>= review_floor): **{surfaceable}**",
        f"- auto-promotable (>= promote_floor): **{auto_promotable}**",
        f"- cross-type matches: **{cross_type}**",
        "",
        "## Per-band counts",
        "",
        "| band | count |",
        "|---|---|",
    ]
    for b in band_order:
        summary.append(f"| {b} | {band_counts.get(b, 0)} |")
    summary.extend(["", "## Top 25 decisions", ""])
    summary.append("| band | source -> target | src_type | tgt_type | emb | llm | final | decision |")
    summary.append("|---|---|---|---|---|---|---|---|")
    for d in decisions[:25]:
        arrow = f"`{d['source_rel_path']}` -> `{d['target_rel_path']}`"
        summary.append(
            f"| {d['band']} | {arrow} | {d['source_type']} | {d['target_type']} | "
            f"{d['embedding_similarity']:.3f} | {d['llm_score']:.2f} | "
            f"{d['final_confidence']:.3f} | {d['decision']} |"
        )

    out_md = ITER_DIR / f"turn-{label}-summary.md"
    out_md.write_text("\n".join(summary) + "\n")
    out_json = ITER_DIR / f"turn-{label}-decisions.json"
    out_json.write_text(json.dumps(decisions, indent=2))

    return {
        "label": label, "config": cfg,
        "total": len(decisions), "surfaceable": surfaceable,
        "auto_promotable": auto_promotable, "cross_type": cross_type,
        "bands": {b: band_counts.get(b, 0) for b in band_order},
    }


def main() -> None:
    import os
    cache_path = Path(os.environ.get("PHASE6_CACHE", str(DEFAULT_CACHE_PATH)))
    if not cache_path.exists():
        raise SystemExit(f"cache not found at {cache_path}; run phase6_precompute.py first")
    cache = json.loads(cache_path.read_text())
    print(f"cache: {cache_path.name} ({len(cache)} entries)")
    if not TURNS_PATH.exists():
        raise SystemExit(f"turns config not found at {TURNS_PATH}")
    turns = json.loads(TURNS_PATH.read_text())

    if len(sys.argv) >= 2 and sys.argv[1] != "--all":
        label = sys.argv[1]
        cfg = next((t for t in turns if t["label"] == label), None)
        if cfg is None:
            raise SystemExit(f"no turn with label={label}")
        result = evaluate_turn(label, cfg, cache)
        print(json.dumps(result, indent=2))
        return

    summary_rows = []
    for cfg in turns:
        r = evaluate_turn(cfg["label"], cfg, cache)
        summary_rows.append(r)
    print()
    print("| turn | formula | min_llm | min_emb | req_yes | promo_fl | total | surf | auto | cross | comment |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in summary_rows:
        c = r["config"]
        print(
            f"| {r['label']} | {c.get('formula', 'weighted')} | "
            f"{c.get('min_llm', 0.0)} | {c.get('min_emb', 0.0)} | "
            f"{c.get('require_llm_yes', False)} | "
            f"{c.get('auto_promote_floor', 0.95)} | "
            f"{r['total']} | {r['surfaceable']} | {r['auto_promotable']} | {r['cross_type']} | "
            f"{c.get('comment', '')[:50]} |"
        )

    (ITER_DIR / "10-turn-summary.json").write_text(json.dumps(summary_rows, indent=2))
    print(f"\nwrote {ITER_DIR / '10-turn-summary.json'}")


if __name__ == "__main__":
    main()
