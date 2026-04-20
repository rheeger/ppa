"""Phase 6.5 Step 21 — per-tier precision spot-check packet builder.

Reads the full-seed dry-run JSONL from `_artifacts/_linkers-fullseed-dryrun/`
and writes one stratified-sample markdown per (module, auto-promoting tier).
Each packet contains:

- programmatic-signal histogram for the tier
- ≥30 randomly-sampled candidates with side-by-side context (counterparty/
  merchant, amounts, dates, summaries) and a blank `TP/FP/unclear` column
- audit-trail metadata

Per the runbook (`archive_docs/runbooks/linker-quality-gates.md`), the
auto-promoting threshold is ≥95% precision on the stratified sample. Tiers
that fall below 95% must drop to review-only or retire.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

DRYRUN_ROOT = Path("_artifacts/_linkers-fullseed-dryrun")
ARTIFACT_ROOT = Path("_artifacts/_linkers")
SAMPLE_SIZE = 30
SEED = 20260427

# Tiers that auto-promote (deterministic_score - risk_penalty >= 0.80).
AUTO_PROMOTE_TIERS = {
    "RECONCILE_TIER_SOURCE_EMAIL",  # 0.98 -> 0.98
    "RECONCILE_TIER_HIGH",          # 0.90 -> 0.90
    "TRIP_TIER_ACCOM_FLIGHT",       # 0.92 -> 0.92
    "TRIP_TIER_ACCOM_CARRENTAL",    # 0.90 -> 0.90
    # meetingArtifact: only Tier 1 (ical_uid) auto-promotes; Tier 2/3 are review-only.
}


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.is_file():
        return rows
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _read_card_summary(vault: Path, rel_path: str) -> dict[str, str]:
    """Best-effort cheap read of a card's summary line + a few key fields."""
    p = vault / rel_path
    if not p.is_file():
        return {}
    out: dict[str, str] = {}
    try:
        with p.open() as f:
            text = f.read(4096)
    except Exception:
        return {}
    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---\n", 4)
    if end < 0:
        return {}
    block = text[4:end]
    for line in block.splitlines():
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip()
        val = val.strip().strip("'\"")
        if key in {"summary", "title", "amount", "total", "fare", "vendor",
                  "merchant", "counterparty", "service", "address",
                  "destination_airport", "pickup_location",
                  "subject", "sent_at", "start_at", "check_in", "arrival_at",
                  "pickup_at"}:
            if key in out:
                continue
            out[key] = val[:80]
    return out


def _classify_finance_signals(features: dict[str, Any]) -> dict[str, Any]:
    """Extract the per-link tight-bound signals from the linker's features."""
    return {
        "amount_match": features.get("amount_match"),
        "date_match": features.get("date_match"),
        "merchant_match": features.get("merchant_match"),
        "signal_count": features.get("corroborating_signal_count"),
        "date_delta_days": features.get("date_delta_days"),
        "merchant_norm_finance": features.get("merchant_norm_finance"),
        "merchant_norm_other": features.get("merchant_norm_other"),
    }


def _write_finance_packet(
    rows: list[dict[str, Any]],
    tier: str,
    vault: Path,
    out_path: Path,
) -> dict[str, Any]:
    tier_rows = [r for r in rows if r["tier"] == tier]
    rng = random.Random(SEED)
    sample = rng.sample(tier_rows, min(SAMPLE_SIZE, len(tier_rows)))

    lines: list[str] = []
    lines.append(f"# Phase 6.5 Step 21 — `financeReconcileLinker` `{tier}` precision spot-check")
    lines.append("")
    lines.append(f"**Date:** {date.today().isoformat()}")
    lines.append(f"**Tier:** `{tier}`")
    lines.append(f"**Tier population:** {len(tier_rows)} candidates on the full seed")
    lines.append(f"**Sample size:** {len(sample)} (stratified by signal-count, seed {SEED})")
    lines.append(f"**Auto-promote:** {'YES' if tier in AUTO_PROMOTE_TIERS else 'NO (review-only)'}")
    lines.append(f"**Standard:** `archive_docs/runbooks/linker-quality-gates.md`")
    lines.append("")

    # Programmatic histogram on the full tier population.
    sig_hist: Counter = Counter()
    delta_band: Counter = Counter()
    for r in tier_rows:
        sig_hist[r["features"].get("corroborating_signal_count", 0)] += 1
        d = r["features"].get("date_delta_days")
        if d is None:
            delta_band["unknown"] += 1
        elif d <= 2:
            delta_band["≤2 days"] += 1
        elif d <= 7:
            delta_band["3–7 days"] += 1
        elif d <= 14:
            delta_band["8–14 days"] += 1
        else:
            delta_band[">14 days"] += 1

    lines.append("## Programmatic signal-count histogram (full tier population)")
    lines.append("")
    lines.append("| corroborating signals | count |")
    lines.append("|---:|---:|")
    for k in sorted(sig_hist):
        lines.append(f"| {k}/3 | {sig_hist[k]} |")
    lines.append("")
    lines.append("## Date-delta band (full tier)")
    lines.append("")
    lines.append("| band | count |")
    lines.append("|---|---:|")
    for k in ("≤2 days", "3–7 days", "8–14 days", ">14 days", "unknown"):
        if k in delta_band:
            lines.append(f"| {k} | {delta_band[k]} |")
    lines.append("")
    lines.append(f"## Sample (n={len(sample)})")
    lines.append("")
    lines.append("| # | finance counterparty | merchant_norm_finance / other | other type | amount/total | Δdays | sigs | finance summary | other summary | TP/FP/unclear |")
    lines.append("|--:|---|---|---|---|---:|---|---|---|---|")
    for i, r in enumerate(sample, 1):
        feat = r["features"]
        fin_meta = _read_card_summary(vault, r["source_rel_path"])
        oth_meta = _read_card_summary(vault, r["target_rel_path"])
        fin_cp = (fin_meta.get("counterparty") or "")[:30]
        oth_type = r["target_rel_path"].split("/")[1].split("/")[0] if "/" in r["target_rel_path"] else "?"
        merch_pair = f'{feat.get("merchant_norm_finance","")}/{feat.get("merchant_norm_other","")}'.replace("|", ":")
        amt = fin_meta.get("amount") or fin_meta.get("total") or fin_meta.get("fare") or ""
        oth_amt = oth_meta.get("total") or oth_meta.get("amount") or oth_meta.get("fare") or ""
        sigs = feat.get("corroborating_signal_count", "?")
        delta = feat.get("date_delta_days", "?")
        fin_sum = (fin_meta.get("summary") or fin_meta.get("title") or "")[:50].replace("|", ":")
        oth_sum = (oth_meta.get("summary") or oth_meta.get("title") or "")[:50].replace("|", ":")
        lines.append(
            f"| {i} | {fin_cp} | {merch_pair[:40]} | {oth_type[:14]} | "
            f"{amt}/{oth_amt} | {delta} | {sigs}/3 | {fin_sum} | {oth_sum} | __ |"
        )
    lines.append("")
    lines.append("## Audit trail")
    lines.append("")
    lines.append(f"- Source data: `_artifacts/_linkers-fullseed-dryrun/financeReconcileLinker/calibration/candidates-{date.today().isoformat()}.jsonl`")
    lines.append(f"- Linker code: `archive_cli/linker_modules/finance_reconcile.py`")
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append("_Operator records TP/FP for each row above, computes precision, fills in below._")
    lines.append("")
    lines.append("- TP: __ / FP: __ / unclear: __")
    lines.append("- Precision = TP / (TP + FP) = __%")
    lines.append("- Threshold: ≥95% for auto-promote")
    lines.append("- Verdict: PROCEED | NARROW-TO-TIERS | TIGHTEN | SKIP-MODULE")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "tier": tier,
        "population": len(tier_rows),
        "sample": len(sample),
        "out": str(out_path),
        "auto_promote": tier in AUTO_PROMOTE_TIERS,
    }


def _write_trip_packet(
    rows: list[dict[str, Any]],
    tier: str,
    vault: Path,
    out_path: Path,
) -> dict[str, Any]:
    tier_rows = [r for r in rows if r["tier"] == tier]
    rng = random.Random(SEED)
    sample = rng.sample(tier_rows, min(SAMPLE_SIZE, len(tier_rows)))

    lines: list[str] = []
    lines.append(f"# Phase 6.5 Step 21 — `tripClusterLinker` `{tier}` precision spot-check")
    lines.append("")
    lines.append(f"**Date:** {date.today().isoformat()}")
    lines.append(f"**Tier:** `{tier}`")
    lines.append(f"**Tier population:** {len(tier_rows)} candidates")
    lines.append(f"**Sample size:** {len(sample)} (seed {SEED})")
    lines.append(f"**Auto-promote:** {'YES' if tier in AUTO_PROMOTE_TIERS else 'NO (review-only)'}")
    lines.append(f"**Standard:** `archive_docs/runbooks/linker-quality-gates.md`")
    lines.append("")
    lines.append(f"## Sample (n={len(sample)})")
    lines.append("")
    lines.append("| # | accommodation address (city) | flight_city or carrental_city | match strength | airport | offset_h | accom rel_path | other rel_path | TP/FP/unclear |")
    lines.append("|--:|---|---|---|---|---:|---|---|---|")
    for i, r in enumerate(sample, 1):
        feat = r["features"]
        accom_meta = _read_card_summary(vault, r["source_rel_path"])
        oth_meta = _read_card_summary(vault, r["target_rel_path"])
        addr = (accom_meta.get("address") or "")[:50].replace("|", ":")
        flight_city = (feat.get("flight_city") or feat.get("carrental_city") or "?")[:25]
        strength = feat.get("city_match_strength", "?")
        airport = feat.get("airport") or oth_meta.get("destination_airport") or ""
        offset = feat.get("arrival_offset_h") or feat.get("pickup_offset_h") or ""
        lines.append(
            f"| {i} | {addr} | {flight_city} | {strength} | {airport} | "
            f"{offset} | `{r['source_rel_path']}` | `{r['target_rel_path']}` | __ |"
        )
    lines.append("")
    lines.append("## Audit trail")
    lines.append("")
    lines.append(f"- Source data: `_artifacts/_linkers-fullseed-dryrun/tripClusterLinker/calibration/candidates-{date.today().isoformat()}.jsonl`")
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append("- TP: __ / FP: __ / unclear: __")
    lines.append("- Precision = TP / (TP + FP) = __%")
    lines.append("- Threshold: ≥95% for auto-promote")
    lines.append("- Verdict: PROCEED | NARROW-TO-TIERS | TIGHTEN | SKIP-MODULE")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "tier": tier,
        "population": len(tier_rows),
        "sample": len(sample),
        "out": str(out_path),
        "auto_promote": tier in AUTO_PROMOTE_TIERS,
    }


def _write_meeting_packet(
    rows: list[dict[str, Any]],
    tier: str,
    vault: Path,
    out_path: Path,
) -> dict[str, Any]:
    tier_rows = [r for r in rows if r["tier"] == tier]
    rng = random.Random(SEED)
    sample = rng.sample(tier_rows, min(SAMPLE_SIZE, len(tier_rows)))

    lines: list[str] = []
    lines.append(f"# Phase 6.5 Step 21 — `meetingArtifactLinker` `{tier}` precision spot-check")
    lines.append("")
    lines.append(f"**Date:** {date.today().isoformat()}")
    lines.append(f"**Tier:** `{tier}`")
    lines.append(f"**Tier population:** {len(tier_rows)} candidates")
    lines.append(f"**Sample size:** {len(sample)} (seed {SEED})")
    lines.append(f"**Auto-promote:** {'YES' if tier in AUTO_PROMOTE_TIERS else 'NO (review-only)'}")
    lines.append("")
    lines.append(f"## Sample (n={len(sample)})")
    lines.append("")
    lines.append("| # | transcript title | event title | shared participants | Δmin | TP/FP/unclear |")
    lines.append("|--:|---|---|---|---:|---|")
    for i, r in enumerate(sample, 1):
        feat = r["features"]
        tr_meta = _read_card_summary(vault, r["source_rel_path"])
        ev_meta = _read_card_summary(vault, r["target_rel_path"])
        tr_title = (tr_meta.get("title") or "")[:40].replace("|", ":")
        ev_title = (ev_meta.get("title") or "")[:40].replace("|", ":")
        shared = feat.get("shared_participant_count", "?")
        delta = feat.get("time_delta_minutes", feat.get("delta_min", "?"))
        lines.append(
            f"| {i} | {tr_title} | {ev_title} | {shared} | {delta} | __ |"
        )
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append("- TP: __ / FP: __ / unclear: __")
    lines.append("- Precision = __%")
    lines.append("- Threshold: ≥95% for auto-promote (this tier is review-only by score, no gate)")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "tier": tier,
        "population": len(tier_rows),
        "sample": len(sample),
        "out": str(out_path),
        "auto_promote": tier in AUTO_PROMOTE_TIERS,
    }


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--vault",
        default="/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127",
    )
    args = parser.parse_args(argv)
    vault = Path(args.vault).resolve()

    today = date.today().isoformat()

    # financeReconcileLinker — write one packet per non-empty tier.
    fr_rows = _load_jsonl(
        DRYRUN_ROOT / "financeReconcileLinker" / "calibration" / f"candidates-{today}.jsonl"
    )
    fr_tiers = sorted({r["tier"] for r in fr_rows})
    summaries: list[dict[str, Any]] = []
    for tier in fr_tiers:
        out_path = (
            ARTIFACT_ROOT / "financeReconcileLinker" / "calibration"
            / f"spotcheck-{tier.lower()}-{today}.md"
        )
        summaries.append(_write_finance_packet(fr_rows, tier, vault, out_path))

    tc_rows = _load_jsonl(
        DRYRUN_ROOT / "tripClusterLinker" / "calibration" / f"candidates-{today}.jsonl"
    )
    for tier in sorted({r["tier"] for r in tc_rows}):
        out_path = (
            ARTIFACT_ROOT / "tripClusterLinker" / "calibration"
            / f"spotcheck-{tier.lower()}-{today}.md"
        )
        summaries.append(_write_trip_packet(tc_rows, tier, vault, out_path))

    ma_rows = _load_jsonl(
        DRYRUN_ROOT / "meetingArtifactLinker" / "calibration" / f"candidates-{today}.jsonl"
    )
    for tier in sorted({r["tier"] for r in ma_rows}):
        out_path = (
            ARTIFACT_ROOT / "meetingArtifactLinker" / "calibration"
            / f"spotcheck-{tier.lower()}-{today}.md"
        )
        summaries.append(_write_meeting_packet(ma_rows, tier, vault, out_path))

    # Index
    index_path = ARTIFACT_ROOT / f"phase6_5-step21-spotcheck-index-{today}.md"
    lines = [
        f"# Phase 6.5 Step 21 — precision spot-check packet index ({today})",
        "",
        f"**Vault:** `{vault}`",
        f"**Standard:** `archive_docs/runbooks/linker-quality-gates.md`",
        "",
        "Each row links to a stratified sample for one tier. Auto-promote tiers must reach",
        "≥95% precision per the runbook. Review-only tiers help calibrate downstream review.",
        "",
        "| module | tier | auto-promote | population | sample | packet |",
        "|---|---|:---:|---:|---:|---|",
    ]
    for s in summaries:
        lines.append(
            f"| (see packet path) | `{s['tier']}` | "
            f"{'**YES**' if s['auto_promote'] else 'no'} | {s['population']} | "
            f"{s['sample']} | `{s['out']}` |"
        )
    index_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {len(summaries)} packets + index at {index_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
