"""Phase 6.5 Step 21 — full-seed dry-run calibration.

Runs the same generators-only loop as `phase6_5_calibrate_1pct.py` but against
the production vault, with tier-2 cache so that body access works. Writes
artifacts under `_artifacts/_linkers-fullseed-dryrun/{module}/calibration/`
so that the existing 1pct artifacts are not clobbered.

This is a read-only operation: no Postgres writes, no edge promotion, just
candidate generation. Output: per-tier candidate counts on the full seed
across all three Phase 6.5 modules. Use the resulting JSONL files as the
seed for the per-tier precision spot-check (linker-quality-gates.md).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

from archive_cli import linker_framework as lf
from archive_cli import seed_links as s
from archive_cli.vault_cache import VaultScanCache

PHASE_6_5_MODULES = (
    "meetingArtifactLinker",
    "tripClusterLinker",
    "financeReconcileLinker",
)


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault", required=True)
    parser.add_argument("--module", action="append", default=[])
    parser.add_argument(
        "--artifact-root", default="_artifacts/_linkers-fullseed-dryrun",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("ppa.calibrate.fullseed")

    modules = tuple(args.module) or PHASE_6_5_MODULES
    vault = Path(args.vault).resolve()
    if not vault.exists():
        print(f"Vault not found: {vault}", file=sys.stderr)
        return 2

    log.info("Building tier-2 vault cache for %s ...", vault)
    cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=10000)
    log.info("Building seed-link catalog ...")
    catalog = s.build_seed_link_catalog(vault, cache=cache)
    log.info(
        "Catalog ready: %d cards by uid, %d card types",
        len(catalog.cards_by_uid),
        len(catalog.cards_by_type),
    )

    artifact_root = Path(args.artifact_root)
    today = date.today().isoformat()
    summary: dict[str, dict] = {}

    for module in modules:
        spec = lf.ALL_LINKERS.get(module)
        if spec is None:
            log.warning("unknown module %s; skipping", module)
            continue
        if spec.lifecycle_state == "retired":
            log.info("skipping retired module %s", module)
            continue

        sources: list[s.SeedCardSketch] = []
        if spec.source_card_types:
            for ct in spec.source_card_types:
                sources.extend(catalog.cards_by_type.get(ct, []))
        else:
            sources = list(catalog.cards_by_uid.values())

        out_dir = artifact_root / module / "calibration"
        out_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = out_dir / f"candidates-{today}.jsonl"

        tier_counts: dict[str, int] = {}
        total = 0
        with jsonl_path.open("w", encoding="utf-8") as out:
            for source in sources:
                try:
                    cands = spec.generator(catalog, source)
                except Exception as exc:
                    log.warning("[%s] generator on %s: %s", module, source.uid, exc)
                    continue
                for c in cands:
                    tier = str(c.features.get("tier") or "UNTIERED")
                    tier_counts[tier] = tier_counts.get(tier, 0) + 1
                    total += 1
                    det = float(c.features.get("deterministic_score") or 0.0)
                    risk = float(c.features.get("risk_penalty") or 0.0)
                    final = max(0.0, min(1.0, det - risk))
                    row = {
                        "module_name": c.module_name,
                        "source_card_uid": c.source_card_uid,
                        "source_rel_path": c.source_rel_path,
                        "target_card_uid": c.target_card_uid,
                        "target_rel_path": c.target_rel_path,
                        "proposed_link_type": c.proposed_link_type,
                        "tier": tier,
                        "deterministic_score": det,
                        "risk_penalty": risk,
                        "final_confidence": final,
                        "features": c.features,
                    }
                    out.write(json.dumps(row, sort_keys=True) + "\n")

        log.info(
            "[%s] sources=%d total=%d tiers=%s -> %s",
            module, len(sources), total, tier_counts, jsonl_path,
        )
        summary[module] = {
            "sources": len(sources),
            "total": total,
            "tiers": tier_counts,
            "jsonl": str(jsonl_path),
        }

    summary_path = artifact_root / f"phase6_5-fullseed-dryrun-{today}.md"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Phase 6.5 Step 21 — full-seed dry-run calibration ({today})",
        "",
        f"**Vault:** `{vault}`",
        f"**Cache tier:** 2",
        "",
        "| module | sources | candidates | tier histogram |",
        "|---|---:|---:|---|",
    ]
    for module, m in summary.items():
        tiers = ", ".join(f"{k}:{v}" for k, v in sorted(m["tiers"].items())) or "none"
        lines.append(f"| `{module}` | {m['sources']} | {m['total']} | {tiers} |")
    lines.append("")
    lines.append("Per-module JSONL artifacts:")
    lines.append("")
    for module, m in summary.items():
        lines.append(f"- `{m['jsonl']}`")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("summary -> %s", summary_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
