"""Shared Phase 6.5 vault calibration loop (generators only, no Postgres)."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from archive_cli import linker_framework as lf
from archive_cli import seed_links as s
from archive_cli.vault_cache import VaultScanCache

log = logging.getLogger("ppa.linker_calibration")


def run_vault_calibration(
    vault_path: Path,
    modules: tuple[str, ...],
    *,
    artifact_root: Path = Path("_artifacts/_linkers"),
    write_phase_summary: bool = True,
) -> dict[str, dict[str, Any]]:
    """Build cache + catalog, run each module's generator, write jsonl + per-module reports.

    Returns per-module summary dicts (sources_scanned, total_candidates, tier_histogram, artifact_path).
    """
    cache = VaultScanCache.build_or_load(vault_path)
    catalog = s.build_seed_link_catalog(vault_path, cache=cache)
    log.info(
        "Catalog ready: %d cards by uid, %d card types",
        len(catalog.cards_by_uid),
        len(catalog.cards_by_type),
    )

    overall: dict[str, dict[str, Any]] = {}

    for module in modules:
        spec = lf.ALL_LINKERS.get(module)
        if spec is None:
            log.warning("Skipping unknown module %s", module)
            continue
        if spec.lifecycle_state == "retired":
            log.warning("Skipping retired module %s", module)
            continue

        sources: list[s.SeedCardSketch] = []
        if spec.source_card_types:
            for ct in spec.source_card_types:
                sources.extend(catalog.cards_by_type.get(ct, []))
        else:
            sources = list(catalog.cards_by_uid.values())

        artifact_dir = artifact_root / module / "calibration"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = artifact_dir / f"candidates-{date.today().isoformat()}.jsonl"

        tier_counts: dict[str, int] = {}
        total = 0
        with jsonl_path.open("w", encoding="utf-8") as out:
            for source in sources:
                try:
                    cands = spec.generator(catalog, source)
                except Exception as exc:  # defensive
                    log.warning("[%s] generator failed on %s: %s", module, source.uid, exc)
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
            "[%s] %d candidates -> %s; tiers: %s",
            module,
            total,
            jsonl_path,
            tier_counts,
        )

        overall[module] = {
            "sources_scanned": len(sources),
            "total_candidates": total,
            "tier_histogram": tier_counts,
            "artifact_path": str(jsonl_path),
        }

        report_path = artifact_dir / f"report-{date.today().isoformat()}.md"
        _write_report(report_path, module, spec, overall[module], jsonl_path)

    if write_phase_summary and overall:
        phase_artifact = artifact_root / f"phase6_5-calibration-summary-{date.today().isoformat()}.md"
        _write_phase_summary(phase_artifact, overall, vault_path)
        log.info("Phase summary -> %s", phase_artifact)

    return overall


def _write_report(
    path: Path,
    module: str,
    spec: lf.LinkerSpec,
    summary: dict[str, Any],
    jsonl_path: Path,
) -> None:
    lines: list[str] = []
    lines.append(f"# Phase 6.5 Step 12 -- {module} calibration report")
    lines.append("")
    lines.append(f"**Date:** {date.today().isoformat()}")
    lines.append("**Scope:** vault (generator-only; no link_decisions writes)")
    lines.append(f"**Scoring mode:** `{spec.scoring_mode}`")
    lines.append(f"**Lifecycle:** `{spec.lifecycle_state}`")
    lines.append(f"**Source types:** `{', '.join(spec.source_card_types) or '(any)'}`")
    lines.append(f"**Emits:** `{', '.join(spec.emits_link_types)}`")
    lines.append("")
    lines.append("## Totals")
    lines.append("")
    lines.append(f"- sources scanned: {summary['sources_scanned']}")
    lines.append(f"- candidates generated: {summary['total_candidates']}")
    lines.append(f"- calibration cache: `{jsonl_path}`")
    lines.append("")
    lines.append("## Tier histogram")
    lines.append("")
    hist = summary.get("tier_histogram") or {}
    if hist:
        lines.append("| tier | count |")
        lines.append("|---|---:|")
        for tier, count in sorted(hist.items()):
            lines.append(f"| `{tier}` | {count} |")
    else:
        lines.append("_No candidates._")
    lines.append("")
    lines.append("## Sample candidates")
    lines.append("")
    samples_by_tier: dict[str, list[dict[str, Any]]] = {}
    with jsonl_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            tier = row.get("tier", "UNTIERED")
            samples_by_tier.setdefault(tier, []).append(row)
    for tier in sorted(samples_by_tier.keys()):
        lines.append(f"### {tier}")
        lines.append("")
        for row in samples_by_tier[tier][:5]:
            lines.append(
                f"- `{row['source_card_uid']}` -> `{row['target_card_uid']}` "
                f"(final={row['final_confidence']:.2f}, features={json.dumps(row['features'])})"
            )
        lines.append("")
    lines.append("## Decision")
    lines.append("")
    lines.append(
        "_Human adds PROCEED / TIGHTEN / NARROW-TO-TIERS / SKIP-MODULE verdict below._"
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_phase_summary(path: Path, overall: dict[str, Any], vault_path: Path) -> None:
    lines = [
        f"# Phase 6.5 Step 12 -- calibration summary ({date.today().isoformat()})",
        "",
        f"**Vault:** `{vault_path}`",
        "",
        "| module | sources | candidates | tiers |",
        "|---|---:|---:|---|",
    ]
    for module, summary in overall.items():
        tiers = ", ".join(
            f"{tier}:{count}" for tier, count in sorted(summary.get("tier_histogram", {}).items())
        ) or "none"
        lines.append(
            f"| `{module}` | {summary['sources_scanned']} | "
            f"{summary['total_candidates']} | {tiers} |"
        )
    lines.append("")
    lines.append("Per-module calibration caches:")
    lines.append("")
    for module, summary in overall.items():
        lines.append(f"- **{module}**: `{summary['artifact_path']}`")
    lines.append("")
    lines.append("## Decision gate")
    lines.append("")
    lines.append(
        "Step 13 (full-seed promotion) only for modules with PROCEED in their per-module report."
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
