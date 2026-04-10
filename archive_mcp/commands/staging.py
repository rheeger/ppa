"""Staging directory inspection and volume estimate validation."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from archive_mcp.features import card_activity_at
from archive_sync.extractors.field_metrics import compute_field_population
from hfa.vault import read_note_frontmatter_file

log = logging.getLogger("ppa.staging")

# Calibrated from Phase 3 full-seed extraction (2026-04-07, hf-archives-seed-20260307-235127).
# Each range is ~±30% around observed counts so staging-report stays OK on re-runs.
VOLUME_ESTIMATES: dict[str, tuple[int, int]] = {
    "meal_order": (717, 1333),
    "purchase": (1, 500),
    "ride": (473, 881),
    "shipment": (486, 904),
    "grocery_order": (11, 23),
    "flight": (423, 787),
    "accommodation": (206, 384),
    "car_rental": (35, 67),
    "subscription": (50, 100),
    "event_ticket": (20, 50),
    "payroll": (100, 200),
}


@dataclass
class StagingTypeSummary:
    card_type: str
    count: int
    date_range: tuple[str, str]
    sample_uids: list[str]
    volume_estimate: tuple[int, int] | None
    within_estimate: bool
    volume_status: str  # OK | LOW | HIGH


@dataclass
class StagingReport:
    total_cards: int = 0
    types: list[StagingTypeSummary] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _volume_status(count: int, est: tuple[int, int] | None) -> tuple[bool, str]:
    if not est:
        return True, "OK"
    lo, hi = est
    if lo <= count <= hi:
        return True, "OK"
    half_lo = int(lo * 0.5)
    if count < half_lo:
        return False, f"LOW (below 50% of lower bound {lo})"
    over_hi = int(hi * 1.5)
    if count > over_hi:
        return False, f"HIGH (above 150% of upper bound {hi})"
    if count < lo:
        return False, f"LOW (below range {lo}-{hi})"
    if count > hi:
        return False, f"HIGH (above range {lo}-{hi})"
    return True, "OK"


def staging_report(staging_dir: str) -> StagingReport:
    """Scan staging directory and produce per-type summary."""
    root = Path(staging_dir)
    report = StagingReport()
    if not root.is_dir():
        log.warning("staging dir missing or not a directory: %s", staging_dir)
        return report

    by_type: dict[str, list[tuple[str, str, str]]] = {}
    # (uid, activity_at, path_str) per type
    for path in root.rglob("*.md"):
        if path.name.startswith("_"):
            continue
        try:
            rec = read_note_frontmatter_file(path)
        except OSError as exc:
            report.warnings.append(f"{path}: {exc}")
            continue
        fm = rec.frontmatter
        ct = str(fm.get("type") or "").strip() or "unknown"
        uid = str(fm.get("uid") or "").strip() or path.stem
        act = card_activity_at(fm)
        by_type.setdefault(ct, []).append((uid, act, str(path)))

    for ct in sorted(by_type.keys()):
        rows = by_type[ct]
        count = len(rows)
        report.total_cards += count
        dates = [r[1] for r in rows if r[1]]
        if dates:
            dmin, dmax = min(dates), max(dates)
        else:
            dmin, dmax = "", ""
        uids = [r[0] for r in rows[:10]]
        est = VOLUME_ESTIMATES.get(ct)
        ok, status = _volume_status(count, est)
        if not ok:
            report.warnings.append(f"{ct}: volume {status} (count={count})")
        report.types.append(
            StagingTypeSummary(
                card_type=ct,
                count=count,
                date_range=(dmin, dmax),
                sample_uids=uids,
                volume_estimate=est,
                within_estimate=ok,
                volume_status=status,
            )
        )
    return report


def emit_full_staging_report(staging_dir: str) -> None:
    """Log human-readable staging summary to stderr (operational logging)."""
    report = staging_report(staging_dir)
    fp = compute_field_population(Path(staging_dir))
    text = format_staging_report_markdown(report, field_population=fp).strip()
    if text:
        log.info("%s", text)


def format_staging_report_markdown(
    report: StagingReport,
    *,
    field_population: dict[str, dict[str, float]] | None = None,
) -> str:
    """Human-readable table for stderr / docs."""
    lines = [
        "",
        "| Type | Count | Expected | Status |",
        "|------|------:|----------|--------|",
    ]
    for t in report.types:
        exp = (
            f"{t.volume_estimate[0]}-{t.volume_estimate[1]}"
            if t.volume_estimate
            else "—"
        )
        lines.append(f"| {t.card_type} | {t.count:,} | {exp} | {t.volume_status} |")
    if field_population:
        lines.append("")
        lines.append("**Critical field population** (fraction of cards with field populated)")
        lines.append("")
        lines.append("| Type | Field | Populated |")
        lines.append("|------|-------|----------:|")
        for ct in sorted(field_population.keys()):
            for fname, rate in sorted(field_population[ct].items()):
                lines.append(f"| {ct} | {fname} | {rate * 100:.1f}% |")
    if report.warnings:
        lines.append("")
        lines.append("**Warnings:**")
        for w in report.warnings:
            lines.append(f"- {w}")
    lines.append("")
    return "\n".join(lines)


def staging_report_to_jsonable(report: StagingReport) -> dict[str, Any]:
    """JSON-serializable dict (tuples -> lists for date_range / volume_estimate)."""
    types_out: list[dict[str, Any]] = []
    for t in report.types:
        d = asdict(t)
        d["date_range"] = list(t.date_range)
        if t.volume_estimate:
            d["volume_estimate"] = list(t.volume_estimate)
        types_out.append(d)
    return {
        "total_cards": report.total_cards,
        "types": types_out,
        "warnings": list(report.warnings),
    }
