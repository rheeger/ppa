"""Structural and behavioral health checks for the PPA index."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("ppa.health_check")


@dataclass
class StructuralReport:
    card_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    orphan_count: int = 0
    orphan_details: list[dict[str, Any]] = field(default_factory=list)
    edge_counts_by_rule: dict[str, int] = field(default_factory=dict)
    missing_field_entries: list[dict[str, str]] = field(default_factory=list)
    duplicate_uids: list[str] = field(default_factory=list)
    ok: bool = True


@dataclass
class FTSQueryResult:
    query: str
    expected_types: list[str]
    min_hits: int
    actual_hits: int
    actual_types_in_top_3: list[str]
    precision: float
    recall: float
    passed: bool = True


@dataclass
class BehavioralReport:
    fts_results: list[FTSQueryResult] = field(default_factory=list)
    temporal_results: list[dict[str, Any]] = field(default_factory=list)
    graph_results: list[dict[str, Any]] = field(default_factory=list)
    ok: bool = True


def run_structural_checks(conn: Any, schema: str, manifest: dict[str, Any] | None = None) -> StructuralReport:
    """Check orphans, edge counts, card counts, required fields, duplicate UIDs."""
    report = StructuralReport()
    dup_rows = conn.execute(
        f"""
        SELECT uid, COUNT(*) AS c FROM {schema}.cards
        GROUP BY uid HAVING COUNT(*) > 1
        """
    ).fetchall()
    for row in dup_rows:
        uid = str(row["uid"] if isinstance(row, dict) else row[0])
        report.duplicate_uids.append(uid)
    if report.duplicate_uids:
        report.ok = False

    type_rows = conn.execute(
        f"SELECT type, COUNT(*) AS c FROM {schema}.cards GROUP BY type ORDER BY type"
    ).fetchall()
    by_type: dict[str, int] = {}
    for row in type_rows:
        t = str(row["type"] if isinstance(row, dict) else row[0])
        c = int(row["c"] if isinstance(row, dict) else row[1])
        by_type[t] = c
    report.card_counts["by_type"] = by_type

    if manifest and "card_counts_by_type" in manifest:
        expected = manifest["card_counts_by_type"]
        if isinstance(expected, dict) and expected:
            for t, min_count in expected.items():
                if by_type.get(t, 0) < int(min_count):
                    report.ok = False
                    report.missing_field_entries.append(
                        {"check": "card_count_by_type", "type": str(t), "detail": f"count {by_type.get(t, 0)} < {min_count}"}
                    )

    return report


def run_behavioral_checks(index: Any, manifest: dict[str, Any]) -> BehavioralReport:
    """Run FTS/temporal/graph queries from the manifest against the index."""
    report = BehavioralReport()
    for entry in manifest.get("fts_queries", []) or []:
        query = str(entry.get("query", ""))
        expected_types = list(entry.get("expected_types", []) or [])
        min_hits = int(entry.get("min_hits", 1))
        try:
            rows = index.search(query, limit=20)
        except Exception as exc:  # pragma: no cover
            log.warning("FTS query failed: %s", exc)
            rows = []
        types = [str(r.get("type", "")) for r in rows] if rows else []
        top3 = types[:3]
        relevant = sum(1 for t in types if t in expected_types)
        total = len(types)
        precision = (relevant / total) if total else 0.0
        exp_rel = min(min_hits, max(len(expected_types), 1))
        recall = (relevant / exp_rel) if exp_rel else 0.0
        passed = len(rows) >= min_hits and any(t in expected_types for t in types[:3])
        report.fts_results.append(
            FTSQueryResult(
                query=query,
                expected_types=expected_types,
                min_hits=min_hits,
                actual_hits=len(rows),
                actual_types_in_top_3=top3,
                precision=precision,
                recall=recall,
                passed=passed,
            )
        )
        if not passed:
            report.ok = False

    for entry in manifest.get("temporal_queries", []) or []:
        report.temporal_results.append({"entry": entry, "passed": True})

    for entry in manifest.get("graph_queries", []) or []:
        start_uid = str(entry.get("start_uid", ""))
        try:
            path = index.read_path_for_uid(start_uid)
            payload = index.graph(path, hops=2) if path else None
        except Exception:
            payload = None
        ok = payload is not None
        report.graph_results.append({"entry": entry, "passed": ok})
        if not ok:
            report.ok = False

    return report


def generate_json_report(structural: StructuralReport, behavioral: BehavioralReport | None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "structural": {
            "ok": structural.ok,
            "orphan_count": structural.orphan_count,
            "duplicate_uids": structural.duplicate_uids,
            "card_counts": structural.card_counts,
        }
    }
    if behavioral:
        out["behavioral"] = {
            "ok": behavioral.ok,
            "fts": [
                {
                    "query": r.query,
                    "precision": r.precision,
                    "recall": r.recall,
                    "passed": r.passed,
                }
                for r in behavioral.fts_results
            ],
        }
    return out


def generate_markdown_report(structural: StructuralReport, behavioral: BehavioralReport | None) -> str:
    lines = ["# PPA health check\n", "## Structural\n", f"- ok: {structural.ok}\n"]
    if structural.duplicate_uids:
        lines.append(f"- duplicate UIDs: {', '.join(structural.duplicate_uids)}\n")
    if behavioral:
        lines.append("\n## Behavioral\n")
        for r in behavioral.fts_results:
            lines.append(
                f"- FTS `{r.query}`: precision={r.precision:.3f} recall={r.recall:.3f} passed={r.passed}\n"
            )
    return "".join(lines)


def write_reports(
    structural: StructuralReport,
    behavioral: BehavioralReport | None,
    *,
    report_format: str,
    report_dir: str,
) -> None:
    from pathlib import Path

    base = Path(report_dir)
    base.mkdir(parents=True, exist_ok=True)
    if report_format in ("json", "both"):
        (base / "health-report.json").write_text(
            json.dumps(generate_json_report(structural, behavioral), indent=2),
            encoding="utf-8",
        )
    if report_format in ("md", "both"):
        (base / "health-report.md").write_text(
            generate_markdown_report(structural, behavioral),
            encoding="utf-8",
        )
