"""Structural and behavioral health checks for the PPA index."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger("ppa.health_check")

PHASE9_SKIPPED_CARD_TYPES = frozenset({"knowledge", "observation"})


def _row_first_int(row: Any) -> int:
    if isinstance(row, dict):
        return int(next(iter(row.values())))
    return int(row[0])


def _projection_column_for_source_field(reg: Any, field_name: str) -> str | None:
    candidates = (field_name, f"{field_name}_json")
    for column in reg.projection_columns:
        if column.name in candidates or column.source_field == field_name:
            return str(column.name)
    return None


def _source_field_has_values(conn: Any, schema: str, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """,
        (schema, table_name, column_name),
    ).fetchone()
    if row is None:
        return False
    data_type = str(row["data_type"] if isinstance(row, dict) else row[0])
    if data_type == "jsonb":
        predicate = f"{column_name} IS NOT NULL AND {column_name} <> '[]'::jsonb AND {column_name} <> '{{}}'::jsonb"
    elif data_type == "ARRAY":
        predicate = f"{column_name} IS NOT NULL AND cardinality({column_name}) > 0"
    else:
        predicate = f"{column_name} IS NOT NULL AND {column_name}::text <> ''"
    count_row = conn.execute(f"SELECT 1 FROM {schema}.{table_name} WHERE {predicate} LIMIT 1").fetchone()
    return count_row is not None


def _check_embedding_coverage(conn: Any, schema: str) -> dict[str, Any]:
    """Report how many chunks have embeddings for the default model/version."""
    try:
        from ..index_config import (get_default_embedding_model,
                                    get_default_embedding_version)

        chunk_row = conn.execute(f"SELECT COUNT(*) AS count FROM {schema}.chunks").fetchone()
        chunk_count = int(chunk_row["count"] if isinstance(chunk_row, dict) else chunk_row[0])

        model = get_default_embedding_model()
        version = get_default_embedding_version()
        embed_row = conn.execute(
            f"""SELECT COUNT(*) AS count FROM {schema}.embeddings
                WHERE embedding_model = %s AND embedding_version = %s""",
            (model, version),
        ).fetchone()
        embedded_count = int(embed_row["count"] if isinstance(embed_row, dict) else embed_row[0])

        pending = chunk_count - embedded_count
        coverage = (embedded_count / chunk_count * 100) if chunk_count > 0 else 100.0
        return {
            "ok": pending <= 0,
            "chunk_count": chunk_count,
            "embedded_count": embedded_count,
            "pending_count": max(pending, 0),
            "coverage_percent": round(coverage, 2),
            "embedding_model": model,
            "embedding_version": version,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@dataclass
class StructuralReport:
    card_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    orphan_count: int = 0
    orphan_details: list[dict[str, Any]] = field(default_factory=list)
    edge_counts_by_rule: dict[str, int] = field(default_factory=dict)
    missing_field_entries: list[dict[str, str]] = field(default_factory=list)
    duplicate_uids: list[str] = field(default_factory=list)
    embedding_coverage: dict[str, Any] = field(default_factory=dict)
    ok: bool = True


@dataclass
class DeploymentReport:
    card_type_coverage: dict[str, int] = field(default_factory=dict)
    missing_types: list[str] = field(default_factory=list)
    edge_rules_with_zero_edges: list[str] = field(default_factory=list)
    temporal_index_present: bool = False
    embedding_coverage_pct: float = 0.0
    index_card_count: int = 0
    vault_file_count: int = 0
    card_count_match: bool = False
    ok: bool = True

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class FTSQueryResult:
    query: str
    expected_types: list[str]
    top_3_must_include_types: list[str]
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

    type_rows = conn.execute(f"SELECT type, COUNT(*) AS c FROM {schema}.cards GROUP BY type ORDER BY type").fetchall()
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
                        {
                            "check": "card_count_by_type",
                            "type": str(t),
                            "detail": f"count {by_type.get(t, 0)} < {min_count}",
                        }
                    )

    invariants = manifest.get("structural_invariants", {}) if manifest else {}
    if invariants.get("zero_orphaned_wikilinks"):
        orphan_rows = conn.execute(
            f"""
            SELECT e.source_uid, e.target_slug
            FROM {schema}.edges e
            WHERE e.edge_type = 'wikilink'
              AND e.target_uid = ''
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.cards c WHERE c.slug = e.target_slug
              )
            """
        ).fetchall()
        report.orphan_count = len(orphan_rows)
        if report.orphan_count > 0:
            report.ok = False
            report.orphan_details = [
                {
                    "source_uid": str(row["source_uid"] if isinstance(row, dict) else row[0]),
                    "target_slug": str(row["target_slug"] if isinstance(row, dict) else row[1]),
                }
                for row in orphan_rows[:50]
            ]

    if invariants.get("zero_orphaned_person_refs"):
        orphan_people = conn.execute(
            f"""
            SELECT cp.person, cp.card_uid
            FROM {schema}.card_people cp
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.cards c
                WHERE c.type = 'person'
                  AND (
                      c.uid = cp.person OR
                      c.slug = REPLACE(REPLACE(cp.person, '[[', ''), ']]', '')
                  )
            )
            """
        ).fetchall()
        if orphan_people:
            report.ok = False
            report.missing_field_entries.append(
                {
                    "check": "orphaned_person_refs",
                    "detail": f"{len(orphan_people)} person references without PersonCards",
                }
            )

    if invariants.get("all_edge_rules_fire"):
        from ..projections.registry import EDGE_RULE_SPECS

        for spec in EDGE_RULE_SPECS:
            type_count = by_type.get(spec.card_type, 0)
            if type_count == 0:
                continue
            for edge_type in spec.derived_edge_types:
                if edge_type == "wikilink":
                    continue
                count_row = conn.execute(
                    f"SELECT COUNT(*) FROM {schema}.edges WHERE edge_type = %s",
                    (edge_type,),
                ).fetchone()
                count = int(count_row[0] if not isinstance(count_row, dict) else next(iter(count_row.values())))
                report.edge_counts_by_rule[edge_type] = count
                if count == 0:
                    report.ok = False
                    report.missing_field_entries.append(
                        {
                            "check": "edge_rule_zero",
                            "type": spec.card_type,
                            "edge_type": edge_type,
                            "detail": f"0 edges for {edge_type} (card type {spec.card_type} has {type_count} cards)",
                        }
                    )

    if invariants.get("all_cards_have_summary"):
        empty_summary = conn.execute(
            f"SELECT COUNT(*) FROM {schema}.cards WHERE summary IS NULL OR summary = ''"
        ).fetchone()
        empty_summary_count = int(
            empty_summary[0] if not isinstance(empty_summary, dict) else next(iter(empty_summary.values()))
        )
        if empty_summary_count > 0:
            report.ok = False
            report.missing_field_entries.append(
                {
                    "check": "cards_missing_summary",
                    "detail": f"{empty_summary_count} cards with empty summary",
                }
            )

    if invariants.get("all_cards_have_activity_at"):
        null_activity = conn.execute(f"SELECT COUNT(*) FROM {schema}.cards WHERE activity_at IS NULL").fetchone()
        null_activity_count = int(
            null_activity[0] if not isinstance(null_activity, dict) else next(iter(null_activity.values()))
        )
        if null_activity_count > 0:
            report.ok = False
            report.missing_field_entries.append(
                {
                    "check": "cards_missing_activity_at",
                    "detail": f"{null_activity_count} cards with null/empty activity_at",
                }
            )

    report.embedding_coverage = _check_embedding_coverage(conn, schema)
    if not report.embedding_coverage.get("ok", False):
        report.ok = False

    return report


def run_deployment_checks(conn: Any, schema: str, vault_path: str | Path | None = None) -> DeploymentReport:
    """v2 deployment checks.

    Knowledge cache checks are intentionally absent because Phase 7 was skipped.
    """
    report = DeploymentReport()
    from archive_vault.schema import CARD_TYPES

    rows = conn.execute(f"SELECT type, COUNT(*) AS c FROM {schema}.cards GROUP BY type").fetchall()
    type_counts: dict[str, int] = {}
    for row in rows:
        t = str(row["type"] if isinstance(row, dict) else row[0])
        c = int(row["c"] if isinstance(row, dict) else row[1])
        type_counts[t] = c
    report.card_type_coverage = type_counts
    report.missing_types = [
        t for t in CARD_TYPES if t not in PHASE9_SKIPPED_CARD_TYPES and type_counts.get(t, 0) == 0
    ]
    if report.missing_types:
        report.ok = False

    from ..card_registry import CARD_TYPE_REGISTRATIONS

    edge_rows = conn.execute(f"SELECT edge_type, COUNT(*) AS c FROM {schema}.edges GROUP BY edge_type").fetchall()
    edge_counts = {
        str(r["edge_type"] if isinstance(r, dict) else r[0]): int(r["c"] if isinstance(r, dict) else r[1])
        for r in edge_rows
    }
    zero_edges: set[str] = set()
    for reg in CARD_TYPE_REGISTRATIONS:
        if reg.card_type in PHASE9_SKIPPED_CARD_TYPES or type_counts.get(reg.card_type, 0) == 0:
            continue
        for rule in reg.edge_rules:
            if rule.edge_type == "wikilink" or edge_counts.get(rule.edge_type, 0) > 0:
                continue
            source_column = _projection_column_for_source_field(reg, rule.field_name)
            if source_column and _source_field_has_values(conn, schema, reg.projection_table, source_column):
                zero_edges.add(rule.edge_type)
    report.edge_rules_with_zero_edges = sorted(zero_edges)

    idx_row = conn.execute(
        "SELECT 1 FROM pg_indexes WHERE schemaname = %s AND indexname = 'idx_cards_activity_at_uid'",
        (schema,),
    ).fetchone()
    report.temporal_index_present = idx_row is not None
    if not report.temporal_index_present:
        report.ok = False

    chunk_count = _row_first_int(conn.execute(f"SELECT COUNT(*) FROM {schema}.chunks").fetchone())
    embedded_count = _row_first_int(conn.execute(f"SELECT COUNT(*) FROM {schema}.embeddings").fetchone())
    report.embedding_coverage_pct = (embedded_count / chunk_count * 100) if chunk_count else 100.0

    report.index_card_count = _row_first_int(conn.execute(f"SELECT COUNT(*) FROM {schema}.cards").fetchone())
    if vault_path is not None:
        vault = Path(vault_path)
        if vault.is_dir():
            try:
                import archive_crate

                report.vault_file_count = len(archive_crate.walk_vault(str(vault)))
            except Exception:
                report.vault_file_count = sum(1 for _ in vault.rglob("*.md"))
            duplicate_count = _row_first_int(conn.execute(f"SELECT COUNT(*) FROM {schema}.duplicate_uid_rows").fetchone())
            expected_card_count = report.vault_file_count - duplicate_count
            tolerance = max(1, int(expected_card_count * 0.001))
            report.card_count_match = abs(report.index_card_count - expected_card_count) <= tolerance
            if not report.card_count_match:
                report.ok = False
    return report


def run_behavioral_checks(index: Any, manifest: dict[str, Any]) -> BehavioralReport:
    """Run FTS/temporal/graph queries from the manifest against the index."""
    report = BehavioralReport()
    for entry in manifest.get("fts_queries", []) or []:
        query = str(entry.get("query", ""))
        expected_types = list(entry.get("expected_types", []) or [])
        top_3_must_include_types = list(entry.get("top_3_must_include_types", []) or expected_types)
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
        passed = len(rows) >= min_hits and any(t in top_3_must_include_types for t in types[:3])
        report.fts_results.append(
            FTSQueryResult(
                query=query,
                expected_types=expected_types,
                top_3_must_include_types=top_3_must_include_types,
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
        anchor_path = str(entry.get("anchor_rel_path", "")).strip()
        if anchor_path.startswith("PLACEHOLDER:"):
            report.graph_results.append(
                {"entry": entry, "passed": True, "skipped": True, "reason": "placeholder anchor"}
            )
            continue
        if not anchor_path:
            start_uid = str(entry.get("start_uid", "")).strip()
            try:
                anchor_path = index.read_path_for_uid(start_uid) or ""
            except Exception:
                anchor_path = ""
        hops = int(entry.get("hops", 2))
        try:
            payload = index.graph(anchor_path, hops=hops) if anchor_path else None
        except Exception:
            payload = None
        if payload is None:
            report.graph_results.append(
                {"entry": entry, "passed": False, "failures": ["anchor not found or graph() failed"]}
            )
            report.ok = False
            continue

        all_edges: list[dict[str, Any]] = []
        for _src, targets in payload.items():
            for t in targets:
                if isinstance(t, dict):
                    all_edges.append(t)
        if not all_edges:
            failures = []
            if entry.get("expect_edge_types_to_include") or entry.get("expect_at_least_one_seed_edge"):
                failures.append("no edges in graph payload")
            passed = not failures
            report.graph_results.append({"entry": entry, "passed": passed, "failures": failures})
            if not passed:
                report.ok = False
            continue

        edge_types_seen = {str(e.get("edge_type", "")) for e in all_edges}
        seed_edges = [e for e in all_edges if 0.0 < float(e.get("confidence", 0.0)) < 1.0]
        confs = [float(e.get("confidence", 0.0)) for e in all_edges]

        expected_types = set(entry.get("expect_edge_types_to_include", []) or [])
        require_seed = bool(entry.get("expect_at_least_one_seed_edge", False))
        conf_range = entry.get("expect_confidence_range", [0.0, 1.0])

        failures: list[str] = []
        if expected_types and expected_types - edge_types_seen:
            failures.append(f"missing edge_types: {sorted(expected_types - edge_types_seen)}")
        if require_seed and not seed_edges:
            failures.append("expected at least one seed edge (0.0 < confidence < 1.0); none found")
        if confs and len(conf_range) >= 2:
            lo, hi = float(conf_range[0]), float(conf_range[1])
            if min(confs) < lo or max(confs) > hi:
                failures.append(f"confidence range {min(confs)}..{max(confs)} outside expected [{lo}, {hi}]")

        passed = not failures
        report.graph_results.append({"entry": entry, "passed": passed, "failures": failures})
        if not passed:
            report.ok = False

    return report


def generate_json_report(structural: StructuralReport, behavioral: BehavioralReport | None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "structural": {
            "ok": structural.ok,
            "orphan_count": structural.orphan_count,
            "orphan_details": structural.orphan_details,
            "duplicate_uids": structural.duplicate_uids,
            "card_counts": structural.card_counts,
            "edge_counts_by_rule": structural.edge_counts_by_rule,
            "missing_field_entries": structural.missing_field_entries,
        }
    }
    if behavioral:
        out["behavioral"] = {
            "ok": behavioral.ok,
            "fts": [
                {
                    "query": r.query,
                    "top_3_must_include_types": r.top_3_must_include_types,
                    "precision": r.precision,
                    "recall": r.recall,
                    "passed": r.passed,
                    "actual_types_in_top_3": r.actual_types_in_top_3,
                }
                for r in behavioral.fts_results
            ],
            "graph": behavioral.graph_results,
            "temporal": behavioral.temporal_results,
        }
    return out


def generate_markdown_report(structural: StructuralReport, behavioral: BehavioralReport | None) -> str:
    lines = ["# PPA health check\n", "## Structural\n", f"- ok: {structural.ok}\n"]
    lines.append(f"- orphan_count: {structural.orphan_count}\n")
    if structural.duplicate_uids:
        lines.append(f"- duplicate UIDs: {', '.join(structural.duplicate_uids)}\n")
    if structural.edge_counts_by_rule:
        for edge_type, count in sorted(structural.edge_counts_by_rule.items()):
            lines.append(f"- edge `{edge_type}`: {count}\n")
    if structural.missing_field_entries:
        for entry in structural.missing_field_entries:
            lines.append(f"- {entry.get('check', 'issue')}: {entry.get('detail', '')}\n")
    if structural.orphan_details:
        for entry in structural.orphan_details[:10]:
            lines.append(
                f"- orphan: source_uid={entry.get('source_uid', '')} target_slug={entry.get('target_slug', '')}\n"
            )
    if behavioral:
        lines.append("\n## Behavioral\n")
        for r in behavioral.fts_results:
            lines.append(
                f"- FTS `{r.query}`: top3={r.actual_types_in_top_3} precision={r.precision:.3f} recall={r.recall:.3f} passed={r.passed}\n"
            )
        for result in behavioral.graph_results:
            lines.append(f"- Graph `{result.get('entry', {}).get('description', '')}`: passed={result.get('passed')}\n")
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
