"""Health-check command tests."""

from __future__ import annotations

from archive_mcp.commands.health_check import (
    BehavioralReport,
    FTSQueryResult,
    StructuralReport,
    generate_json_report,
    generate_markdown_report,
    run_behavioral_checks,
)


def test_report_json_format() -> None:
    s = StructuralReport(ok=True)
    b = BehavioralReport(
        ok=True,
        fts_results=[
            FTSQueryResult(
                query="q",
                expected_types=["person"],
                min_hits=1,
                actual_hits=2,
                actual_types_in_top_3=["person", "person"],
                precision=1.0,
                recall=1.0,
            )
        ],
    )
    payload = generate_json_report(s, b)
    assert "structural" in payload and "behavioral" in payload


def test_report_markdown_has_sections() -> None:
    s = StructuralReport(ok=True)
    b = BehavioralReport(ok=True, fts_results=[])
    md = generate_markdown_report(s, b)
    assert "Structural" in md


def test_behavioral_fts_precision_recall() -> None:
    class Idx:
        def search(self, query: str, limit: int = 20):
            return [{"type": "person", "uid": "u1"}, {"type": "document", "uid": "u2"}]

    manifest = {
        "fts_queries": [
            {
                "query": "jane",
                "expected_types": ["person"],
                "min_hits": 1,
            }
        ]
    }
    rep = run_behavioral_checks(Idx(), manifest)
    assert rep.fts_results
    assert rep.fts_results[0].precision == 0.5
    assert rep.fts_results[0].recall >= 0.0
