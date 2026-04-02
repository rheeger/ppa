"""Health-check command tests."""

from __future__ import annotations

from archive_mcp.commands.health_check import (
    BehavioralReport,
    FTSQueryResult,
    StructuralReport,
    generate_json_report,
    generate_markdown_report,
    run_behavioral_checks,
    run_structural_checks,
)


def test_report_json_format() -> None:
    s = StructuralReport(ok=True)
    b = BehavioralReport(
        ok=True,
        fts_results=[
            FTSQueryResult(
                query="q",
                expected_types=["person"],
                top_3_must_include_types=["person"],
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


class _FakeRows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, responses):
        self._responses = responses

    def execute(self, query: str, params=None):
        query_text = " ".join(query.split())
        for key, rows in self._responses.items():
            if key in query_text:
                if callable(rows):
                    return _FakeRows(rows(query_text, params))
                return _FakeRows(rows)
        raise AssertionError(f"Unexpected query: {query_text}")


def test_structural_orphan_wikilink_detected() -> None:
    conn = _FakeConn(
        {
            "GROUP BY uid HAVING COUNT(*) > 1": [],
            "SELECT type, COUNT(*) AS c FROM test.cards GROUP BY type ORDER BY type": [("email_message", 1)],
            "FROM test.edges e": [("u1", "missing-card")],
            "FROM test.card_people cp": [],
            "SELECT COUNT(*) FROM test.edges WHERE edge_type = %s": [(1,)],
            "SELECT COUNT(*) FROM test.cards WHERE summary IS NULL OR summary = ''": [(0,)],
            "SELECT COUNT(*) FROM test.cards WHERE activity_at IS NULL OR activity_at = ''": [(0,)],
        }
    )
    report = run_structural_checks(
        conn,
        "test",
        {
            "structural_invariants": {
                "zero_orphaned_wikilinks": True,
            }
        },
    )
    assert report.ok is False
    assert report.orphan_count == 1
    assert report.orphan_details[0]["target_slug"] == "missing-card"


def test_structural_edge_rules_fire() -> None:
    def count_rows(query_text: str, params):
        edge_type = params[0]
        return [(1 if edge_type in {"thread_has_message", "message_has_attachment"} else 0,)]

    conn = _FakeConn(
        {
            "GROUP BY uid HAVING COUNT(*) > 1": [],
            "SELECT type, COUNT(*) AS c FROM test.cards GROUP BY type ORDER BY type": [
                ("email_thread", 1),
                ("email_message", 1),
            ],
            "FROM test.edges e": [],
            "FROM test.card_people cp": [],
            "SELECT COUNT(*) FROM test.edges WHERE edge_type = %s": count_rows,
            "SELECT COUNT(*) FROM test.cards WHERE summary IS NULL OR summary = ''": [(0,)],
            "SELECT COUNT(*) FROM test.cards WHERE activity_at IS NULL OR activity_at = ''": [(0,)],
        }
    )
    report = run_structural_checks(
        conn,
        "test",
        {
            "structural_invariants": {
                "all_edge_rules_fire": True,
            }
        },
    )
    assert report.edge_counts_by_rule
    assert report.ok is False


def test_structural_all_cards_have_summary() -> None:
    conn = _FakeConn(
        {
            "GROUP BY uid HAVING COUNT(*) > 1": [],
            "SELECT type, COUNT(*) AS c FROM test.cards GROUP BY type ORDER BY type": [("email_message", 1)],
            "FROM test.edges e": [],
            "FROM test.card_people cp": [],
            "SELECT COUNT(*) FROM test.edges WHERE edge_type = %s": [(1,)],
            "SELECT COUNT(*) FROM test.cards WHERE summary IS NULL OR summary = ''": [(2,)],
            "SELECT COUNT(*) FROM test.cards WHERE activity_at IS NULL OR activity_at = ''": [(0,)],
        }
    )
    report = run_structural_checks(
        conn,
        "test",
        {
            "structural_invariants": {
                "all_cards_have_summary": True,
            }
        },
    )
    assert report.ok is False
    assert any(item["check"] == "cards_missing_summary" for item in report.missing_field_entries)
