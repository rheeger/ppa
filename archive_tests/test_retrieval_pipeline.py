"""Unit tests for staged retrieval, query planner, and rerank blending."""

from __future__ import annotations

from archive_cli.query_planner import DeterministicQueryPlanner, build_query_plan, effective_filters_from_plan
from archive_cli.reranker import HeuristicReranker, RerankResult, blend_rerank_scores
from archive_cli.retrieval_pipeline import (
    HybridFetchInputs,
    fuse_and_rank_hybrid,
    merge_lexical_rows,
    score_breakdown_for_row,
)


def test_merge_lexical_rows_keeps_best_score():
    a = [
        {
            "card_uid": "u1",
            "lexical_score": 0.5,
            "rel_path": "a.md",
            "summary": "x",
            "type": "person",
            "slug_exact": 0,
            "summary_exact": 0,
            "external_id_exact": 0,
            "person_exact": 0,
        }
    ]
    b = [
        {
            "card_uid": "u1",
            "lexical_score": 0.9,
            "rel_path": "a.md",
            "summary": "x",
            "type": "person",
            "slug_exact": 1,
            "summary_exact": 0,
            "external_id_exact": 0,
            "person_exact": 0,
        }
    ]
    m = merge_lexical_rows(a, b)
    assert len(m) == 1
    assert m[0]["lexical_score"] == 0.9


def test_hybrid_pipeline_explain_includes_score_components():
    lexical = [
        {
            "card_uid": "u1",
            "rel_path": "a.md",
            "summary": "Hello",
            "type": "person",
            "activity_at": "2026-01-01",
            "slug_exact": 1,
            "summary_exact": 0,
            "external_id_exact": 0,
            "person_exact": 0,
            "lexical_score": 0.5,
        }
    ]
    vector = [
        {
            "card_uid": "u1",
            "rel_path": "a.md",
            "summary": "Hello",
            "type": "person",
            "activity_at": "2026-01-01",
            "similarity": 0.5,
            "preview": "Hello",
            "chunk_type": "body",
            "chunk_index": 0,
            "matched_chunk_count": 1,
            "provenance_bias": "deterministic",
            "provenance_score": 0.08,
            "matched_by": "vector",
            "score": 0.0,
            "graph_hops": "",
        }
    ]
    meta: dict = {}
    rows = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=lexical,
            vector_rows=vector,
            neighbor_trust={},
            query_cleaned="hello",
            subqueries_used=("hello",),
        ),
        final_limit=5,
        pipeline_meta=meta,
    )
    assert rows
    bd = score_breakdown_for_row(rows[0])
    assert "lexical_component" in bd
    assert "vector_component" in bd
    assert "exact_boost" in bd
    assert meta.get("pipeline_version")


def test_query_planner_extracts_filters_from_archive_language():
    p = DeterministicQueryPlanner().plan("gmail thread about meeting in 2024")
    assert "gmail" in p.inferred.source_hints
    assert "email_thread" in p.inferred.type_hints or "meeting_transcript" in p.inferred.type_hints
    t, s, sd, ed = effective_filters_from_plan(
        p, type_filter="", source_filter="", start_date="", end_date="", allow_merge=True
    )
    assert "gmail" in s or s == "gmail"
    assert "2024" in sd


def test_query_planner_variants_are_visible_in_subqueries():
    cfg = {"query_planner": {"enabled": True, "max_variants": 2, "allow_filter_inference": True}}
    plan = build_query_plan('board "Jane Smith"', config=cfg)
    texts = [q.text for q in plan.queries]
    assert any("Jane Smith" in t for t in texts)


def test_reranker_improves_semantic_candidate_ordering():
    rows = [
        {"card_uid": "a", "summary": "unrelated topic", "preview": "foo", "type": "person"},
        {"card_uid": "b", "summary": "Endaoment donor operations", "preview": "philanthropy donors", "type": "person"},
    ]
    rr = HeuristicReranker().rerank("endaoment donor", rows)
    by_uid = {r.card_uid: r.score for r in rr}
    assert by_uid["b"] > by_uid["a"]


def test_reranker_does_not_bury_exact_match_hits():
    rows = [
        {
            "card_uid": "a",
            "score": 10.0,
            "exact_match": True,
            "vector_similarity": 0.1,
            "lexical_score": 0.5,
            "rel_path": "a.md",
        },
        {
            "card_uid": "b",
            "score": 8.0,
            "exact_match": False,
            "vector_similarity": 0.9,
            "lexical_score": 0.1,
            "rel_path": "b.md",
        },
    ]
    rr = {"a": RerankResult(card_uid="a", score=0.0), "b": RerankResult(card_uid="b", score=1.0)}
    out = blend_rerank_scores(rows, rr, preserve_exact_match_floor=True)
    assert out[0]["card_uid"] == "a"


def test_lexical_sql_contains_no_literal_brace_schema():
    """The SQL built by _lexical_candidates must not contain un-interpolated {self.schema}."""
    from unittest.mock import MagicMock

    from archive_cli.index_query import QueryMixin

    captured_sql: list[str] = []

    class FakeCursor:
        def fetchall(self):
            return []

    class FakeConn:
        def execute(self, sql, params=None):
            captured_sql.append(sql)
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

    mixin = MagicMock(spec=QueryMixin)
    mixin.schema = "test_schema"
    mixin._connect = MagicMock(return_value=FakeConn())
    mixin._filter_clauses = QueryMixin._filter_clauses.__get__(mixin)
    mixin._merge_lexical_uid_rows = QueryMixin._merge_lexical_uid_rows
    mixin._lexical_row_sort_key = QueryMixin._lexical_row_sort_key

    QueryMixin._lexical_candidates(mixin, query="hello", limit=10)

    assert captured_sql, "No SQL was captured"
    for sql in captured_sql:
        assert "{self.schema}" not in sql, f"Literal '{{self.schema}}' found in SQL: {sql[:200]}"
        assert "{self" not in sql, f"Literal '{{self' found in SQL: {sql[:200]}"
        assert "test_schema" in sql, f"Schema not interpolated in SQL: {sql[:200]}"


def test_fetch_hybrid_propagates_lexical_error():
    """A lexical retrieval error must propagate out of fetch_hybrid_lexical_vector."""
    from unittest.mock import MagicMock

    from archive_cli.index_query import QueryMixin

    mixin = MagicMock(spec=QueryMixin)
    mixin.schema = "test_schema"
    mixin.ensure_ready = MagicMock()
    mixin._lexical_candidates = MagicMock(side_effect=RuntimeError("lexical boom"))
    mixin._aggregate_vector_candidates = MagicMock(return_value=[])
    mixin._vector_candidate_rows = MagicMock(return_value=[])

    try:
        QueryMixin.fetch_hybrid_lexical_vector(
            mixin,
            query="test",
            query_vector=[0.1] * 8,
            embedding_model="m",
            embedding_version=1,
            candidate_limit=5,
        )
        raise AssertionError("Should have raised")
    except RuntimeError as exc:
        assert "lexical boom" in str(exc)


def test_fetch_hybrid_propagates_vector_error():
    """A vector retrieval error must propagate out of fetch_hybrid_lexical_vector."""
    from unittest.mock import MagicMock

    from archive_cli.index_query import QueryMixin

    mixin = MagicMock(spec=QueryMixin)
    mixin.schema = "test_schema"
    mixin.ensure_ready = MagicMock()
    mixin._lexical_candidates = MagicMock(return_value=[])

    def _bad_connect():
        raise RuntimeError("vector boom")

    mixin._connect = _bad_connect

    try:
        QueryMixin.fetch_hybrid_lexical_vector(
            mixin,
            query="test",
            query_vector=[0.1] * 8,
            embedding_model="m",
            embedding_version=1,
            candidate_limit=5,
        )
        raise AssertionError("Should have raised")
    except RuntimeError as exc:
        assert "vector boom" in str(exc)


def test_card_type_priors_covers_all_registered_types():
    """Every type in CARD_TYPES has an entry in CARD_TYPE_PRIORS."""
    from archive_cli.index_config import CARD_TYPE_PRIORS
    from archive_vault.schema import CARD_TYPES

    missing = set(CARD_TYPES.keys()) - set(CARD_TYPE_PRIORS.keys())
    assert not missing, (
        f"CARD_TYPE_PRIORS is missing entries for: {sorted(missing)}. "
        f"Add priors so these types rank correctly in hybrid search."
    )


def test_card_type_priors_values_are_in_valid_range():
    """All CARD_TYPE_PRIORS values are between 0.0 and 1.0 exclusive."""
    from archive_cli.index_config import CARD_TYPE_PRIORS

    for card_type, prior in CARD_TYPE_PRIORS.items():
        assert 0.0 < prior < 1.0, (
            f"CARD_TYPE_PRIORS['{card_type}'] = {prior} is out of range (0, 1)"
        )
