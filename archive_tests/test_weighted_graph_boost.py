"""Tests for Tier 2: confidence-weighted graph_boost in hybrid fusion."""

from __future__ import annotations

import pytest

from archive_cli.retrieval_pipeline import HybridFetchInputs, fuse_and_rank_hybrid, score_breakdown_for_row


def _vector_row(card_uid: str, rel_path: str) -> dict:
    return {
        "card_uid": card_uid,
        "rel_path": rel_path,
        "type": "person",
        "summary": "x",
        "activity_at": "2026-01-01",
        "similarity": 0.6,
        "preview": "x",
        "chunk_type": "body",
        "chunk_index": 0,
        "matched_chunk_count": 1,
        "provenance_bias": "mixed",
        "provenance_score": 0.04,
        "matched_by": "vector",
        "score": 0.0,
        "graph_hops": "",
    }


def _build_inputs(*, lexical_rows, vector_rows, neighbor_trust):
    return HybridFetchInputs(
        lexical_rows=lexical_rows,
        vector_rows=vector_rows,
        neighbor_trust=neighbor_trust,
        query_cleaned="q",
        subqueries_used=("q",),
    )


class TestWeightedGraphBoost:
    def test_full_trust_neighbor_full_boost(self):
        vector_rows = [_vector_row("uid-neighbor", "n.md")]
        inputs = _build_inputs(
            lexical_rows=[],
            vector_rows=vector_rows,
            neighbor_trust={"uid-neighbor": 1.0},
        )
        out = fuse_and_rank_hybrid(inputs, final_limit=10)
        row = next(r for r in out if r["card_uid"] == "uid-neighbor")
        assert row["graph_hops"] == "1"
        assert row["graph_neighbor_trust"] == pytest.approx(1.0)
        assert score_breakdown_for_row(row)["graph_boost"] == pytest.approx(0.22)

    def test_half_trust_neighbor_half_boost(self):
        inputs = _build_inputs(
            lexical_rows=[],
            vector_rows=[_vector_row("uid-n", "n.md")],
            neighbor_trust={"uid-n": 0.5},
        )
        out = fuse_and_rank_hybrid(inputs, final_limit=10)
        row = next(r for r in out if r["card_uid"] == "uid-n")
        assert score_breakdown_for_row(row)["graph_boost"] == pytest.approx(0.11)

    def test_zero_trust_neighbor_no_boost(self):
        inputs = _build_inputs(
            lexical_rows=[],
            vector_rows=[_vector_row("uid-x", "x.md")],
            neighbor_trust={},
        )
        out = fuse_and_rank_hybrid(inputs, final_limit=10)
        row = next(r for r in out if r["card_uid"] == "uid-x")
        assert row.get("graph_hops", "") == ""
        assert score_breakdown_for_row(row)["graph_boost"] == 0.0

    def test_breakdown_exposes_trust_weight(self):
        row = {
            "graph_hops": "1",
            "graph_neighbor_trust": 0.78,
            "exact_match": False,
            "lexical_score": 0.5,
            "vector_similarity": 0.6,
            "matched_by": "hybrid",
            "type": "person",
            "recency_score": 0.0,
            "provenance_score": 0.0,
        }
        bd = score_breakdown_for_row(row)
        assert bd["graph_boost"] == pytest.approx(0.22 * 0.78, rel=1e-6)
        assert bd["graph_neighbor_trust"] == pytest.approx(0.78)


def test_component_weights_sum_to_one():
    assert 0.45 + 0.12 + 0.13 + 0.18 + 0.12 == pytest.approx(1.00, rel=1e-9)
