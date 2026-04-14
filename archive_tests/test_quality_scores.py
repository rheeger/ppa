"""Phase 1: quality scoring during materialization."""

from __future__ import annotations

from archive_cli.materializer import _compute_quality_score


def test_meal_order_rich_vs_sparse():
    rich_fm = {"items": [{"n": 1}], "restaurant": "X", "total": 9.99}
    s_rich, f_rich = _compute_quality_score("meal_order", rich_fm, body="x" * 100, summary="ok")
    sparse_fm: dict = {}
    s_sparse, f_sparse = _compute_quality_score("meal_order", sparse_fm, body="", summary="")
    assert s_rich > s_sparse
    assert any("missing:" in x for x in f_sparse)
