"""Tests for confidence computation and gap detection."""

from __future__ import annotations

import logging
from unittest import mock

import pytest
from psycopg import errors as pg_errors

from archive_cli.commands.confidence import ConfidenceLevel, GapEntry, compute_confidence, detect_gaps, log_gaps
from archive_cli.index_query import QueryMixin


def test_confidence_high_many_results() -> None:
    assert compute_confidence(result_count=11) == ConfidenceLevel.HIGH


def test_confidence_high_exact_match() -> None:
    assert compute_confidence(result_count=0, exact_match=True) == ConfidenceLevel.HIGH


def test_confidence_medium_moderate_results() -> None:
    assert compute_confidence(result_count=5) == ConfidenceLevel.MEDIUM


def test_confidence_low_sparse_results() -> None:
    assert compute_confidence(result_count=2) == ConfidenceLevel.LOW


def test_confidence_low_no_results() -> None:
    assert compute_confidence(result_count=0) == ConfidenceLevel.LOW


def test_detect_gaps_no_results() -> None:
    gaps = detect_gaps(query_text="q", result_count=0)
    assert len(gaps) == 1
    assert gaps[0].gap_type == "no_results"


def test_detect_gaps_sparse() -> None:
    gaps = detect_gaps(query_text="q", result_count=1)
    assert any(g.gap_type == "sparse_results" for g in gaps)


def test_detect_gaps_type_mismatch() -> None:
    gaps = detect_gaps(
        query_text="q",
        result_count=5,
        expected_types=["meal_order", "ride"],
        actual_types=["meal_order"],
    )
    tm = [g for g in gaps if g.gap_type == "type_mismatch"]
    assert len(tm) == 1
    assert "ride" in tm[0].detail


def test_detect_gaps_none_when_healthy() -> None:
    gaps = detect_gaps(
        query_text="q",
        result_count=15,
        expected_types=["meal_order"],
        actual_types=["meal_order"],
    )
    assert gaps == []


def test_log_gaps_writes_to_db() -> None:
    idx = mock.MagicMock()
    log_gaps(
        [GapEntry(query_text="x", gap_type="no_results")],
        index=idx,
        logger=logging.getLogger("t"),
    )
    idx.log_retrieval_gaps.assert_called_once()
    args = idx.log_retrieval_gaps.call_args[0][0]
    assert args[0]["query_text"] == "x"
    assert args[0]["gap_type"] == "no_results"


def test_log_retrieval_gaps_undefined_table_no_crash(caplog: pytest.LogCaptureFixture) -> None:
    class _Conn:
        def execute(self, *a, **k):
            raise pg_errors.UndefinedTable("retrieval_gaps")

        def commit(self) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Idx:
        schema = "ppa"

        def _connect(self):
            return _Conn()

    caplog.set_level(logging.WARNING)
    QueryMixin.log_retrieval_gaps(
        _Idx(),
        [{"query_text": "q", "gap_type": "no_results", "detail": "", "card_uid": ""}],
    )
    assert any("gap logging skipped" in r.message for r in caplog.records)


def test_log_retrieval_gaps_re_raises_other_errors() -> None:
    class _Conn:
        def execute(self, *a, **k):
            raise RuntimeError("boom")

        def commit(self) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Idx:
        schema = "ppa"

        def _connect(self):
            return _Conn()

    with pytest.raises(RuntimeError, match="boom"):
        QueryMixin.log_retrieval_gaps(
            _Idx(),
            [{"query_text": "q", "gap_type": "no_results"}],
        )
