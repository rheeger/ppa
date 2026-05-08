"""Tests for Phase 9 latency check definitions."""

from archive_cli.commands.latency_check import (LATENCY_TARGETS_MS,
                                                LatencyResult)


def test_all_query_types_have_targets() -> None:
    assert set(LATENCY_TARGETS_MS) == {
        "fts_search",
        "temporal_neighbors",
        "hybrid_search",
        "type_filter_query",
    }


def test_no_knowledge_read_target() -> None:
    assert "knowledge_read" not in LATENCY_TARGETS_MS


def test_latency_result_pass_when_under_target() -> None:
    r = LatencyResult("fts_search", 2000, 1500, True, "test")
    assert r.passed


def test_latency_result_fail_when_over_target() -> None:
    r = LatencyResult("fts_search", 2000, 2500, False, "test")
    assert not r.passed
