"""P01-B2: current_ops decay is query-aware and never deletes history."""

from __future__ import annotations

from datetime import datetime, timezone

from archive_cli.rank_fusion import (
    FusionOptions,
    age_decay_factor,
    query_has_historical_dates,
    query_requests_current_ops,
)
from archive_cli.retrieval_pipeline import HybridFetchInputs, fuse_and_rank_hybrid

NOW = datetime(2026, 9, 6, tzinfo=timezone.utc)


def _hit(uid: str, *, activity: str, score: float = 0.8) -> dict:
    return {
        "card_uid": uid,
        "rel_path": f"{uid}.md",
        "summary": uid,
        "type": "email_thread",
        "activity_at": activity,
        "slug_exact": 0,
        "summary_exact": 0,
        "external_id_exact": 0,
        "person_exact": 0,
        "lexical_score": score,
        "corpus_state": "active",
    }


def test_age_decay_is_elapsed_time_not_percentile():
    recent = age_decay_factor("2026-09-01", half_life_days=30, now=NOW)
    old = age_decay_factor("2024-01-01", half_life_days=30, now=NOW)
    assert 0 < old < recent <= 1.0


def test_current_ops_promotes_fresh_hits():
    rows = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=[
                _hit("old", activity="2024-01-01", score=0.81),
                _hit("fresh", activity="2026-09-01", score=0.80),
            ],
            vector_rows=[],
            neighbor_trust={},
            query_cleaned="latest deploy status",
        ),
        final_limit=5,
        fusion=FusionOptions(ranking_profile="current_ops", now=NOW, historical_query=False),
    )
    assert rows[0]["card_uid"] == "fresh"
    assert rows[0]["age_decay"] > rows[1]["age_decay"]
    assert {row["card_uid"] for row in rows} == {"old", "fresh"}


def test_historical_date_disables_decay():
    assert query_has_historical_dates("what happened in 2024")
    rows = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=[
                _hit("old", activity="2024-01-01", score=0.81),
                _hit("fresh", activity="2026-09-01", score=0.80),
            ],
            vector_rows=[],
            neighbor_trust={},
            query_cleaned="board dinner 2024-01-01",
        ),
        final_limit=5,
        fusion=FusionOptions(ranking_profile="current_ops", now=NOW, historical_query=True),
    )
    assert rows[0]["card_uid"] == "old"
    assert all(row["age_decay"] == 1.0 for row in rows)


def test_exact_id_disables_decay():
    rows = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=[
                {
                    **_hit("exact", activity="2020-01-01", score=0.1),
                    "slug_exact": 1,
                },
                _hit("fresh", activity="2026-09-01", score=0.9),
            ],
            vector_rows=[],
            neighbor_trust={},
            query_cleaned="latest exact",
        ),
        final_limit=5,
        fusion=FusionOptions(ranking_profile="current_ops", now=NOW, exact_ids_present=True),
    )
    assert rows[0]["card_uid"] == "exact"
    assert all(row["age_decay"] == 1.0 for row in rows)


def test_default_profile_does_not_decay():
    assert not query_requests_current_ops("email from Jane")
    rows = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=[
                _hit("old", activity="2020-01-01", score=0.9),
                _hit("fresh", activity="2026-09-01", score=0.2),
            ],
            vector_rows=[],
            neighbor_trust={},
            query_cleaned="email from Jane",
        ),
        final_limit=5,
        fusion=FusionOptions(ranking_profile="default", now=NOW),
    )
    assert rows[0]["card_uid"] == "old"
    assert all(row["age_decay"] == 1.0 for row in rows)
