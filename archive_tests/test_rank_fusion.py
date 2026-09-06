"""P01-B2: RRF is scale-stable, protects exact IDs, and diversifies threads."""

from __future__ import annotations

from pathlib import Path

from archive_cli.index_config import CHUNK_SCHEMA_VERSION, DEFAULT_RARE_TOKEN_WEIGHT
from archive_cli.rank_fusion import (
    FUSION_STRATEGY,
    FusionOptions,
    apply_thread_diversity,
    mrr,
    ndcg_at_k,
    parent_thread_of,
    recall_at_k,
)
from archive_cli.retrieval_pipeline import HybridFetchInputs, fuse_and_rank_hybrid, score_breakdown_for_row

REPO = Path(__file__).resolve().parents[1]


def _lex(uid: str, score: float, *, exact: bool = False, thread: str = "", activity: str = "2026-03-10") -> dict:
    return {
        "card_uid": uid,
        "rel_path": f"{uid}.md",
        "summary": uid,
        "type": "email_message" if thread else "email_thread",
        "activity_at": activity,
        "slug_exact": int(exact),
        "summary_exact": 0,
        "external_id_exact": 0,
        "person_exact": 0,
        "lexical_score": score,
        "corpus_state": "active",
        "thread": thread,
        "parent_thread": thread,
    }


def _vec(uid: str, sim: float, *, thread: str = "") -> dict:
    return {
        "card_uid": uid,
        "rel_path": f"{uid}.md",
        "summary": uid,
        "type": "email_message" if thread else "email_thread",
        "activity_at": "2026-03-10",
        "similarity": sim,
        "preview": uid,
        "chunk_type": "body",
        "chunk_index": 0,
        "matched_chunk_count": 1,
        "provenance_bias": "mixed",
        "provenance_score": 0.04,
        "thread": thread,
        "parent_thread": thread,
    }


def test_rrf_stable_across_raw_score_scales():
    small = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=[_lex("rel", 0.9), _lex("other", 0.2)],
            vector_rows=[_vec("rel", 0.8), _vec("other", 0.1)],
            neighbor_trust={},
            query_cleaned="q",
        ),
        final_limit=5,
    )
    huge = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=[_lex("rel", 9000.0), _lex("other", 2000.0)],
            vector_rows=[_vec("rel", 0.8), _vec("other", 0.1)],
            neighbor_trust={},
            query_cleaned="q",
        ),
        final_limit=5,
    )
    assert [row["card_uid"] for row in small] == [row["card_uid"] for row in huge]
    assert small[0]["card_uid"] == "rel"
    assert small[0]["match_channel"]


def test_exact_identifier_stays_first():
    rows = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=[_lex("exact", 0.05, exact=True), _lex("semantic", 9.0)],
            vector_rows=[_vec("semantic", 0.99)],
            neighbor_trust={},
            query_cleaned="exact-id",
        ),
        final_limit=5,
    )
    assert rows[0]["card_uid"] == "exact"
    assert rows[0]["exact_match"] is True
    assert "exact" in rows[0]["match_channel"]


def test_diversity_caps_non_exact_hits_per_thread():
    lexical = [_lex(f"t1-{index}", 1.0 - (index * 0.01), thread="thread-a") for index in range(6)]
    for label, base in (("thread-b", 0.40), ("thread-c", 0.30), ("thread-d", 0.20), ("thread-e", 0.10)):
        lexical.append(_lex(f"{label}-0", base, thread=label))
        lexical.append(_lex(f"{label}-1", base - 0.01, thread=label))
    rows = fuse_and_rank_hybrid(
        HybridFetchInputs(lexical_rows=lexical, vector_rows=[], neighbor_trust={}, query_cleaned="q"),
        final_limit=10,
        fusion=FusionOptions(diversity_cap=2, diversity_window=10),
    )
    top = rows[:10]
    thread_a = [row for row in top if parent_thread_of(row) == "thread-a" and not row["exact_match"]]
    thread_b = [row for row in top if parent_thread_of(row) == "thread-b"]
    assert len(thread_a) == 2
    assert thread_b, "diversity should surface the second thread in the top ten"


def test_diversity_bypasses_exact_and_multi_event():
    lexical = [
        _lex("evt-1", 0.9, thread="same"),
        _lex("evt-2", 0.8, thread="same"),
        _lex("evt-3", 0.7, thread="same"),
    ]
    for row in lexical:
        row["type"] = "calendar_event"
    rows = fuse_and_rank_hybrid(
        HybridFetchInputs(lexical_rows=lexical, vector_rows=[], neighbor_trust={}, query_cleaned="q"),
        final_limit=10,
        fusion=FusionOptions(diversity_cap=2, diversity_window=10, multi_event=True),
    )
    assert [row["card_uid"] for row in rows] == ["evt-1", "evt-2", "evt-3"]
    bypassed = apply_thread_diversity(
        [{"card_uid": "e", "exact_match": True, "type": "email_message", "parent_thread": "t"}] * 3,
        FusionOptions(diversity_cap=2, multi_event=False),
    )
    assert len(bypassed) == 3


def test_rare_token_weight_defaults_off():
    assert DEFAULT_RARE_TOKEN_WEIGHT == 0.0
    rows = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=[_lex("a", 5.0), _lex("b", 1.0)],
            vector_rows=[],
            neighbor_trust={},
            query_cleaned="q",
        ),
        final_limit=5,
    )
    assert all(row["rare_token_boost"] == 0.0 for row in rows)
    breakdown = score_breakdown_for_row(rows[0])
    assert breakdown["rare_token_boost"] == 0.0
    assert breakdown["lexical_rarity"] == 5.0


def test_fusion_strategy_and_format_v2_untouched():
    schema = (REPO / "archive_crate/src/serving_index/schema.rs").read_text(encoding="utf-8")
    freeze = (REPO / "archive_tests/acceptance/data/p01b_format_freeze.json").read_text(encoding="utf-8")
    assert FUSION_STRATEGY == "rrf_k60"
    assert CHUNK_SCHEMA_VERSION == 6
    assert "pub const SERVING_INDEX_FORMAT_VERSION: u32 = 2;" in schema
    assert 'pub const VECTOR_IMPL: &str = "ivf_centroids_v2";' in schema
    assert '"serving_index_format_version": 2' in freeze


def test_ablation_rrf_beats_raw_blend_on_scaled_channels():
    relevant = {"rel"}
    lexical = [_lex("rel", 0.2), _lex(" distractor", 9.0)]
    lexical[1]["card_uid"] = "distractor"
    lexical[1]["rel_path"] = "distractor.md"
    vector = [_vec("rel", 0.95), _vec("distractor", 0.01)]
    rrf = fuse_and_rank_hybrid(
        HybridFetchInputs(lexical_rows=lexical, vector_rows=vector, neighbor_trust={}, query_cleaned="q"),
        final_limit=5,
    )
    raw_order = ["distractor", "rel"]
    rrf_ids = [row["card_uid"] for row in rrf]
    assert ndcg_at_k(rrf_ids, relevant, k=2) >= ndcg_at_k(raw_order, relevant, k=2)
    assert mrr(rrf_ids, relevant) >= mrr(raw_order, relevant)
    assert recall_at_k(rrf_ids, relevant, k=1) == 1.0
