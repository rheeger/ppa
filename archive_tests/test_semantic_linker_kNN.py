"""Tests for semantic linker (Tier 3) constants and kNN helper.

Phase 6 Tier 3 is RETIRED (see archive_docs/runbooks/phase6-retirement-rationale.md).
MODULE_SEMANTIC code is kept for reference but is no longer wired into
CARD_TYPE_MODULES, so no `semanticLinker` jobs are emitted by the enqueue
pipeline. These tests assert the retired state.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.seed_links import (
    CARD_TYPE_MODULES,
    LINK_SURFACE_BY_TYPE,
    LINK_TYPE_SEMANTICALLY_RELATED,
    LLM_REVIEW_MODULES,
    MODULE_SEMANTIC,
    SEED_LINK_POLICY_VERSION,
    SeedCardSketch,
    SeedLinkCandidate,
    SeedLinkCatalog,
    _component_scores,
    _generate_semantic_candidates,
)
from archive_vault.schema import CARD_TYPES


def test_module_semantic_is_retired_from_dispatch():
    """MODULE_SEMANTIC is intentionally absent from every card type's tuple so no
    `semanticLinker` jobs are enqueued. The constant and implementation survive
    in the module for reference and to support an optional opt-in revival."""
    assert len(CARD_TYPE_MODULES) == len(CARD_TYPES), "every card type has a tuple"
    assert not any(MODULE_SEMANTIC in mods for mods in CARD_TYPE_MODULES.values()), (
        "MODULE_SEMANTIC must be retired from CARD_TYPE_MODULES; see "
        "archive_docs/runbooks/phase6-retirement-rationale.md"
    )
    assert MODULE_SEMANTIC not in LLM_REVIEW_MODULES


def test_policy_version_bumped():
    # v1-v4 = earlier calibration milestones (see git log for trail).
    # v5 = Phase 6 Tier 4 (2026-04-19) — type-allowlist + summary-template filter +
    #      same-type restriction + dual-tier formula (strict for same-type, lenient
    #      for cross-type). The formula code remains in evaluate_seed_link_candidate
    #      for reference, but no jobs are emitted for it.
    assert SEED_LINK_POLICY_VERSION >= 5


def test_link_surface_semantic_policy_preserved():
    """The policy entry survives so the link type is known to the review surface
    (useful if anyone opts MODULE_SEMANTIC back in for a one-off analysis)."""
    p = LINK_SURFACE_BY_TYPE[LINK_TYPE_SEMANTICALLY_RELATED]
    assert p.module_name == MODULE_SEMANTIC
    assert p.auto_promote_floor >= 0.50
    assert p.auto_review_floor >= 0.40


def test_component_scores_semantic_embedding():
    c = SeedLinkCandidate(
        module_name=MODULE_SEMANTIC,
        source_card_uid="a",
        source_rel_path="a.md",
        target_card_uid="b",
        target_rel_path="b.md",
        target_kind="card",
        proposed_link_type=LINK_TYPE_SEMANTICALLY_RELATED,
        candidate_group="",
        input_hash="x",
        evidence_hash="y",
        features={"embedding_similarity": 0.85, "deterministic_hits": [], "ambiguous_target_count": 0},
        evidences=[],
        surface="derived_only",
        promotion_target="derived_edge",
    )
    det, lex, graph, emb, risk = _component_scores(c)
    assert emb == pytest.approx(0.85)
    assert det == 0.0
    assert risk == 0.0

    c_low = SeedLinkCandidate(
        module_name=MODULE_SEMANTIC,
        source_card_uid="a",
        source_rel_path="a.md",
        target_card_uid="b",
        target_rel_path="b.md",
        target_kind="card",
        proposed_link_type=LINK_TYPE_SEMANTICALLY_RELATED,
        candidate_group="",
        input_hash="x",
        evidence_hash="y",
        features={"embedding_similarity": 0.65, "deterministic_hits": [], "ambiguous_target_count": 0},
        evidences=[],
        surface="derived_only",
        promotion_target="derived_edge",
    )
    _, _, _, _, risk_low = _component_scores(c_low)
    assert risk_low >= 0.19


@pytest.mark.integration
def test_generate_semantic_candidates_empty_without_embeddings(pgvector_dsn: str, tmp_path: Path) -> None:
    from archive_cli.index_store import PostgresArchiveIndex

    idx = PostgresArchiveIndex(vault=tmp_path, dsn=pgvector_dsn)
    idx.schema = "ppa_sem_empty"
    with idx._connect() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {idx.schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {idx.schema}")
        idx._create_schema(conn)
        conn.execute(
            f"""
            INSERT INTO {idx.schema}.cards (uid, rel_path, slug, type, summary, content_hash)
            VALUES ('u1', 'a.md', 'a', 'person', 'hi', 'sha256:x')
            """
        )
        conn.commit()
    src = SeedCardSketch(
        uid="u1",
        rel_path="a.md",
        slug="a",
        card_type="person",
        summary="hi",
        frontmatter={},
        body="",
        content_hash="x",
        activity_at="2026-01-01",
        wikilinks=[],
    )
    catalog = SeedLinkCatalog(
        cards_by_uid={"u1": src},
        cards_by_exact_slug={},
        cards_by_slug={},
        cards_by_type={"person": [src]},
        person_by_email={},
        person_by_phone={},
        person_by_handle={},
        person_by_alias={},
        email_threads_by_thread_id={},
        email_messages_by_thread_id={},
        email_messages_by_message_id={},
        email_attachments_by_message_id={},
        email_attachments_by_thread_id={},
        imessage_threads_by_chat_id={},
        imessage_messages_by_chat_id={},
        calendar_events_by_event_id={},
        calendar_events_by_ical_uid={},
        media_by_day={},
        events_by_day={},
        path_buckets={},
    )
    with idx._connect() as conn:
        out = _generate_semantic_candidates(conn, idx.schema, catalog, "u1")
    assert out == []
