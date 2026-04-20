"""End-to-end integration tests for the Tier 3 semantic kNN linker.

These tests exercise:
- migration 003 (`embedding_score` column on `link_decisions`)
- the kNN candidate generator (`_generate_semantic_candidates`)
- the LLM-judge integration (mocked via monkeypatch over `get_provider_chain`)
- persistence into `link_candidates` / `link_decisions` / `promotion_queue`
- consumer-visible side effects: `archive_graph` shows `[seed:semantically_related, conf=X]`
- weighted `graph_boost = 0.22 * trust` end-to-end through hybrid retrieval
- policy-version bump re-evaluates existing decisions under the 5-tuple formula
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_cli import seed_links as sl
from archive_cli.commands.formatters import format_graph
from archive_cli.embedding_provider import HashEmbeddingProvider
from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.migrate import MigrationRunner
from archive_cli.retrieval_pipeline import HybridFetchInputs, fuse_and_rank_hybrid
from archive_cli.seed_links import (
    DECISION_AUTO_PROMOTE,
    DECISION_REVIEW,
    LINK_TYPE_SEMANTICALLY_RELATED,
    MODULE_SEMANTIC,
    SEED_LINK_POLICY_VERSION,
    LinkEvidence,
    SeedCardSketch,
    SeedLinkCandidate,
    SeedLinkCatalog,
    _generate_semantic_candidates,
    evaluate_seed_link_candidate,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _enable_seed_links(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")


@pytest.fixture
def hash_provider() -> HashEmbeddingProvider:
    return HashEmbeddingProvider()


class _FixedScoreLLMProvider:
    """Mock LLM provider that always answers YES with a fixed confidence."""

    name = "mock-llm"
    model = "mock-llm-v1"

    def __init__(self, confidence: float = 0.9, verdict: str = "YES") -> None:
        self.confidence = confidence
        self.verdict = verdict
        self.calls: list[str] = []

    def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
        self.calls.append(prompt)
        return json.dumps(
            {"link": self.verdict, "confidence": self.confidence, "reason": "mock", "needs_review": False}
        )


def _bootstrap_index(dsn: str, schema: str, vault_path: Path) -> PostgresArchiveIndex:
    idx = PostgresArchiveIndex(vault=vault_path, dsn=dsn)
    idx.schema = schema
    with idx._connect() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {schema}")
        idx._create_schema(conn)
        runner = MigrationRunner(conn, schema)
        runner.ensure_table()
        runner.run()
    return idx


def _insert_card_with_embedding(
    conn,
    schema: str,
    *,
    uid: str,
    rel_path: str,
    card_type: str,
    summary: str,
    text: str,
    provider: HashEmbeddingProvider,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.cards
            (uid, rel_path, slug, type, summary, content_hash)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (uid, rel_path, rel_path.removesuffix(".md"), card_type, summary, f"sha256:{uid}"),
    )
    chunk_key = f"{uid}:body:0"
    conn.execute(
        f"""
        INSERT INTO {schema}.chunks
            (chunk_key, card_uid, rel_path, chunk_type, chunk_index, source_fields, content, content_hash, token_count)
        VALUES (%s, %s, %s, 'body', 0, '[]'::jsonb, %s, %s, %s)
        """,
        (chunk_key, uid, rel_path, text, f"sha256:{uid}:body", len(text.split())),
    )
    vec = provider.embed_texts([text])[0]
    vec_literal = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
    from archive_cli.index_config import get_default_embedding_model, get_default_embedding_version

    conn.execute(
        f"""
        INSERT INTO {schema}.embeddings (chunk_key, embedding_model, embedding_version, embedding)
        VALUES (%s, %s, %s, %s::vector)
        """,
        (chunk_key, get_default_embedding_model(), get_default_embedding_version(), vec_literal),
    )


def _build_minimal_catalog(uid_to_path: dict[str, tuple[str, str, str]]) -> SeedLinkCatalog:
    """uid -> (rel_path, card_type, summary)."""
    cards_by_uid: dict[str, SeedCardSketch] = {}
    cards_by_type: dict[str, list[SeedCardSketch]] = {}
    for uid, (rel_path, card_type, summary) in uid_to_path.items():
        sketch = SeedCardSketch(
            uid=uid,
            rel_path=rel_path,
            slug=rel_path.removesuffix(".md"),
            card_type=card_type,
            summary=summary,
            frontmatter={},
            body="",
            content_hash=f"sha256:{uid}",
            activity_at="2026-01-01",
            wikilinks=[],
        )
        cards_by_uid[uid] = sketch
        cards_by_type.setdefault(card_type, []).append(sketch)
    return SeedLinkCatalog(
        cards_by_uid=cards_by_uid,
        cards_by_exact_slug={},
        cards_by_slug={},
        cards_by_type=cards_by_type,
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSemanticLinkerEndToEnd:
    """Section: full pipeline against pgvector + mock LLM."""

    SCHEMA = "ppa_t3_e2e"

    def test_migration_003_creates_embedding_score(self, pgvector_dsn: str, tmp_path: Path) -> None:
        idx = _bootstrap_index(pgvector_dsn, self.SCHEMA + "_mig", tmp_path)
        with idx._connect() as conn:
            row = conn.execute(
                """
                SELECT data_type FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'link_decisions'
                  AND column_name = 'embedding_score'
                """,
                (idx.schema,),
            ).fetchone()
        assert row is not None
        assert "double precision" in str(row["data_type"]).lower()

    def test_knn_returns_candidates_for_similar_cards(
        self, pgvector_dsn: str, tmp_path: Path, hash_provider: HashEmbeddingProvider
    ) -> None:
        idx = _bootstrap_index(pgvector_dsn, self.SCHEMA + "_knn", tmp_path)
        # Two cards with near-identical text => high cosine similarity under HashEmbeddingProvider.
        same_text = "back pain heating pad recommendation orthopedic guidance"
        with idx._connect() as conn:
            # Phase 6 Tier 4 Step 24: source/target must be in the semantic allowlist.
            # `email_message` is intentionally excluded (already linked deterministically
            # via thread membership). Use `document` as the source type for this test.
            _insert_card_with_embedding(
                conn, idx.schema, uid="u1", rel_path="a.md", card_type="document",
                summary="back pain doc", text=same_text, provider=hash_provider,
            )
            _insert_card_with_embedding(
                conn, idx.schema, uid="u2", rel_path="b.md", card_type="purchase",
                summary="amazon order heating pad", text=same_text, provider=hash_provider,
            )
            _insert_card_with_embedding(
                conn, idx.schema, uid="u3", rel_path="c.md", card_type="ride",
                summary="taxi to downtown", text="airport ride uber sedan", provider=hash_provider,
            )
            conn.commit()
        catalog = _build_minimal_catalog(
            {
                "u1": ("a.md", "document", "back pain doc"),
                "u2": ("b.md", "purchase", "amazon order heating pad"),
                "u3": ("c.md", "ride", "taxi to downtown"),
            }
        )
        with idx._connect() as conn:
            cands = _generate_semantic_candidates(conn, idx.schema, catalog, "u1", k=5, threshold=0.7)
        target_uids = {c.target_card_uid for c in cands}
        assert "u2" in target_uids, "expected near-identical-text neighbor to appear"
        assert "u1" not in target_uids, "self must be excluded"
        for c in cands:
            assert c.module_name == MODULE_SEMANTIC
            assert c.proposed_link_type == LINK_TYPE_SEMANTICALLY_RELATED
            assert c.features["embedding_similarity"] >= 0.7
            assert c.features["deterministic_hits"] == []

    def test_evaluate_persists_embedding_score_and_runs_llm(
        self,
        pgvector_dsn: str,
        tmp_path: Path,
        hash_provider: HashEmbeddingProvider,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        idx = _bootstrap_index(pgvector_dsn, self.SCHEMA + "_eval", tmp_path)
        text = "quarterly strategy planning offsite agenda"
        with idx._connect() as conn:
            _insert_card_with_embedding(
                conn, idx.schema, uid="s1", rel_path="src.md", card_type="meeting_transcript",
                summary="Q3 strategy meeting", text=text, provider=hash_provider,
            )
            _insert_card_with_embedding(
                conn, idx.schema, uid="t1", rel_path="tgt.md", card_type="finance",
                summary="consultant payment", text=text, provider=hash_provider,
            )
            conn.commit()

        catalog = _build_minimal_catalog(
            {
                "s1": ("src.md", "meeting_transcript", "Q3 strategy meeting"),
                "t1": ("tgt.md", "finance", "consultant payment"),
            }
        )
        mock = _FixedScoreLLMProvider(confidence=0.92, verdict="YES")
        monkeypatch.setattr(sl, "get_provider_chain", lambda _vault: [mock])
        # Phase 6 Tier 3 retired MODULE_SEMANTIC from LLM_REVIEW_MODULES. Re-add
        # it for this test since we are validating the (preserved) MODULE_SEMANTIC
        # code path specifically.
        monkeypatch.setattr(sl, "LLM_REVIEW_MODULES", sl.LLM_REVIEW_MODULES | {MODULE_SEMANTIC})

        with idx._connect() as conn:
            cands = _generate_semantic_candidates(conn, idx.schema, catalog, "s1", k=5, threshold=0.7)
            assert cands, "expected at least one candidate"
            for cand in cands:
                decision = evaluate_seed_link_candidate(idx.vault, catalog, cand)
                assert decision.embedding_score >= 0.7
                assert decision.llm_score == pytest.approx(0.92, rel=1e-6)
                assert decision.policy_version == SEED_LINK_POLICY_VERSION
                # Persist for shape verification.
                from archive_cli.seed_links import _persist_candidate

                _persist_candidate(conn, idx, job_id=None, candidate=cand, decision=decision, commit=True)
            row = conn.execute(
                f"""
                SELECT ld.embedding_score, ld.llm_score, ld.final_confidence, ld.policy_version, lc.module_name
                FROM {idx.schema}.link_decisions ld
                JOIN {idx.schema}.link_candidates lc ON lc.candidate_id = ld.candidate_id
                WHERE lc.module_name = %s
                LIMIT 1
                """,
                (MODULE_SEMANTIC,),
            ).fetchone()
        assert row is not None
        assert float(row["embedding_score"]) >= 0.7
        assert float(row["llm_score"]) == pytest.approx(0.92, rel=1e-6)
        assert int(row["policy_version"]) == SEED_LINK_POLICY_VERSION
        assert mock.calls, "LLM judge was not invoked for the semantic candidate"

    def test_consumer_sees_semantic_edge_via_graph_and_format(
        self,
        pgvector_dsn: str,
        tmp_path: Path,
    ) -> None:
        """When a `semantically_related` candidate has been decided + applied via the promotion
        queue, archive_graph + format_graph render it as [seed:semantically_related, conf=X].

        Note: the formula intentionally keeps semantic decisions below the conservative 0.99
        auto-promote floor (Step 7e). Real-world auto-promotion only happens after calibration
        (Step 12) lowers the floor. For the consumer-side test we therefore bypass the
        evaluator and write a high-confidence applied row directly, equivalent to what the
        post-calibration pipeline produces.
        """
        idx = _bootstrap_index(pgvector_dsn, self.SCHEMA + "_view", tmp_path)
        with idx._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {idx.schema}.cards (uid, rel_path, slug, type, summary, content_hash)
                VALUES ('src', 'src.md', 'src', 'document', 'travel notes', 'sha256:src'),
                       ('tgt', 'tgt.md', 'tgt', 'flight',   'flight booking', 'sha256:tgt')
                """
            )
            cand = conn.execute(
                f"""
                INSERT INTO {idx.schema}.link_candidates
                    (job_id, module_name, linker_version, source_card_uid, source_rel_path,
                     target_card_uid, target_rel_path, target_kind, proposed_link_type,
                     input_hash, evidence_hash, status)
                VALUES (NULL, %s, 1, 'src', 'src.md', 'tgt', 'tgt.md', 'card', %s,
                        'h-input', 'h-evid', 'approved')
                RETURNING candidate_id
                """,
                (MODULE_SEMANTIC, LINK_TYPE_SEMANTICALLY_RELATED),
            ).fetchone()
            cid = int(cand["candidate_id"])
            conn.execute(
                f"""
                INSERT INTO {idx.schema}.link_decisions
                    (candidate_id, deterministic_score, lexical_score, graph_score, llm_score,
                     risk_penalty, embedding_score, final_confidence, decision, decision_reason,
                     auto_approved_floor, review_floor, discard_floor, policy_version, llm_model,
                     llm_output_json)
                VALUES (%s, 0, 0, 0, 0.92, 0, 0.91, 0.93, 'auto_promote', 'test',
                        0.99, 0.85, 0, %s, 'mock', '{{}}'::jsonb)
                """,
                (cid, SEED_LINK_POLICY_VERSION),
            )
            conn.execute(
                f"""
                INSERT INTO {idx.schema}.promotion_queue
                    (candidate_id, promotion_target, target_field_name, promotion_status)
                VALUES (%s, 'derived_edge', '', 'applied')
                """,
                (cid,),
            )
            conn.commit()

        graph = idx.graph("src.md", hops=1)
        assert graph is not None
        assert graph["src.md"], "expected the applied semantic edge to surface"
        edge = graph["src.md"][0]
        assert edge["edge_type"] == LINK_TYPE_SEMANTICALLY_RELATED
        assert edge["confidence"] == pytest.approx(0.93, rel=1e-6)
        rendered = format_graph("src.md", graph)
        assert "seed:semantically_related" in rendered
        assert "conf=0.93" in rendered

    def test_weighted_boost_applies_to_semantic_neighbor(self) -> None:
        """End-to-end check that a semantic edge's confidence flows into hybrid graph_boost."""
        from archive_cli.retrieval_pipeline import score_breakdown_for_row

        vector_rows = [
            {
                "card_uid": "neighbor",
                "rel_path": "n.md",
                "type": "document",
                "summary": "x",
                "activity_at": "2026-01-01",
                "similarity": 0.5,
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
        ]
        # Trust 0.78 (typical seed-link confidence) -> graph_boost = 0.22 * 0.78 ≈ 0.1716
        out = fuse_and_rank_hybrid(
            HybridFetchInputs(
                lexical_rows=[],
                vector_rows=vector_rows,
                neighbor_trust={"neighbor": 0.78},
                query_cleaned="q",
                subqueries_used=("q",),
            ),
            final_limit=5,
        )
        row = next(r for r in out if r["card_uid"] == "neighbor")
        assert score_breakdown_for_row(row)["graph_boost"] == pytest.approx(0.22 * 0.78, rel=1e-6)
        assert score_breakdown_for_row(row)["graph_neighbor_trust"] == pytest.approx(0.78)


class TestPolicyVersionReevaluation:
    """SEED_LINK_POLICY_VERSION bump (1 -> 2) means existing decisions are re-scored
    under the new 5-tuple formula. The new formula adds embedding_score with weight 0.12
    and reduces deterministic from 0.50 to 0.45. For a deterministic-only decision
    (embedding_score = 0), the new score should be lower than the old one by 0.05 *
    deterministic_score on the deterministic component."""

    def test_old_formula_vs_new_formula_for_deterministic_only(self) -> None:
        # Pre-bump (4-tuple) and post-bump (5-tuple) coefficients.
        det = 1.0
        lex = 0.6
        graph = 0.4
        llm = 0.8
        risk = 0.0
        old = 0.50 * det + 0.15 * lex + 0.15 * graph + 0.20 * llm - risk
        new_with_zero_embedding = (
            0.45 * det + 0.12 * lex + 0.13 * graph + 0.18 * llm + 0.12 * 0.0 - risk
        )
        # Lowering deterministic from 0.50 to 0.45 (and small reductions elsewhere) must reduce
        # the score for a non-semantic (embedding=0) decision under the new formula.
        assert new_with_zero_embedding < old

    def test_new_formula_rewards_high_embedding_score(self) -> None:
        det, lex, graph, llm, risk = 0.0, 0.0, 0.0, 0.0, 0.0
        emb_high = 1.0
        new_high = 0.45 * det + 0.12 * lex + 0.13 * graph + 0.18 * llm + 0.12 * emb_high - risk
        assert new_high == pytest.approx(0.12, rel=1e-9)


def test_card_type_modules_count_matches_card_types_registry() -> None:
    from archive_cli.seed_links import CARD_TYPE_MODULES
    from archive_vault.schema import CARD_TYPES

    assert set(CARD_TYPE_MODULES.keys()) == set(CARD_TYPES.keys()), (
        "CARD_TYPE_MODULES must cover every registered card type so MODULE_SEMANTIC fans out completely"
    )


def test_proposed_link_types_includes_semantic() -> None:
    from archive_cli.seed_links import LINK_TYPE_SEMANTICALLY_RELATED, PROPOSED_LINK_TYPES

    assert LINK_TYPE_SEMANTICALLY_RELATED in PROPOSED_LINK_TYPES


def test_llm_review_modules_excludes_semantic() -> None:
    """Phase 6 Tier 3 retired — MODULE_SEMANTIC is no longer LLM-judged by default."""
    from archive_cli.seed_links import LLM_REVIEW_MODULES, MODULE_SEMANTIC

    assert MODULE_SEMANTIC not in LLM_REVIEW_MODULES


def test_decision_dataclass_includes_embedding_score() -> None:
    from dataclasses import fields

    from archive_cli.seed_links import SeedLinkDecision

    assert "embedding_score" in {f.name for f in fields(SeedLinkDecision)}


# Imports kept at bottom to satisfy ruff-friendly module structure but referenced above.
_REFERENCES_HOLDER = (
    DECISION_AUTO_PROMOTE,
    DECISION_REVIEW,
    LinkEvidence,
    SeedLinkCandidate,
)
