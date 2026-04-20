"""Tests for Tier 1: edge_type + confidence surfaced to graph consumers."""

from __future__ import annotations

import pytest

from archive_cli.commands.formatters import format_graph
from archive_cli.index_store import PostgresArchiveIndex


@pytest.fixture(autouse=True)
def _enable_seed_links(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")


def _bootstrap_test_schema(dsn: str, schema: str, vault_path) -> PostgresArchiveIndex:
    idx = PostgresArchiveIndex(vault=vault_path, dsn=dsn)
    idx.schema = schema
    with idx._connect() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {schema}")
        idx._create_schema(conn)
    return idx


def _insert_card(conn, schema: str, uid: str, rel_path: str) -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.cards
            (uid, rel_path, slug, type, summary, content_hash)
        VALUES (%s, %s, %s, 'person', %s, 'sha256:fake')
        """,
        (uid, rel_path, rel_path.removesuffix(".md"), f"summary for {uid}"),
    )


def _insert_wikilink_edge(conn, schema: str, src_uid: str, src_path: str, tgt_uid: str, tgt_path: str) -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.edges
            (source_uid, source_path, target_uid, target_slug, target_path, target_kind, edge_type, field_name)
        VALUES (%s, %s, %s, %s, %s, 'card', 'wikilink', 'body')
        """,
        (src_uid, src_path, tgt_uid, tgt_path.removesuffix(".md"), tgt_path),
    )


def _insert_applied_seed_link(
    conn,
    schema: str,
    src_uid: str,
    src_path: str,
    tgt_uid: str,
    tgt_path: str,
    link_type: str,
    confidence: float,
) -> None:
    cand_id = conn.execute(
        f"""
        INSERT INTO {schema}.link_candidates
            (job_id, module_name, linker_version, source_card_uid, source_rel_path,
             target_card_uid, target_rel_path, target_kind, proposed_link_type,
             input_hash, evidence_hash, status)
        VALUES (NULL, 'identityLinker', 1, %s, %s, %s, %s, 'card', %s,
                'fake-input-hash', 'fake-evidence-hash', 'approved')
        RETURNING candidate_id
        """,
        (src_uid, src_path, tgt_uid, tgt_path, link_type),
    ).fetchone()["candidate_id"]
    conn.execute(
        f"""
        INSERT INTO {schema}.link_decisions
            (candidate_id, deterministic_score, lexical_score, graph_score, llm_score, risk_penalty,
             embedding_score,
             final_confidence, decision, decision_reason, auto_approved_floor, review_floor, discard_floor,
             policy_version, llm_model, llm_output_json)
        VALUES (%s, 0, 0, 0, %s, 0, 0, %s, 'auto_promote', 'test', 0.8, 0.45, 0, 1, 'mock', '{{}}'::jsonb)
        """,
        (cand_id, confidence, confidence),
    )
    conn.execute(
        f"""
        INSERT INTO {schema}.promotion_queue
            (candidate_id, promotion_target, target_field_name, promotion_status)
        VALUES (%s, 'derived_edge', '', 'applied')
        """,
        (cand_id,),
    )
    conn.commit()


@pytest.mark.integration
class TestGraphReturnsEdgeMetadata:
    SCHEMA = "ppa_t1_graph"

    def test_graph_round_trips_metadata(self, pgvector_dsn, tmp_path):
        idx = _bootstrap_test_schema(pgvector_dsn, self.SCHEMA, tmp_path)
        with idx._connect() as conn:
            _insert_card(conn, self.SCHEMA, "u-a", "a.md")
            _insert_card(conn, self.SCHEMA, "u-b", "b.md")
            _insert_card(conn, self.SCHEMA, "u-c", "c.md")
            _insert_wikilink_edge(conn, self.SCHEMA, "u-a", "a.md", "u-b", "b.md")
            _insert_applied_seed_link(
                conn,
                self.SCHEMA,
                "u-a",
                "a.md",
                "u-c",
                "c.md",
                link_type="possible_same_person",
                confidence=0.78,
            )
            conn.commit()
        graph = idx.graph("a.md", hops=1)
        assert graph is not None
        targets = graph["a.md"]
        targets_sorted = sorted(targets, key=lambda t: t["edge_type"])
        assert targets_sorted == [
            {"path": "c.md", "edge_type": "possible_same_person", "confidence": 0.78},
            {"path": "b.md", "edge_type": "wikilink", "confidence": 1.0},
        ]

    def test_neighbor_uids_returns_dict_with_max_trust(self, pgvector_dsn, tmp_path):
        idx = _bootstrap_test_schema(pgvector_dsn, self.SCHEMA + "_max", tmp_path)
        with idx._connect() as conn:
            _insert_card(conn, self.SCHEMA + "_max", "u-a", "a.md")
            _insert_card(conn, self.SCHEMA + "_max", "u-b", "b.md")
            _insert_wikilink_edge(conn, self.SCHEMA + "_max", "u-a", "a.md", "u-b", "b.md")
            _insert_applied_seed_link(
                conn,
                self.SCHEMA + "_max",
                "u-a",
                "a.md",
                "u-b",
                "b.md",
                link_type="possible_same_person",
                confidence=0.5,
            )
            conn.commit()
        neighbors = idx.fetch_graph_neighbors_for_uids(["u-a"])
        assert neighbors == {"u-b": 1.0}

    def test_anchor_self_excluded(self, pgvector_dsn, tmp_path):
        idx = _bootstrap_test_schema(pgvector_dsn, self.SCHEMA + "_self", tmp_path)
        with idx._connect() as conn:
            _insert_card(conn, self.SCHEMA + "_self", "u-a", "a.md")
            _insert_card(conn, self.SCHEMA + "_self", "u-b", "b.md")
            _insert_wikilink_edge(conn, self.SCHEMA + "_self", "u-a", "a.md", "u-b", "b.md")
            conn.commit()
        neighbors = idx.fetch_graph_neighbors_for_uids(["u-a", "u-b"])
        assert neighbors == {}


class TestFormatGraph:
    def test_renders_wikilink_without_confidence_suffix(self):
        out = format_graph(
            "a.md",
            {"a.md": [{"path": "b.md", "edge_type": "wikilink", "confidence": 1.0}]},
        )
        assert out == "Graph from a.md:\n- a.md\n  -> b.md  [wikilink]"

    def test_renders_seed_link_with_confidence_suffix(self):
        out = format_graph(
            "a.md",
            {"a.md": [{"path": "c.md", "edge_type": "possible_same_person", "confidence": 0.78}]},
        )
        assert "[seed:possible_same_person, conf=0.78]" in out

    def test_empty_graph_returns_no_linked_notes(self):
        assert format_graph("a.md", {}) == "No linked notes"
