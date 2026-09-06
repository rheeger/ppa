"""P01-A: warehouse policy/identity/edge fields survive export → native query."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_cli.corpus_hygiene.state_store import QUARANTINE_RETRIEVAL_WEIGHT, ensure_corpus_hygiene_tables
from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.serving_index import (
    ServingFidelityError,
    build_inferred_edge,
    build_serving_card,
    build_warehouse_edge,
    get_serving_handle,
    publish_serving_index,
    serving_corpus_state,
)
from archive_cli.store import DefaultArchiveStore
from archive_engine.contracts import UNKNOWN, EmbeddingSpec, ServingEdge

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate  # noqa: E402


def test_missing_corpus_state_is_unknown_not_active() -> None:
    assert serving_corpus_state(None) == UNKNOWN
    assert serving_corpus_state("") == UNKNOWN
    assert serving_corpus_state("active") == "active"
    row = {"uid": "u1", "rel_path": "a.md", "summary": "A", "type": "person", "slug": "a", "activity_at": "", "activity_end_at": "", "search_text": "", "content_hash": "sha256:abc"}
    maps = {
        "people": {},
        "sources": {},
        "orgs": {},
        "aliases": {},
        "emails": {},
        "external_ids": {},
        "corpus_states": {},
        "corpus_state_table": False,
    }
    rec = build_serving_card(row, maps)
    assert rec["corpus_state"] == UNKNOWN
    assert rec["retrieval_weight"] is None
    assert rec["provenance_summary"] == UNKNOWN
    assert rec["source_revision"] == "sha256:abc"


def test_missing_policy_cannot_be_upgraded_to_active() -> None:
    row = {"uid": "u1", "rel_path": "a.md", "summary": "A", "type": "person", "slug": "a", "activity_at": "", "activity_end_at": "", "search_text": "", "content_hash": ""}
    maps = {
        "people": {},
        "sources": {},
        "orgs": {},
        "aliases": {},
        "emails": {},
        "external_ids": {},
        "corpus_states": {"u1": "active"},
        "corpus_state_table": False,
    }
    with pytest.raises(ServingFidelityError):
        build_serving_card(row, maps)


def test_inferred_edge_without_confidence_has_no_trust_default() -> None:
    rec = build_inferred_edge(
        {
            "source_uid": "a",
            "target_uid": "b",
            "edge_type": "possible_same_person",
            "field_name": "",
            "confidence": None,
        }
    )
    edge = ServingEdge.from_payload(rec)
    assert edge.method == "inferred"
    assert edge.confidence is None
    assert "trust" not in rec


def test_warehouse_edge_trust_is_derived_from_contract() -> None:
    rec = build_warehouse_edge(
        {"source_uid": "a", "target_uid": "b", "edge_type": "wikilink", "field_name": "body"}
    )
    edge = ServingEdge.from_payload(rec)
    assert edge.method == UNKNOWN
    assert edge.confidence == 1.0
    assert rec["trust"] == 1.0
    assert rec["direction"] == "forward"


def test_serving_embedding_spec_is_the_frozen_record() -> None:
    spec = EmbeddingSpec(
        provider_namespace="hash",
        model="archive-hash-dev",
        model_revision="1",
        dimension=8,
        metric="cosine",
        normalization="l2",
        chunk_schema="6",
    )
    assert EmbeddingSpec.from_payload(spec.to_payload()) == spec


def _write_fidelity_export(work: Path) -> None:
    cards = [
        {
            "card_uid": "hfa-person-active0001",
            "rel_path": "People/active.md",
            "summary": "Active Person",
            "type": "person",
            "slug": "active-person",
            "activity_at": "2026-09-06T00:00:00Z",
            "search_text": "Active Person",
            "people": ["Active Person"],
            "sources": ["acceptance"],
            "orgs": [],
            "corpus_state": "active",
            "retrieval_weight": 1.0,
            "aliases": ["acty"],
            "emails": ["active@example.test"],
            "external_ids": ["ext-active-1"],
            "source_revision": "sha256:active",
            "provenance_summary": "manual",
        },
        {
            "card_uid": "hfa-email-quarantine01",
            "rel_path": "Email/quarantine.md",
            "summary": "Quarantine Thread",
            "type": "email_message",
            "slug": "quarantine-thread",
            "activity_at": "2026-09-05T00:00:00Z",
            "search_text": "Quarantine Thread",
            "people": [],
            "sources": ["gmail"],
            "orgs": [],
            "corpus_state": "quarantine",
            "retrieval_weight": QUARANTINE_RETRIEVAL_WEIGHT,
            "aliases": [],
            "emails": [],
            "external_ids": [],
            "source_revision": "sha256:quarantine",
            "provenance_summary": UNKNOWN,
        },
        {
            "card_uid": "hfa-email-suppressed01",
            "rel_path": "Email/suppressed.md",
            "summary": "Suppressed Thread",
            "type": "email_message",
            "slug": "suppressed-thread",
            "activity_at": "2026-09-04T00:00:00Z",
            "search_text": "Suppressed Thread",
            "people": [],
            "sources": ["gmail"],
            "orgs": [],
            "corpus_state": "suppressed",
            "retrieval_weight": 0.0,
            "aliases": [],
            "emails": [],
            "external_ids": [],
            "source_revision": "sha256:suppressed",
            "provenance_summary": UNKNOWN,
        },
        {
            "card_uid": "hfa-person-inferred0001",
            "rel_path": "People/inferred.md",
            "summary": "Inferred Neighbor",
            "type": "person",
            "slug": "inferred-neighbor",
            "activity_at": "2026-09-03T00:00:00Z",
            "search_text": "Inferred Neighbor",
            "people": ["Inferred Neighbor"],
            "sources": [],
            "orgs": [],
            "corpus_state": "active",
            "retrieval_weight": 1.0,
            "aliases": [],
            "emails": [],
            "external_ids": [],
            "source_revision": "sha256:inferred",
            "provenance_summary": "llm",
        },
        {
            "card_uid": "hfa-person-unknown0001",
            "rel_path": "People/unknown.md",
            "summary": "Unknown Policy",
            "type": "person",
            "slug": "unknown-policy",
            "activity_at": "2026-09-02T00:00:00Z",
            "search_text": "Unknown Policy",
            "people": [],
            "sources": [],
            "orgs": [],
            "corpus_state": UNKNOWN,
            "retrieval_weight": None,
            "aliases": [],
            "emails": [],
            "external_ids": [],
            "source_revision": "sha256:unknown",
            "provenance_summary": UNKNOWN,
        },
    ]
    (work / "cards.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cards), encoding="utf-8")
    (work / "chunks.jsonl").write_text("", encoding="utf-8")
    edges = [
        {
            "source_uid": "hfa-person-active0001",
            "target_uid": "hfa-person-inferred0001",
            "edge_type": "possible_same_person",
            "field_name": "",
            "direction": "forward",
            "method": "inferred",
            "confidence": 0.61,
            "evidence_uids": ["hfa-email-quarantine01"],
            "trust": 0.61,
        },
        {
            "source_uid": "hfa-person-active0001",
            "target_uid": "hfa-email-quarantine01",
            "edge_type": "wikilink",
            "field_name": "body",
            "direction": "forward",
            "method": UNKNOWN,
            "confidence": 1.0,
            "evidence_uids": [],
            "trust": 1.0,
        },
    ]
    (work / "edges.jsonl").write_text("".join(json.dumps(e) + "\n" for e in edges), encoding="utf-8")
    (work / "embedding_keys.txt").write_text("", encoding="utf-8")
    (work / "embeddings.bin").write_bytes(b"")


def _publish_fidelity(root: Path) -> str:
    gid = "gen-fidelity"
    dest = root / "generations" / gid
    dest.mkdir(parents=True)
    work = dest / "_inbox"
    work.mkdir()
    _write_fidelity_export(work)
    archive_crate.serving_index_build(
        str(dest),
        str(work / "cards.jsonl"),
        str(work / "chunks.jsonl"),
        str(work / "embedding_keys.txt"),
        str(work / "embeddings.bin"),
        4,
        str(work / "edges.jsonl"),
    )
    archive_crate.serving_index_publish(str(root), gid)
    archive_crate.serving_index_truncate_dirty(str(root))
    return gid


def test_native_round_trip_preserves_policy_and_inferred_edge(tmp_path, monkeypatch) -> None:
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    gid = _publish_fidelity(root)
    handle = get_serving_handle(tmp_path)

    exact = handle.search("hfa-person-active0001", limit=5)
    assert exact
    hit = next(r for r in exact if r.get("card_uid") == "hfa-person-active0001")
    assert hit["match_channel"] == "exact"
    assert hit["source_revision"] == "sha256:active"
    assert hit["corpus_state"] == "active"
    assert hit["citation"]["generation"] == gid
    assert hit["citation"]["card_uid"] == "hfa-person-active0001"

    alias = handle.search("acty", limit=5)
    assert any(r.get("card_uid") == "hfa-person-active0001" for r in alias)
    ext = handle.search("ext-active-1", limit=5)
    assert any(r.get("card_uid") == "hfa-person-active0001" for r in ext)
    email = handle.search("active@example.test", limit=5)
    assert any(r.get("card_uid") == "hfa-person-active0001" for r in email)

    listed = handle.query(limit=20)
    uids = {row.get("card_uid") for row in listed}
    assert "hfa-email-quarantine01" in uids
    assert "hfa-person-unknown0001" in uids
    assert "hfa-email-suppressed01" not in uids
    quarantine = next(r for r in listed if r["card_uid"] == "hfa-email-quarantine01")
    assert quarantine["corpus_state"] == "quarantine"
    assert quarantine["retrieval_weight"] == pytest.approx(QUARANTINE_RETRIEVAL_WEIGHT)
    unknown = next(r for r in listed if r["card_uid"] == "hfa-person-unknown0001")
    assert unknown["corpus_state"] == UNKNOWN
    assert unknown.get("retrieval_weight") in (None, )

    graph = handle.graph("People/active.md", hops=1)
    neighbors = graph["People/active.md"]
    inferred = next(item for item in neighbors if item["edge_type"] == "possible_same_person")
    assert inferred["method"] == "inferred"
    assert inferred["confidence"] == pytest.approx(0.61)
    assert inferred["evidence_uids"] == ["hfa-email-quarantine01"]
    assert inferred["match_channel"] == "seed-link"
    wikilink = next(item for item in neighbors if item["edge_type"] == "wikilink")
    assert wikilink["method"] == UNKNOWN
    assert wikilink["confidence"] == pytest.approx(1.0)

    suppressed = handle.search("hfa-email-suppressed01", limit=5)
    assert not any(r.get("card_uid") == "hfa-email-suppressed01" for r in suppressed)
    person = handle.person("suppressed-thread")
    assert person.get("found") is False


def _bootstrap(dsn: str, schema: str, vault: Path) -> PostgresArchiveIndex:
    idx = PostgresArchiveIndex(vault=vault, dsn=dsn)
    idx.schema = schema
    with idx._connect() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {schema}")
        idx._create_schema(conn)
        ensure_corpus_hygiene_tables(conn, schema)
        conn.commit()
    return idx


def _insert_card(conn, schema: str, uid: str, rel_path: str, summary: str, card_type: str = "person") -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.cards
            (uid, rel_path, slug, type, summary, content_hash, search_text)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (uid, rel_path, rel_path.removesuffix(".md").rsplit("/", 1)[-1], card_type, summary, f"sha256:{uid}", summary),
    )


@pytest.mark.integration
class TestWarehouseExportFidelity:
    SCHEMA = "ppa_p01a_fidelity"

    def test_exporter_to_native_preserves_warehouse_fields(self, pgvector_dsn, tmp_path, monkeypatch):
        monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")
        monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(tmp_path / "rust-search-index"))
        monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
        monkeypatch.setenv("PPA_EMBEDDING_MODEL", "archive-hash-dev")
        monkeypatch.setenv("PPA_EMBEDDING_VERSION", "1")
        monkeypatch.setenv("PPA_VECTOR_DIMENSION", "8")
        from archive_cli import serving_index as si

        si._HANDLE = None
        idx = _bootstrap(pgvector_dsn, self.SCHEMA, tmp_path)
        with idx._connect() as conn:
            _insert_card(conn, self.SCHEMA, "hfa-person-wh-active", "People/wh-active.md", "Warehouse Active")
            _insert_card(conn, self.SCHEMA, "hfa-person-wh-neighbor", "People/wh-neighbor.md", "Warehouse Neighbor")
            _insert_card(
                conn, self.SCHEMA, "hfa-email-wh-q", "Email/wh-q.md", "Warehouse Quarantine", card_type="email_message"
            )
            _insert_card(
                conn, self.SCHEMA, "hfa-email-wh-s", "Email/wh-s.md", "Warehouse Suppressed", card_type="email_message"
            )
            conn.execute(
                f"""
                INSERT INTO {self.SCHEMA}.people
                    (card_uid, rel_path, card_type, summary, created, updated, primary_source, source_id,
                     aliases_json, emails_json)
                VALUES (%s, %s, 'person', %s, '', '', 'acceptance', '', %s::jsonb, %s::jsonb)
                """,
                (
                    "hfa-person-wh-active",
                    "People/wh-active.md",
                    "Warehouse Active",
                    json.dumps(["wh-alias"]),
                    json.dumps(["wh-active@example.test"]),
                ),
            )
            conn.execute(
                f"""
                INSERT INTO {self.SCHEMA}.external_ids (card_uid, field_name, provider, external_id)
                VALUES (%s, 'source_id', 'acceptance', 'wh-ext-1')
                """,
                ("hfa-person-wh-active",),
            )
            conn.execute(
                f"""
                INSERT INTO {self.SCHEMA}.card_corpus_state (card_uid, corpus_state)
                VALUES (%s, 'active'), (%s, 'quarantine'), (%s, 'suppressed')
                """,
                ("hfa-person-wh-active", "hfa-email-wh-q", "hfa-email-wh-s"),
            )
            conn.execute(
                f"""
                INSERT INTO {self.SCHEMA}.edges
                    (source_uid, source_path, target_uid, target_slug, target_path, target_kind, edge_type, field_name)
                VALUES (%s, %s, %s, %s, %s, 'card', 'wikilink', 'body')
                """,
                (
                    "hfa-person-wh-active",
                    "People/wh-active.md",
                    "hfa-email-wh-q",
                    "wh-q",
                    "Email/wh-q.md",
                ),
            )
            cand_id = conn.execute(
                f"""
                INSERT INTO {self.SCHEMA}.link_candidates
                    (job_id, module_name, linker_version, source_card_uid, source_rel_path,
                     target_card_uid, target_rel_path, target_kind, proposed_link_type,
                     input_hash, evidence_hash, status)
                VALUES (NULL, 'identityLinker', 1, %s, %s, %s, %s, 'card', 'possible_same_person',
                        'fake-input', 'fake-evidence', 'approved')
                RETURNING candidate_id
                """,
                (
                    "hfa-person-wh-active",
                    "People/wh-active.md",
                    "hfa-person-wh-neighbor",
                    "People/wh-neighbor.md",
                ),
            ).fetchone()["candidate_id"]
            conn.execute(
                f"""
                INSERT INTO {self.SCHEMA}.link_decisions
                    (candidate_id, deterministic_score, lexical_score, graph_score, llm_score, risk_penalty,
                     embedding_score, final_confidence, decision, decision_reason,
                     auto_approved_floor, review_floor, discard_floor, policy_version, llm_model, llm_output_json)
                VALUES (%s, 0, 0, 0, 0.7, 0, 0, 0.64, 'auto_promote', 'test', 0.8, 0.45, 0, 1, 'mock', '{{}}'::jsonb)
                """,
                (cand_id,),
            )
            conn.execute(
                f"""
                INSERT INTO {self.SCHEMA}.promotion_queue
                    (candidate_id, promotion_target, target_field_name, promotion_status)
                VALUES (%s, 'derived_edge', '', 'applied')
                """,
                (cand_id,),
            )
            conn.commit()

        published = publish_serving_index(DefaultArchiveStore(vault=tmp_path, index=idx))
        assert published["ok"] is True
        cards_path = tmp_path / "rust-search-index" / "generations" / published["generation"] / "cards.jsonl"
        exported = [json.loads(line) for line in cards_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        by_uid = {row["card_uid"]: row for row in exported}
        assert by_uid["hfa-person-wh-active"]["corpus_state"] == "active"
        assert by_uid["hfa-person-wh-active"]["aliases"] == ["wh-alias"]
        assert by_uid["hfa-person-wh-active"]["emails"] == ["wh-active@example.test"]
        assert "wh-ext-1" in by_uid["hfa-person-wh-active"]["external_ids"]
        assert by_uid["hfa-email-wh-q"]["corpus_state"] == "quarantine"
        assert by_uid["hfa-email-wh-q"]["retrieval_weight"] == pytest.approx(QUARANTINE_RETRIEVAL_WEIGHT)
        assert by_uid["hfa-email-wh-s"]["corpus_state"] == "suppressed"
        assert by_uid["hfa-person-wh-neighbor"]["corpus_state"] == UNKNOWN
        assert by_uid["hfa-person-wh-neighbor"]["retrieval_weight"] is None

        edges_path = tmp_path / "rust-search-index" / "generations" / published["generation"] / "edges.jsonl"
        edges = [json.loads(line) for line in edges_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        inferred = next(e for e in edges if e["edge_type"] == "possible_same_person")
        assert inferred["method"] == "inferred"
        assert inferred["confidence"] == pytest.approx(0.64)
        assert inferred["trust"] == pytest.approx(0.64)
        wikilink = next(e for e in edges if e["edge_type"] == "wikilink")
        assert wikilink["method"] == UNKNOWN

        handle = get_serving_handle(tmp_path)
        exact = handle.search("hfa-person-wh-active", limit=5)
        assert any(r.get("card_uid") == "hfa-person-wh-active" and r.get("match_channel") == "exact" for r in exact)
        listed = handle.query(limit=20)
        assert not any(r.get("card_uid") == "hfa-email-wh-s" for r in listed)
        graph = handle.graph("People/wh-active.md", hops=1)
        inferred_hit = next(
            item for item in graph["People/wh-active.md"] if item["edge_type"] == "possible_same_person"
        )
        assert inferred_hit["method"] == "inferred"
        assert inferred_hit["confidence"] == pytest.approx(0.64)
