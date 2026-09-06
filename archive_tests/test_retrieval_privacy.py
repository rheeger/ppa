"""P05-B retrieval privacy through native serving, store, and MCP.

Synthetic fixtures only. No seed vault, no real private files, no inherited DSN.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from archive_cli.query_embed_cache import QueryEmbedCache, QueryEmbedSpec, query_embed_cache_key
from archive_cli.serving_index import get_serving_handle
from archive_cli.store import DefaultArchiveStore
from archive_engine.access import access_request_fields, policy_identity
from archive_engine.contracts import AccessContext
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate  # noqa: E402

PPA_ROOT = Path(__file__).resolve().parents[1]
GMAIL_UID = "hfa-email-gmail000001"
MEDICAL_UID = "hfa-medical-private001"
MIXED_UID = "hfa-purchase-mixed0001"
PERSON_UID = "hfa-person-allowed0001"
UNKNOWN_UID = "hfa-derived-unknown001"
DENIED_TOKEN = "P05B_DENIED_SYNTHETIC_SENTINEL"
ALLOWED_TOKEN = "P05B_ALLOWED_GMAIL_SENTINEL"


def _ctx(*, sources: tuple[str, ...] = (), deny: bool = False) -> AccessContext:
    return AccessContext(
        archive_id="p05b",
        principal="alice",
        profile="read-only",
        allowed_sources=sources,
        allowed_domains=(),
        allowed_tools=(),
        egress_policy_revision="p05b-test",
        deny=deny,
    )


def _write_privacy_export(work: Path) -> None:
    cards = [
        {
            "card_uid": GMAIL_UID,
            "rel_path": "Email/gmail.md",
            "summary": f"Gmail note {ALLOWED_TOKEN}",
            "type": "email_message",
            "slug": "gmail-note",
            "activity_at": "2026-09-06T12:00:00Z",
            "search_text": f"Gmail note {ALLOWED_TOKEN}",
            "people": ["Alice"],
            "sources": ["gmail"],
            "required_sources": ["gmail"],
            "domains": ["communication"],
            "orgs": [],
            "corpus_state": "active",
            "retrieval_weight": 1.0,
            "aliases": [],
            "emails": [],
            "external_ids": [],
            "source_revision": "sha256:gmail",
            "provenance_summary": "deterministic",
            "lineage_complete": True,
        },
        {
            "card_uid": MEDICAL_UID,
            "rel_path": "Medical/private.md",
            "summary": f"Medical note {DENIED_TOKEN}",
            "type": "medical_record",
            "slug": "medical-note",
            "activity_at": "2026-09-06T11:00:00Z",
            "search_text": f"Medical note {DENIED_TOKEN}",
            "people": ["Alice"],
            "sources": ["medical"],
            "required_sources": ["medical"],
            "domains": ["medical"],
            "orgs": [],
            "corpus_state": "active",
            "retrieval_weight": 1.0,
            "aliases": ["medical-note"],
            "emails": [],
            "external_ids": [],
            "source_revision": "sha256:medical",
            "provenance_summary": "deterministic",
            "lineage_complete": True,
        },
        {
            "card_uid": MIXED_UID,
            "rel_path": "Derived/mixed.md",
            "summary": f"Mixed purchase {DENIED_TOKEN}",
            "type": "purchase",
            "slug": "mixed-purchase",
            "activity_at": "2026-09-06T10:00:00Z",
            "search_text": f"Mixed purchase {DENIED_TOKEN}",
            "people": [],
            "sources": ["gmail", "medical"],
            "required_sources": ["gmail", "medical"],
            "domains": ["finance"],
            "orgs": [],
            "corpus_state": "active",
            "retrieval_weight": 1.0,
            "aliases": [],
            "emails": [],
            "external_ids": [],
            "source_revision": "sha256:mixed",
            "provenance_summary": "llm",
            "lineage_complete": True,
        },
        {
            "card_uid": PERSON_UID,
            "rel_path": "People/allowed.md",
            "summary": "Allowed Person",
            "type": "person",
            "slug": "allowed-person",
            "activity_at": "2026-09-05T00:00:00Z",
            "search_text": "Allowed Person",
            "people": ["Allowed Person"],
            "sources": ["gmail"],
            "required_sources": ["gmail"],
            "domains": ["identity"],
            "orgs": [],
            "corpus_state": "active",
            "retrieval_weight": 1.0,
            "aliases": ["allowed-person"],
            "emails": ["allowed@example.test"],
            "external_ids": [],
            "source_revision": "sha256:person",
            "provenance_summary": "manual",
            "lineage_complete": True,
        },
        {
            "card_uid": UNKNOWN_UID,
            "rel_path": "Derived/unknown.md",
            "summary": f"Unknown lineage {DENIED_TOKEN}",
            "type": "purchase",
            "slug": "unknown-lineage",
            "activity_at": "2026-09-04T00:00:00Z",
            "search_text": f"Unknown lineage {DENIED_TOKEN}",
            "people": [],
            "sources": [],
            "required_sources": [],
            "domains": [],
            "orgs": [],
            "corpus_state": "active",
            "retrieval_weight": 1.0,
            "aliases": [],
            "emails": [],
            "external_ids": [],
            "source_revision": "sha256:unknown",
            "provenance_summary": "unknown",
            "lineage_complete": False,
        },
    ]
    (work / "cards.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cards), encoding="utf-8")
    (work / "chunks.jsonl").write_text("", encoding="utf-8")
    edges = [
        {
            "source_uid": GMAIL_UID,
            "target_uid": MEDICAL_UID,
            "edge_type": "wikilink",
            "field_name": "body",
            "direction": "forward",
            "method": "unknown",
            "confidence": 1.0,
            "evidence_uids": [],
            "trust": 1.0,
        },
        {
            "source_uid": GMAIL_UID,
            "target_uid": MIXED_UID,
            "edge_type": "derived_from",
            "field_name": "source_email",
            "direction": "forward",
            "method": "inferred",
            "confidence": 0.7,
            "evidence_uids": [MEDICAL_UID],
            "trust": 0.7,
        },
        {
            "source_uid": MEDICAL_UID,
            "target_uid": PERSON_UID,
            "edge_type": "wikilink",
            "field_name": "body",
            "direction": "forward",
            "method": "unknown",
            "confidence": 1.0,
            "evidence_uids": [],
            "trust": 1.0,
        },
    ]
    (work / "edges.jsonl").write_text("".join(json.dumps(e) + "\n" for e in edges), encoding="utf-8")
    (work / "embedding_keys.txt").write_text("", encoding="utf-8")
    (work / "embeddings.bin").write_bytes(b"")


def _publish_privacy(root: Path) -> str:
    gid = "gen-privacy"
    dest = root / "generations" / gid
    dest.mkdir(parents=True)
    work = dest / "_inbox"
    work.mkdir()
    _write_privacy_export(work)
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


def _uids(rows) -> set[str]:
    return {str(row.get("card_uid") or row.get("uid") or "") for row in rows if row.get("card_uid") or row.get("uid")}


@pytest.fixture
def privacy_index(tmp_path, monkeypatch):
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    _publish_privacy(root)
    handle = get_serving_handle(tmp_path)
    yield handle
    si._HANDLE = None


def test_unrestricted_native_keeps_authorized_visibility(privacy_index) -> None:
    listed = privacy_index.query(limit=20)
    uids = _uids(listed)
    assert {GMAIL_UID, MEDICAL_UID, MIXED_UID, PERSON_UID, UNKNOWN_UID} <= uids
    exact = privacy_index.search(MEDICAL_UID, limit=5)
    assert any(row.get("card_uid") == MEDICAL_UID for row in exact)


def test_restricted_native_filters_before_limit_and_hides_indirect(privacy_index) -> None:
    policy = access_request_fields(_ctx(sources=("gmail",)))
    listed = privacy_index.query(limit=20, **policy)
    uids = _uids(listed)
    assert GMAIL_UID in uids
    assert PERSON_UID in uids
    assert MEDICAL_UID not in uids
    assert MIXED_UID not in uids
    assert UNKNOWN_UID not in uids
    assert DENIED_TOKEN not in json.dumps(listed)

    one = privacy_index.search("note", limit=1, **policy)
    assert _uids(one) == {GMAIL_UID}

    medical = privacy_index.search(MEDICAL_UID, limit=5, **policy)
    assert not medical
    person = privacy_index.person("medical-note", **policy)
    assert person.get("found") is False

    graph = privacy_index.graph("Email/gmail.md", hops=2, **policy)
    blob = json.dumps(graph)
    assert MEDICAL_UID not in blob
    assert MIXED_UID not in blob
    assert DENIED_TOKEN not in blob
    neighbors = graph.get("Email/gmail.md") or []
    assert all("medical" not in json.dumps(edge).lower() or "gmail" in json.dumps(edge) for edge in neighbors)


def test_store_read_and_evidence_omit_denied(privacy_index, tmp_path, monkeypatch) -> None:
    vault = tmp_path / "disposable-vault"
    (vault / "Email").mkdir(parents=True)
    (vault / "Medical").mkdir()
    (vault / "People").mkdir()
    (vault / "Derived").mkdir()
    write_card(
        vault,
        "Email/gmail.md",
        PersonCard(
            uid=GMAIL_UID,
            type="person",
            source=["gmail"],
            source_id="gmail-1",
            created="2026-09-06",
            updated="2026-09-06",
            summary=f"Gmail note {ALLOWED_TOKEN}",
        ),
        body=ALLOWED_TOKEN,
        provenance={"summary": ProvenanceEntry("gmail", "2026-09-06", "deterministic")},
    )
    write_card(
        vault,
        "Medical/private.md",
        PersonCard(
            uid=MEDICAL_UID,
            type="person",
            source=["medical"],
            source_id="med-1",
            created="2026-09-06",
            updated="2026-09-06",
            summary=f"Medical note {DENIED_TOKEN}",
        ),
        body=DENIED_TOKEN,
        provenance={"summary": ProvenanceEntry("medical", "2026-09-06", "deterministic")},
    )
    write_card(
        vault,
        "People/allowed.md",
        PersonCard(
            uid=PERSON_UID,
            type="person",
            source=["gmail"],
            source_id="allowed@example.test",
            created="2026-09-06",
            updated="2026-09-06",
            summary="Allowed Person",
        ),
        body="person-ok",
        provenance={"summary": ProvenanceEntry("gmail", "2026-09-06", "deterministic")},
    )
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://unused:unused@127.0.0.1:1/unused")

    class _ForceServing(DefaultArchiveStore):
        def _is_warehouse_index(self) -> bool:
            return True

    class _Index:
        def search(self, *args, **kwargs):
            return []

        def query_cards(self, **kwargs):
            return []

        def timeline(self, **kwargs):
            return []

        def graph(self, *args, **kwargs):
            return {}

        def person_path(self, name):
            return None

        def card_stack_pointers(self, uids):
            return {}

    store = _ForceServing(vault=vault, index=_Index(), access=_ctx(sources=("gmail",)))
    denied = store.read(MEDICAL_UID)
    assert denied.get("found") is False
    assert DENIED_TOKEN not in json.dumps(denied)
    allowed = store.read(GMAIL_UID)
    assert allowed.get("found") is True
    assert ALLOWED_TOKEN in str(allowed.get("content") or "")

    ev = store.evidence(query="note", limit=8)
    blob = json.dumps(ev)
    assert MEDICAL_UID not in blob
    assert MIXED_UID not in blob
    assert DENIED_TOKEN not in blob

    person = store.person("medical-note")
    assert person.get("found") is False
    assert DENIED_TOKEN not in json.dumps(person)


def test_policy_scoped_query_cache_does_not_reuse_across_principals(tmp_path) -> None:
    cache = QueryEmbedCache(tmp_path / "q.sqlite", ram_entries=8)
    alice = QueryEmbedSpec(
        model="archive-hash-dev",
        version=1,
        provider="hash",
        dimension=4,
        policy_identity=policy_identity(_ctx(sources=("gmail",))),
    )
    bob = QueryEmbedSpec(
        model="archive-hash-dev",
        version=1,
        provider="hash",
        dimension=4,
        policy_identity=policy_identity(_ctx(sources=("medical",))),
    )
    cache.put("note", alice, [1.0, 0.0, 0.0, 0.0])
    assert cache.get("note", alice) == [1.0, 0.0, 0.0, 0.0]
    assert cache.get("note", bob) is None
    assert query_embed_cache_key("note", model="archive-hash-dev", version=1, provider="hash", dimension=4, policy_identity=alice.policy_identity) != query_embed_cache_key(
        "note", model="archive-hash-dev", version=1, provider="hash", dimension=4, policy_identity=bob.policy_identity
    )
    cache.close()


def test_mcp_and_cli_payloads_omit_denied(privacy_index, tmp_path, monkeypatch) -> None:
    vault = tmp_path / "disposable-vault"
    vault.mkdir()
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_ACCESS_ALLOWED_SOURCES", "gmail")
    monkeypatch.setenv("PPA_ACCESS_PRINCIPAL", "alice")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://unused:unused@127.0.0.1:1/unused")
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    from archive_cli.server import archive_search

    class _ForceServing(DefaultArchiveStore):
        def _is_warehouse_index(self) -> bool:
            return True

    class _Index:
        def search(self, *args, **kwargs):
            return []

    monkeypatch.setattr("archive_cli.commands._resolve.resolve_store", lambda: _ForceServing(vault=vault, index=_Index(), access=_ctx(sources=("gmail",))))
    out = archive_search("note", limit=8)
    assert MEDICAL_UID not in out
    assert DENIED_TOKEN not in out
    assert GMAIL_UID in out or ALLOWED_TOKEN in out or "Gmail" in out

    env = {
        **os.environ,
        "PPA_PATH": str(vault),
        "PPA_ACCESS_ALLOWED_SOURCES": "gmail",
        "PPA_ACCESS_PRINCIPAL": "alice",
        "PPA_EMBEDDING_PROVIDER": "hash",
        "PPA_SERVING_INDEX_PATH": os.environ["PPA_SERVING_INDEX_PATH"],
        "PPA_INDEX_DSN": "postgresql://unused:unused@127.0.0.1:1/unused",
    }
    env.pop("PPA_TEST_PG_DSN", None)
    proc = subprocess.run(
        [sys.executable, "-c", "from archive_cli.serving_index import get_serving_handle; from archive_engine.access import access_request_fields; from archive_engine.contracts import AccessContext; from pathlib import Path; ctx=AccessContext(archive_id='p05b', principal='alice', profile='read-only', allowed_sources=('gmail',)); h=get_serving_handle(Path('.')); rows=h.search('note', limit=8, **access_request_fields(ctx)); print(rows)"],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert MEDICAL_UID not in proc.stdout
    assert DENIED_TOKEN not in proc.stdout
    assert DENIED_TOKEN not in proc.stderr
