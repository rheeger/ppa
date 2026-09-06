"""P02-B: incremental and full publish resolve the same live search universe."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_engine.contracts import (
    CHUNK_EVIDENCE_REF_VERSION,
    UNKNOWN,
    ChunkEvidenceRef,
    EmbeddingSpec,
    MessageEvidenceRef,
    SourceSpan,
)
from archive_engine.errors import IncompatibleContractError
from archive_engine.publication import (
    ServingSnapshot,
    diff_universes,
    publish_snapshot,
    resolve_live_universe,
    should_compact,
)

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate  # noqa: E402
from archive_cli import serving_index as si  # noqa: E402
from archive_cli.serving_index import get_serving_handle  # noqa: E402

SPEC = EmbeddingSpec(
    provider_namespace="hash",
    model="archive-hash-dev",
    model_revision="1",
    dimension=4,
    metric="cosine",
    normalization="l2",
    chunk_schema="6",
)


def _card(uid: str, summary: str, *, activity: str = "2026-09-06T00:00:00Z") -> dict:
    return {
        "card_uid": uid,
        "rel_path": f"Cards/{uid}.md",
        "summary": summary,
        "type": "email_message" if uid.startswith("hfa-email") else "person",
        "slug": uid,
        "activity_at": activity,
        "search_text": summary,
        "people": [summary],
        "sources": ["acceptance"],
        "orgs": [],
        "corpus_state": "active",
        "retrieval_weight": 1.0,
        "aliases": [],
        "emails": [],
        "external_ids": [],
        "source_revision": f"sha256:{uid}:{summary}",
        "provenance_summary": "manual",
    }


def _evidence(uid: str, key: str, *, schema: str = "6", burst: bool = False) -> dict:
    span = SourceSpan(
        source_uid=uid,
        source_message_id=f"msg-{uid}",
        source_revision=f"sha256:{key}",
        representation="canonical_body_utf8",
        start_byte=0,
        end_byte=12,
    )
    ref = ChunkEvidenceRef(
        version=CHUNK_EVIDENCE_REF_VERSION,
        archive_id="p02b",
        card_uid=uid,
        chunk_id=key,
        chunk_schema_version=schema,
        algorithm_version="p01b-freeze-1",
        evidence_kind="derived" if burst else UNKNOWN,
        lineage_complete=burst,
        parent_thread="thread-p02b" if burst else "",
        source_revisions=(f"sha256:{key}",) if burst else (),
        source_spans=(span,) if burst else (),
        span_unavailable=not burst,
        message_refs=(MessageEvidenceRef(message_id=f"msg-{uid}", source_revision=f"sha256:{key}", spans=(span,)),)
        if burst
        else (),
        message_refs_available=burst,
    )
    return ref.to_payload()


def _chunk(uid: str, key: str, *, schema: str = "6", burst: bool = False) -> dict:
    return {
        "chunk_key": key,
        "card_uid": uid,
        "chunk_type": "body",
        "chunk_index": 0,
        "evidence": _evidence(uid, key, schema=schema, burst=burst),
    }


def _edge(src: str, tgt: str, etype: str = "mentions", *, method: str = UNKNOWN, confidence: float = 1.0) -> dict:
    return {
        "source_uid": src,
        "target_uid": tgt,
        "edge_type": etype,
        "field_name": "body",
        "method": method,
        "confidence": confidence,
        "evidence_uids": [src],
        "direction": "forward",
        "trust": confidence,
    }


def _vec(*ones: int) -> tuple[float, ...]:
    out = [0.0, 0.0, 0.0, 0.0]
    for idx in ones:
        out[idx] = 1.0
    return tuple(out)


def _snapshot(
    *,
    cards: list[dict],
    chunks: list[dict],
    edges: list[dict],
    embeddings: list[tuple[str, tuple[float, ...]]],
    deleted: tuple[str, ...] = (),
    dirty: tuple[str, ...] = (),
    name: str = "snap",
    spec: EmbeddingSpec = SPEC,
) -> ServingSnapshot:
    return ServingSnapshot(
        snapshot_id=name,
        source_watermark=1,
        cards=tuple(cards),
        chunks=tuple(chunks),
        edges=tuple(edges),
        embeddings=tuple(embeddings),
        embedding_spec=spec,
        deleted_uids=deleted,
        dirty_uids=dirty,
    )


def _base_state() -> dict[str, object]:
    keep = _card("hfa-person-keep000001", "Keep Person")
    gone = _card("hfa-person-gone000001", "Gone Person")
    msg = _card("hfa-email-msg00000001", "Old Message")
    return {
        "cards": [keep, gone, msg],
        "chunks": [
            _chunk("hfa-person-keep000001", "ck-keep", burst=True),
            _chunk("hfa-person-gone000001", "ck-gone"),
            _chunk("hfa-email-msg00000001", "ck-msg-old", schema="5"),
        ],
        "edges": [
            _edge("hfa-person-keep000001", "hfa-person-gone000001"),
            _edge("hfa-email-msg00000001", "hfa-person-keep000001", "from", method="inferred", confidence=0.61),
        ],
        "embeddings": [
            ("ck-keep", _vec(0)),
            ("ck-gone", _vec(1)),
            ("ck-msg-old", _vec(2)),
        ],
    }


def _mutated_full() -> dict[str, object]:
    keep = _card("hfa-person-keep000001", "Keep Person")
    msg = _card("hfa-email-msg00000001", "New Message")
    added = _card("hfa-person-new0000001", "New Neighbor")
    return {
        "cards": [keep, msg, added],
        "chunks": [
            _chunk("hfa-person-keep000001", "ck-keep", burst=True),
            _chunk("hfa-email-msg00000001", "ck-msg-new", schema="6", burst=True),
            _chunk("hfa-person-new0000001", "ck-new"),
        ],
        "edges": [
            _edge("hfa-email-msg00000001", "hfa-person-new0000001", "from", method="inferred", confidence=0.8),
            _edge("hfa-person-keep000001", "hfa-person-new0000001"),
        ],
        "embeddings": [
            ("ck-keep", _vec(0)),
            ("ck-msg-new", _vec(3)),
            ("ck-new", _vec(1)),
        ],
    }


def _delta_from_base() -> ServingSnapshot:
    return _snapshot(
        cards=[
            _card("hfa-email-msg00000001", "New Message"),
            _card("hfa-person-new0000001", "New Neighbor"),
        ],
        chunks=[
            _chunk("hfa-email-msg00000001", "ck-msg-new", schema="6", burst=True),
            _chunk("hfa-person-new0000001", "ck-new"),
        ],
        edges=[
            _edge("hfa-email-msg00000001", "hfa-person-new0000001", "from", method="inferred", confidence=0.8),
            _edge("hfa-person-keep000001", "hfa-person-new0000001"),
        ],
        embeddings=[("ck-msg-new", _vec(3)), ("ck-new", _vec(1))],
        deleted=("hfa-person-gone000001",),
        dirty=("hfa-person-gone000001", "hfa-email-msg00000001", "hfa-person-new0000001"),
        name="delta",
    )


def _publish_full(root: Path, state: dict[str, object], gid: str) -> None:
    publish_snapshot(
        root,
        _snapshot(
            cards=list(state["cards"]),  # type: ignore[arg-type]
            chunks=list(state["chunks"]),  # type: ignore[arg-type]
            edges=list(state["edges"]),  # type: ignore[arg-type]
            embeddings=list(state["embeddings"]),  # type: ignore[arg-type]
            name=f"full-{gid}",
        ),
        generation_id=gid,
        mode="full",
    )


def test_full_and_incremental_universes_match(tmp_path: Path, monkeypatch) -> None:
    si._HANDLE = None
    full_root = tmp_path / "full"
    delta_root = tmp_path / "delta"
    _publish_full(full_root, _mutated_full(), "gen-full")
    _publish_full(delta_root, _base_state(), "gen-base")
    receipt = publish_snapshot(
        delta_root,
        _delta_from_base(),
        generation_id="gen-delta",
        parent_generation="gen-base",
        mode="delta",
    )
    assert receipt.mode == "delta"
    dest = delta_root / "generations" / "gen-delta"
    dest_uids = {
        json.loads(line)["card_uid"]
        for line in (dest / "cards.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    assert dest_uids == {"hfa-email-msg00000001", "hfa-person-new0000001"}
    assert (delta_root / "generations" / "gen-base").exists()
    left = resolve_live_universe(full_root)
    right = resolve_live_universe(delta_root)
    diff = diff_universes(left, right)
    assert diff.unexplained == 0, diff
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(delta_root))
    handle = get_serving_handle(tmp_path)
    uids = {row.get("card_uid") for row in handle.query(limit=20)}
    assert "hfa-person-gone000001" not in uids
    assert "hfa-person-new0000001" in uids
    assert handle.search("Gone Person", limit=5) == [] or all(
        row.get("card_uid") != "hfa-person-gone000001" for row in handle.search("Gone Person", limit=5)
    )
    graph = handle.graph("Cards/hfa-person-keep000001.md", hops=1)
    blob = json.dumps(graph)
    assert "hfa-person-gone000001" not in blob
    assert "ck-msg-old" not in json.dumps(archive_crate.serving_index_resolve_layout(str(delta_root)))
    new_hits = handle.vector(list(_vec(3)), limit=5)
    assert new_hits
    assert new_hits[0]["card_uid"] == "hfa-email-msg00000001"
    handle.close()
    si._HANDLE = None


def test_embedding_only_update_changes_vector_hits(tmp_path: Path, monkeypatch) -> None:
    si._HANDLE = None
    root = tmp_path / "idx"
    _publish_full(root, _base_state(), "gen-base")
    publish_snapshot(
        root,
        _snapshot(
            cards=[_card("hfa-email-msg00000001", "Old Message")],
            chunks=[_chunk("hfa-email-msg00000001", "ck-msg-new", schema="6", burst=True)],
            edges=[],
            embeddings=[("ck-msg-new", _vec(3))],
            dirty=("hfa-email-msg00000001",),
            name="embed-only",
        ),
        generation_id="gen-embed",
        parent_generation="gen-base",
        mode="delta",
    )
    live = resolve_live_universe(root)
    assert "ck-msg-old" not in live.live_chunk_keys
    assert "ck-msg-new" in live.live_chunk_keys
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    handle = get_serving_handle(tmp_path)
    hits = handle.vector(list(_vec(3)), limit=5)
    assert hits and hits[0]["card_uid"] == "hfa-email-msg00000001"
    handle.close()
    si._HANDLE = None


def test_empty_generation_opens(tmp_path: Path, monkeypatch) -> None:
    si._HANDLE = None
    root = tmp_path / "empty"
    publish_snapshot(
        root,
        _snapshot(cards=[], chunks=[], edges=[], embeddings=[], name="empty"),
        generation_id="gen-empty",
        mode="full",
    )
    live = resolve_live_universe(root)
    assert live.live_uids == ()
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    handle = get_serving_handle(tmp_path)
    assert handle.search("anything", limit=5) == []
    assert handle.vector([1.0, 0.0, 0.0, 0.0], limit=5) == []
    handle.close()
    si._HANDLE = None


def test_mixed_embedding_spec_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "mix"
    _publish_full(root, _base_state(), "gen-base")
    other = EmbeddingSpec(
        provider_namespace="hash",
        model="other-model",
        model_revision="1",
        dimension=8,
        metric="cosine",
        normalization="l2",
        chunk_schema="6",
    )
    with pytest.raises(IncompatibleContractError, match="EmbeddingSpec"):
        publish_snapshot(
            root,
            _snapshot(
                cards=[_card("hfa-person-new0000001", "New")],
                chunks=[],
                edges=[],
                embeddings=[],
                dirty=("hfa-person-new0000001",),
                name="bad-spec",
                spec=other,
            ),
            generation_id="gen-bad",
            parent_generation="gen-base",
            mode="delta",
        )


def test_compaction_matches_full(tmp_path: Path) -> None:
    si._HANDLE = None
    full_root = tmp_path / "full"
    compact_root = tmp_path / "compact"
    _publish_full(full_root, _mutated_full(), "gen-full")
    _publish_full(compact_root, _base_state(), "gen-base")
    mutated = _mutated_full()
    receipt = publish_snapshot(
        compact_root,
        _snapshot(
            cards=list(mutated["cards"]),  # type: ignore[arg-type]
            chunks=list(mutated["chunks"]),  # type: ignore[arg-type]
            edges=list(mutated["edges"]),  # type: ignore[arg-type]
            embeddings=list(mutated["embeddings"]),  # type: ignore[arg-type]
            name="compact",
        ),
        generation_id="gen-compact",
        parent_generation="gen-base",
        mode="compact",
        force_compact=True,
    )
    assert receipt.mode == "compact"
    assert receipt.parent_generation == ""
    diff = diff_universes(resolve_live_universe(full_root), resolve_live_universe(compact_root))
    assert diff.unexplained == 0, diff
    assert should_compact(chain_depth=2, max_depth=1) is True
    si._HANDLE = None
