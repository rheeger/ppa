"""P10-B: native graph node/edge/depth budgets and high-degree truncation."""

from __future__ import annotations

import json
import struct
from pathlib import Path

import pytest

from archive_cli.serving_index import get_serving_handle

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate  # noqa: E402

HUB = "hfa-person-p04balex0001"
DENIED = "hfa-email-message-denied01"
SECRET = "hfa-email-message-secret01"
OK_A = "hfa-person-huba0001"
OK_B = "hfa-person-hubb0001"
MULTI = "hfa-note-multichunk01"


def _card(uid: str, rel_path: str, sources: list[str], summary: str = "") -> dict:
    return {
        "card_uid": uid,
        "rel_path": rel_path,
        "summary": summary or uid,
        "type": "person" if uid.startswith("hfa-person") else "email_message" if "email" in uid else "note",
        "slug": rel_path.removesuffix(".md").rsplit("/", 1)[-1],
        "activity_at": "2026-04-11T00:00:00Z",
        "search_text": summary or uid,
        "people": ["Alex Rivera"],
        "sources": sources,
        "required_sources": sources,
        "orgs": [],
        "corpus_state": "active",
        "source_revision": f"sha256:{uid}",
    }


def _edge(source: str, target: str, *, edge_type: str = "wikilink", evidence: str = "") -> dict:
    return {
        "source_uid": source,
        "target_uid": target,
        "edge_type": edge_type,
        "field_name": "body",
        "method": "source_reported",
        "evidence_uids": [evidence or f"ev-{target}"],
        "confidence": 1.0,
    }


def _publish_hub(root: Path, *, neighbors: int = 80) -> str:
    gid = "gen-p10b-hub"
    dest = root / "generations" / gid
    work = dest / "_inbox"
    work.mkdir(parents=True)
    cards = [
        _card(HUB, "People/p04-alex-rivera-primary.md", ["gmail:personal"], "Alex Rivera"),
        _card(OK_A, "People/ok-a.md", ["gmail:personal"], "Ok A"),
        _card(OK_B, "People/ok-b.md", ["gmail:personal"], "Ok B"),
        _card(DENIED, "Email/denied.md", ["gmail:other"], "Denied"),
        _card(SECRET, "Email/secret.md", ["gmail:other"], "Secret"),
        _card(MULTI, "Notes/multi.md", ["gmail:personal"], "Multi chunk"),
    ]
    edges = [
        _edge(HUB, OK_A, evidence="ev-oka"),
        _edge(HUB, OK_B, evidence="ev-okb"),
        _edge(HUB, DENIED, evidence="ev-denied"),
        _edge(DENIED, SECRET, evidence="ev-secret"),
        _edge(HUB, MULTI, edge_type="mentions", evidence="ev-multi"),
    ]
    chunks = [
        {"chunk_key": "ck-hub", "card_uid": HUB, "chunk_type": "summary", "chunk_index": 0},
        {"chunk_key": "ck-multi-0", "card_uid": MULTI, "chunk_type": "body", "chunk_index": 0},
        {"chunk_key": "ck-multi-1", "card_uid": MULTI, "chunk_type": "body", "chunk_index": 1},
        {"chunk_key": "ck-multi-2", "card_uid": MULTI, "chunk_type": "body", "chunk_index": 2},
    ]
    for index in range(neighbors):
        uid = f"hfa-person-hubn{index:04d}"
        cards.append(_card(uid, f"People/hubn-{index:04d}.md", ["gmail:personal"], f"Neighbor {index}"))
        edges.append(_edge(HUB, uid, evidence=f"ev-n{index:04d}"))
        chunks.append({"chunk_key": f"ck-n{index:04d}", "card_uid": uid, "chunk_type": "summary", "chunk_index": 0})
    dim = 4
    (work / "cards.jsonl").write_text("".join(json.dumps(card) + "\n" for card in cards), encoding="utf-8")
    (work / "chunks.jsonl").write_text("".join(json.dumps(chunk) + "\n" for chunk in chunks), encoding="utf-8")
    (work / "edges.jsonl").write_text("".join(json.dumps(edge) + "\n" for edge in edges), encoding="utf-8")
    keys = [str(chunk["chunk_key"]) for chunk in chunks]
    (work / "embedding_keys.txt").write_text("\n".join(keys), encoding="utf-8")
    with (work / "embeddings.bin").open("wb") as handle:
        for index, _key in enumerate(keys):
            vector = [1.0 if index == 0 else 0.0, 0.0, 0.0, 0.0]
            handle.write(struct.pack(f"<{dim}f", *vector))
    archive_crate.serving_index_build(
        str(dest),
        str(work / "cards.jsonl"),
        str(work / "chunks.jsonl"),
        str(work / "embedding_keys.txt"),
        str(work / "embeddings.bin"),
        dim,
        str(work / "edges.jsonl"),
    )
    archive_crate.serving_index_publish(str(root), gid)
    return gid


def test_high_degree_hub_truncates_in_native_traversal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    _publish_hub(root, neighbors=80)
    handle = get_serving_handle(tmp_path)
    payload = handle.graph_bounded(
        HUB,
        hops=1,
        max_nodes=100,
        max_edges=32,
        max_elapsed_ms=0,
    )
    assert payload["truncated"] is True
    assert payload["truncation_reason"] == "max_edges"
    assert payload["edges_emitted"] <= 32
    assert payload["nodes_visited"] <= 100
    assert payload["depth"] == 1
    graph = payload["graph"]
    hub_edges = graph.get("People/p04-alex-rivera-primary.md") or graph.get(HUB)
    assert hub_edges
    assert len(hub_edges) <= 32
    assert hub_edges[0]["method"] == "source_reported"
    assert hub_edges[0]["evidence_uids"]
    assert payload["frontier"]


def test_denied_neighbors_absent_and_relation_filter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    _publish_hub(root, neighbors=0)
    handle = get_serving_handle(tmp_path)
    payload = handle.graph_bounded(
        HUB,
        hops=2,
        max_nodes=32,
        max_edges=32,
        max_elapsed_ms=0,
        access_restricted=True,
        access_sources=["gmail:personal"],
    )
    dumped = json.dumps(payload)
    assert DENIED not in dumped
    assert SECRET not in dumped
    assert "Email/denied.md" not in dumped
    neighbors = []
    for edges in payload["graph"].values():
        neighbors.extend(edge.get("path") or edge.get("neighbor_uid") or "" for edge in edges)
    assert "People/ok-a.md" in neighbors
    assert "People/ok-b.md" in neighbors
    filtered = handle.graph_bounded(
        HUB,
        hops=1,
        max_nodes=32,
        max_edges=32,
        max_elapsed_ms=0,
        allowed_relation_types=["mentions"],
    )
    mention_paths = [edge.get("path") for edges in filtered["graph"].values() for edge in edges]
    assert "Notes/multi.md" in mention_paths
    assert "People/ok-a.md" not in mention_paths


def test_adjacent_chunks_follow_chunk_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    _publish_hub(root, neighbors=0)
    handle = get_serving_handle(tmp_path)
    middle = handle.adjacent_chunks("ck-multi-1")
    assert middle is not None
    assert middle["preceding"] == "ck-multi-0"
    assert middle["following"] == "ck-multi-2"
    assert middle["card_uid"] == MULTI
