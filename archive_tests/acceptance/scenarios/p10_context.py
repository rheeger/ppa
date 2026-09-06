"""P10-B acceptance: reply context plus a bounded high-degree graph."""

from __future__ import annotations

import json
import struct
import time
from pathlib import Path
from typing import Any

from archive_engine.context import expand_neighbors, neighbor_units_from_cards
from archive_tests.acceptance.corpus import build_cards, build_queries, build_relations, materialize_corpus
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register

REPLY = "hfa-email-message-p04breply01"
REQUEST = "hfa-email-message-p04breq0001"
STALE = "hfa-email-message-p04bstale01"
HUB = "hfa-person-p04balex0001"
ARCHIVE = "archive-p10b"


def _publish_hub(root: Path) -> None:
    import archive_crate

    gid = "gen-p10b-accept"
    dest = root / "generations" / gid
    work = dest / "_inbox"
    work.mkdir(parents=True)
    cards = [
        {
            "card_uid": HUB,
            "rel_path": "People/p04-alex-rivera-primary.md",
            "summary": "Alex Rivera",
            "type": "person",
            "slug": "p04-alex-rivera-primary",
            "activity_at": "2026-04-11T00:00:00Z",
            "search_text": "Alex Rivera",
            "people": ["Alex Rivera"],
            "sources": ["acceptance.p04b"],
            "orgs": [],
            "corpus_state": "active",
        }
    ]
    chunks = [{"chunk_key": "ck-hub", "card_uid": HUB, "chunk_type": "summary", "chunk_index": 0}]
    edges = []
    for index in range(64):
        uid = f"hfa-person-hubn{index:04d}"
        cards.append(
            {
                "card_uid": uid,
                "rel_path": f"People/hubn-{index:04d}.md",
                "summary": f"Neighbor {index}",
                "type": "person",
                "slug": f"hubn-{index:04d}",
                "activity_at": "2026-04-11T00:00:00Z",
                "search_text": f"Neighbor {index}",
                "people": ["Alex Rivera"],
                "sources": ["acceptance.p04b"],
                "orgs": [],
                "corpus_state": "active",
            }
        )
        chunks.append({"chunk_key": f"ck-n{index:04d}", "card_uid": uid, "chunk_type": "summary", "chunk_index": 0})
        edges.append(
            {
                "source_uid": HUB,
                "target_uid": uid,
                "edge_type": "wikilink",
                "field_name": "body",
                "method": "source_reported",
                "evidence_uids": [f"ev-n{index:04d}"],
            }
        )
    (work / "cards.jsonl").write_text("".join(json.dumps(card) + "\n" for card in cards), encoding="utf-8")
    (work / "chunks.jsonl").write_text("".join(json.dumps(chunk) + "\n" for chunk in chunks), encoding="utf-8")
    (work / "edges.jsonl").write_text("".join(json.dumps(edge) + "\n" for edge in edges), encoding="utf-8")
    keys = [str(chunk["chunk_key"]) for chunk in chunks]
    (work / "embedding_keys.txt").write_text("\n".join(keys), encoding="utf-8")
    with (work / "embeddings.bin").open("wb") as handle:
        for _key in keys:
            handle.write(struct.pack("<4f", 1.0, 0.0, 0.0, 0.0))
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


def run_p10_context(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    materialized = materialize_corpus(runtime.vault, owned_root=runtime.root)
    cards = [card for card in build_cards() if card["uid"] in {REPLY, REQUEST, STALE}]
    units = neighbor_units_from_cards(cards, archive_id=ARCHIVE)
    hit = next(item for item in units if item.uid == REPLY)
    ranked_alone = {"uid": hit.uid, "quoted_text": hit.text, "interpretable": "book the loft for April" in hit.text}
    expanded = expand_neighbors(hit, units, excluded_uids=(STALE,))
    query = next(item for item in build_queries() if item["query_id"] == "q-p04b-lex-auth-reply")
    relation = next(item for item in build_relations() if item["case_id"] == "rel-p04b-reply-is-authorization")
    if set(expanded.support_uids) != set(query["expected_uids"]):
        raise AssertionError(f"support set {expanded.support_uids} != {query['expected_uids']}")
    if STALE in expanded.support_uids or STALE not in relation["negative_evidence_ids"]:
        raise AssertionError("stale reply leaked into context")
    if not expanded.context or expanded.context[0].uid != REQUEST:
        raise AssertionError("reply did not expand the preceding request")
    if "authorized" in expanded.to_payload():
        raise AssertionError("authorized=true must not be emitted")

    import archive_crate

    _publish_hub(runtime.serving_index_path)
    native = archive_crate.serving_index_open(str(runtime.serving_index_path))
    graph = archive_crate.serving_index_graph_bounded(
        native,
        HUB,
        1,
        {"max_nodes": 100, "max_edges": 24, "max_elapsed_ms": 0},
    )
    if not graph.get("truncated") or graph.get("truncation_reason") != "max_edges":
        raise AssertionError(f"hub was not bounded: {graph}")
    if int(graph.get("edges_emitted") or 0) > 24:
        raise AssertionError("native traversal exceeded max_edges")

    payload = {
        "id": "p10.context.neighbor_and_graph",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "materialized_cards": materialized["card_count"],
        "ranked_alone": ranked_alone,
        "expanded": expanded.to_payload(),
        "matched_uids": [item.uid for item in expanded.matched],
        "context_uids": [item.uid for item in expanded.context],
        "excluded_uids": list(expanded.excluded_uids),
        "graph": {
            "truncated": graph["truncated"],
            "truncation_reason": graph["truncation_reason"],
            "nodes_visited": graph["nodes_visited"],
            "edges_emitted": graph["edges_emitted"],
            "frontier": graph["frontier"],
        },
    }
    artifact = Path(runtime.root.parent) / "p10-context.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


register(
    Scenario(
        id="p10.context.neighbor_and_graph",
        suite="p10",
        product_guarantee="An answer-bearing reply expands its preceding request with revision-checked spans, and a high-degree graph stays bounded",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p10-context.json",),
        run=run_p10_context,
    )
)
