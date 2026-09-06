"""P01-B synthetic ANN corpus helpers. Independent of the seed."""

from __future__ import annotations

import json
import struct
from pathlib import Path
from typing import Any

from archive_engine.contracts import CHUNK_EVIDENCE_REF_VERSION, UNKNOWN, ChunkEvidenceRef, EmbeddingSpec, SourceSpan
from archive_tests.acceptance.oracle import build_adversarial_modulo_ivf_dataset


def _spec(dimension: int = 8) -> EmbeddingSpec:
    return EmbeddingSpec(
        provider_namespace="hash",
        model="archive-hash-dev",
        model_revision="1",
        dimension=dimension,
        metric="cosine",
        normalization="l2",
        chunk_schema="6",
    )


def populated_burst_evidence(card_uid: str, chunk_key: str) -> ChunkEvidenceRef:
    """Synthetic burst fields for format freeze before burst generation exists."""

    span = SourceSpan(
        source_uid=card_uid,
        source_message_id="msg-p01b-1",
        source_revision="sha256:p01b-burst",
        representation="canonical_body_utf8",
        start_byte=0,
        end_byte=len("café\r\nuse port 8432".encode("utf-8")),
    )
    return ChunkEvidenceRef(
        version=CHUNK_EVIDENCE_REF_VERSION,
        archive_id="p01b-freeze",
        card_uid=card_uid,
        chunk_id=chunk_key,
        chunk_schema_version="6",
        algorithm_version="p01b-freeze-1",
        evidence_kind="derived",
        lineage_complete=True,
        parent_thread="thread-p01b",
        source_revisions=("sha256:p01b-burst", "sha256:p01b-parent"),
        source_spans=(span,),
        span_unavailable=False,
        message_refs_available=True,
        message_refs=(),
    )


def write_adversary_export(work: Path) -> dict[str, Any]:
    dataset = build_adversarial_modulo_ivf_dataset()
    work.mkdir(parents=True, exist_ok=True)
    cards = []
    chunks = []
    for item_id, _vector in dataset.items:
        cards.append(
            {
                "card_uid": item_id,
                "rel_path": f"Ann/{item_id}.md",
                "summary": item_id,
                "type": "note",
                "slug": item_id,
                "activity_at": "2026-09-06T00:00:00Z",
                "search_text": item_id,
                "people": [],
                "sources": ["acceptance"],
                "orgs": [],
                "corpus_state": "active",
                "retrieval_weight": 1.0,
                "aliases": [],
                "emails": [],
                "external_ids": [],
                "source_revision": f"sha256:{item_id}",
                "provenance_summary": UNKNOWN,
            }
        )
        evidence = populated_burst_evidence(item_id, item_id) if item_id == dataset.true_neighbor_id else ChunkEvidenceRef(
            version=CHUNK_EVIDENCE_REF_VERSION,
            archive_id="p01b-freeze",
            card_uid=item_id,
            chunk_id=item_id,
            chunk_schema_version="6",
            algorithm_version="p01b-freeze-1",
            evidence_kind=UNKNOWN,
            span_unavailable=True,
            message_refs_available=False,
        )
        chunks.append(
            {
                "chunk_key": item_id,
                "card_uid": item_id,
                "chunk_type": "body",
                "chunk_index": 0,
                "evidence": evidence.to_payload(),
            }
        )
    (work / "cards.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cards), encoding="utf-8")
    (work / "chunks.jsonl").write_text("".join(json.dumps(c) + "\n" for c in chunks), encoding="utf-8")
    (work / "edges.jsonl").write_text("", encoding="utf-8")
    (work / "embedding_keys.txt").write_text("\n".join(item_id for item_id, _ in dataset.items), encoding="utf-8")
    with (work / "embeddings.bin").open("wb") as fh:
        for _item_id, vector in dataset.items:
            fh.write(struct.pack(f"<{dataset.dimension}f", *[float(v) for v in vector]))
    spec = _spec(dataset.dimension)
    (work / "embedding_spec.json").write_text(json.dumps(spec.to_payload(), indent=2), encoding="utf-8")
    return {
        "dataset": dataset,
        "spec": spec,
        "work": work,
    }


def publish_adversary_generation(root: Path, *, generation_id: str = "gen-p01b-ann") -> dict[str, Any]:
    import archive_crate

    dest = root / "generations" / generation_id
    dest.mkdir(parents=True, exist_ok=True)
    work = dest / "_inbox"
    payload = write_adversary_export(work)
    dataset = payload["dataset"]
    spec = payload["spec"]
    train = {
        "nlist": dataset.nlist,
        "nprobe": dataset.nprobe,
        "train_sample": 100_000,
        "train_iters": 25,
        "seed": dataset.seed,
        "candidate_budget": 4096,
        "memory_mb": 8192,
        "embedding_spec": spec.to_payload(),
    }
    report = archive_crate.serving_index_build(
        str(dest),
        str(work / "cards.jsonl"),
        str(work / "chunks.jsonl"),
        str(work / "embedding_keys.txt"),
        str(work / "embeddings.bin"),
        dataset.dimension,
        str(work / "edges.jsonl"),
        json.dumps(train),
    )
    archive_crate.serving_index_publish(str(root), generation_id)
    archive_crate.serving_index_truncate_dirty(str(root))
    return {
        "generation": generation_id,
        "dest": dest,
        "dataset": dataset,
        "spec": spec,
        "build": report,
    }
