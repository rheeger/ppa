"""Selective ANN proof at production dimension. Isolated generations only."""

from __future__ import annotations

import json
import math
import os
import random
import struct
from pathlib import Path
from typing import Any


def _unit(rng: random.Random, dim: int) -> list[float]:
    del rng
    raw = os.urandom(dim * 4)
    vals = list(struct.unpack(f"<{dim}f", raw))
    return _normalize(vals)


def _normalize(vals: list[float]) -> list[float]:
    clean = [v if math.isfinite(v) else 0.0 for v in vals]
    norm = math.sqrt(sum(v * v for v in clean)) or 1.0
    if norm == 1.0 and not any(clean):
        clean[0] = 1.0
        return clean
    return [v / norm for v in clean]


def run_selective_ann(root: Path, *, n: int, dim: int = 1536, seed: int = 31) -> dict[str, Any]:
    import archive_crate

    rng = random.Random(seed)
    work = Path(root) / f"ann-{n}x{dim}"
    dest = work / "generations" / "gen-pr31"
    inbox = dest / "_inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    query = _unit(rng, dim)
    neighbor = _normalize([query[i] + rng.gauss(0.0, 0.01) for i in range(dim)])
    keys = [f"v{i:08d}" for i in range(n)]
    cards = []
    chunks = []
    with (inbox / "embeddings.bin").open("wb") as handle:
        handle.write(struct.pack(f"<{dim}f", *neighbor))
        cards.append(
            {
                "card_uid": keys[0],
                "rel_path": f"Ann/{keys[0]}.md",
                "summary": keys[0],
                "type": "note",
                "slug": keys[0],
                "activity_at": "2026-09-06T00:00:00Z",
                "search_text": keys[0],
                "people": [],
                "sources": ["acceptance"],
                "orgs": [],
                "corpus_state": "active",
                "retrieval_weight": 1.0,
                "aliases": [],
                "emails": [],
                "external_ids": [],
                "source_revision": f"sha256:{keys[0]}",
                "provenance_summary": "unknown",
            }
        )
        chunks.append({"chunk_key": keys[0], "card_uid": keys[0], "chunk_type": "body", "chunk_index": 0})
        for key in keys[1:]:
            vec = _unit(rng, dim)
            handle.write(struct.pack(f"<{dim}f", *vec))
            cards.append(
                {
                    "card_uid": key,
                    "rel_path": f"Ann/{key}.md",
                    "summary": key,
                    "type": "note",
                    "slug": key,
                    "activity_at": "2026-09-06T00:00:00Z",
                    "search_text": key,
                    "people": [],
                    "sources": ["acceptance"],
                    "orgs": [],
                    "corpus_state": "active",
                    "retrieval_weight": 1.0,
                    "aliases": [],
                    "emails": [],
                    "external_ids": [],
                    "source_revision": f"sha256:{key}",
                    "provenance_summary": "unknown",
                }
            )
            chunks.append({"chunk_key": key, "card_uid": key, "chunk_type": "body", "chunk_index": 0})
    (inbox / "cards.jsonl").write_text("".join(json.dumps(row) + "\n" for row in cards), encoding="utf-8")
    (inbox / "chunks.jsonl").write_text("".join(json.dumps(row) + "\n" for row in chunks), encoding="utf-8")
    (inbox / "edges.jsonl").write_text("", encoding="utf-8")
    (inbox / "embedding_keys.txt").write_text("\n".join(keys) + "\n", encoding="utf-8")
    nlist = max(32, int(n**0.5))
    nprobe = max(1, nlist // 16)
    budget = max(64, n // 20)
    spec = {
        "provider_namespace": "hash",
        "model": "pr31-scale",
        "model_revision": "1",
        "dimension": dim,
        "metric": "ip",
        "normalization": "l2",
        "chunk_schema": "6",
    }
    (inbox / "embedding_spec.json").write_text(json.dumps(spec), encoding="utf-8")
    train = {
        "nlist": nlist,
        "nprobe": nprobe,
        "train_sample": min(n, 10_000),
        "train_iters": 8,
        "seed": seed,
        "candidate_budget": budget,
        "memory_mb": 8192,
        "embedding_spec": spec,
    }
    build = archive_crate.serving_index_build(
        str(dest),
        str(inbox / "cards.jsonl"),
        str(inbox / "chunks.jsonl"),
        str(inbox / "embedding_keys.txt"),
        str(inbox / "embeddings.bin"),
        dim,
        str(inbox / "edges.jsonl"),
        json.dumps(train),
    )
    report = archive_crate.serving_index_ann_knn(str(dest), query, 5, nprobe, budget)
    hit_keys = [hit["key"] for hit in report["hits"]]
    fraction = float(report["candidates_scored"]) / float(n)
    if keys[0] not in hit_keys[:5]:
        raise AssertionError(f"planted neighbor missed at {n}x{dim}: {hit_keys} report={report}")
    if report.get("scanned_all"):
        raise AssertionError(f"full scan at {n}x{dim}: {report}")
    if fraction > 0.10:
        raise AssertionError(f"candidate fraction {fraction:.3f} exceeds 10% at {n}x{dim}: {report}")
    import gc

    gc.collect()
    return {
        "n": n,
        "dimension": dim,
        "nlist": report.get("nlist", nlist),
        "nprobe": report.get("nprobe", nprobe),
        "candidates_scored": report["candidates_scored"],
        "candidate_fraction": fraction,
        "hits": hit_keys,
        "planted": keys[0],
        "build": build,
    }
