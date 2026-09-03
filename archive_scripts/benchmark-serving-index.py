#!/usr/bin/env python3
"""Benchmark ServingIndexHandle / store-method clocks against the cutover SLOs."""

from __future__ import annotations

import json
import os
import statistics
import time
from pathlib import Path

from archive_cli.index_config import (
    get_default_embedding_model,
    get_default_embedding_version,
)
from archive_cli.log import configure_logging
from archive_cli.store import get_archive_store

SLO_MS = {
    "search": 250,
    "query": 250,
    "timeline": 250,
    "temporal_neighbors": 250,
    "person": 250,
    "graph": 250,
    "evidence": 250,
    "vector_hit": 250,
    "hybrid_hit": 250,
    "vector_miss": 1000,
    "hybrid_miss": 1000,
}


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(int(round((p / 100.0) * (len(ordered) - 1))), len(ordered) - 1)
    return ordered[idx]


def _repeat(name: str, n: int, fn) -> dict:
    samples: list[float] = []
    last = None
    for _ in range(n):
        t0 = time.monotonic()
        last = fn()
        samples.append((time.monotonic() - t0) * 1000.0)
    count = 0
    if isinstance(last, dict):
        count = len(last.get("rows") or last.get("hits") or last.get("results") or last.get("graph") or [])
        if last.get("found") is True:
            count = 1
    elif isinstance(last, list):
        count = len(last)
    return {
        "op": name,
        "n": n,
        "p50_ms": round(statistics.median(samples), 2),
        "p95_ms": round(_pct(samples, 95), 2),
        "max_ms": round(max(samples), 2),
        "slo_ms": SLO_MS.get(name),
        "pass": SLO_MS.get(name) is None or _pct(samples, 95) <= SLO_MS[name],
        "last_count": count,
    }


def main() -> None:
    configure_logging()
    n = int(os.environ.get("PPA_SERVING_BENCH_N", "8"))
    store = get_archive_store()
    handle = store._serving()
    # Warm mmap / tantivy.
    handle.search("warmup", limit=3)
    model = get_default_embedding_model()
    version = get_default_embedding_version()

    rows = []
    rows.append(_repeat("search", n, lambda: store.search("Jane Smith", limit=8)))
    rows.append(_repeat("query", n, lambda: store.query(type_filter="person", limit=8)))
    rows.append(_repeat("timeline", n, lambda: store.timeline(start_date="2026-01-01", end_date="2026-03-31", limit=8)))
    rows.append(
        _repeat(
            "temporal_neighbors",
            n,
            lambda: store.temporal_neighbors("2026-03-10", limit=8),
        )
    )
    rows.append(_repeat("person", n, lambda: store.person("Jane Smith")))
    rows.append(_repeat("graph", n, lambda: store.graph("People/jane-smith.md", hops=2)))
    rows.append(_repeat("evidence", n, lambda: store.evidence(query="Jane Smith", limit=8)))

    # First hybrid/vector is a cache miss (provider); remaining are hits.
    miss = _repeat("hybrid_miss", 1, lambda: store.hybrid_search("Endaoment donor operations", limit=8))
    rows.append(miss)
    rows.append(_repeat("hybrid_hit", n, lambda: store.hybrid_search("Endaoment donor operations", limit=8)))
    rows.append(_repeat("vector_miss", 1, lambda: store.vector_search("board dinner with Jane", limit=8)))
    rows.append(_repeat("vector_hit", n, lambda: store.vector_search("board dinner with Jane", limit=8)))

    # Engine-only: skip provider, clock handle.vector / search.
    query_vector = store._embed_query(
        "Endaoment donor operations",
        model=model,
        version=version,
        phases=__import__("archive_cli.query_timing", fromlist=["QueryPhaseTimes"]).QueryPhaseTimes(),
    )
    rows.append(_repeat("handle_search", n, lambda: handle.search("Jane Smith", limit=8)))
    rows.append(_repeat("handle_vector", n, lambda: handle.vector(query_vector, limit=8)))
    rows.append(_repeat("handle_hybrid", n, lambda: handle.hybrid("Jane Smith", query_vector, limit=8)))

    payload = {
        "vault": str(store.vault),
        "generation": handle.generation_id,
        "embedding_model": model,
        "embedding_version": version,
        "n": n,
        "rows": rows,
        "all_pass": all(r.get("pass", True) for r in rows),
    }
    print(json.dumps(payload, indent=2))
    out = Path(os.environ.get("PPA_SERVING_BENCH_OUT", "logs/serving-index-bench.json"))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
