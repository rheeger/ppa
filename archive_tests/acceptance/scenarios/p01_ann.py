"""P01-B acceptance: trained IVF beats modulo-IVF on the frozen adversary."""

from __future__ import annotations

import time
from typing import Any

import archive_crate
from archive_tests.acceptance.ann_corpus import publish_adversary_generation
from archive_tests.acceptance.environment import IsolatedRuntime, reset_serving_handle
from archive_tests.acceptance.oracle import exact_nearest_neighbors, modulo_ivf_knn, recall_at_k
from archive_tests.acceptance.registry import Scenario, register


def run_p01_ann(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    reset_serving_handle()
    published = publish_adversary_generation(runtime.serving_index_path)
    dataset = published["dataset"]
    dest = published["dest"]
    exact = exact_nearest_neighbors(dataset.query, dataset.items, k=5)
    modulo = modulo_ivf_knn(dataset.query, dataset.items, k=5)
    modulo_keys = [row.item_id for row in modulo.neighbors]
    if dataset.true_neighbor_id in modulo_keys:
        raise AssertionError("modulo-IVF adversary is not a failure; old algorithm found the true neighbor")
    report = archive_crate.serving_index_ann_knn(str(dest), list(dataset.query), 5, 32, 4096)
    keys = [hit["key"] for hit in report["hits"]]
    if keys[:1] != [dataset.true_neighbor_id]:
        raise AssertionError(f"trained IVF missed planted neighbor: {keys}")
    if report["scanned_all"] or report["candidates_scored"] >= len(dataset.items):
        raise AssertionError(f"pass-by-full-scan: {report}")
    if report["nprobe"] != 32 or report["nlist"] != 33:
        raise AssertionError(f"selective probe settings lost: {report}")
    return {
        "id": "p01.ann.modulo_adversary",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "exact": [row.item_id for row in exact],
        "modulo": modulo_keys,
        "ann": keys,
        "recall_at_5": recall_at_k(keys, [dataset.true_neighbor_id], k=5),
        "modulo_recall_at_5": recall_at_k(modulo_keys, [dataset.true_neighbor_id], k=5),
        "candidates_scored": report["candidates_scored"],
        "n": len(dataset.items),
        "nlist": report["nlist"],
        "nprobe": report["nprobe"],
        "generation": published["generation"],
        "build": published["build"],
    }


register(
    Scenario(
        id="p01.ann.modulo_adversary",
        suite="p01",
        product_guarantee="Trained IVF finds the planted neighbor that modulo-IVF misses without scanning every vector",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine", "hash_embeddings"),
        expected_artifacts=("p01-ann.json",),
        run=run_p01_ann,
    )
)
