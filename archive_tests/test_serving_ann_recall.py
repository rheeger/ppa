"""P01-B: trained IVF beats the modulo-IVF adversary at selective probe sizes."""

from __future__ import annotations

import json
import resource
import time
from pathlib import Path

import pytest

from archive_tests.acceptance.ann_corpus import publish_adversary_generation
from archive_tests.acceptance.oracle import (
    build_adversarial_modulo_ivf_dataset,
    exact_nearest_neighbors,
    modulo_ivf_knn,
    recall_at_k,
)

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate  # noqa: E402


def _held_out_queries(dataset, *, holdout: bool) -> list[tuple[str, tuple[float, ...]]]:
    extras = [
        (item_id, vector)
        for index, (item_id, vector) in enumerate(dataset.items)
        if index not in {dataset.true_neighbor_index, dataset.unprobed_list} and index % 40 == 0
    ]
    mid = len(extras) // 2
    return extras[mid:] if holdout else extras[:mid]


def test_modulo_adversary_misses_true_neighbor() -> None:
    dataset = build_adversarial_modulo_ivf_dataset()
    assert dataset.nlist == 33
    assert dataset.nprobe == 32
    assert len(dataset.items) == 1089
    exact = exact_nearest_neighbors(dataset.query, dataset.items, k=5)
    assert exact[0].item_id == "ann-p04b-0065"
    simulated = modulo_ivf_knn(dataset.query, dataset.items, k=5)
    assert dataset.true_neighbor_id not in [row.item_id for row in simulated.neighbors]
    assert recall_at_k([row.item_id for row in simulated.neighbors], [dataset.true_neighbor_id], k=5) == 0.0


def test_trained_ivf_finds_adversary_without_scanning_all(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    published = publish_adversary_generation(root)
    dataset = published["dataset"]
    dest = published["dest"]
    manifest = json.loads((dest / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["serving_index_format_version"] == 2
    assert manifest["vector_impl"] == "ivf_centroids_v2"
    assert manifest["nlist"] == 33
    assert manifest["nprobe"] == 32
    assert (dest / "ivf_centroids.bin").stat().st_size == 33 * 8 * 4

    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.perf_counter()
    report = archive_crate.serving_index_ann_knn(str(dest), list(dataset.query), 5, 32, 4096)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    keys = [hit["key"] for hit in report["hits"]]
    exact = exact_nearest_neighbors(dataset.query, dataset.items, k=5)
    assert keys[0] == dataset.true_neighbor_id
    assert recall_at_k(keys, [dataset.true_neighbor_id], k=5) == 1.0
    assert report["nlist"] == 33
    assert report["nprobe"] == 32
    assert report["lists_probed"] == 32
    assert report["scanned_all"] is False
    assert report["candidates_scored"] < len(dataset.items)
    assert report["candidates_scored"] > 0
    (tmp_path / "ann_metrics.json").write_text(
        json.dumps(
            {
                "recall_at_5": 1.0,
                "modulo_recall_at_5": 0.0,
                "elapsed_ms": elapsed_ms,
                "rss_max_kb": max(rss_before, rss_after),
                "candidates_scored": report["candidates_scored"],
                "n": len(dataset.items),
                "exact": [row.item_id for row in exact],
                "ann": keys,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    handle = si.get_serving_handle(tmp_path)
    rows = handle.vector(list(dataset.query), limit=5, nprobe=32)
    assert rows and rows[0]["card_uid"] == dataset.true_neighbor_id
    assert rows[0]["ann_scanned_all"] is False
    evidence = rows[0].get("evidence_ref") or {}
    assert evidence.get("version") == 1
    assert evidence.get("span_unavailable") is False
    assert evidence.get("parent_thread") == "thread-p01b"


def test_held_out_queries_keep_selective_probe(tmp_path: Path) -> None:
    published = publish_adversary_generation(tmp_path / "rust-search-index", generation_id="gen-holdout")
    dataset = published["dataset"]
    dest = published["dest"]
    cal = _held_out_queries(dataset, holdout=False)
    hold = _held_out_queries(dataset, holdout=True)
    assert cal and hold
    for item_id, vector in cal + hold:
        exact = exact_nearest_neighbors(vector, dataset.items, k=1)
        report = archive_crate.serving_index_ann_knn(str(dest), list(vector), 1, 32, 4096)
        assert report["scanned_all"] is False
        assert report["hits"][0]["key"] == exact[0].item_id


def test_invalid_query_is_rejected(tmp_path: Path) -> None:
    published = publish_adversary_generation(tmp_path / "rust-search-index", generation_id="gen-invalid")
    dest = published["dest"]
    with pytest.raises(ValueError, match="non-finite"):
        archive_crate.serving_index_ann_knn(str(dest), [float("nan")] * 8, 5, 32, 4096)
    with pytest.raises(ValueError, match="dimension"):
        archive_crate.serving_index_ann_knn(str(dest), [1.0, 0.0], 5, 32, 4096)


def test_old_format_refuses_to_open(tmp_path: Path) -> None:
    dest = tmp_path / "generations" / "gen-old"
    dest.mkdir(parents=True)
    (dest / "manifest.json").write_text(json.dumps({"serving_index_format_version": 1, "vector_impl": "ivf_mmap_v1"}), encoding="utf-8")
    (dest / "embedding_keys.txt").write_text("ck-1\n", encoding="utf-8")
    (dest / "embeddings.bin").write_bytes(b"\x00" * 16)
    with pytest.raises(ValueError, match="serving_index_format_unsupported|ivf_centroids"):
        archive_crate.serving_index_ann_knn(str(dest), [1.0, 0.0, 0.0, 0.0], 1, 1, 16)
