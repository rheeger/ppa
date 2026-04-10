"""Tests for archive_sync.llm_enrichment.cache."""

from __future__ import annotations

from pathlib import Path

from archive_sync.llm_enrichment.cache import (InferenceCache,
                                               build_inference_cache_key)
from hfa.provenance import compute_input_hash


def test_build_inference_cache_key_matches_compute_input_hash() -> None:
    payload = {
        "content_hash": "aa",
        "model_id": "gemma4:31b",
        "prompt_version": "triage-v1",
        "schema_version": "bb",
        "temperature": 0.0,
        "seed": 42,
    }
    assert build_inference_cache_key(**payload) == compute_input_hash(payload)


def test_inference_cache_put_get_roundtrip(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    key = build_inference_cache_key(
        content_hash="c1",
        model_id="m1",
        prompt_version="p1",
        schema_version="s1",
        temperature=0.0,
        seed=42,
    )
    resp = {"classification": "noise", "confidence": 0.2}
    with InferenceCache(db) as cache:
        assert cache.get(key) is None
        cache.put(
            key,
            stage="triage",
            model_id="m1",
            prompt_version="p1",
            content_hash="c1",
            response=resp,
            tokens=(10, 20),
            latency_ms=12.5,
            run_id="run-a",
        )
        assert cache.get(key) == resp
        st = cache.stats()
        assert st["total"] == 1
        assert st["by_stage"]["triage"] == 1
        assert st["by_model"]["m1"] == 1
        assert st["by_run"]["run-a"] == 1


def test_inference_cache_different_prompt_version_miss(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    base = dict(
        content_hash="c1",
        model_id="m1",
        schema_version="s1",
        temperature=0.0,
        seed=42,
    )
    k1 = build_inference_cache_key(prompt_version="p1", **base)
    k2 = build_inference_cache_key(prompt_version="p2", **base)
    with InferenceCache(db) as cache:
        cache.put(
            k1,
            stage="triage",
            model_id="m1",
            prompt_version="p1",
            content_hash="c1",
            response={"a": 1},
            tokens=(1, 1),
            latency_ms=1.0,
        )
        assert cache.get(k1) == {"a": 1}
        assert cache.get(k2) is None


def test_purge_run(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    with InferenceCache(db) as cache:
        k = build_inference_cache_key(
            content_hash="c",
            model_id="m",
            prompt_version="p",
            schema_version="s",
            temperature=0.0,
            seed=1,
        )
        cache.put(
            k,
            stage="extract",
            model_id="m",
            prompt_version="p",
            content_hash="c",
            response={},
            tokens=(0, 0),
            latency_ms=0.0,
            run_id="r1",
        )
        assert cache.purge_run("r1") == 1
        assert cache.get(k) is None
