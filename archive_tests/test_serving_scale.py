"""P01-D: million-vector production envelope is blocked, not waived."""

from __future__ import annotations

from archive_cli.serving_scale import (
    MILLION_VECTOR_N,
    PRODUCTION_VECTOR_DIM,
    estimate_vector_envelope,
    probe_million_vector_scale,
)


def test_million_vector_1536_envelope_is_measured() -> None:
    envelope = estimate_vector_envelope(n=MILLION_VECTOR_N, dimension=PRODUCTION_VECTOR_DIM)
    assert envelope["n"] == 1_000_000
    assert envelope["dimension"] == 1536
    assert envelope["embeddings_bytes"] == 1_000_000 * 1536 * 4
    assert envelope["nlist"] == 1000
    assert envelope["train_rss_bytes"] > envelope["query_rss_bytes"]


def test_million_vector_default_cap_is_blocked_not_waived() -> None:
    report = probe_million_vector_scale()
    assert report["profile"] == "million_vector"
    assert report["executed"] is False
    assert report["production_proven"] is False
    assert report["status"] == "blocked"
    assert report["reasons"]
    assert report["envelope"]["dimension"] == 1536
