from __future__ import annotations

from pathlib import Path

import pytest

from archive_engine.adapters.serving_export import ExportFailed, install_streamed_embeddings, validate_staged_embeddings
from archive_engine.contracts import EmbeddingSpec
from archive_engine.publication import ServingSnapshot


def _spec(dim: int = 4) -> EmbeddingSpec:
    return EmbeddingSpec(
        provider_namespace="hash",
        model="test",
        model_revision="1",
        dimension=dim,
        metric="ip",
        normalization="none",
        chunk_schema="v1",
    )


def _snapshot(**kwargs) -> ServingSnapshot:
    return ServingSnapshot(
        snapshot_id="snap",
        source_watermark=1,
        cards=(),
        chunks=(),
        edges=(),
        embeddings=kwargs.pop("embeddings", ()),
        embedding_spec=_spec(),
        **kwargs,
    )


def test_missing_staged_files_do_not_fallback(tmp_path: Path) -> None:
    dest = tmp_path / "gen"
    dest.mkdir(parents=True, exist_ok=True)
    snap = _snapshot(
        embedding_keys_path=str(tmp_path / "missing.txt"), embeddings_bin_path=str(tmp_path / "missing.bin")
    )
    with pytest.raises(ExportFailed):
        install_streamed_embeddings(dest, snap)
    assert not (dest / "embedding_keys.txt").exists()


def test_unpaired_staged_paths_fail(tmp_path: Path) -> None:
    dest = tmp_path / "gen"
    dest.mkdir(parents=True, exist_ok=True)
    keys = tmp_path / "embedding_keys.txt"
    keys.write_text("k1\n", encoding="utf-8")
    snap = _snapshot(embedding_keys_path=str(keys), embeddings_bin_path="")
    with pytest.raises(ExportFailed):
        install_streamed_embeddings(dest, snap)


def test_truncated_bin_fails(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    keys = tmp_path / "embedding_keys.txt"
    binary = tmp_path / "embeddings.bin"
    keys.write_text("k1\nk2\n", encoding="utf-8")
    binary.write_bytes(b"\x00" * 4)
    with pytest.raises(ExportFailed):
        validate_staged_embeddings(keys, binary, dimension=4, expected_count=2)


def test_zero_eligible_vectors_is_complete_empty(tmp_path: Path) -> None:
    dest = tmp_path / "gen"
    dest.mkdir(parents=True, exist_ok=True)
    count = install_streamed_embeddings(dest, _snapshot(embeddings=()))
    assert count == 0
    assert (dest / "embedding_keys.txt").is_file()
