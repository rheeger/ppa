from __future__ import annotations

from pathlib import Path

import pytest

from archive_engine.errors import IncompatibleStateError
from archive_engine.publication import validate_generation


def test_require_native_open_rejects_unreadable_candidate(tmp_path: Path) -> None:
    dest = tmp_path / "gen"
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "manifest.json").write_text(
        '{"serving_index_format_version": 2, "card_count": 1, "embedding_spec": {"dimension": 4}}',
        encoding="utf-8",
    )
    (dest / "cards.jsonl").write_text('{"card_uid": "c1"}\n', encoding="utf-8")
    (dest / "chunks.jsonl").write_text('{"chunk_key": "k1", "card_uid": "c1"}\n', encoding="utf-8")
    (dest / "edges.jsonl").write_text("", encoding="utf-8")
    (dest / "embedding_keys.txt").write_text("k1\n", encoding="utf-8")
    (dest / "embeddings.bin").write_bytes(b"\x00" * 16)
    (dest / "ivf_meta.json").write_text("{}", encoding="utf-8")
    with pytest.raises(IncompatibleStateError, match="native_open"):
        validate_generation(dest, require_native_open=True)


def test_file_validation_still_works_without_native_open(tmp_path: Path) -> None:
    dest = tmp_path / "gen"
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "manifest.json").write_text('{"serving_index_format_version": 2, "card_count": 0}', encoding="utf-8")
    (dest / "cards.jsonl").write_text("", encoding="utf-8")
    (dest / "chunks.jsonl").write_text("", encoding="utf-8")
    (dest / "edges.jsonl").write_text("", encoding="utf-8")
    (dest / "embedding_keys.txt").write_text("", encoding="utf-8")
    report = validate_generation(dest)
    assert report["ok"] is True
    assert report["native_open"] == "skipped"
