"""Tests for ``ppa embed-cache-rotate`` (move + prune logic)."""

from __future__ import annotations

import logging
from pathlib import Path

from archive_cli.commands.batch_embed import embed_cache_rotate


def _make_batch_files(d: Path, count: int) -> list[Path]:
    d.mkdir(parents=True, exist_ok=True)
    paths = []
    for i in range(count):
        p = d / f"batch_fake{i:03d}-out.jsonl"
        p.write_text(f"row-{i}\n", encoding="utf-8")
        paths.append(p)
    return paths


def test_rotate_dry_run_does_not_move_or_prune(tmp_path: Path) -> None:
    art = tmp_path / "batches"
    cache = tmp_path / "cache"
    files = _make_batch_files(art, 3)
    out = embed_cache_rotate(
        logger=logging.getLogger("test"),
        artifact_dir=str(art),
        cache_dir=str(cache),
        keep=1,
        dry_run=True,
    )
    assert out["dry_run"] is True
    assert out["moved"] == 3
    # Dry-run still reports the proposed run dir so operators see where
    # files would land, but the dir is not created on disk.
    proposed = Path(out["new_run"])
    assert proposed.parent == cache
    assert proposed.name.startswith("run-")
    assert all(f.exists() for f in files)
    assert not cache.exists()


def test_rotate_moves_files_into_new_run_dir(tmp_path: Path) -> None:
    art = tmp_path / "batches"
    cache = tmp_path / "cache"
    files = _make_batch_files(art, 4)
    out = embed_cache_rotate(
        logger=logging.getLogger("test"),
        artifact_dir=str(art),
        cache_dir=str(cache),
        keep=1,
        dry_run=False,
    )
    assert out["moved"] == 4
    new_run = Path(out["new_run"])
    assert new_run.exists()
    assert new_run.parent == cache
    assert sorted(p.name for p in new_run.glob("*-out.jsonl")) == sorted(
        f.name for f in files
    )
    # Manifest written.
    assert (new_run / "MANIFEST.txt").is_file()
    # Originals were moved (not copied).
    assert not list(art.glob("*-out.jsonl"))


def test_rotate_prunes_to_keep_n(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    art = tmp_path / "batches"
    # Pre-existing older runs, lexicographically earlier than the new one.
    for run_name in ("run-20250101-000000Z", "run-20250201-000000Z", "run-20250301-000000Z"):
        d = cache / run_name
        d.mkdir(parents=True)
        (d / "batch_old-out.jsonl").write_text("x", encoding="utf-8")
    _make_batch_files(art, 1)
    out = embed_cache_rotate(
        logger=logging.getLogger("test"),
        artifact_dir=str(art),
        cache_dir=str(cache),
        keep=2,
        dry_run=False,
    )
    remaining = sorted(p.name for p in cache.glob("run-*"))
    assert len(remaining) == 2
    assert any(name.startswith("run-202") and name >= "run-20250301" for name in remaining)
    assert "run-20250101-000000Z" not in remaining


def test_rotate_no_files_no_op(tmp_path: Path) -> None:
    art = tmp_path / "batches"
    cache = tmp_path / "cache"
    art.mkdir()
    out = embed_cache_rotate(
        logger=logging.getLogger("test"),
        artifact_dir=str(art),
        cache_dir=str(cache),
        keep=1,
        dry_run=False,
    )
    assert out["moved"] == 0
    assert out["new_run"] == ""
    assert not cache.exists()


def test_rotate_keep_one_with_no_existing_runs(tmp_path: Path) -> None:
    art = tmp_path / "batches"
    cache = tmp_path / "cache"
    _make_batch_files(art, 2)
    out = embed_cache_rotate(
        logger=logging.getLogger("test"),
        artifact_dir=str(art),
        cache_dir=str(cache),
        keep=1,
        dry_run=False,
    )
    runs = list(cache.glob("run-*"))
    assert len(runs) == 1
    assert out["moved"] == 2
