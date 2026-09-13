"""Mode-aware publish RAM/disk checks, before the catalog export starts."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from archive_engine.errors import IncompatibleStateError
from archive_engine.publication import (
    check_publication_resources,
    choose_publication_mode,
    estimate_publication_vectors,
    plan_publication_resources,
)


def test_choose_publication_mode_compacts_past_chain_cap() -> None:
    assert (
        choose_publication_mode(
            ready=True,
            parent_generation="gen-8",
            dirty_count=20,
            chain_depth=9,
        )
        == "compact"
    )
    assert (
        choose_publication_mode(
            ready=True,
            parent_generation="gen-3",
            dirty_count=20,
            chain_depth=4,
        )
        == "delta"
    )
    assert (
        choose_publication_mode(
            ready=False,
            parent_generation="",
            dirty_count=20,
            chain_depth=1,
        )
        == "full"
    )


def test_estimate_publication_vectors_clip_vs_reprint() -> None:
    assert estimate_publication_vectors(mode="delta", live_vectors=4_690_000, dirty_count=10) == 80
    assert estimate_publication_vectors(mode="compact", live_vectors=4_690_000, dirty_count=10) == 4_690_000


def test_plan_publication_resources_compact_needs_more_than_delta(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    root.mkdir()
    shared = {
        "index_root": root,
        "live_vectors": 100_000,
        "dirty_count": 20,
        "dimension": 8,
        "reclaim": False,
        "free_disk_bytes": 50 * 1024 * 1024 * 1024,
        "available_memory": 64 * 1024 * 1024 * 1024,
        "physical_memory": 64 * 1024 * 1024 * 1024,
        "rss_cap_mb": 32_768,
        "disk_budget_mb": 1_000_000,
    }
    clip = plan_publication_resources(mode="delta", chain_depth=3, **shared)
    reprint = plan_publication_resources(mode="compact", chain_depth=9, **shared)
    assert clip.ok
    assert reprint.ok
    assert reprint.estimated_disk_bytes > clip.estimated_disk_bytes
    assert reprint.estimated_rss_bytes > clip.estimated_rss_bytes


def test_plan_publication_resources_refuses_compact_when_disk_is_short(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    root.mkdir()
    plan = plan_publication_resources(
        root,
        mode="compact",
        live_vectors=4_000_000,
        dirty_count=20,
        chain_depth=9,
        dimension=1536,
        reclaim=False,
        free_disk_bytes=8 * 1024 * 1024 * 1024,
        available_memory=64 * 1024 * 1024 * 1024,
        physical_memory=64 * 1024 * 1024 * 1024,
        rss_cap_mb=32_768,
        disk_budget_mb=1_000_000,
    )
    assert plan.ok is False
    assert "disk_short" in plan.reasons
    with pytest.raises(IncompatibleStateError, match="publication_resources"):
        check_publication_resources(plan)


def test_plan_publication_resources_allows_incremental_on_same_short_disk(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    root.mkdir()
    plan = plan_publication_resources(
        root,
        mode="delta",
        live_vectors=4_000_000,
        dirty_count=20,
        chain_depth=3,
        dimension=1536,
        reclaim=False,
        free_disk_bytes=8 * 1024 * 1024 * 1024,
        available_memory=64 * 1024 * 1024 * 1024,
        physical_memory=64 * 1024 * 1024 * 1024,
        rss_cap_mb=32_768,
        disk_budget_mb=1_000_000,
    )
    assert plan.ok is True


def test_plan_publication_resources_reclaims_stale_export_tmp(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    stale = root / ".export-tmp" / "old"
    stale.mkdir(parents=True)
    (stale / "embeddings.bin").write_bytes(b"x" * 64)
    plan = plan_publication_resources(
        root,
        mode="delta",
        live_vectors=10,
        dirty_count=1,
        dimension=8,
        reclaim=True,
        free_disk_bytes=8 * 1024 * 1024 * 1024,
        available_memory=8 * 1024 * 1024 * 1024,
        physical_memory=8 * 1024 * 1024 * 1024,
        rss_cap_mb=32_768,
    )
    assert plan.ok
    assert plan.reclaimed_export_tmp_bytes >= 64
    assert not stale.exists()


def _write_chain(root: Path, *, depth: int, embeddings: int) -> str:
    parent = ""
    last = ""
    for idx in range(depth):
        gid = f"gen-{idx}"
        dest = root / "generations" / gid
        dest.mkdir(parents=True)
        (dest / "layout.json").write_text(
            json.dumps(
                {
                    "layout_version": 1,
                    "mode": "full" if idx == 0 else "delta",
                    "parent_generation": parent,
                    "base_generation": "gen-0",
                    "snapshot_id": gid,
                    "source_watermark": idx,
                    "tombstone_uids": [],
                    "tombstone_chunk_keys": [],
                    "replaced_uids": [],
                    "embedding_spec": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        count = embeddings if idx == 0 else 4
        (dest / "manifest.json").write_text(json.dumps({"embedding_count": count}) + "\n", encoding="utf-8")
        (dest / "embedding_keys.txt").write_text("k\n", encoding="utf-8")
        parent = gid
        last = gid
    (root / "ACTIVE").write_text(last + "\n", encoding="utf-8")
    return last


def test_publish_refuses_compact_before_warehouse_export(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_cli.serving_index import publish_serving_index

    root = tmp_path / "rust-search-index"
    last = _write_chain(root, depth=9, embeddings=4_000_000)
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    monkeypatch.setattr(
        "archive_cli.serving_index.serving_index_status",
        lambda _vault: {
            "serving_index_ready": True,
            "serving_index_generation": last,
            "serving_index_dirty_records": 1,
            "serving_index_format": 2,
        },
    )
    monkeypatch.setattr(
        "archive_engine.publication.shutil.disk_usage",
        lambda _path: MagicMock(free=2 * 1024 * 1024 * 1024, total=100 * 1024 * 1024 * 1024, used=98 * 1024 * 1024 * 1024),
    )
    monkeypatch.setattr("archive_engine.publication.get_serving_index_max_rss_mb", lambda: 32_768)
    store = MagicMock()
    store.vault = tmp_path
    store.index.schema = "ppa"
    result = publish_serving_index(store, dirty_uids=["uid-new"], dest_generation="gen-blocked")
    assert result["ok"] is False
    assert result["error"] == "publication_resources"
    assert "disk_short" in result["reasons"]
    store.index._connect.assert_not_called()
    assert not (root / "generations" / "gen-blocked").exists()
