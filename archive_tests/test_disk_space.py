"""Finder-available disk space, not the unpurgeable remainder."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.embedder import EmbedderMixin
from archive_engine.disk_space import volume_available_bytes


def test_volume_available_prefers_finder_capacity(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "archive_engine.disk_space._important_usage_bytes",
        lambda _path: 113 * 1_000_000_000,
    )
    monkeypatch.setattr(
        "archive_engine.disk_space.shutil.disk_usage",
        lambda _path: type("Usage", (), {"free": 39 * 1024**3})(),
    )
    assert volume_available_bytes(tmp_path) == 113 * 1_000_000_000


def test_volume_available_falls_back_when_finder_key_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr("archive_engine.disk_space._important_usage_bytes", lambda _path: None)
    monkeypatch.setattr(
        "archive_engine.disk_space.shutil.disk_usage",
        lambda _path: type("Usage", (), {"free": 39 * 1024**3})(),
    )
    assert volume_available_bytes(tmp_path) == 39 * 1024**3


def test_warehouse_floor_uses_finder_gigabytes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("PPA_WAREHOUSE_MIN_FREE_GB", raising=False)
    seen: list[Path] = []

    def _available(path: Path) -> int:
        seen.append(path)
        return 113 * 1_000_000_000

    monkeypatch.setattr("archive_cli.embedder.volume_available_bytes", _available)
    embedder = EmbedderMixin.__new__(EmbedderMixin)
    embedder.vault = tmp_path
    embedder._require_warehouse_free_space()
    assert seen == [tmp_path]


def test_warehouse_floor_still_refuses_under_forty_finder_gb(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("PPA_WAREHOUSE_MIN_FREE_GB", raising=False)
    monkeypatch.setattr("archive_cli.embedder.volume_available_bytes", lambda _path: 10 * 1_000_000_000)
    embedder = EmbedderMixin.__new__(EmbedderMixin)
    embedder.vault = tmp_path
    with pytest.raises(RuntimeError, match=r"warehouse_disk_low free_gb=10\.0 min_gb=40"):
        embedder._require_warehouse_free_space()
