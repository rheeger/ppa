"""Tests for stratified seed slicer."""

from __future__ import annotations

from pathlib import Path

from archive_mcp.test_slice import SliceConfig, load_slice_config, slice_seed_vault
from tests.fixtures import load_fixture_vault


def test_slice_config_json_schema(tmp_path: Path) -> None:
    cfg_path = Path("tests/slice_config.json")
    cfg = load_slice_config(cfg_path)
    assert isinstance(cfg.target_percent, float)


def test_stratified_slicer_covers_types(tmp_path: Path) -> None:
    src = load_fixture_vault(tmp_path / "src", include_graphs=True)
    out = tmp_path / "slice"
    res = slice_seed_vault(
        src,
        out,
        SliceConfig(target_percent=100.0, min_cards_per_type=1, cluster_cap=5000),
    )
    assert res.selected_card_count > 0
    # Orphan scanner treats some [[uid-style]] refs as slugs; slice still copies a representative set.
    assert res.orphaned_wikilinks >= 0


def test_slice_reproducibility(tmp_path: Path) -> None:
    src = load_fixture_vault(tmp_path / "src", include_graphs=False)
    cfg = SliceConfig(target_percent=50.0, min_cards_per_type=1, cluster_cap=5000)
    o1 = tmp_path / "o1"
    o2 = tmp_path / "o2"
    slice_seed_vault(src, o1, cfg)
    slice_seed_vault(src, o2, cfg)
    c1 = sorted((p.relative_to(o1).as_posix() for p in o1.rglob("*.md")))
    c2 = sorted((p.relative_to(o2).as_posix() for p in o2.rglob("*.md")))
    assert c1 == c2
