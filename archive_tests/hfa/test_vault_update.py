"""Tests for vault frontmatter update helper."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_vault.vault import update_frontmatter_fields


def test_update_frontmatter_fields_adds_new_fields(tmp_path: Path) -> None:
    """Adding latitude/longitude to a card that didn't have them."""
    p = tmp_path / "Places" / "cafe.md"
    p.parent.mkdir(parents=True)
    p.write_text(
        '---\nuid: "uid-1"\ntype: place\nname: "Cafe"\n---\n\nBody here.\n',
        encoding="utf-8",
    )
    update_frontmatter_fields(tmp_path, "Places/cafe.md", {"latitude": 37.77, "longitude": -122.42})
    assert "latitude: 37.77" in p.read_text(encoding="utf-8")
    assert "longitude: -122.42" in p.read_text(encoding="utf-8")


def test_update_frontmatter_fields_overwrites_existing(tmp_path: Path) -> None:
    """Overwriting latitude=0.0 with real value."""
    p = tmp_path / "Places" / "x.md"
    p.parent.mkdir(parents=True)
    p.write_text(
        '---\nuid: "u1"\ntype: place\nlatitude: 0.0\nlongitude: 0.0\n---\n\nBody.\n',
        encoding="utf-8",
    )
    update_frontmatter_fields(tmp_path, "Places/x.md", {"latitude": 40.0, "longitude": -74.0})
    text = p.read_text(encoding="utf-8")
    assert "latitude: 40.0" in text
    assert "longitude: -74.0" in text


def test_update_frontmatter_fields_preserves_body(tmp_path: Path) -> None:
    """Body text is unchanged after frontmatter update."""
    body = "\n\n## Section\n\nKeep this **exact**.\n"
    p = tmp_path / "Places" / "b.md"
    p.parent.mkdir(parents=True)
    p.write_text(f'---\nuid: "u"\ntype: place\n---\n{body}', encoding="utf-8")
    update_frontmatter_fields(tmp_path, "Places/b.md", {"latitude": 1.0})
    assert p.read_text(encoding="utf-8").endswith(body)


def test_update_frontmatter_fields_preserves_other_fields(tmp_path: Path) -> None:
    """Fields not in updates dict are unchanged."""
    p = tmp_path / "Places" / "c.md"
    p.parent.mkdir(parents=True)
    p.write_text(
        '---\nuid: "u"\ntype: place\nname: "N"\ncity: "SF"\n---\n\nx\n',
        encoding="utf-8",
    )
    update_frontmatter_fields(tmp_path, "Places/c.md", {"latitude": 2.0})
    text = p.read_text(encoding="utf-8")
    assert "name: " in text
    assert "city: " in text


def test_update_frontmatter_fields_handles_missing_file(tmp_path: Path) -> None:
    """Raises FileNotFoundError for nonexistent path."""
    with pytest.raises(FileNotFoundError):
        update_frontmatter_fields(tmp_path, "Places/nope.md", {"latitude": 1.0})


def test_update_frontmatter_fields_preserves_yaml_formatting(tmp_path: Path) -> None:
    """YAML formatting (list style, quoting) is preserved via ruamel.yaml."""
    p = tmp_path / "Places" / "d.md"
    p.parent.mkdir(parents=True)
    p.write_text(
        '---\nuid: "u"\ntype: place\nname: "Spot"\ntags:\n  - "a"\n  - "b"\n---\n\nBody.\n',
        encoding="utf-8",
    )
    update_frontmatter_fields(tmp_path, "Places/d.md", {"latitude": 3.0})
    text = p.read_text(encoding="utf-8")
    assert "tags:" in text
    assert "latitude: 3.0" in text
