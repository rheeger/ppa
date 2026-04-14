"""Smoke test: verify archive_crate Rust module imports and basic functions work."""

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")


def test_archive_crate_imports():
    """The Rust module is importable."""
    import archive_crate

    assert hasattr(archive_crate, "walk_vault")
    assert hasattr(archive_crate, "walk_vault_count")
    assert hasattr(archive_crate, "walk_vault_monolithic")
    assert hasattr(archive_crate, "raw_content_sha256")
    assert hasattr(archive_crate, "content_hash")
    assert hasattr(archive_crate, "parse_frontmatter")
    assert hasattr(archive_crate, "vault_paths_and_fingerprint")
    assert hasattr(archive_crate, "cards_by_type") and hasattr(archive_crate, "cards_by_type_from_cache")
    assert hasattr(archive_crate, "rebuild_index")
    assert hasattr(archive_crate, "build_person_index")
    assert hasattr(archive_crate, "PersonResolutionIndex")
    assert hasattr(archive_crate, "resolve_person_batch")


def test_walk_vault_returns_list(tmp_path):
    """walk_vault on an empty directory returns an empty list."""
    import archive_crate

    result = archive_crate.walk_vault(str(tmp_path))
    assert isinstance(result, list)
    assert len(result) == 0
    assert archive_crate.walk_vault_count(str(tmp_path)) == 0


def test_build_person_index_empty_vault(tmp_path):
    import archive_crate

    (tmp_path / "People").mkdir(parents=True)
    idx = archive_crate.build_person_index(str(tmp_path))
    assert idx is not None
    assert len(idx) == 0


def test_resolve_person_batch_empty(tmp_path):
    import archive_crate

    out = archive_crate.resolve_person_batch(str(tmp_path), [])
    assert out == []
