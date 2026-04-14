"""Compare Rust walk_vault against hfa.vault.iter_note_paths (Python os.walk reference).

Ground truth for exclusion rules is always ``_iter_note_paths_python`` so tests stay valid when
``PPA_ENGINE=rust`` (which would otherwise make ``iter_note_paths`` call ``walk_vault``).
"""

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

from archive_vault.vault import _iter_note_paths_python


def _path_set(vault):
    return {str(p).replace("\\", "/") for p in _iter_note_paths_python(vault)}


def test_walk_vault_matches_python_empty(tmp_path):
    import archive_crate

    rust = set(archive_crate.walk_vault(str(tmp_path)))
    py = _path_set(tmp_path)
    assert rust == py


def test_walk_vault_excludes_meta_and_templates(tmp_path):
    import archive_crate

    (tmp_path / "ok.md").write_text("---\ntype: test\n---\n", encoding="utf-8")
    (tmp_path / "_meta").mkdir()
    (tmp_path / "_meta" / "hidden.md").write_text("---\ntype: test\n---\n", encoding="utf-8")
    (tmp_path / "_templates").mkdir()
    (tmp_path / "_templates" / "t.md").write_text("---\ntype: test\n---\n", encoding="utf-8")

    rust = set(archive_crate.walk_vault(str(tmp_path)))
    py = _path_set(tmp_path)
    assert rust == py == {"ok.md"}


def test_walk_vault_excludes_non_md(tmp_path):
    import archive_crate

    (tmp_path / "note.md").write_text("---\ntype: test\n---\n", encoding="utf-8")
    (tmp_path / "image.png").write_bytes(b"PNG")

    rust = set(archive_crate.walk_vault(str(tmp_path)))
    py = _path_set(tmp_path)
    assert rust == py == {"note.md"}


def test_walk_parallel_matches_monolithic_reference(tmp_path):
    """Rayon top-level split must match single WalkDir from vault root (Step 2 parity)."""
    import archive_crate

    (tmp_path / "root.md").write_text("---\n---\n", encoding="utf-8")
    for d in ("Email", "People", "Deep"):
        (tmp_path / d / "sub").mkdir(parents=True)
        (tmp_path / d / "sub" / "n.md").write_text("---\n---\n", encoding="utf-8")
    (tmp_path / "_meta").mkdir()
    (tmp_path / "_meta" / "x.md").write_text("---\n---\n", encoding="utf-8")

    v = str(tmp_path)
    par = set(archive_crate.walk_vault(v))
    mono = set(archive_crate.walk_vault_monolithic(v))
    assert par == mono


def test_walk_vault_matches_fixture_vault(tmp_path):
    import archive_crate
    from archive_tests.fixtures import load_fixture_vault

    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    v = str(vault)
    rust = set(archive_crate.walk_vault(v))
    py = _path_set(vault)
    assert rust == py
    assert archive_crate.walk_vault_count(v) == len(rust)
    assert set(archive_crate.walk_vault_monolithic(v)) == rust


def test_walk_vault_count_matches_without_collecting_paths(tmp_path):
    import archive_crate

    (tmp_path / "a.md").write_text("---\n---\n", encoding="utf-8")
    (tmp_path / "Sub").mkdir()
    (tmp_path / "Sub" / "b.md").write_text("---\n---\n", encoding="utf-8")
    v = str(tmp_path)
    assert archive_crate.walk_vault_count(v) == len(archive_crate.walk_vault(v))
