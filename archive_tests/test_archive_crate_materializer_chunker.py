"""Step 8–9: native Rust materializer / chunker parity vs Python."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate
from archive_cli.chunking import render_chunks_for_card
from archive_cli.materializer import _build_person_lookup, _build_search_text, _content_hash, _materialize_row_batch
from archive_cli.scanner import CanonicalRow
from archive_tests.archive_crate_projection_parity import assert_projection_buffers_equal
from archive_tests.fixtures import load_fixture_vault
from archive_vault.schema import validate_card_permissive
from archive_vault.vault import iter_parsed_notes


def test_build_search_text_matches_python(tmp_path):
    """Per-note search text, content hash, and chunk list vs Python (Steps 8–9)."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)

    for note in iter_parsed_notes(vault):
        fm = note.frontmatter
        rust = archive_crate.build_search_text(fm, note.body)
        py = _build_search_text(fm, note.body)
        assert rust == py, note.rel_path
        rust_h = archive_crate.materialize_content_hash(fm, note.body)
        assert rust_h == _content_hash(fm, note.body)
        rust_chunks = archive_crate.render_chunks_for_card(fm, note.body)
        py_chunks = render_chunks_for_card(fm, note.body)
        assert rust_chunks == py_chunks, note.rel_path


def test_materialize_row_batch_rust_matches_python(tmp_path, monkeypatch):
    """Full native materializer vs legacy Python on the fixture vault (Step 8)."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    rows: list[CanonicalRow] = []
    slug_map: dict[str, str] = {}
    path_to_uid: dict[str, str] = {}
    for note in iter_parsed_notes(vault):
        rel = note.rel_path.as_posix()
        stem = Path(rel).stem
        slug_map[stem] = rel
        uid = str(note.frontmatter.get("uid", ""))
        path_to_uid[rel] = uid
        rows.append(
            CanonicalRow(
                rel_path=rel,
                frontmatter=dict(note.frontmatter),
                card=validate_card_permissive(note.frontmatter),
            )
        )
    person_lookup = _build_person_lookup(rows)
    monkeypatch.setenv("PPA_ENGINE", "python")
    py_batch = _materialize_row_batch(
        rows,
        vault_root=str(vault),
        slug_map=slug_map,
        path_to_uid=path_to_uid,
        person_lookup=person_lookup,
        batch_id="parity-test",
    )
    monkeypatch.setenv("PPA_ENGINE", "rust")
    rust_batch = _materialize_row_batch(
        rows,
        vault_root=str(vault),
        slug_map=slug_map,
        path_to_uid=path_to_uid,
        person_lookup=person_lookup,
        batch_id="parity-test",
    )
    assert_projection_buffers_equal(py_batch, rust_batch, context="fixture vault materialize_row_batch")
