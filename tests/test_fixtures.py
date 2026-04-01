"""Validation tests for Phase 0 synthetic fixtures."""

from __future__ import annotations

from pathlib import Path

from archive_mcp.materializer import _build_edges, _build_person_lookup
from archive_mcp.scanner import CanonicalRow
from hfa.schema import CARD_TYPES, card_to_frontmatter, validate_card_permissive, validate_card_strict
from hfa.vault import extract_wikilinks, read_note_file
from tests.fixtures import (
    EDGE_CASES_DIR,
    FIXTURES_DIR,
    GRAPHS_DIR,
    iter_card_fixture_paths,
    iter_graph_fixture_sets,
    load_fixture_vault,
)


def test_every_existing_card_type_has_fixture() -> None:
    fixture_types: set[str] = set()
    for path in iter_card_fixture_paths():
        note = read_note_file(path, vault_root=FIXTURES_DIR)
        fixture_types.add(str(note.frontmatter["type"]))
    missing = set(CARD_TYPES.keys()) - fixture_types
    assert not missing, f"Missing fixtures for types: {missing}"


def test_fixture_pydantic_roundtrip() -> None:
    for path in iter_card_fixture_paths():
        note = read_note_file(path, vault_root=FIXTURES_DIR)
        card = validate_card_strict(note.frontmatter)
        dumped = card_to_frontmatter(card)
        restored = validate_card_strict(dumped)
        assert card.uid == restored.uid


def test_fixture_required_fields() -> None:
    for path in iter_card_fixture_paths():
        note = read_note_file(path, vault_root=FIXTURES_DIR)
        assert note.frontmatter.get("summary"), f"{path.name}: empty summary"
        assert note.frontmatter.get("source"), f"{path.name}: empty source"


def test_graph_zero_orphans() -> None:
    for graph_name, paths in iter_graph_fixture_sets().items():
        slugs = {p.stem for p in paths}
        for p in paths:
            note = read_note_file(p, vault_root=FIXTURES_DIR)
            for wikilink_slug in extract_wikilinks(note.body):
                assert wikilink_slug in slugs, (
                    f"Graph {graph_name}: orphaned wikilink [[{wikilink_slug}]] in {p.name}"
                )


def test_graph_edge_rules_fire() -> None:
    paths = iter_graph_fixture_sets().get("email_thread_graph", [])
    assert paths
    rows: list = []
    slug_map: dict[str, str] = {}
    path_to_uid: dict[str, str] = {}
    person_lookup: dict[str, str] = {}
    for p in paths:
        note = read_note_file(p, vault_root=FIXTURES_DIR)
        rel = p.relative_to(FIXTURES_DIR).as_posix()
        uid = str(note.frontmatter.get("uid", ""))
        slug_map[Path(rel).stem] = rel
        path_to_uid[rel] = uid
        rows.append(
            CanonicalRow(
                rel_path=rel,
                frontmatter=dict(note.frontmatter),
                card=validate_card_permissive(note.frontmatter),
            )
        )
    person_lookup = _build_person_lookup(rows)
    edges: list = []
    for p in paths:
        note = read_note_file(p, vault_root=FIXTURES_DIR)
        rel = p.relative_to(FIXTURES_DIR).as_posix()
        card = validate_card_permissive(note.frontmatter)
        edges.extend(
            _build_edges(
                rel_path=rel,
                frontmatter=dict(note.frontmatter),
                card=card,
                body=note.body,
                slug_map=slug_map,
                path_to_uid=path_to_uid,
                person_lookup=person_lookup,
            )
        )
    assert len(edges) >= 3


def test_edge_case_long_body_parses() -> None:
    p = EDGE_CASES_DIR / "long_body.md"
    note = read_note_file(p, vault_root=FIXTURES_DIR)
    assert len(note.body) > 10_000
    validate_card_strict(note.frontmatter)


def test_edge_case_unicode_roundtrips() -> None:
    p = EDGE_CASES_DIR / "unicode_heavy.md"
    note = read_note_file(p, vault_root=FIXTURES_DIR)
    card = validate_card_strict(note.frontmatter)
    assert "日本語" in card.summary or "emoji" in card.summary


def test_load_fixture_vault_copies(tmp_path: Path) -> None:
    v = load_fixture_vault(tmp_path / "v", include_graphs=False)
    assert (v / "People").exists()
    assert any((v / "People").glob("*.md"))


def test_graphs_dir_exists() -> None:
    assert GRAPHS_DIR.is_dir()
