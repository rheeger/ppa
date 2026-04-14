"""Phase 1: declarative edge rules for new derived types."""

from __future__ import annotations

from pathlib import Path

from archive_cli.materializer import _build_edges, _build_person_lookup
from archive_cli.scanner import CanonicalRow
from archive_tests.fixtures import FIXTURES_DIR, iter_graph_fixture_sets
from archive_vault.schema import validate_card_permissive
from archive_vault.vault import read_note_file


def test_derived_meal_order_graph_edges():
    paths = iter_graph_fixture_sets().get("derived_card_graph", [])
    assert paths, "derived_card_graph fixtures missing"
    rows: list[CanonicalRow] = []
    slug_map: dict[str, str] = {}
    path_to_uid: dict[str, str] = {}
    for p in paths:
        note = read_note_file(p, vault_root=FIXTURES_DIR)
        rel = p.relative_to(FIXTURES_DIR).as_posix()
        uid = str(note.frontmatter.get("uid", ""))
        slug_map[Path(rel).stem] = rel
        path_to_uid[rel] = uid
        rows.append(
            CanonicalRow(rel_path=rel, frontmatter=dict(note.frontmatter), card=validate_card_permissive(note.frontmatter))
        )
    person_lookup = _build_person_lookup(rows)
    meal_path = next(p for p in paths if "graph-meal-order" in p.name)
    note = read_note_file(meal_path, vault_root=FIXTURES_DIR)
    rel = meal_path.relative_to(FIXTURES_DIR).as_posix()
    card = validate_card_permissive(note.frontmatter)
    edges = _build_edges(
        rel_path=rel,
        frontmatter=dict(note.frontmatter),
        card=card,
        body=note.body,
        slug_map=slug_map,
        path_to_uid=path_to_uid,
        person_lookup=person_lookup,
    )
    _ = {(e["edge_type"], e["target_path"]) for e in edges}
    email_rel = slug_map["derived-email-src"]
    place_rel = slug_map["graph-brooklyn-hero"]
    org_rel = slug_map["graph-doordash"]
    assert any(e["edge_type"] == "derived_from" and e["target_path"] == email_rel for e in edges)
    assert any(e["edge_type"] == "located_at" and e["target_path"] == place_rel for e in edges)
    assert any(e["edge_type"] == "provided_by" and e["target_path"] == org_rel for e in edges)
