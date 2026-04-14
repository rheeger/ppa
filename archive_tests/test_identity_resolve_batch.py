"""Step 15 — batch resolve matches per-call resolve_person."""

from __future__ import annotations

from archive_tests.fixtures import load_fixture_vault
from archive_vault.identity_resolver import resolve_person, resolve_person_batch


def test_resolve_person_batch_matches_serial(tmp_path):
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    ids = [{"name": "Nobody Zzzunknown123"}, {"name": "Someone Else Unknown456"}]
    batch = resolve_person_batch(vault, ids)
    serial = [resolve_person(vault, i) for i in ids]
    assert len(batch) == len(serial)
    for a, b in zip(batch, serial):
        assert a.action == b.action
        assert a.confidence == b.confidence
        assert a.wikilink == b.wikilink
