"""Incremental vs full rebuild parity (integration) — Phase 0 Step 9."""

from __future__ import annotations

import json
import random
import re
from pathlib import Path

import pytest

from archive_mcp.commands.health_check import run_behavioral_checks
from archive_mcp.index_store import PostgresArchiveIndex
from hfa.vault import extract_wikilinks, iter_note_paths
from tests.fixtures import load_fixture_vault
from tests.index_snapshot import snapshot_projection_state


def _patch_summary(path: Path, text: str) -> None:
    raw = path.read_text(encoding="utf-8")
    if not raw.startswith("---"):
        return
    m = re.match(r"^---\n(.*?\n)---\n", raw, re.DOTALL)
    if not m:
        return
    front = m.group(1)
    rest = raw[m.end() :]
    new_front = re.sub(r"(?m)^summary:.*$", f'summary: "{text}"', front)
    path.write_text("---\n" + new_front + "---\n" + rest, encoding="utf-8")


def _mutate_vault(vault: Path, rng: random.Random) -> None:
    """Apply ~5% summary edits, ~2% new cards, ~1% deletes (Step 9).

    Deletes are chosen from notes that are not referenced as wikilink targets elsewhere
    in the vault, so incremental rebuild does not miss edge rows on cards that were not
    in the materialize set (sources that link to a changed target must be re-materialized).
    """
    paths = sorted([p for p in iter_note_paths(vault) if p.suffix == ".md"], key=lambda p: p.as_posix())
    n = len(paths)
    if n < 8:
        pytest.skip("fixture vault too small for Step 9 mutation ratios")
    n_change = max(1, int(n * 0.05))
    n_add = max(1, int(n * 0.02))
    n_del = max(1, int(n * 0.01)) if n >= 15 else max(0, int(n * 0.01))
    if n_del == 0:
        n_del = 1

    # Slugs that appear as wikilink targets anywhere in a note — avoid deleting those cards.
    target_slugs: set[str] = set()
    for rel in paths:
        raw = (vault / rel).read_text(encoding="utf-8")
        target_slugs.update(extract_wikilinks(raw))

    change_set = set(rng.sample(paths, min(n_change, n)))
    for rel in change_set:
        _patch_summary(vault / rel, f"mutated {rel.stem}")

    norm_targets = {s.replace(" ", "-").lower() for s in target_slugs}
    del_candidates = [
        p
        for p in paths
        if p not in change_set
        and p.stem not in target_slugs
        and p.stem.replace(" ", "-").lower() not in norm_targets
    ]
    if len(del_candidates) < n_del:
        pytest.skip("fixture has no safe leaf notes to delete for Step 9 parity test")
    for rel in rng.sample(del_candidates, min(n_del, len(del_candidates))):
        (vault / rel).unlink()

    template = (Path(__file__).resolve().parent / "fixtures" / "cards" / "document.md").read_text(encoding="utf-8")
    for i in range(n_add):
        uid = f"hfa-phase0-new{i:03d}"
        body = template.replace("hfa-document-fix01", uid).replace("doc.fix001", f"doc.phase0.{i}")
        dest = vault / "People" / f"_phase0_new_{i}.md"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(body, encoding="utf-8")


def _index_env(monkeypatch: pytest.MonkeyPatch, vault: Path, dsn: str, schema: str) -> PostgresArchiveIndex:
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.setenv("PPA_EMBEDDING_MODEL", "archive-hash-dev")
    monkeypatch.setenv("PPA_EMBEDDING_VERSION", "1")
    idx = PostgresArchiveIndex(vault, dsn=dsn)
    idx.schema = schema
    return idx


@pytest.mark.integration
@pytest.mark.slow
def test_incremental_matches_full_after_mutations(
    pgvector_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Full rebuild snapshot == incremental snapshot after corpus mutations (Step 9)."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    schema = "archive_incr_parity"
    idx = _index_env(monkeypatch, vault, pgvector_dsn, schema)
    idx.bootstrap()
    idx.rebuild_with_metrics(
        force_full=True,
        workers=1,
        batch_size=50,
        commit_interval=50,
        executor_kind="serial",
    )

    _mutate_vault(vault, random.Random(42))

    idx.rebuild_with_metrics(
        force_full=False,
        workers=1,
        batch_size=50,
        commit_interval=50,
        executor_kind="serial",
    )
    with idx._connect() as conn:
        snap_incr = snapshot_projection_state(conn, schema)

    idx.rebuild_with_metrics(
        force_full=True,
        workers=1,
        batch_size=50,
        commit_interval=50,
        executor_kind="serial",
    )
    with idx._connect() as conn:
        snap_full = snapshot_projection_state(conn, schema)

    assert snap_incr == snap_full

    manifest = json.loads((Path(__file__).resolve().parent / "slice_manifest.json").read_text(encoding="utf-8"))
    br = run_behavioral_checks(idx, manifest)
    assert br.ok, f"behavioral manifest failed: {br}"
