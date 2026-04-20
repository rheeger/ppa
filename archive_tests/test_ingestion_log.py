"""Tests for ingestion_log emission during rebuild."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest

from archive_cli.materializer import _materialize_row
from archive_cli.scanner import CanonicalRow
from archive_tests.fixtures import FIXTURES_DIR
from archive_vault.schema import validate_card_permissive
from archive_vault.vault import read_note_file


def test_materialize_row_produces_ingestion_log_row(tmp_path: Path) -> None:
    """_materialize_row adds exactly 1 row to buffer.ingestion_log_rows."""
    src = FIXTURES_DIR / "cards" / "document.md"
    dest_dir = tmp_path / "Documents"
    dest_dir.mkdir(parents=True)
    dest = dest_dir / "document.md"
    shutil.copy(src, dest)
    note = read_note_file(dest, vault_root=tmp_path)
    card = validate_card_permissive(dict(note.frontmatter))
    row = CanonicalRow(rel_path="Documents/document.md", frontmatter=dict(note.frontmatter), card=card)
    batch = _materialize_row(
        row,
        vault_root=str(tmp_path),
        slug_map={},
        path_to_uid={},
        person_lookup={},
        batch_id="b1",
    )
    assert len(batch.ingestion_log_rows) == 1
    assert batch.ingestion_log_rows[0][0] == card.uid
    assert batch.ingestion_log_rows[0][1] == "created"


@pytest.mark.integration
def test_ingestion_log_populated_during_rebuild(pgvector_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """After rebuild, ingestion_log row count matches cards row count."""
    from archive_cli.index_store import PostgresArchiveIndex
    from archive_tests.fixtures import load_fixture_vault

    vault = tmp_path / "v"
    load_fixture_vault(vault)
    schema = f"ing_{uuid.uuid4().hex[:10]}"
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)
    monkeypatch.setenv("PPA_VECTOR_DIMENSION", "8")

    index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    index.schema = schema
    index.bootstrap()
    index.rebuild_with_metrics(force_full=True, workers=2, progress_every=0)

    with index._connect() as conn:
        n_cards = conn.execute(f"SELECT COUNT(*) AS c FROM {schema}.cards").fetchone()["c"]
        n_log = conn.execute(f"SELECT COUNT(*) AS c FROM {schema}.ingestion_log").fetchone()["c"]
    assert n_cards == n_log
    assert n_cards > 0


@pytest.mark.integration
def test_ingestion_log_action_is_created(pgvector_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """All ingestion_log rows have action='created' for a full rebuild."""
    from archive_cli.index_store import PostgresArchiveIndex
    from archive_tests.fixtures import load_fixture_vault

    vault = tmp_path / "v2"
    load_fixture_vault(vault)
    schema = f"ing2_{uuid.uuid4().hex[:10]}"
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)
    monkeypatch.setenv("PPA_VECTOR_DIMENSION", "8")

    index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    index.schema = schema
    index.bootstrap()
    index.rebuild_with_metrics(force_full=True, workers=2, progress_every=0)

    with index._connect() as conn:
        rows = conn.execute(
            f"SELECT DISTINCT action FROM {schema}.ingestion_log",
        ).fetchall()
    actions = {r["action"] for r in rows}
    assert actions == {"created"}
