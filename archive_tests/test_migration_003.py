"""Tests for migration 003: embedding_score on link_decisions."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.migrate import MigrationRunner


@pytest.fixture(autouse=True)
def _enable_seed_links(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")


@pytest.mark.integration
class TestMigration003:
    def test_creates_embedding_score_column(self, pgvector_dsn: str) -> None:
        vault = Path(".")
        index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
        index.schema = "ppa_mig_003"
        index.bootstrap()
        with index._connect() as conn:
            runner = MigrationRunner(conn, index.schema)
            runner.ensure_table()
            runner.run()
            row = conn.execute(
                """
                SELECT data_type, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'link_decisions' AND column_name = 'embedding_score'
                """,
                (index.schema,),
            ).fetchone()
            assert row is not None
            assert "double precision" in str(row["data_type"]).lower()

    def test_idempotent(self, pgvector_dsn: str) -> None:
        vault = Path(".")
        index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
        index.schema = "ppa_mig_003b"
        index.bootstrap()
        with index._connect() as conn:
            runner = MigrationRunner(conn, index.schema)
            runner.ensure_table()
            runner.run()
            runner.run()

    def test_migration_status_includes_v3(self, pgvector_dsn: str) -> None:
        from archive_cli.migrations import discover_migrations

        versions = {m.version for m in discover_migrations()}
        assert 3 in versions
