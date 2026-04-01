"""Validate MigrationRunner against a live Postgres (integration)."""

from __future__ import annotations

import pytest

from archive_mcp.migrate import MigrationRunner
from archive_mcp.migrations import discover_migrations


@pytest.mark.integration
class TestMigrationInfrastructure:
    def test_runner_discovers_baseline_migration(self) -> None:
        migrations = discover_migrations()
        assert any(m.version == 1 for m in migrations)

    def test_runner_applies_and_tracks(self, pgvector_dsn: str) -> None:
        from pathlib import Path

        from archive_mcp.index_store import PostgresArchiveIndex

        vault = Path(".")
        index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
        index.schema = "archive_mig_test"
        index.bootstrap()
        with index._connect() as conn:
            runner = MigrationRunner(conn, index.schema)
            runner.ensure_table()
            result = runner.run()
            assert 1 in result.applied or 1 in runner.applied_versions()

    def test_runner_idempotent(self, pgvector_dsn: str) -> None:
        from pathlib import Path

        from archive_mcp.index_store import PostgresArchiveIndex

        vault = Path(".")
        index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
        index.schema = "archive_mig_test2"
        index.bootstrap()
        with index._connect() as conn:
            runner = MigrationRunner(conn, index.schema)
            runner.ensure_table()
            runner.run()
            r2 = runner.run()
            assert not r2.applied
            assert 1 in r2.already_applied or 1 in runner.applied_versions()

    def test_runner_status_reports_correctly(self, pgvector_dsn: str) -> None:
        from pathlib import Path

        from archive_mcp.index_store import PostgresArchiveIndex

        vault = Path(".")
        index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
        index.schema = "archive_mig_test3"
        index.bootstrap()
        with index._connect() as conn:
            runner = MigrationRunner(conn, index.schema)
            runner.run()
            st = runner.status()
            assert st["pending_count"] == 0
            assert st["applied_count"] >= 1
