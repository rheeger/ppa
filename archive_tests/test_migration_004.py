"""Tests for migration 004: decouple embeddings from chunks lifecycle.

Three invariants the codebase must hold so that ``make rebuild-indexes``
never silently incinerates millions of dollars of OpenAI embedding cost
again:

1. The migration drops the historical FK on ``embeddings.chunk_key``.
2. ``_clear()`` no longer truncates ``embeddings``.
3. After a full rebuild that drops + recreates ``chunks``, embedding rows
   whose ``chunk_key`` survives in the new ``chunks`` are still present.
   Orphaned embeddings remain until ``ppa embed-gc --apply`` removes them.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.commands.admin import embed_gc as embed_gc_cmd
from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.migrate import MigrationRunner
from archive_cli.migrations import discover_migrations
from archive_cli.store import DefaultArchiveStore


@pytest.fixture(autouse=True)
def _enable_seed_links(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")


def _bootstrap(dsn: str, schema: str, vault: Path) -> PostgresArchiveIndex:
    index = PostgresArchiveIndex(vault, dsn=dsn)
    index.schema = schema
    with index._connect() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.commit()
    index.bootstrap()
    return index


def _insert_chunk(conn, schema: str, *, chunk_key: str, card_uid: str = "u1") -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.chunks (chunk_key, card_uid, rel_path, chunk_type,
            chunk_index, source_fields, content, content_hash, token_count)
        VALUES (%s, %s, 'p', 'body', 0, '[]'::jsonb, 'x', %s, 1)
        ON CONFLICT (chunk_key) DO NOTHING
        """,
        (chunk_key, card_uid, chunk_key),
    )


def _insert_embedding(conn, schema: str, *, chunk_key: str, dim: int) -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.embeddings (chunk_key, embedding_model, embedding_version, embedding)
        VALUES (%s, 'test-model', 1, %s::vector)
        """,
        (chunk_key, "[" + ",".join("0.0" for _ in range(dim)) + "]"),
    )


def test_discoverable() -> None:
    versions = {m.version for m in discover_migrations()}
    assert 4 in versions


@pytest.mark.integration
class TestMigration004:
    def test_drops_chunks_fk_on_embeddings(self, pgvector_dsn: str, tmp_path: Path) -> None:
        index = _bootstrap(pgvector_dsn, "ppa_mig_004_drop", tmp_path)
        with index._connect() as conn:
            MigrationRunner(conn, index.schema).run()
            row = conn.execute(
                """
                SELECT COUNT(*) AS c FROM pg_constraint
                WHERE conrelid = (%s::regclass) AND contype = 'f'
                """,
                (f"{index.schema}.embeddings",),
            ).fetchone()
            assert int(row["c"]) == 0, "embeddings should have no FK after migration 004"

    def test_idempotent(self, pgvector_dsn: str, tmp_path: Path) -> None:
        index = _bootstrap(pgvector_dsn, "ppa_mig_004_idem", tmp_path)
        with index._connect() as conn:
            MigrationRunner(conn, index.schema).run()
            MigrationRunner(conn, index.schema).run()  # second pass = no-op

    def test_drop_chunks_table_does_not_cascade_to_embeddings(
        self, pgvector_dsn: str, tmp_path: Path
    ) -> None:
        """The whole point of migration 004."""
        index = _bootstrap(pgvector_dsn, "ppa_mig_004_no_cascade", tmp_path)
        with index._connect() as conn:
            MigrationRunner(conn, index.schema).run()
            _insert_chunk(conn, index.schema, chunk_key="ckA")
            _insert_chunk(conn, index.schema, chunk_key="ckB")
            _insert_embedding(conn, index.schema, chunk_key="ckA", dim=index.vector_dimension)
            _insert_embedding(conn, index.schema, chunk_key="ckB", dim=index.vector_dimension)
            conn.commit()
            pre_row = conn.execute(
                f"SELECT COUNT(*) AS c FROM {index.schema}.embeddings"
            ).fetchone()
            assert int(pre_row["c"]) == 2
            conn.execute(f"DROP TABLE {index.schema}.chunks CASCADE")
            conn.commit()
            post_row = conn.execute(
                f"SELECT COUNT(*) AS c FROM {index.schema}.embeddings"
            ).fetchone()
            assert int(post_row["c"]) == 2, "embeddings must survive DROP TABLE chunks CASCADE"

    def test_clear_does_not_truncate_embeddings(
        self, pgvector_dsn: str, tmp_path: Path
    ) -> None:
        index = _bootstrap(pgvector_dsn, "ppa_mig_004_clear", tmp_path)
        with index._connect() as conn:
            MigrationRunner(conn, index.schema).run()
            _insert_chunk(conn, index.schema, chunk_key="ckC")
            _insert_embedding(conn, index.schema, chunk_key="ckC", dim=index.vector_dimension)
            conn.commit()
            index._clear(conn)
            conn.commit()
            post_row = conn.execute(
                f"SELECT COUNT(*) AS c FROM {index.schema}.embeddings"
            ).fetchone()
            assert int(post_row["c"]) == 1, "_clear() must not touch embeddings"


@pytest.mark.integration
class TestEmbedGc:
    def test_dry_run_reports_orphans_without_deleting(
        self, pgvector_dsn: str, tmp_path: Path
    ) -> None:
        index = _bootstrap(pgvector_dsn, "ppa_embed_gc_dry", tmp_path)
        with index._connect() as conn:
            MigrationRunner(conn, index.schema).run()
            _insert_chunk(conn, index.schema, chunk_key="live")
            _insert_embedding(conn, index.schema, chunk_key="live", dim=index.vector_dimension)
            _insert_embedding(conn, index.schema, chunk_key="orph1", dim=index.vector_dimension)
            _insert_embedding(conn, index.schema, chunk_key="orph2", dim=index.vector_dimension)
            conn.commit()
        store = DefaultArchiveStore(vault=tmp_path, index=index)
        import logging

        result = embed_gc_cmd(
            store=store, logger=logging.getLogger("test"), dry_run=True
        )
        assert result["orphan_embeddings"] == 2
        assert result["deleted"] == 0
        with index._connect() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS c FROM {index.schema}.embeddings"
            ).fetchone()
        assert int(row["c"]) == 3

    def test_apply_drops_only_orphans(
        self, pgvector_dsn: str, tmp_path: Path
    ) -> None:
        index = _bootstrap(pgvector_dsn, "ppa_embed_gc_apply", tmp_path)
        with index._connect() as conn:
            MigrationRunner(conn, index.schema).run()
            _insert_chunk(conn, index.schema, chunk_key="keep1")
            _insert_chunk(conn, index.schema, chunk_key="keep2")
            _insert_embedding(conn, index.schema, chunk_key="keep1", dim=index.vector_dimension)
            _insert_embedding(conn, index.schema, chunk_key="keep2", dim=index.vector_dimension)
            _insert_embedding(conn, index.schema, chunk_key="orphan", dim=index.vector_dimension)
            conn.commit()
        store = DefaultArchiveStore(vault=tmp_path, index=index)
        import logging

        result = embed_gc_cmd(
            store=store, logger=logging.getLogger("test"), dry_run=False
        )
        assert result["deleted"] == 1
        with index._connect() as conn:
            remaining = sorted(
                str(r["chunk_key"])
                for r in conn.execute(
                    f"SELECT chunk_key FROM {index.schema}.embeddings"
                ).fetchall()
            )
        assert remaining == ["keep1", "keep2"]
