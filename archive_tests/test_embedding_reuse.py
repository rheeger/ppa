"""Reuse-by-content, slot remap, and publication coverage for rematerialize."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from archive_cli.commands.admin import embed_gc as embed_gc_cmd
from archive_cli.embedder import EmbedderMixin
from archive_cli.index_config import _vector_literal
from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.migrations import discover_migrations
from archive_cli.serving_index import prune_retired_serving_generations
from archive_cli.store import DefaultArchiveStore
from archive_engine.contracts import EmbeddingSpec
from archive_engine.errors import IncompatibleStateError
from archive_engine.publication import validate_generation


def test_migration_011_is_discoverable() -> None:
    versions = {item.version for item in discover_migrations()}
    assert 11 in versions
    named = next(item for item in discover_migrations() if item.version == 11)
    assert named.name == "embedding_content_identity"


def _minimal_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in ("People", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    return vault


def _configure(monkeypatch: pytest.MonkeyPatch, *, vault: Path, dsn: str, schema: str) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    monkeypatch.delenv("PPA_CONFIG_PATH", raising=False)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)
    monkeypatch.setenv("PPA_VECTOR_DIMENSION", "8")
    monkeypatch.setenv("PPA_EMBEDDING_MODEL", "reuse-hash")
    monkeypatch.setenv("PPA_EMBEDDING_VERSION", "1")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.setenv("PPA_BOOTSTRAP_FORCE", "1")


def _bootstrap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, dsn: str) -> PostgresArchiveIndex:
    vault = _minimal_vault(tmp_path)
    schema = f"reuse_{uuid.uuid4().hex[:10]}"
    _configure(monkeypatch, vault=vault, dsn=dsn, schema=schema)
    index = PostgresArchiveIndex(vault, dsn=dsn)
    index.schema = schema
    index.vector_dimension = 8
    index.bootstrap()
    return index


def _vec(dim: int, fill: float) -> str:
    return _vector_literal([fill] * dim)


def _insert_chunk(conn, schema: str, *, key: str, uid: str, index: int, content_hash: str) -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.chunks (
            chunk_key, card_uid, rel_path, chunk_type, chunk_index,
            chunk_schema_version, content, content_hash, token_count
        )
        VALUES (%s, %s, %s, 'body', %s, 6, 'body', %s, 1)
        """,
        (key, uid, f"{uid}.md", index, content_hash),
    )


def _insert_embedding(
    conn,
    schema: str,
    *,
    key: str,
    dim: int,
    fill: float,
    content_hash: str = "",
    uid: str = "",
    chunk_index: int = 0,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.embeddings (
            chunk_key, embedding_model, embedding_version, embedding,
            content_hash, card_uid, chunk_type, chunk_index
        )
        VALUES (%s, 'reuse-hash', 1, %s::vector, %s, %s, 'body', %s)
        """,
        (key, _vec(dim, fill), content_hash, uid, chunk_index),
    )


@pytest.mark.integration
def test_reuse_by_content_copies_vector_onto_new_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    with index._connect() as conn:
        _insert_chunk(conn, index.schema, key="old-key", uid="card-a", index=0, content_hash="hash-same")
        _insert_embedding(
            conn,
            index.schema,
            key="old-key",
            dim=8,
            fill=0.25,
            content_hash="hash-same",
            uid="card-a",
        )
        conn.execute(f"DELETE FROM {index.schema}.chunks WHERE chunk_key = 'old-key'")
        _insert_chunk(conn, index.schema, key="new-key", uid="card-a", index=0, content_hash="hash-same")
        conn.commit()
    result = index.reuse_embeddings_by_content(embedding_model="reuse-hash", embedding_version=1)
    assert result["copied"] == 1
    assert result["pending_after"] == 0
    with index._connect() as conn:
        row = conn.execute(
            f"""
            SELECT chunk_key, content_hash, embedding::text
            FROM {index.schema}.embeddings
            WHERE chunk_key = 'new-key'
            """
        ).fetchone()
    assert row["content_hash"] == "hash-same"
    assert "0.25" in str(row["embedding"])


@pytest.mark.integration
def test_reuse_copies_live_list_that_shares_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    with index._connect() as conn:
        _insert_chunk(conn, index.schema, key="live-src", uid="card-src", index=0, content_hash="shared-hash")
        _insert_embedding(
            conn,
            index.schema,
            key="live-src",
            dim=8,
            fill=0.41,
            content_hash="shared-hash",
            uid="card-src",
        )
        _insert_chunk(conn, index.schema, key="pending-dst", uid="card-dst", index=0, content_hash="shared-hash")
        conn.commit()
    result = index.reuse_embeddings_by_content(embedding_model="reuse-hash", embedding_version=1)
    assert result["copied"] == 1
    assert result["cleaned"] == 0
    assert result["pending_after"] == 0
    with index._connect() as conn:
        keys = {
            str(row["chunk_key"]) for row in conn.execute(f"SELECT chunk_key FROM {index.schema}.embeddings").fetchall()
        }
    assert keys == {"live-src", "pending-dst"}


@pytest.mark.integration
def test_slot_remap_copies_orphan_onto_current_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    jsonl = tmp_path / "chunks.jsonl"
    jsonl.write_text(
        json.dumps(
            {
                "chunk_key": "legacy-key",
                "card_uid": "card-slot",
                "chunk_type": "body",
                "chunk_index": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    with index._connect() as conn:
        _insert_embedding(conn, index.schema, key="legacy-key", dim=8, fill=0.75)
        _insert_chunk(conn, index.schema, key="current-key", uid="card-slot", index=0, content_hash="now-hash")
        conn.commit()
    loaded = index.load_slot_map_from_chunks_jsonl(jsonl)
    remapped = index.remap_embeddings_by_slot(embedding_model="reuse-hash", embedding_version=1)
    assert loaded == 1
    assert remapped["copied"] == 1
    with index._connect() as conn:
        current = conn.execute(
            f"SELECT content_hash FROM {index.schema}.embeddings WHERE chunk_key = 'current-key'"
        ).fetchone()
        legacy = conn.execute(
            f"SELECT content_hash, card_uid FROM {index.schema}.embeddings WHERE chunk_key = 'legacy-key'"
        ).fetchone()
    assert current["content_hash"] == "now-hash"
    assert legacy["content_hash"] == "now-hash"
    assert legacy["card_uid"] == "card-slot"


@pytest.mark.integration
def test_embed_gc_keeps_reusable_and_unknown_orphans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    with index._connect() as conn:
        _insert_chunk(conn, index.schema, key="live", uid="card-live", index=0, content_hash="keep-hash")
        _insert_embedding(
            conn,
            index.schema,
            key="live",
            dim=8,
            fill=0.1,
            content_hash="keep-hash",
            uid="card-live",
        )
        _insert_embedding(conn, index.schema, key="unknown", dim=8, fill=0.2)
        _insert_embedding(conn, index.schema, key="reusable", dim=8, fill=0.3, content_hash="keep-hash")
        _insert_embedding(conn, index.schema, key="dead", dim=8, fill=0.4, content_hash="unused-hash")
        conn.commit()
    store = DefaultArchiveStore(vault=tmp_path, index=index)
    import logging

    result = embed_gc_cmd(store=store, logger=logging.getLogger("test"), dry_run=False)
    assert result["deleted"] == 1
    with index._connect() as conn:
        keys = {
            str(row["chunk_key"]) for row in conn.execute(f"SELECT chunk_key FROM {index.schema}.embeddings").fetchall()
        }
    assert keys == {"live", "unknown", "reusable"}


def _write_generation(dest: Path, *, chunks: list[str], keys: list[str], namespace: str) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    spec = EmbeddingSpec(
        provider_namespace=namespace,
        model="t",
        model_revision="1",
        dimension=4,
        metric="ip",
        normalization="none",
        chunk_schema="v1",
    )
    (dest / "manifest.json").write_text(
        json.dumps(
            {
                "serving_index_format_version": 2,
                "card_count": 1,
                "embedding_spec": spec.to_payload(),
            }
        ),
        encoding="utf-8",
    )
    (dest / "cards.jsonl").write_text('{"card_uid": "c1"}\n', encoding="utf-8")
    (dest / "chunks.jsonl").write_text(
        "".join(json.dumps({"chunk_key": key, "card_uid": "c1"}) + "\n" for key in chunks),
        encoding="utf-8",
    )
    (dest / "edges.jsonl").write_text("", encoding="utf-8")
    (dest / "embedding_keys.txt").write_text("".join(f"{key}\n" for key in keys), encoding="utf-8")
    (dest / "embeddings.bin").write_bytes(b"\x00" * (len(keys) * 4 * 4))
    (dest / "ivf_meta.json").write_text("{}", encoding="utf-8")


def test_coverage_gate_rejects_thin_openai_publish(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_PUBLICATION_MIN_EMBED_COVERAGE", "0.9")
    monkeypatch.setenv("PPA_PUBLICATION_MIN_EMBED_CHUNKS", "2")
    dest = tmp_path / "thin"
    _write_generation(dest, chunks=["k1", "k2"], keys=["k1"], namespace="openai")
    with pytest.raises(IncompatibleStateError, match="embedding_coverage:1/2"):
        validate_generation(dest)


def test_coverage_gate_allows_hash_and_full_openai(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_PUBLICATION_MIN_EMBED_COVERAGE", "0.9")
    monkeypatch.setenv("PPA_PUBLICATION_MIN_EMBED_CHUNKS", "2")
    hashed = tmp_path / "hash"
    _write_generation(hashed, chunks=["k1", "k2"], keys=["k1"], namespace="hash")
    assert validate_generation(hashed)["ok"] is True
    full = tmp_path / "full"
    _write_generation(full, chunks=["k1", "k2"], keys=["k1", "k2"], namespace="openai")
    assert validate_generation(full)["embeddings"] == 2


def test_prune_keeps_higher_coverage_retired_generation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "rust-search-index"
    gens = root / "generations"
    thin = gens / "thin-active"
    fat = gens / "fat-retired"
    thin.mkdir(parents=True)
    fat.mkdir()
    (thin / "manifest.json").write_text(json.dumps({"embedding_count": 10}), encoding="utf-8")
    (fat / "manifest.json").write_text(json.dumps({"embedding_count": 100}), encoding="utf-8")
    (root / "ACTIVE").write_text("thin-active\n", encoding="utf-8")
    empty = gens / "empty-old"
    empty.mkdir()
    (empty / "manifest.json").write_text(json.dumps({"embedding_count": 0}), encoding="utf-8")
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    removed = prune_retired_serving_generations(tmp_path)
    assert removed == ["empty-old"]
    assert fat.exists()
    assert thin.exists()


@pytest.mark.integration
def test_prior_schema_remap_copies_v5_vector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    from archive_cli.chunk_builders import _chunk_hash_for_schema
    from archive_cli.materializer import _chunk_key

    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    content = "same body after schema bump"
    fields = ["body"]
    old_hash = _chunk_hash_for_schema(5, "body", content, fields)
    new_hash = _chunk_hash_for_schema(6, "body", content, fields)
    old_key = _chunk_key("card-schema", "body", 0, old_hash)
    new_key = _chunk_key("card-schema", "body", 0, new_hash)
    assert old_key != new_key
    with index._connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {index.schema}.chunks (
                chunk_key, card_uid, rel_path, chunk_type, chunk_index,
                chunk_schema_version, source_fields, content, content_hash, token_count
            )
            VALUES (%s, 'card-schema', 'card-schema.md', 'body', 0, 6, %s::jsonb, %s, %s, 1)
            """,
            (new_key, json.dumps(fields), content, new_hash),
        )
        _insert_embedding(conn, index.schema, key=old_key, dim=8, fill=0.55)
        conn.commit()
    result = index.remap_embeddings_by_prior_schema(
        embedding_model="reuse-hash",
        embedding_version=1,
        schema_versions=(5,),
    )
    assert result["copied"] == 1
    with index._connect() as conn:
        row = conn.execute(
            f"SELECT content_hash FROM {index.schema}.embeddings WHERE chunk_key = %s",
            (new_key,),
        ).fetchone()
    assert row["content_hash"] == new_hash


@pytest.mark.integration
def test_scoped_embed_pending_reuses_reminted_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    from archive_cli.embedding_provider import HashEmbeddingProvider
    from archive_cli.engine_factory import embedding_spec_from_env

    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    with index._connect() as conn:
        _insert_chunk(conn, index.schema, key="old-key", uid="card-remint", index=0, content_hash="hash-same")
        _insert_embedding(
            conn,
            index.schema,
            key="old-key",
            dim=8,
            fill=0.33,
            content_hash="hash-same",
            uid="card-remint",
        )
        conn.execute(f"DELETE FROM {index.schema}.chunks WHERE chunk_key = 'old-key'")
        _insert_chunk(conn, index.schema, key="new-key", uid="card-remint", index=0, content_hash="hash-same")
        conn.commit()

    class CountingHash:
        name = "hash"

        def __init__(self) -> None:
            self.inner = HashEmbeddingProvider(model="reuse-hash", dimension=8)
            self.model = "reuse-hash"
            self.dimension = 8
            self.calls = 0

        def embed_texts(self, texts: list[str]) -> list[list[float]]:
            self.calls += 1
            return self.inner.embed_texts(texts)

    provider = CountingHash()
    first = index.embed_pending(
        provider=provider,
        embedding_model="reuse-hash",
        embedding_version=1,
        limit=0,
        include_context_prefix=False,
        uid_allowlist={"card-remint"},
        embedding_spec=embedding_spec_from_env(),
    )
    assert first.get("reused_by_content") == 1
    assert first["embedded"] == 0
    assert provider.calls == 0
    second = index.embed_pending(
        provider=provider,
        embedding_model="reuse-hash",
        embedding_version=1,
        limit=0,
        include_context_prefix=False,
        uid_allowlist={"card-remint"},
        embedding_spec=embedding_spec_from_env(),
    )
    assert second["embedded"] == 0
    assert second.get("reused_by_content") == 0
    assert provider.calls == 0


def test_reuse_methods_are_on_embedder_mixin() -> None:
    assert hasattr(EmbedderMixin, "reuse_embeddings_by_content")
    assert hasattr(EmbedderMixin, "remap_embeddings_by_slot")
    assert hasattr(EmbedderMixin, "load_slot_map_from_chunks_jsonl")
    assert hasattr(EmbedderMixin, "attach_embeddings_after_rematerialize")
    assert hasattr(EmbedderMixin, "delete_duplicate_leftover_embeddings")


@pytest.mark.integration
def test_reuse_pages_pending_and_cleans_leftover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    monkeypatch.setenv("PPA_WAREHOUSE_MIN_FREE_GB", "0")
    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    with index._connect() as conn:
        _insert_chunk(conn, index.schema, key="live-a", uid="card-a", index=0, content_hash="hash-a")
        _insert_chunk(conn, index.schema, key="live-b", uid="card-b", index=0, content_hash="hash-b")
        _insert_embedding(conn, index.schema, key="old-a", dim=8, fill=0.21, content_hash="hash-a")
        _insert_embedding(conn, index.schema, key="old-b", dim=8, fill=0.22, content_hash="hash-b")
        conn.commit()
    result = index.reuse_embeddings_by_content(
        embedding_model="reuse-hash",
        embedding_version=1,
        batch_size=1,
        cleanup_duplicates=True,
    )
    assert result["copied"] == 2
    assert result["cleaned"] == 2
    assert result["pending_after"] == 0
    with index._connect() as conn:
        keys = {
            str(row["chunk_key"]) for row in conn.execute(f"SELECT chunk_key FROM {index.schema}.embeddings").fetchall()
        }
    assert keys == {"live-a", "live-b"}


def test_chunk_hash_ignores_schema_version() -> None:
    from archive_cli.chunk_builders import _chunk_hash, _chunk_hash_for_schema

    identity = _chunk_hash("body", "hello", ["body"])
    assert identity == _chunk_hash("body", "hello", ["body"])
    assert identity != _chunk_hash_for_schema(5, "body", "hello", ["body"])
    assert identity != _chunk_hash_for_schema(6, "body", "hello", ["body"])
    assert identity != _chunk_hash_for_schema(7, "body", "hello", ["body"])
    assert _chunk_hash("body", "hello", ["body"]) != _chunk_hash("body", "hello!", ["body"])


@pytest.mark.integration
def test_version_only_attach_copies_v6_list_onto_content_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    from archive_cli.chunk_builders import _chunk_hash, _chunk_hash_for_schema
    from archive_cli.materializer import _chunk_key

    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    content = "same body after dropping version from the hash"
    fields = ["body"]
    old_hash = _chunk_hash_for_schema(6, "body", content, fields)
    new_hash = _chunk_hash("body", content, fields)
    old_key = _chunk_key("card-identity", "body", 0, old_hash)
    new_key = _chunk_key("card-identity", "body", 0, new_hash)
    assert old_key != new_key
    with index._connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {index.schema}.chunks (
                chunk_key, card_uid, rel_path, chunk_type, chunk_index,
                chunk_schema_version, source_fields, content, content_hash, token_count
            )
            VALUES (%s, 'card-identity', 'card-identity.md', 'body', 0, 7, %s::jsonb, %s, %s, 1)
            """,
            (new_key, json.dumps(fields), content, new_hash),
        )
        _insert_embedding(conn, index.schema, key=old_key, dim=8, fill=0.42)
        conn.commit()
    result = index.attach_embeddings_after_rematerialize(
        embedding_model="reuse-hash",
        embedding_version=1,
        fail_if_thin=False,
    )
    assert result["remapped"] == 1
    assert result["pending_after"] == 0
    with index._connect() as conn:
        row = conn.execute(
            f"SELECT content_hash FROM {index.schema}.embeddings WHERE chunk_key = %s",
            (new_key,),
        ).fetchone()
    assert row["content_hash"] == new_hash


@pytest.mark.integration
def test_text_change_does_not_copy_old_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str) -> None:
    from archive_cli.chunk_builders import _chunk_hash, _chunk_hash_for_schema
    from archive_cli.materializer import _chunk_key

    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    old_hash = _chunk_hash_for_schema(6, "body", "old text", ["body"])
    new_hash = _chunk_hash("body", "new text", ["body"])
    old_key = _chunk_key("card-changed", "body", 0, old_hash)
    new_key = _chunk_key("card-changed", "body", 0, new_hash)
    with index._connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {index.schema}.chunks (
                chunk_key, card_uid, rel_path, chunk_type, chunk_index,
                chunk_schema_version, source_fields, content, content_hash, token_count
            )
            VALUES (%s, 'card-changed', 'card-changed.md', 'body', 0, 7, %s::jsonb, %s, %s, 1)
            """,
            (new_key, json.dumps(["body"]), "new text", new_hash),
        )
        _insert_embedding(conn, index.schema, key=old_key, dim=8, fill=0.11)
        conn.commit()
    result = index.attach_embeddings_after_rematerialize(
        embedding_model="reuse-hash",
        embedding_version=1,
        fail_if_thin=False,
    )
    assert result["remapped"] == 0
    assert result["reused"] == 0
    assert result["pending_after"] == 1


@pytest.mark.integration
def test_attach_fails_closed_when_coverage_is_thin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    monkeypatch.setenv("PPA_PUBLICATION_MIN_EMBED_COVERAGE", "0.9")
    monkeypatch.setenv("PPA_PUBLICATION_MIN_EMBED_CHUNKS", "1")
    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    with index._connect() as conn:
        _insert_chunk(conn, index.schema, key="pending-key", uid="card-thin", index=0, content_hash="alone")
        conn.commit()
    with pytest.raises(IncompatibleStateError, match="embedding_coverage:0/1"):
        index.attach_embeddings_after_rematerialize(
            embedding_model="reuse-hash",
            embedding_version=1,
            fail_if_thin=True,
        )


@pytest.mark.integration
def test_duplicate_gc_drops_only_when_live_has_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    index = _bootstrap(tmp_path, monkeypatch, pgvector_dsn)
    with index._connect() as conn:
        _insert_chunk(conn, index.schema, key="live", uid="card-live", index=0, content_hash="keep-hash")
        _insert_embedding(
            conn,
            index.schema,
            key="live",
            dim=8,
            fill=0.1,
            content_hash="keep-hash",
            uid="card-live",
        )
        _insert_embedding(conn, index.schema, key="dup", dim=8, fill=0.2, content_hash="keep-hash")
        _insert_embedding(conn, index.schema, key="pending-old", dim=8, fill=0.3, content_hash="still-needed")
        conn.commit()
    store = DefaultArchiveStore(vault=tmp_path, index=index)
    import logging

    dry = embed_gc_cmd(
        store=store,
        logger=logging.getLogger("test"),
        dry_run=True,
        duplicates=True,
    )
    assert dry["duplicate_embeddings"] == 1
    assert dry["deleted"] == 0
    result = embed_gc_cmd(
        store=store,
        logger=logging.getLogger("test"),
        dry_run=False,
        duplicates=True,
        batch_size=1,
    )
    assert result["deleted"] == 1
    with index._connect() as conn:
        keys = {
            str(row["chunk_key"]) for row in conn.execute(f"SELECT chunk_key FROM {index.schema}.embeddings").fetchall()
        }
    assert keys == {"live", "pending-old"}
