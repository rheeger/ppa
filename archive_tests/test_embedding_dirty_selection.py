"""P03-B: dirty embed selects by UID/chunk-key, not by draining the global backlog."""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import pytest

from archive_cli.embedder import embed_selection_sql, normalize_embed_allowlist, require_embed_selection
from archive_cli.embedding_provider import HashEmbeddingProvider
from archive_cli.engine_factory import embedding_spec_from_env
from archive_cli.index_config import CHUNK_SCHEMA_VERSION, _vector_literal
from archive_cli.index_store import PostgresArchiveIndex
from archive_engine.contracts import EmbeddingSpec
from archive_sync.processors.batch import ProcessorPlanItem
from archive_sync.processors.constants import (
    INPUT_STATUS_COMPLETE,
    INPUT_STATUS_FAILED,
    PROCESSOR_EMBEDDING,
)
from archive_sync.processors.runner import ExecuteContext, _execute_embedding

DIRTY_UID = "hfa-person-p03bdirty01"
BACKLOG_UID = "hfa-person-p03bbacklog"
DIRTY_KEYS = ("ck-dirty-0", "ck-dirty-1", "ck-dirty-2")
BACKLOG_KEYS = ("ck-backlog-0", "ck-backlog-1")


class CountingHashProvider:
    name = "hash"

    def __init__(self, inner: HashEmbeddingProvider):
        self.inner = inner
        self.model = inner.model
        self.dimension = inner.dimension
        self.calls = 0
        self.texts: list[str] = []

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        self.texts.extend(texts)
        return self.inner.embed_texts(texts)


class FailingProvider:
    name = "hash"

    def __init__(self, *, model: str, dimension: int):
        self.model = model
        self.dimension = dimension
        self.calls = 0

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        raise RuntimeError("injected provider failure")


def test_require_embed_selection_rejects_unscoped_dirty_call() -> None:
    with pytest.raises(ValueError, match="uid_allowlist or chunk_key_allowlist"):
        require_embed_selection(uid_allowlist=None, chunk_key_allowlist=None, unscoped=False)


def test_embed_selection_sql_applies_predicate_before_limit() -> None:
    sql, params = embed_selection_sql(
        uid_allowlist=normalize_embed_allowlist({DIRTY_UID}),
        chunk_key_allowlist=None,
    )
    assert "c.card_uid = ANY(%s)" in sql
    assert params == [[DIRTY_UID]]
    empty_sql, empty_params = embed_selection_sql(uid_allowlist=(), chunk_key_allowlist=None)
    assert "c.card_uid = ANY(%s)" in empty_sql
    assert empty_params == [[]]


def test_batch_claim_applies_allowlist_before_limit() -> None:
    import inspect

    from archive_cli.batch_embedder import _claim_pending_chunks_for_batch
    from archive_cli.embedder import embed_selection_sql

    src = inspect.getsource(_claim_pending_chunks_for_batch)
    helper = inspect.getsource(embed_selection_sql)
    assert "embed_selection_sql" in src
    assert "c.card_uid = ANY(%s)" in helper
    assert "c.chunk_key = ANY(%s)" in helper
    assert "LIMIT %s" in src
    assert src.find("extra_sql") < src.rfind("LIMIT %s")


def _minimal_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in ("People", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    return vault


def _configure_embed_env(monkeypatch: pytest.MonkeyPatch, *, vault: Path, dsn: str, schema: str, model: str) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    monkeypatch.delenv("PPA_CONFIG_PATH", raising=False)
    monkeypatch.delenv("PPA_SERVING_INDEX_PATH", raising=False)
    monkeypatch.delenv("PPA_QUERY_EMBED_CACHE_PATH", raising=False)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)
    monkeypatch.setenv("PPA_VECTOR_DIMENSION", "8")
    monkeypatch.setenv("PPA_EMBEDDING_MODEL", model)
    monkeypatch.setenv("PPA_EMBEDDING_VERSION", "1")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.setenv("PPA_EMBED_CONCURRENCY", "1")
    monkeypatch.setenv("PPA_EMBED_BATCH_SIZE", "8")
    monkeypatch.setenv("PPA_EMBED_MAX_RETRIES", "0")
    monkeypatch.setenv("PPA_BOOTSTRAP_FORCE", "1")


def _insert_card(conn, schema: str, *, uid: str, rel: str) -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.cards (uid, rel_path, slug, type, summary, content_hash)
        VALUES (%s, %s, %s, 'person', %s, %s)
        """,
        (uid, rel, uid, uid, hashlib.sha256(uid.encode()).hexdigest()),
    )


def _insert_chunk(
    conn,
    schema: str,
    *,
    key: str,
    uid: str,
    rel: str,
    index: int,
    content: str,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.chunks (
            chunk_key, card_uid, rel_path, chunk_type, chunk_index,
            chunk_schema_version, content, content_hash, token_count
        )
        VALUES (%s, %s, %s, 'body', %s, %s, %s, %s, 1)
        """,
        (
            key,
            uid,
            rel,
            index,
            CHUNK_SCHEMA_VERSION,
            content,
            hashlib.sha256(content.encode()).hexdigest(),
        ),
    )


def _seed_dirty_and_backlog(index: PostgresArchiveIndex) -> None:
    with index._connect() as conn:
        _insert_card(conn, index.schema, uid=BACKLOG_UID, rel="0-backlog.md")
        _insert_card(conn, index.schema, uid=DIRTY_UID, rel="1-dirty.md")
        for idx, key in enumerate(BACKLOG_KEYS):
            _insert_chunk(
                conn,
                index.schema,
                key=key,
                uid=BACKLOG_UID,
                rel="0-backlog.md",
                index=idx,
                content=f"older backlog chunk {idx} " + ("backlog " * 20),
            )
        for idx, key in enumerate(DIRTY_KEYS):
            _insert_chunk(
                conn,
                index.schema,
                key=key,
                uid=DIRTY_UID,
                rel="1-dirty.md",
                index=idx,
                content=f"dirty selected chunk {idx} " + ("dirty " * 20),
            )
        conn.commit()


def _embedded_keys(index: PostgresArchiveIndex, *, model: str, version: int = 1) -> set[str]:
    with index._connect() as conn:
        rows = conn.execute(
            f"""
            SELECT chunk_key FROM {index.schema}.embeddings
            WHERE embedding_model = %s AND embedding_version = %s
            """,
            (model, version),
        ).fetchall()
    return {str(row["chunk_key"]) for row in rows}


def _make_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, dsn: str, *, model: str) -> PostgresArchiveIndex:
    vault = _minimal_vault(tmp_path)
    schema = f"p03b_{uuid.uuid4().hex[:10]}"
    _configure_embed_env(monkeypatch, vault=vault, dsn=dsn, schema=schema, model=model)
    index = PostgresArchiveIndex(vault, dsn=dsn)
    index.schema = schema
    index.vector_dimension = 8
    index.bootstrap()
    return index


@pytest.mark.integration
def test_dirty_multi_chunk_card_ignores_older_backlog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    model = "p03b-hash"
    index = _make_index(tmp_path, monkeypatch, pgvector_dsn, model=model)
    _seed_dirty_and_backlog(index)
    provider = CountingHashProvider(HashEmbeddingProvider(model=model, dimension=8))
    spec = embedding_spec_from_env()
    result = index.embed_pending(
        provider=provider,
        embedding_model=model,
        embedding_version=1,
        limit=0,
        include_context_prefix=False,
        uid_allowlist={DIRTY_UID},
        embedding_spec=spec,
    )
    selected = set(result["selected_chunk_keys"])
    completed = set(result["completed_chunk_keys"])
    assert selected == set(DIRTY_KEYS)
    assert completed == set(DIRTY_KEYS)
    assert set(result["pending_chunk_keys"]) == set()
    assert set(result["failed_chunk_keys"]) == set()
    assert result["embedded"] == 3
    assert provider.calls >= 1
    warehouse = _embedded_keys(index, model=model)
    assert warehouse == set(DIRTY_KEYS)
    assert warehouse.isdisjoint(BACKLOG_KEYS)


@pytest.mark.integration
def test_compatible_cache_hit_skips_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    model = "p03b-hash"
    index = _make_index(tmp_path, monkeypatch, pgvector_dsn, model=model)
    _seed_dirty_and_backlog(index)
    inner = HashEmbeddingProvider(model=model, dimension=8)
    with index._connect() as conn:
        for key in DIRTY_KEYS:
            vector = inner.embed_texts([f"cached {key}"])[0]
            conn.execute(
                f"""
                INSERT INTO {index.schema}.embeddings
                    (chunk_key, embedding_model, embedding_version, embedding)
                VALUES (%s, %s, %s, %s::vector)
                """,
                (key, model, 1, _vector_literal(vector)),
            )
        conn.commit()
    provider = CountingHashProvider(inner)
    result = index.embed_pending(
        provider=provider,
        embedding_model=model,
        embedding_version=1,
        limit=0,
        include_context_prefix=False,
        uid_allowlist={DIRTY_UID},
        embedding_spec=embedding_spec_from_env(),
    )
    assert provider.calls == 0
    assert set(result["reused_chunk_keys"]) == set(DIRTY_KEYS)
    assert set(result["completed_chunk_keys"]) == set(DIRTY_KEYS)
    assert result["embedded"] == 0
    assert result["submitted_chunk_keys"] == []


@pytest.mark.integration
def test_incompatible_spec_does_not_reuse_stale_vectors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    model = "p03b-hash"
    index = _make_index(tmp_path, monkeypatch, pgvector_dsn, model=model)
    _seed_dirty_and_backlog(index)
    stale = HashEmbeddingProvider(model="stale-model", dimension=8)
    with index._connect() as conn:
        vector = stale.embed_texts(["stale"])[0]
        conn.execute(
            f"""
            INSERT INTO {index.schema}.embeddings
                (chunk_key, embedding_model, embedding_version, embedding)
            VALUES (%s, %s, %s, %s::vector)
            """,
            (DIRTY_KEYS[0], "stale-model", 1, _vector_literal(vector)),
        )
        conn.commit()
    provider = CountingHashProvider(HashEmbeddingProvider(model=model, dimension=8))
    result = index.embed_pending(
        provider=provider,
        embedding_model=model,
        embedding_version=1,
        limit=0,
        include_context_prefix=False,
        chunk_key_allowlist={DIRTY_KEYS[0]},
        embedding_spec=embedding_spec_from_env(),
    )
    assert provider.calls >= 1
    assert DIRTY_KEYS[0] not in set(result["reused_chunk_keys"])
    assert DIRTY_KEYS[0] in set(result["completed_chunk_keys"])
    warehouse = _embedded_keys(index, model=model)
    assert DIRTY_KEYS[0] in warehouse
    stale_rows = _embedded_keys(index, model="stale-model")
    assert DIRTY_KEYS[0] in stale_rows


@pytest.mark.integration
def test_limit_is_budget_after_allowlist(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str) -> None:
    model = "p03b-hash"
    index = _make_index(tmp_path, monkeypatch, pgvector_dsn, model=model)
    _seed_dirty_and_backlog(index)
    provider = CountingHashProvider(HashEmbeddingProvider(model=model, dimension=8))
    result = index.embed_pending(
        provider=provider,
        embedding_model=model,
        embedding_version=1,
        limit=1,
        include_context_prefix=False,
        uid_allowlist={DIRTY_UID},
        embedding_spec=embedding_spec_from_env(),
    )
    assert set(result["selected_chunk_keys"]) == set(DIRTY_KEYS)
    assert len(result["completed_chunk_keys"]) == 1
    assert len(result["pending_chunk_keys"]) == 2
    assert _embedded_keys(index, model=model).isdisjoint(BACKLOG_KEYS)


@pytest.mark.integration
def test_unscoped_without_allowlist_rejected_on_dirty_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    model = "p03b-hash"
    index = _make_index(tmp_path, monkeypatch, pgvector_dsn, model=model)
    provider = HashEmbeddingProvider(model=model, dimension=8)
    with pytest.raises(ValueError, match="uid_allowlist or chunk_key_allowlist"):
        index.embed_pending(
            provider=provider,
            embedding_model=model,
            embedding_version=1,
            limit=1,
            include_context_prefix=False,
        )


@pytest.mark.integration
def test_provider_failure_does_not_complete_card(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    model = "p03b-hash"
    index = _make_index(tmp_path, monkeypatch, pgvector_dsn, model=model)
    _seed_dirty_and_backlog(index)
    provider = FailingProvider(model=model, dimension=8)
    from archive_cli.store import DefaultArchiveStore

    store = DefaultArchiveStore(
        vault=index.vault,
        index=index,
        provider_factory=lambda model="": provider,
    )
    store.config.retrieval.setdefault("context", {})["include_in_embeddings"] = False
    ctx = ExecuteContext(
        vault_path=str(index.vault),
        store=store,
        apply=True,
        dry_run=False,
        provider_available=True,
        run_id="p03b-fail",
    )
    items = [
        ProcessorPlanItem(
            processor_key=PROCESSOR_EMBEDDING,
            input_uid=DIRTY_UID,
            current_input_hash="rev-dirty",
            input_revision="rev-dirty",
        )
    ]
    out = _execute_embedding(ctx, items)
    assert out.results
    assert out.results[0].status == INPUT_STATUS_FAILED
    assert out.results[0].receipt is not None
    assert out.results[0].receipt.status == "failed"
    assert _embedded_keys(index, model=model) == set()
    assert provider.calls >= 1


@pytest.mark.integration
def test_processor_completes_only_selected_dirty_card(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    model = "p03b-hash"
    index = _make_index(tmp_path, monkeypatch, pgvector_dsn, model=model)
    _seed_dirty_and_backlog(index)
    provider = CountingHashProvider(HashEmbeddingProvider(model=model, dimension=8))
    from archive_cli.store import DefaultArchiveStore

    store = DefaultArchiveStore(
        vault=index.vault,
        index=index,
        provider_factory=lambda model="": provider,
    )
    store.config.retrieval.setdefault("context", {})["include_in_embeddings"] = False
    ctx = ExecuteContext(
        vault_path=str(index.vault),
        store=store,
        apply=True,
        dry_run=False,
        provider_available=True,
        run_id="p03b-ok",
    )
    items = [
        ProcessorPlanItem(
            processor_key=PROCESSOR_EMBEDDING,
            input_uid=DIRTY_UID,
            current_input_hash="rev-dirty",
            input_revision="rev-dirty",
        )
    ]
    out = _execute_embedding(ctx, items)
    assert out.results[0].status == INPUT_STATUS_COMPLETE
    assert out.results[0].receipt is not None
    assert set(out.results[0].receipt.chunk_keys) == set(DIRTY_KEYS)
    assert isinstance(out.results[0].receipt.embedding_spec, EmbeddingSpec)
    assert _embedded_keys(index, model=model) == set(DIRTY_KEYS)
