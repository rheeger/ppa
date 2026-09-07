"""Embedding pipeline mixin for PostgresArchiveIndex."""

from __future__ import annotations

import json
import logging
import shutil
import time
from collections.abc import Collection
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from threading import Lock
from typing import Any

from archive_engine.contracts import EmbeddingSpec

from .features import build_context_prefix_for_embed_row
from archive_engine.errors import IncompatibleStateError

from .index_config import (
    CHUNK_SCHEMA_VERSION,
    EmbeddingBatchResult,
    _vector_literal,
    embed_defer_vector_index,
    get_embed_batch_size,
    get_embed_concurrency,
    get_embed_max_retries,
    get_embed_progress_every,
    get_embed_write_batch_size,
    get_prior_chunk_schema_versions,
    get_publication_min_embed_chunks,
    get_publication_min_embed_coverage,
    get_default_embedding_model,
    get_default_embedding_version,
    get_embed_gc_batch_size,
    get_embed_reuse_batch_size,
    get_warehouse_min_free_gb,
)
from .loader import _chunked, _log_rebuild_step, _RebuildProgressReporter

logger = logging.getLogger("ppa.embedder")


def normalize_embed_allowlist(values: Collection[str] | None) -> tuple[str, ...] | None:
    """Normalize an allowlist. ``None`` means no predicate; empty means match nothing."""

    if values is None:
        return None
    return tuple(sorted({str(value).strip() for value in values if str(value).strip()}))


def require_embed_selection(
    *,
    uid_allowlist: Collection[str] | None,
    chunk_key_allowlist: Collection[str] | None,
    unscoped: bool,
) -> None:
    """Dirty embed must name UIDs or chunk keys. Unscoped is the admin route only."""

    if uid_allowlist is None and chunk_key_allowlist is None and not unscoped:
        raise ValueError(
            "embed_pending requires uid_allowlist or chunk_key_allowlist; "
            "unscoped=True is reserved for the admin embed-pending route"
        )


def embed_selection_sql(
    *,
    uid_allowlist: tuple[str, ...] | None,
    chunk_key_allowlist: tuple[str, ...] | None,
) -> tuple[str, list[Any]]:
    """SQL predicate applied before LIMIT. Empty allowlists match nothing."""

    clauses: list[str] = []
    params: list[Any] = []
    if uid_allowlist is not None:
        clauses.append("c.card_uid = ANY(%s)")
        params.append(list(uid_allowlist))
    if chunk_key_allowlist is not None:
        clauses.append("c.chunk_key = ANY(%s)")
        params.append(list(chunk_key_allowlist))
    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params


def current_chunk_schema_id() -> str:
    return f"chunk_schema_v{CHUNK_SCHEMA_VERSION}"


def embedding_spec_matches_index(
    spec: EmbeddingSpec,
    *,
    model: str,
    version: int,
    dimension: int,
) -> bool:
    """True when ``spec`` names the same cache identity the index will write."""

    return (
        spec.model == model
        and spec.model_revision == str(version)
        and spec.dimension == dimension
        and spec.chunk_schema == current_chunk_schema_id()
    )


def _calculate_embed_progress(
    *,
    embedded_so_far: int,
    failed_so_far: int,
    total_pending: int,
    elapsed_seconds: float,
) -> dict[str, float | int]:
    """Calculate embedding progress metrics for structured logging."""
    rate = embedded_so_far / elapsed_seconds if elapsed_seconds > 0 else 0.0
    remaining = max(total_pending - embedded_so_far - failed_so_far, 0)
    eta = remaining / rate if rate > 0 else 0.0
    return {
        "embedded": embedded_so_far,
        "failed": failed_so_far,
        "remaining": remaining,
        "rate_per_second": round(rate, 1),
        "elapsed_seconds": round(elapsed_seconds, 1),
        "eta_seconds": round(eta, 1),
    }


def _format_mss(seconds: float) -> str:
    """Format seconds as M:SS per PPA operational logging convention."""
    total = int(seconds)
    return f"{total // 60}:{total % 60:02d}"


class EmbedderMixin:
    """Mixin that implements the full embedding pipeline.

    Assumes the host class provides:
    - self.schema: str
    - self.vector_dimension: int
    - self._connect() -> psycopg connection
    - self.ensure_ready()
    - self._drop_embeddings_vector_index(conn)
    - self._ensure_embeddings_vector_index(conn)
    """

    def embedding_status(self, *, embedding_model: str, embedding_version: int) -> dict[str, int | str]:
        self.ensure_ready()
        with self._connect() as conn:
            total_row = conn.execute(f"SELECT COUNT(*) AS count FROM {self.schema}.chunks").fetchone()
            embedded_row = conn.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM {self.schema}.embeddings
                WHERE embedding_model = %s AND embedding_version = %s
                """,
                (embedding_model, embedding_version),
            ).fetchone()
            # Orphan embeddings (chunk deleted/replaced) make chunks-embeddings
            # negative even when current chunks still lack vectors. embed_pending
            # uses this count as its work limit, so it must be the anti-join.
            pending_row = conn.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM {self.schema}.chunks c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {self.schema}.embeddings e
                    WHERE e.chunk_key = c.chunk_key
                      AND e.embedding_model = %s
                      AND e.embedding_version = %s
                )
                """,
                (embedding_model, embedding_version),
            ).fetchone()
        total_chunks = int(total_row["count"])
        embedded_chunks = int(embedded_row["count"])
        pending_chunks = int(pending_row["count"])
        return {
            "embedding_model": embedding_model,
            "embedding_version": embedding_version,
            "chunk_schema_version": CHUNK_SCHEMA_VERSION,
            "chunk_count": total_chunks,
            "embedded_chunk_count": embedded_chunks,
            "pending_chunk_count": pending_chunks,
        }

    def embedding_backlog(
        self,
        *,
        embedding_model: str,
        embedding_version: int,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        self.ensure_ready()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT c.rel_path, c.chunk_type, c.chunk_index, c.content, c.token_count
                FROM {self.schema}.chunks c
                LEFT JOIN {self.schema}.embeddings e
                    ON e.chunk_key = c.chunk_key
                    AND e.embedding_model = %s
                    AND e.embedding_version = %s
                WHERE e.chunk_key IS NULL
                ORDER BY c.rel_path, c.chunk_type, c.chunk_index
                LIMIT %s
                """,
                (embedding_model, embedding_version, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def _materialize_embed_context(self, conn) -> int:
        """Pre-aggregate card context (type, summary, sources, people, orgs) into a
        lookup table so the embedding claim query avoids per-batch correlated subqueries.
        """
        conn.execute(f"DROP TABLE IF EXISTS {self.schema}.card_embed_context")
        conn.execute(
            f"""
            CREATE TABLE {self.schema}.card_embed_context (
                card_uid TEXT NOT NULL PRIMARY KEY,
                card_type TEXT NOT NULL DEFAULT '',
                summary TEXT NOT NULL DEFAULT '',
                activity_at TIMESTAMPTZ,
                sources_agg TEXT NOT NULL DEFAULT '',
                people_agg TEXT NOT NULL DEFAULT '',
                orgs_agg TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO {self.schema}.card_embed_context
                (card_uid, card_type, summary, activity_at, sources_agg, people_agg, orgs_agg)
            SELECT
                card.uid,
                card.type,
                card.summary,
                card.activity_at,
                COALESCE(src.sources_agg, ''),
                COALESCE(ppl.people_agg, ''),
                COALESCE(org.orgs_agg, '')
            FROM {self.schema}.cards card
            LEFT JOIN LATERAL (
                SELECT string_agg(cs.source, '|' ORDER BY cs.source) AS sources_agg
                FROM {self.schema}.card_sources cs WHERE cs.card_uid = card.uid
            ) src ON true
            LEFT JOIN LATERAL (
                SELECT string_agg(cp.person, '|' ORDER BY cp.person) AS people_agg
                FROM {self.schema}.card_people cp WHERE cp.card_uid = card.uid
            ) ppl ON true
            LEFT JOIN LATERAL (
                SELECT string_agg(co.org, '|' ORDER BY co.org) AS orgs_agg
                FROM {self.schema}.card_orgs co WHERE co.card_uid = card.uid
            ) org ON true
            WHERE card.uid IN (
                SELECT DISTINCT c.card_uid
                FROM {self.schema}.embed_queue q
                JOIN {self.schema}.chunks c ON c.chunk_key = q.chunk_key
            )
            """
        )
        row = conn.execute(f"SELECT count(*) AS cnt FROM {self.schema}.card_embed_context").fetchone()
        count = int(row["cnt"]) if row else 0
        conn.commit()
        return count

    def _list_selected_embed_chunks(
        self,
        conn,
        *,
        embedding_model: str,
        embedding_version: int,
        uid_allowlist: tuple[str, ...] | None,
        chunk_key_allowlist: tuple[str, ...] | None,
    ) -> list[dict[str, Any]]:
        extra_sql, extra_params = embed_selection_sql(
            uid_allowlist=uid_allowlist,
            chunk_key_allowlist=chunk_key_allowlist,
        )
        rows = conn.execute(
            f"""
            SELECT c.chunk_key, c.card_uid,
                   (e.chunk_key IS NOT NULL) AS has_compatible
            FROM {self.schema}.chunks c
            LEFT JOIN {self.schema}.embeddings e
                ON e.chunk_key = c.chunk_key
                AND e.embedding_model = %s
                AND e.embedding_version = %s
            WHERE 1=1{extra_sql}
            ORDER BY c.rel_path, c.chunk_type, c.chunk_index
            """,
            (embedding_model, embedding_version, *extra_params),
        ).fetchall()
        return [dict(row) for row in rows]

    def _materialize_embed_queue(
        self,
        conn,
        *,
        embedding_model: str,
        embedding_version: int,
        uid_allowlist: tuple[str, ...] | None = None,
        chunk_key_allowlist: tuple[str, ...] | None = None,
    ) -> int:
        """Build a work queue of chunk_keys that need embedding. Workers pop from this
        queue instead of scanning the full chunks table with LEFT JOIN on every batch.

        Allowlists are applied in SQL before any worker budget (LIMIT) is claimed.
        """
        extra_sql, extra_params = embed_selection_sql(
            uid_allowlist=uid_allowlist,
            chunk_key_allowlist=chunk_key_allowlist,
        )
        conn.execute(f"DROP TABLE IF EXISTS {self.schema}.embed_queue")
        conn.execute(
            f"""
            CREATE UNLOGGED TABLE {self.schema}.embed_queue (
                queue_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                chunk_key TEXT NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO {self.schema}.embed_queue (chunk_key)
            SELECT c.chunk_key
            FROM {self.schema}.chunks c
            LEFT JOIN {self.schema}.embeddings e
                ON e.chunk_key = c.chunk_key
                AND e.embedding_model = %s
                AND e.embedding_version = %s
            WHERE e.chunk_key IS NULL{extra_sql}
            ORDER BY c.rel_path, c.chunk_type, c.chunk_index
            """,
            (embedding_model, embedding_version, *extra_params),
        )
        row = conn.execute(f"SELECT count(*) AS cnt FROM {self.schema}.embed_queue").fetchone()
        count = int(row["cnt"]) if row else 0
        conn.commit()
        return count

    def _drop_embed_work_tables(self, conn) -> None:
        conn.execute(f"DROP TABLE IF EXISTS {self.schema}.embed_queue")
        conn.execute(f"DROP TABLE IF EXISTS {self.schema}.card_embed_context")
        conn.commit()

    def _claim_embedding_batch(
        self,
        conn,
        *,
        embedding_model: str,
        embedding_version: int,
        limit: int,
        include_context_prefix: bool = False,
    ) -> list[dict[str, Any]]:
        claimed_keys = conn.execute(
            f"""
            DELETE FROM {self.schema}.embed_queue
            WHERE queue_id IN (
                SELECT queue_id FROM {self.schema}.embed_queue
                ORDER BY queue_id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING chunk_key
            """,
            (limit,),
        ).fetchall()
        if not claimed_keys:
            return []
        keys = [row["chunk_key"] for row in claimed_keys]
        placeholders = ",".join(["%s"] * len(keys))
        if include_context_prefix:
            rows = conn.execute(
                f"""
                SELECT c.chunk_key, c.card_uid, c.rel_path, c.chunk_type, c.chunk_index, c.content, c.token_count,
                       ctx.card_type AS ctype,
                       ctx.summary,
                       ctx.activity_at,
                       ctx.sources_agg,
                       ctx.people_agg,
                       ctx.orgs_agg
                FROM {self.schema}.chunks c
                JOIN {self.schema}.card_embed_context ctx ON ctx.card_uid = c.card_uid
                WHERE c.chunk_key IN ({placeholders})
                """,
                keys,
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT c.chunk_key, c.card_uid, c.rel_path, c.chunk_type, c.chunk_index, c.content, c.token_count
                FROM {self.schema}.chunks c
                WHERE c.chunk_key IN ({placeholders})
                """,
                keys,
            ).fetchall()
        return [dict(row) for row in rows]

    def _bulk_upsert_embeddings(
        self,
        conn,
        *,
        embedding_model: str,
        embedding_version: int,
        rows: list[tuple[str, str]],
    ) -> None:
        if not rows:
            return
        values_sql = ",".join(["(%s, %s, %s, %s::vector)"] * len(rows))
        params: list[Any] = []
        for chunk_key, vector_literal in rows:
            params.extend([chunk_key, embedding_model, embedding_version, vector_literal])
        conn.execute(
            f"""
            INSERT INTO {self.schema}.embeddings(chunk_key, embedding_model, embedding_version, embedding)
            VALUES {values_sql}
            ON CONFLICT (chunk_key, embedding_model, embedding_version)
            DO UPDATE SET embedding = EXCLUDED.embedding
            """,
            params,
        )
        keys = [chunk_key for chunk_key, _vector in rows]
        conn.execute(
            f"""
            UPDATE {self.schema}.embeddings e
            SET
                content_hash = c.content_hash,
                card_uid = c.card_uid,
                chunk_type = c.chunk_type,
                chunk_index = c.chunk_index
            FROM {self.schema}.chunks c
            WHERE e.chunk_key = c.chunk_key
              AND e.embedding_model = %s
              AND e.embedding_version = %s
              AND e.chunk_key = ANY(%s)
            """,
            (embedding_model, embedding_version, keys),
        )

    def _embed_batch_with_retry(
        self,
        provider: Any,
        *,
        texts: list[str],
        max_retries: int,
    ) -> tuple[list[list[float]] | None, str]:
        last_error = ""
        for attempt in range(max_retries + 1):
            try:
                return provider.embed_texts(texts), last_error
            except Exception as exc:
                last_error = str(exc)
                if attempt >= max_retries:
                    break
                time.sleep(min(0.5 * (2**attempt), 4.0))
        return None, last_error

    def _process_embedding_claim(
        self,
        *,
        provider: Any,
        embedding_model: str,
        embedding_version: int,
        claim_size: int,
        max_retries: int,
        write_batch_size: int,
        include_context_prefix: bool = False,
    ) -> EmbeddingBatchResult:
        result = EmbeddingBatchResult()
        with self._connect() as conn:
            batch = self._claim_embedding_batch(
                conn,
                embedding_model=embedding_model,
                embedding_version=embedding_version,
                limit=claim_size,
                include_context_prefix=include_context_prefix,
            )
            if not batch:
                conn.rollback()
                return result
            claimed_keys = [str(row["chunk_key"]) for row in batch]
            result.claimed = len(batch)
            result.claimed_keys = list(claimed_keys)
            texts: list[str] = []
            for row in batch:
                body = str(row["content"])
                if include_context_prefix:
                    body = build_context_prefix_for_embed_row(row) + body
                texts.append(body)
            vectors, last_error = self._embed_batch_with_retry(
                provider,
                texts=texts,
                max_retries=max_retries,
            )
            if vectors is None:
                conn.rollback()
                result.failed = len(batch)
                result.failed_keys = list(claimed_keys)
                result.last_error = last_error
                return result
            if len(vectors) != len(batch):
                conn.rollback()
                result.failed = len(batch)
                result.failed_keys = list(claimed_keys)
                result.last_error = "Embedding provider returned mismatched vector count"
                return result

            payload: list[tuple[str, str]] = []
            for row, vector in zip(batch, vectors, strict=True):
                if len(vector) != self.vector_dimension:
                    conn.rollback()
                    result.failed = len(batch)
                    result.failed_keys = list(claimed_keys)
                    result.last_error = (
                        f"Embedding dimension mismatch for {row['chunk_key']}: "
                        f"got={len(vector)} expected={self.vector_dimension}"
                    )
                    return result
                payload.append((str(row["chunk_key"]), _vector_literal(vector)))

            for upsert_batch in _chunked(payload, write_batch_size):
                self._bulk_upsert_embeddings(
                    conn,
                    embedding_model=embedding_model,
                    embedding_version=embedding_version,
                    rows=upsert_batch,
                )
            conn.commit()
            result.embedded = len(batch)
            result.embedded_keys = list(claimed_keys)
            result.card_uids = [
                str(row.get("card_uid") or "").strip() for row in batch if str(row.get("card_uid") or "").strip()
            ]
            return result

    def copy_embeddings_from_schema(
        self,
        *,
        source_schema: str,
        embedding_model: str,
        embedding_version: int,
    ) -> dict[str, int | str]:
        """Copy embeddings from another schema in the same DB into this one.

        Phase 6 Tier 4 / Step 22: chunk_key is a deterministic hash of the chunk's
        content (see materializer._chunk_key), so the same .md content produces the
        same chunk_key in any schema. After re-slicing the production seed into a
        new schema (`ppa_1pct`, `ppa_5pct`, etc.) and rebuilding chunks, every
        chunk row has an identical row already embedded in the source schema. This
        method JOINs them and copies the embedding bytes — no API calls, no cost.

        Returns counts of chunks_copied / chunks_already_present / chunks_no_source.
        Idempotent (ON CONFLICT DO NOTHING).
        """
        # Skip ensure_ready() — this op doesn't depend on the meta table; the explicit
        # preflight below verifies the only tables we touch (embeddings + chunks).
        with self._connect() as conn:
            # Bulk INSERT of ~500k rows can exceed the default statement_timeout
            # (often set to 60s by the operator). Disable for this connection.
            conn.execute("SET statement_timeout = 0")
            ensure_cols = getattr(self, "_ensure_embeddings_reuse_columns", None)
            if callable(ensure_cols):
                ensure_cols(conn)
            # Sanity check both schemas exist + have the expected tables.
            for schema in (source_schema, self.schema):
                row = conn.execute(
                    """
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = 'embeddings'
                    """,
                    (schema,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(f"schema '{schema}' does not have an embeddings table")

            def _scalar(row: Any) -> int:
                if row is None:
                    return 0
                if isinstance(row, dict):
                    return int(next(iter(row.values())))
                return int(row[0])

            target_chunks = _scalar(conn.execute(f"SELECT COUNT(*) AS n FROM {self.schema}.chunks").fetchone())
            already = _scalar(
                conn.execute(
                    f"""
                    SELECT COUNT(*) AS n FROM {self.schema}.embeddings
                    WHERE embedding_model = %s AND embedding_version = %s
                    """,
                    (embedding_model, embedding_version),
                ).fetchone()
            )
            t0 = time.time()
            by_key = conn.execute(
                f"""
                INSERT INTO {self.schema}.embeddings (
                    chunk_key, embedding_model, embedding_version, embedding,
                    content_hash, card_uid, chunk_type, chunk_index
                )
                SELECT
                    e.chunk_key, e.embedding_model, e.embedding_version, e.embedding,
                    c.content_hash, c.card_uid, c.chunk_type, c.chunk_index
                FROM {source_schema}.embeddings e
                JOIN {self.schema}.chunks c ON c.chunk_key = e.chunk_key
                WHERE e.embedding_model = %s
                  AND e.embedding_version = %s
                ON CONFLICT (chunk_key, embedding_model, embedding_version) DO NOTHING
                """,
                (embedding_model, embedding_version),
            )
            source_has_hash = conn.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'embeddings' AND column_name = 'content_hash'
                """,
                (source_schema,),
            ).fetchone()
            copied_by_hash = 0
            if source_has_hash is not None:
                by_hash = conn.execute(
                    f"""
                    INSERT INTO {self.schema}.embeddings (
                        chunk_key, embedding_model, embedding_version, embedding,
                        content_hash, card_uid, chunk_type, chunk_index
                    )
                    SELECT DISTINCT ON (c.chunk_key)
                        c.chunk_key, e.embedding_model, e.embedding_version, e.embedding,
                        c.content_hash, c.card_uid, c.chunk_type, c.chunk_index
                    FROM {self.schema}.chunks c
                    JOIN {source_schema}.embeddings e
                        ON e.content_hash = c.content_hash
                        AND e.embedding_model = %s
                        AND e.embedding_version = %s
                        AND e.content_hash <> ''
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {self.schema}.embeddings have
                        WHERE have.chunk_key = c.chunk_key
                          AND have.embedding_model = e.embedding_model
                          AND have.embedding_version = e.embedding_version
                    )
                    ORDER BY c.chunk_key, e.created_at DESC
                    ON CONFLICT (chunk_key, embedding_model, embedding_version) DO NOTHING
                    """,
                    (embedding_model, embedding_version),
                )
                copied_by_hash = int(by_hash.rowcount or 0)
            inserted = int(by_key.rowcount or 0) + copied_by_hash
            conn.commit()
            after = _scalar(
                conn.execute(
                    f"""
                    SELECT COUNT(*) AS n FROM {self.schema}.embeddings
                    WHERE embedding_model = %s AND embedding_version = %s
                    """,
                    (embedding_model, embedding_version),
                ).fetchone()
            )
        elapsed = time.time() - t0
        no_source = max(target_chunks - after, 0)
        logger.info(
            "copy_embeddings_from_schema source=%s -> %s model=%s v%d "
            "chunks_in_target=%d already_embedded=%d copied=%d no_source=%d elapsed=%.1fs",
            source_schema,
            self.schema,
            embedding_model,
            embedding_version,
            target_chunks,
            already,
            inserted,
            no_source,
            elapsed,
        )
        return {
            "source_schema": source_schema,
            "target_schema": self.schema,
            "embedding_model": embedding_model,
            "embedding_version": embedding_version,
            "chunks_in_target": target_chunks,
            "already_embedded_before_copy": already,
            "copied_from_source": inserted,
            "still_no_embedding": no_source,
            "elapsed_seconds": round(elapsed, 2),
        }

    def copy_classifications_from_schema(
        self,
        *,
        source_schema: str,
    ) -> dict[str, int | str]:
        """Copy card_classifications rows from another schema for any card_uid present here.

        Phase 6 Tier 4 / Step 23: card_uid is stable across schemas (it's stored in the
        .md frontmatter, not generated). After re-slicing the production seed into a
        new schema, every card_uid that exists in the new schema's cards table also
        exists in the source — so any triage classification the source has for that
        card is valid for the slice too. This avoids re-running the (expensive,
        LLM-driven) triage pipeline against the slice.

        Idempotent (ON CONFLICT DO NOTHING).
        """
        with self._connect() as conn:
            conn.execute("SET statement_timeout = 0")
            for schema in (source_schema, self.schema):
                row = conn.execute(
                    """
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = 'card_classifications'
                    """,
                    (schema,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(f"schema '{schema}' does not have a card_classifications table")

            def _scalar(row: Any) -> int:
                if row is None:
                    return 0
                if isinstance(row, dict):
                    return int(next(iter(row.values())))
                return int(row[0])

            target_cards = _scalar(conn.execute(f"SELECT COUNT(*) AS n FROM {self.schema}.cards").fetchone())
            already = _scalar(conn.execute(f"SELECT COUNT(*) AS n FROM {self.schema}.card_classifications").fetchone())
            t0 = time.time()
            inserted_rows = conn.execute(
                f"""
                INSERT INTO {self.schema}.card_classifications
                    (card_uid, classification, confidence, card_types, classified_at, classify_model)
                SELECT src.card_uid, src.classification, src.confidence, src.card_types,
                       src.classified_at, src.classify_model
                FROM {source_schema}.card_classifications src
                JOIN {self.schema}.cards c ON c.uid = src.card_uid
                ON CONFLICT (card_uid) DO NOTHING
                RETURNING 1
                """
            ).fetchall()
            inserted = len(inserted_rows)
            conn.commit()
            after = _scalar(conn.execute(f"SELECT COUNT(*) AS n FROM {self.schema}.card_classifications").fetchone())
        elapsed = time.time() - t0
        logger.info(
            "copy_classifications_from_schema source=%s -> %s "
            "cards_in_target=%d already_present=%d copied=%d total_after=%d elapsed=%.1fs",
            source_schema,
            self.schema,
            target_cards,
            already,
            inserted,
            after,
            elapsed,
        )
        return {
            "source_schema": source_schema,
            "target_schema": self.schema,
            "cards_in_target": target_cards,
            "already_present_before_copy": already,
            "copied_from_source": inserted,
            "total_after": after,
            "elapsed_seconds": round(elapsed, 2),
        }

    def build_vector_index(self) -> dict[str, int | str]:
        """Build the IVFFlat embeddings index, disabling statement_timeout.

        Phase 6 Tier 4 / Step 23: Phase 5's `_ensure_embeddings_vector_index` is
        gated behind the rebuild_indexes flow and may be skipped by `embed-pending
        --incremental`. This is a direct, idempotent way to ensure the index exists
        on a fresh slice schema after embeddings are populated. Without it, kNN
        queries fall back to sequential scans (we measured 1 sec/query vs 7ms with
        the index — 140x slowdown).
        """
        with self._connect() as conn:
            conn.execute("SET statement_timeout = 0")
            t0 = time.time()
            self._ensure_embeddings_vector_index(conn)
            conn.commit()
            elapsed = time.time() - t0
            row = conn.execute(
                """
                SELECT pg_size_pretty(pg_relation_size(c.oid)) AS size
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = 'idx_embeddings_vector'
                """,
                (self.schema,),
            ).fetchone()
            size = ""
            if row is not None:
                size = str(row["size"] if isinstance(row, dict) else row[0])
        logger.info(
            "build_vector_index schema=%s elapsed=%.1fs size=%s",
            self.schema,
            elapsed,
            size,
        )
        return {
            "schema": self.schema,
            "elapsed_seconds": round(elapsed, 2),
            "index_size": size,
        }

    def backfill_embedding_content_identity(self) -> int:
        """Copy current chunk identity onto embeddings that already share a key."""

        self.ensure_ready()
        with self._connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE {self.schema}.embeddings e
                SET
                    content_hash = c.content_hash,
                    card_uid = c.card_uid,
                    chunk_type = c.chunk_type,
                    chunk_index = c.chunk_index
                FROM {self.schema}.chunks c
                WHERE e.chunk_key = c.chunk_key
                  AND (e.content_hash = '' OR e.card_uid = '')
                """
            )
            updated = int(cur.rowcount or 0)
            conn.commit()
        logger.info("embeddings_identity_backfill updated=%s", updated)
        return updated

    def _warehouse_free_bytes(self) -> int:
        root = Path(getattr(self, "vault", None) or Path.home()).expanduser()
        try:
            return int(shutil.disk_usage(root).free)
        except OSError:
            return int(shutil.disk_usage(Path.home()).free)

    def _require_warehouse_free_space(self) -> None:
        min_gb = get_warehouse_min_free_gb()
        if min_gb <= 0:
            return
        free = self._warehouse_free_bytes()
        if free < min_gb * 1024**3:
            raise RuntimeError(
                f"warehouse_disk_low free_gb={free / 1024**3:.1f} min_gb={min_gb}"
            )

    def _count_pending_embeddings(self, conn, *, embedding_model: str, embedding_version: int) -> int:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM {self.schema}.chunks c
            WHERE NOT EXISTS (
                SELECT 1 FROM {self.schema}.embeddings e
                WHERE e.chunk_key = c.chunk_key
                  AND e.embedding_model = %s
                  AND e.embedding_version = %s
            )
            """,
            (embedding_model, embedding_version),
        ).fetchone()
        return int(row["n"] if isinstance(row, dict) else row[0])

    def _list_pending_chunk_keys(
        self,
        conn,
        *,
        embedding_model: str,
        embedding_version: int,
        after_key: str,
        limit: int,
    ) -> list[str]:
        rows = conn.execute(
            f"""
            SELECT c.chunk_key
            FROM {self.schema}.chunks c
            WHERE c.chunk_key > %s
              AND NOT EXISTS (
                SELECT 1 FROM {self.schema}.embeddings e
                WHERE e.chunk_key = c.chunk_key
                  AND e.embedding_model = %s
                  AND e.embedding_version = %s
              )
            ORDER BY c.chunk_key
            LIMIT %s
            """,
            (after_key, embedding_model, embedding_version, limit),
        ).fetchall()
        return [str(row["chunk_key"] if isinstance(row, dict) else row[0]) for row in rows]

    def delete_duplicate_leftover_embeddings(
        self,
        *,
        embedding_model: str,
        embedding_version: int,
        batch_size: int | None = None,
        max_batches: int | None = 1,
        content_hashes: Collection[str] | None = None,
    ) -> int:
        """Delete leftover keys whose content identity already has a live list.

        A scoped hash list is an index lookup. A full pass builds a key table
        once, then deletes by ``chunk_key`` so each batch is not a warehouse scan.
        """
        batch = max(int(batch_size or get_embed_gc_batch_size()), 1)
        hashes = [str(item) for item in (content_hashes or ()) if str(item).strip()]
        deleted_total = 0
        batches = 0
        with self._connect() as conn:
            conn.execute("SET statement_timeout = 0")
            if hashes:
                while True:
                    self._require_warehouse_free_space()
                    cur = conn.execute(
                        f"""
                        DELETE FROM {self.schema}.embeddings e
                        WHERE e.embedding_model = %s
                          AND e.embedding_version = %s
                          AND e.content_hash = ANY(%s)
                          AND NOT EXISTS (
                            SELECT 1 FROM {self.schema}.chunks c
                            WHERE c.chunk_key = e.chunk_key
                          )
                          AND EXISTS (
                            SELECT 1
                            FROM {self.schema}.chunks live
                            JOIN {self.schema}.embeddings have
                              ON have.chunk_key = live.chunk_key
                             AND have.embedding_model = e.embedding_model
                             AND have.embedding_version = e.embedding_version
                            WHERE live.content_hash = e.content_hash
                          )
                        """,
                        (embedding_model, embedding_version, hashes),
                    )
                    n = int(cur.rowcount or 0)
                    conn.commit()
                    deleted_total += n
                    batches += 1
                    logger.info(
                        "embeddings_duplicate_gc_scoped deleted=%s total_deleted=%s",
                        n,
                        deleted_total,
                    )
                    break
                return deleted_total
            conn.execute(
                f"""
                CREATE UNLOGGED TABLE IF NOT EXISTS {self.schema}.embedding_dup_gc (
                    chunk_key TEXT PRIMARY KEY
                )
                """
            )
            conn.execute(
                f"""
                CREATE UNLOGGED TABLE IF NOT EXISTS {self.schema}.embedding_live_hashes (
                    content_hash TEXT PRIMARY KEY
                )
                """
            )
            conn.commit()
            conn.execute(f"TRUNCATE {self.schema}.embedding_dup_gc")
            conn.execute(f"TRUNCATE {self.schema}.embedding_live_hashes")
            live = conn.execute(
                f"""
                INSERT INTO {self.schema}.embedding_live_hashes (content_hash)
                SELECT DISTINCT c.content_hash
                FROM {self.schema}.chunks c
                JOIN {self.schema}.embeddings have
                  ON have.chunk_key = c.chunk_key
                 AND have.embedding_model = %s
                 AND have.embedding_version = %s
                WHERE c.content_hash <> ''
                ON CONFLICT DO NOTHING
                """,
                (embedding_model, embedding_version),
            )
            conn.commit()
            logger.info(
                "embeddings_duplicate_gc_live_hashes rows=%s",
                int(live.rowcount or 0),
            )
            loaded = conn.execute(
                f"""
                INSERT INTO {self.schema}.embedding_dup_gc (chunk_key)
                SELECT e.chunk_key
                FROM {self.schema}.embeddings e
                JOIN {self.schema}.embedding_live_hashes h
                  ON h.content_hash = e.content_hash
                WHERE e.embedding_model = %s
                  AND e.embedding_version = %s
                  AND NOT EXISTS (
                    SELECT 1 FROM {self.schema}.chunks c WHERE c.chunk_key = e.chunk_key
                  )
                ON CONFLICT DO NOTHING
                """,
                (embedding_model, embedding_version),
            )
            conn.commit()
            queued = int(loaded.rowcount or 0)
            logger.info("embeddings_duplicate_gc_queued keys=%s", queued)
            while queued > 0:
                self._require_warehouse_free_space()
                keys = [
                    str(row["chunk_key"] if isinstance(row, dict) else row[0])
                    for row in conn.execute(
                        f"SELECT chunk_key FROM {self.schema}.embedding_dup_gc LIMIT %s",
                        (batch,),
                    ).fetchall()
                ]
                if not keys:
                    break
                cur = conn.execute(
                    f"""
                    DELETE FROM {self.schema}.embeddings e
                    WHERE e.chunk_key = ANY(%s)
                      AND e.embedding_model = %s
                      AND e.embedding_version = %s
                    """,
                    (keys, embedding_model, embedding_version),
                )
                n = int(cur.rowcount or 0)
                conn.execute(
                    f"DELETE FROM {self.schema}.embedding_dup_gc WHERE chunk_key = ANY(%s)",
                    (keys,),
                )
                conn.commit()
                deleted_total += n
                batches += 1
                queued -= len(keys)
                logger.info(
                    "embeddings_duplicate_gc_batch deleted=%s total_deleted=%s remaining=%s batch=%s",
                    n,
                    deleted_total,
                    queued,
                    batches,
                )
                if max_batches is not None and batches >= max_batches:
                    break
        return deleted_total

    def reuse_embeddings_by_content(
        self,
        *,
        embedding_model: str,
        embedding_version: int,
        batch_size: int | None = None,
        cleanup_duplicates: bool = True,
    ) -> dict[str, int]:
        """Attach existing vectors to new chunk_keys that share ``content_hash``.

        Copies a page of pending keys, commits, then deletes leftover old keys
        whose identity now has a live list. Stops if the vault volume is low.
        """

        self.ensure_ready()
        started = time.monotonic()
        page = max(int(batch_size or get_embed_reuse_batch_size()), 1)
        copied = 0
        cleaned = 0
        pages = 0
        after_key = ""
        with self._connect() as conn:
            conn.execute("SET statement_timeout = 0")
            pending_before = self._count_pending_embeddings(
                conn, embedding_model=embedding_model, embedding_version=embedding_version
            )
        while True:
            self._require_warehouse_free_space()
            with self._connect() as conn:
                conn.execute("SET statement_timeout = 0")
                keys = self._list_pending_chunk_keys(
                    conn,
                    embedding_model=embedding_model,
                    embedding_version=embedding_version,
                    after_key=after_key,
                    limit=page,
                )
                if not keys:
                    break
                inserted = conn.execute(
                    f"""
                    INSERT INTO {self.schema}.embeddings (
                        chunk_key, embedding_model, embedding_version, embedding,
                        content_hash, card_uid, chunk_type, chunk_index
                    )
                    SELECT DISTINCT ON (c.chunk_key)
                        c.chunk_key,
                        e.embedding_model,
                        e.embedding_version,
                        e.embedding,
                        c.content_hash,
                        c.card_uid,
                        c.chunk_type,
                        c.chunk_index
                    FROM {self.schema}.chunks c
                    JOIN {self.schema}.embeddings e
                        ON e.content_hash = c.content_hash
                        AND e.embedding_model = %s
                        AND e.embedding_version = %s
                        AND e.content_hash <> ''
                    WHERE c.chunk_key = ANY(%s)
                      AND NOT EXISTS (
                        SELECT 1 FROM {self.schema}.embeddings have
                        WHERE have.chunk_key = c.chunk_key
                          AND have.embedding_model = e.embedding_model
                          AND have.embedding_version = e.embedding_version
                      )
                    ORDER BY c.chunk_key, (e.card_uid = c.card_uid) DESC, e.created_at DESC
                    ON CONFLICT (chunk_key, embedding_model, embedding_version) DO NOTHING
                    """,
                    (embedding_model, embedding_version, keys),
                )
                conn.commit()
                page_copied = int(inserted.rowcount or 0)
                hash_rows = conn.execute(
                    f"SELECT content_hash FROM {self.schema}.chunks WHERE chunk_key = ANY(%s)",
                    (keys,),
                ).fetchall()
                page_hashes = [
                    str(row["content_hash"] if isinstance(row, dict) else row[0])
                    for row in hash_rows
                    if str(row["content_hash"] if isinstance(row, dict) else row[0]).strip()
                ]
            copied += page_copied
            pages += 1
            after_key = keys[-1]
            if cleanup_duplicates and page_hashes:
                cleaned += self.delete_duplicate_leftover_embeddings(
                    embedding_model=embedding_model,
                    embedding_version=embedding_version,
                    content_hashes=page_hashes,
                )
            logger.info(
                "embeddings_reuse_batch copied=%s page_copied=%s cleaned=%s page=%s keys=%s",
                copied,
                page_copied,
                cleaned,
                pages,
                len(keys),
            )
            if len(keys) < page:
                break
        with self._connect() as conn:
            pending_after = self._count_pending_embeddings(
                conn, embedding_model=embedding_model, embedding_version=embedding_version
            )
        logger.info(
            "embeddings_reuse_by_content copied=%s cleaned=%s pending_before=%s pending_after=%s pages=%s elapsed=%.1fs",
            copied,
            cleaned,
            pending_before,
            pending_after,
            pages,
            time.monotonic() - started,
        )
        return {
            "copied": copied,
            "cleaned": cleaned,
            "pending_before": pending_before,
            "pending_after": pending_after,
            "pages": pages,
        }

    def load_slot_map_from_chunks_jsonl(self, path: str | Path, *, progress_every: int = 100_000) -> int:
        """COPY ``(old_chunk_key, card_uid, chunk_type, chunk_index)`` from a serving chunks.jsonl."""

        src = Path(path)
        self.ensure_ready()
        loaded = 0
        with self._connect() as conn, src.open(encoding="utf-8") as fh:
            conn.execute("SET statement_timeout = 0")
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.embedding_slot_map (
                    old_chunk_key TEXT NOT NULL,
                    card_uid TEXT NOT NULL,
                    chunk_type TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL
                )
                """
            )
            conn.execute(f"TRUNCATE {self.schema}.embedding_slot_map")
            with conn.cursor() as cur:
                with cur.copy(
                    f"COPY {self.schema}.embedding_slot_map "
                    "(old_chunk_key, card_uid, chunk_type, chunk_index) FROM STDIN"
                ) as copy:
                    for line in fh:
                        if not line.strip():
                            continue
                        row = json.loads(line)
                        key = str(row.get("chunk_key") or "").strip()
                        uid = str(row.get("card_uid") or "").strip()
                        chunk_type = str(row.get("chunk_type") or "").strip()
                        if not key or not uid or not chunk_type:
                            continue
                        copy.write_row((key, uid, chunk_type, int(row.get("chunk_index") or 0)))
                        loaded += 1
                        if progress_every and loaded % progress_every == 0:
                            logger.info(
                                "embedding_slot_map_load rows=%s path=%s",
                                loaded,
                                src,
                            )
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_embedding_slot_map_old
                ON {self.schema}.embedding_slot_map (old_chunk_key)
                """
            )
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_embedding_slot_map_slot
                ON {self.schema}.embedding_slot_map (card_uid, chunk_type, chunk_index)
                """
            )
            conn.commit()
        logger.info("embedding_slot_map_load done rows=%s path=%s", loaded, src)
        return loaded

    def remap_embeddings_by_slot(
        self,
        *,
        embedding_model: str,
        embedding_version: int,
    ) -> dict[str, int]:
        """Copy orphan vectors onto current keys that share card/type/index.

        One-time recovery when rematerialize changed ``content_hash`` (and
        therefore ``chunk_key``) but not the chunk layout. Requires
        ``embedding_slot_map``.
        """

        self.ensure_ready()
        started = time.monotonic()
        with self._connect() as conn:
            conn.execute("SET statement_timeout = 0")
            exists = conn.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = 'embedding_slot_map'
                """,
                (self.schema,),
            ).fetchone()
            if exists is None:
                raise RuntimeError("embedding_slot_map is missing; load a chunks.jsonl first")
            inserted = conn.execute(
                f"""
                INSERT INTO {self.schema}.embeddings (
                    chunk_key, embedding_model, embedding_version, embedding,
                    content_hash, card_uid, chunk_type, chunk_index
                )
                SELECT DISTINCT ON (c.chunk_key)
                    c.chunk_key,
                    e.embedding_model,
                    e.embedding_version,
                    e.embedding,
                    c.content_hash,
                    c.card_uid,
                    c.chunk_type,
                    c.chunk_index
                FROM {self.schema}.embedding_slot_map m
                JOIN {self.schema}.chunks c
                    ON c.card_uid = m.card_uid
                    AND c.chunk_type = m.chunk_type
                    AND c.chunk_index = m.chunk_index
                JOIN {self.schema}.embeddings e
                    ON e.chunk_key = m.old_chunk_key
                    AND e.embedding_model = %s
                    AND e.embedding_version = %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.schema}.embeddings have
                    WHERE have.chunk_key = c.chunk_key
                      AND have.embedding_model = e.embedding_model
                      AND have.embedding_version = e.embedding_version
                )
                ORDER BY c.chunk_key, e.created_at DESC
                ON CONFLICT (chunk_key, embedding_model, embedding_version) DO NOTHING
                """,
                (embedding_model, embedding_version),
            )
            identity = conn.execute(
                f"""
                UPDATE {self.schema}.embeddings e
                SET
                    content_hash = c.content_hash,
                    card_uid = c.card_uid,
                    chunk_type = c.chunk_type,
                    chunk_index = c.chunk_index
                FROM {self.schema}.embedding_slot_map m
                JOIN {self.schema}.chunks c
                    ON c.card_uid = m.card_uid
                    AND c.chunk_type = m.chunk_type
                    AND c.chunk_index = m.chunk_index
                WHERE e.chunk_key = m.old_chunk_key
                  AND e.embedding_model = %s
                  AND e.embedding_version = %s
                  AND (e.content_hash = '' OR e.card_uid = '')
                """,
                (embedding_model, embedding_version),
            )
            conn.commit()
        copied = int(inserted.rowcount or 0)
        identified = int(identity.rowcount or 0)
        logger.info(
            "embeddings_remap_by_slot copied=%s identified=%s elapsed=%.1fs",
            copied,
            identified,
            time.monotonic() - started,
        )
        return {"copied": copied, "identified": identified}

    def remap_embeddings_by_prior_schema(
        self,
        *,
        embedding_model: str,
        embedding_version: int,
        schema_versions: tuple[int, ...] | None = None,
        progress_every: int = 100_000,
    ) -> dict[str, int]:
        """Rebuild pre-bump ``chunk_key``s from live content and copy those vectors.

        ``chunk_key`` includes ``content_hash``. Older recipes folded
        ``chunk_schema_version`` into that hash. Rebuild those keys from live
        text and copy the leftover lists. No provider call.
        """

        from archive_cli.chunk_builders import _chunk_hash_for_schema
        from archive_cli.materializer import _chunk_key

        raw_versions = schema_versions if schema_versions is not None else get_prior_chunk_schema_versions()
        versions = tuple(int(v) for v in raw_versions if int(v) > 0)
        if not versions:
            raise ValueError("schema_versions must contain at least one positive version")
        self.ensure_ready()
        started = time.monotonic()
        loaded = 0
        with self._connect() as write_conn, self._connect() as read_conn:
            write_conn.execute("SET statement_timeout = 0")
            read_conn.execute("SET statement_timeout = 0")
            write_conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.embedding_schema_remap (
                    old_chunk_key TEXT NOT NULL,
                    new_chunk_key TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    card_uid TEXT NOT NULL,
                    chunk_type TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL
                )
                """
            )
            write_conn.execute(f"TRUNCATE {self.schema}.embedding_schema_remap")
            with read_conn.cursor(name="ppa_prior_schema_chunks") as rcur:
                rcur.itersize = 10_000
                rcur.execute(
                    f"""
                    SELECT c.chunk_key, c.card_uid, c.chunk_type, c.chunk_index,
                           c.content, c.source_fields, c.content_hash
                    FROM {self.schema}.chunks c
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {self.schema}.embeddings e
                        WHERE e.chunk_key = c.chunk_key
                          AND e.embedding_model = %s
                          AND e.embedding_version = %s
                    )
                    """,
                    (embedding_model, embedding_version),
                )
                with write_conn.cursor() as wcur:
                    with wcur.copy(
                        f"COPY {self.schema}.embedding_schema_remap "
                        "(old_chunk_key, new_chunk_key, content_hash, card_uid, chunk_type, chunk_index) "
                        "FROM STDIN"
                    ) as copy:
                        for row in rcur:
                            payload = dict(row)
                            fields = payload.get("source_fields") or []
                            if not isinstance(fields, list):
                                fields = list(fields)
                            fields = [str(item) for item in fields]
                            uid = str(payload.get("card_uid") or "")
                            chunk_type = str(payload.get("chunk_type") or "")
                            index = int(payload.get("chunk_index") or 0)
                            new_key = str(payload.get("chunk_key") or "")
                            content = str(payload.get("content") or "")
                            live_hash = str(payload.get("content_hash") or "")
                            if not uid or not chunk_type or not new_key:
                                continue
                            for version in versions:
                                old_hash = _chunk_hash_for_schema(version, chunk_type, content, fields)
                                old_key = _chunk_key(uid, chunk_type, index, old_hash)
                                if old_key == new_key:
                                    continue
                                copy.write_row((old_key, new_key, live_hash, uid, chunk_type, index))
                                loaded += 1
                            if progress_every and loaded and loaded % progress_every == 0:
                                logger.info(
                                    "embedding_schema_remap_load rows=%s versions=%s",
                                    loaded,
                                    versions,
                                )
            logger.info("embedding_schema_remap_load done rows=%s versions=%s", loaded, versions)
            write_conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_embedding_schema_remap_old
                ON {self.schema}.embedding_schema_remap (old_chunk_key)
                """
            )
            write_conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_embedding_schema_remap_new
                ON {self.schema}.embedding_schema_remap (new_chunk_key)
                """
            )
            write_conn.commit()
            logger.info("embeddings_remap_by_prior_schema insert_start map_rows=%s", loaded)
            inserted = write_conn.execute(
                f"""
                INSERT INTO {self.schema}.embeddings (
                    chunk_key, embedding_model, embedding_version, embedding,
                    content_hash, card_uid, chunk_type, chunk_index
                )
                SELECT DISTINCT ON (m.new_chunk_key)
                    m.new_chunk_key,
                    e.embedding_model,
                    e.embedding_version,
                    e.embedding,
                    m.content_hash,
                    m.card_uid,
                    m.chunk_type,
                    m.chunk_index
                FROM {self.schema}.embedding_schema_remap m
                JOIN {self.schema}.embeddings e
                    ON e.chunk_key = m.old_chunk_key
                    AND e.embedding_model = %s
                    AND e.embedding_version = %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.schema}.embeddings have
                    WHERE have.chunk_key = m.new_chunk_key
                      AND have.embedding_model = e.embedding_model
                      AND have.embedding_version = e.embedding_version
                )
                ORDER BY m.new_chunk_key, e.created_at DESC
                ON CONFLICT (chunk_key, embedding_model, embedding_version) DO NOTHING
                """,
                (embedding_model, embedding_version),
            )
            identity = write_conn.execute(
                f"""
                UPDATE {self.schema}.embeddings e
                SET
                    content_hash = m.content_hash,
                    card_uid = m.card_uid,
                    chunk_type = m.chunk_type,
                    chunk_index = m.chunk_index
                FROM {self.schema}.embedding_schema_remap m
                WHERE e.chunk_key = m.old_chunk_key
                  AND e.embedding_model = %s
                  AND e.embedding_version = %s
                  AND (e.content_hash = '' OR e.card_uid = '')
                """,
                (embedding_model, embedding_version),
            )
            write_conn.commit()
        copied = int(inserted.rowcount or 0)
        identified = int(identity.rowcount or 0)
        logger.info(
            "embeddings_remap_by_prior_schema copied=%s identified=%s map_rows=%s versions=%s elapsed=%.1fs",
            copied,
            identified,
            loaded,
            versions,
            time.monotonic() - started,
        )
        return {"copied": copied, "identified": identified, "map_rows": loaded, "schema_versions": list(versions)}

    def attach_embeddings_after_rematerialize(
        self,
        *,
        embedding_model: str | None = None,
        embedding_version: int | None = None,
        fail_if_thin: bool = True,
    ) -> dict[str, int]:
        """Backfill identity, remap leftover versioned keys, then reuse by content."""
        model = (embedding_model or "").strip() or get_default_embedding_model()
        version = int(embedding_version or 0) or get_default_embedding_version()
        self.backfill_embedding_content_identity()
        remapped = self.remap_embeddings_by_prior_schema(
            embedding_model=model,
            embedding_version=version,
        )
        reused = self.reuse_embeddings_by_content(
            embedding_model=model,
            embedding_version=version,
        )
        pending_after = int(reused.get("pending_after") or 0)
        result = {
            "remapped": int(remapped.get("copied") or 0),
            "reused": int(reused.get("copied") or 0),
            "pending_after": pending_after,
        }
        logger.info(
            "embeddings_attach_after_rematerialize remapped=%s reused=%s pending_after=%s",
            result["remapped"],
            result["reused"],
            pending_after,
        )
        if fail_if_thin:
            self._fail_if_embed_coverage_thin(
                embedding_model=model,
                embedding_version=version,
                pending_after=pending_after,
            )
        return result

    def _fail_if_embed_coverage_thin(
        self,
        *,
        embedding_model: str,
        embedding_version: int,
        pending_after: int,
    ) -> None:
        min_cov = get_publication_min_embed_coverage()
        min_chunks = get_publication_min_embed_chunks()
        if min_cov <= 0:
            return
        with self._connect() as conn:
            row = conn.execute(f"SELECT COUNT(*) AS n FROM {self.schema}.chunks").fetchone()
        chunk_count = int(row["n"] if isinstance(row, dict) else row[0])
        if chunk_count < min_chunks or chunk_count <= 0:
            return
        coverage = (chunk_count - pending_after) / chunk_count
        if coverage < min_cov:
            raise IncompatibleStateError(
                f"embedding_coverage:{chunk_count - pending_after}/{chunk_count}"
            )

    def embed_pending(
        self,
        *,
        provider: Any,
        embedding_model: str,
        embedding_version: int,
        limit: int = 20,
        include_context_prefix: bool = False,
        uid_allowlist: Collection[str] | None = None,
        chunk_key_allowlist: Collection[str] | None = None,
        embedding_spec: EmbeddingSpec | None = None,
        unscoped: bool = False,
    ) -> dict[str, Any]:
        uid_allowlist = normalize_embed_allowlist(uid_allowlist)
        chunk_key_allowlist = normalize_embed_allowlist(chunk_key_allowlist)
        require_embed_selection(
            uid_allowlist=uid_allowlist,
            chunk_key_allowlist=chunk_key_allowlist,
            unscoped=unscoped,
        )
        scoped = uid_allowlist is not None or chunk_key_allowlist is not None
        with self._embed_pending_lock:
            total_steps = 6
            provider_name = str(getattr(provider, "name", "unknown"))

            _log_rebuild_step(
                1,
                total_steps,
                "validate embedding provider",
                f"provider={provider_name} model={embedding_model} version={embedding_version}",
            )
            self.ensure_ready()
            if embedding_spec is not None and not embedding_spec_matches_index(
                embedding_spec,
                model=embedding_model,
                version=embedding_version,
                dimension=self.vector_dimension,
            ):
                raise ValueError(
                    "EmbeddingSpec is incompatible with the requested model/version/dimension/"
                    f"chunk_schema: spec={embedding_spec.to_payload()} "
                    f"model={embedding_model} version={embedding_version} "
                    f"dimension={self.vector_dimension} chunk_schema={current_chunk_schema_id()}"
                )
            provider_model = str(getattr(provider, "model", "") or "").strip()
            if provider_model and provider_model != embedding_model:
                raise RuntimeError(
                    f"Embedding provider model mismatch: provider={provider_model} requested={embedding_model}"
                )
            provider_dimension = int(getattr(provider, "dimension", self.vector_dimension))
            if provider_dimension != self.vector_dimension:
                raise RuntimeError(
                    f"Embedding provider dimension mismatch: provider={provider_dimension} index={self.vector_dimension}"
                )
            batch_size = get_embed_batch_size()
            max_retries = get_embed_max_retries()
            write_batch_size = min(get_embed_write_batch_size(), batch_size)
            concurrency = get_embed_concurrency()
            progress_every = get_embed_progress_every()
            _log_rebuild_step(
                1,
                total_steps,
                "validate embedding provider complete",
                f"dimension={provider_dimension} batch_size={batch_size} concurrency={concurrency} context_prefix={include_context_prefix}",
            )
            if not scoped:
                attach = self.attach_embeddings_after_rematerialize(
                    embedding_model=embedding_model,
                    embedding_version=embedding_version,
                    fail_if_thin=False,
                )
                logger.info(
                    "embed_pending attach remapped=%s reused=%s pending_after=%s",
                    attach.get("remapped"),
                    attach.get("reused"),
                    attach.get("pending_after"),
                )

            _log_rebuild_step(2, total_steps, "count embedding backlog")
            selected_keys: list[str] = []
            reused_keys: list[str] = []
            pending_keys: list[str] = []
            chunk_keys_by_uid: dict[str, list[str]] = {}
            if scoped:
                with self._connect() as conn:
                    selected_rows = self._list_selected_embed_chunks(
                        conn,
                        embedding_model=embedding_model,
                        embedding_version=embedding_version,
                        uid_allowlist=uid_allowlist,
                        chunk_key_allowlist=chunk_key_allowlist,
                    )
                for row in selected_rows:
                    key = str(row["chunk_key"])
                    uid = str(row["card_uid"])
                    selected_keys.append(key)
                    chunk_keys_by_uid.setdefault(uid, []).append(key)
                    if row["has_compatible"]:
                        reused_keys.append(key)
                    else:
                        pending_keys.append(key)
                pending_chunks = len(pending_keys)
                total_chunks = len(selected_keys)
                already_embedded = len(reused_keys)
            else:
                backlog_status = self.embedding_status(
                    embedding_model=embedding_model, embedding_version=embedding_version
                )
                pending_chunks = int(backlog_status["pending_chunk_count"])
                total_chunks = int(backlog_status["chunk_count"])
                already_embedded = int(backlog_status["embedded_chunk_count"])
            _log_rebuild_step(
                2,
                total_steps,
                "count embedding backlog complete",
                f"total_chunks={total_chunks} already_embedded={already_embedded} pending={pending_chunks} scoped={scoped}",
            )

            def _empty_result(*, last_error: str = "") -> dict[str, Any]:
                payload: dict[str, Any] = {
                    "provider": provider_name,
                    "embedding_model": embedding_model,
                    "embedding_version": embedding_version,
                    "batch_size": batch_size,
                    "write_batch_size": write_batch_size,
                    "concurrency": concurrency,
                    "failed": 0,
                    "chunk_schema_version": CHUNK_SCHEMA_VERSION,
                    "embedded": 0,
                    "reused": len(reused_keys),
                    "selected": len(selected_keys) if scoped else total_chunks,
                    "unscoped": not scoped,
                    "selected_chunk_keys": list(selected_keys),
                    "reused_chunk_keys": list(reused_keys),
                    "submitted_chunk_keys": [],
                    "completed_chunk_keys": list(reused_keys),
                    "failed_chunk_keys": [],
                    "pending_chunk_keys": list(pending_keys) if scoped else [],
                    "chunk_keys_by_uid": {uid: list(keys) for uid, keys in chunk_keys_by_uid.items()},
                    "card_uids": [],
                }
                if embedding_spec is not None:
                    payload["embedding_spec"] = embedding_spec.to_payload()
                if last_error:
                    payload["last_error"] = last_error
                return payload

            if pending_chunks <= 0:
                _log_rebuild_step(6, total_steps, "nothing to embed")
                return _empty_result()

            remaining_limit = pending_chunks if limit <= 0 else min(limit, pending_chunks)
            target_total = remaining_limit
            embedded = 0
            failed = 0
            last_error = ""
            submitted_keys: list[str] = []
            completed_new_keys: list[str] = []
            failed_keys: list[str] = []
            embedded_uids: set[str] = set()
            reserve_lock = Lock()
            progress_lock = Lock()
            should_rebuild_vector_index = embed_defer_vector_index()

            def reserve_claim_size() -> int:
                nonlocal remaining_limit
                with reserve_lock:
                    if remaining_limit <= 0:
                        return 0
                    claim = min(batch_size, remaining_limit)
                    remaining_limit -= claim
                    return claim

            def refund_claim_size(amount: int) -> None:
                nonlocal remaining_limit
                if amount <= 0:
                    return
                with reserve_lock:
                    remaining_limit += amount

            if should_rebuild_vector_index:
                _log_rebuild_step(3, total_steps, "drop vector index for bulk load")
                with self._connect() as conn:
                    self._drop_embeddings_vector_index(conn)
                    conn.commit()
                _log_rebuild_step(3, total_steps, "drop vector index complete")
            else:
                _log_rebuild_step(3, total_steps, "skip vector index drop (incremental mode)")

            _log_rebuild_step(4, total_steps, "materialize embed work queue and context")
            t0 = time.time()
            with self._connect() as conn:
                queue_count = self._materialize_embed_queue(
                    conn,
                    embedding_model=embedding_model,
                    embedding_version=embedding_version,
                    uid_allowlist=uid_allowlist,
                    chunk_key_allowlist=chunk_key_allowlist,
                )
                logger.info(
                    "step 4/%d materialize embed work queue complete pending_chunks=%d elapsed=%.1fs",
                    total_steps,
                    queue_count,
                    time.time() - t0,
                )
                if include_context_prefix:
                    t1 = time.time()
                    ctx_count = self._materialize_embed_context(conn)
                    logger.info(
                        "step 4/%d materialize embed context lookup complete cards=%d elapsed=%.1fs",
                        total_steps,
                        ctx_count,
                        time.time() - t1,
                    )
            _log_rebuild_step(
                4,
                total_steps,
                "materialize complete",
                f"queue={queue_count} context={'yes' if include_context_prefix else 'no'} total_elapsed={time.time() - t0:.1f}s",
            )

            progress = _RebuildProgressReporter(
                step_number=5,
                total_steps=total_steps,
                stage="embed",
                total_items=target_total,
                progress_every=progress_every,
                started_at=time.time(),
                min_interval_seconds=5.0,
            )
            _log_rebuild_step(
                5,
                total_steps,
                "embed chunks",
                f"target={target_total} workers={concurrency} batch_size={batch_size}",
            )

            t_start = time.monotonic()
            prev_logged_bucket = 0

            def run_worker() -> EmbeddingBatchResult:
                worker_result = EmbeddingBatchResult()
                nonlocal embedded, failed, last_error, prev_logged_bucket
                while True:
                    claim_size = reserve_claim_size()
                    if claim_size <= 0:
                        return worker_result
                    batch_result = self._process_embedding_claim(
                        provider=provider,
                        embedding_model=embedding_model,
                        embedding_version=embedding_version,
                        claim_size=claim_size,
                        max_retries=max_retries,
                        write_batch_size=write_batch_size,
                        include_context_prefix=include_context_prefix,
                    )
                    if batch_result.claimed < claim_size:
                        refund_claim_size(claim_size - batch_result.claimed)
                    if batch_result.claimed == 0:
                        return worker_result
                    worker_result.claimed += batch_result.claimed
                    worker_result.embedded += batch_result.embedded
                    worker_result.failed += batch_result.failed
                    worker_result.claimed_keys.extend(batch_result.claimed_keys)
                    worker_result.embedded_keys.extend(batch_result.embedded_keys)
                    worker_result.failed_keys.extend(batch_result.failed_keys)
                    worker_result.card_uids.extend(batch_result.card_uids)
                    if batch_result.last_error:
                        worker_result.last_error = batch_result.last_error
                    with progress_lock:
                        embedded += batch_result.embedded
                        failed += batch_result.failed
                        submitted_keys.extend(batch_result.claimed_keys)
                        completed_new_keys.extend(batch_result.embedded_keys)
                        failed_keys.extend(batch_result.failed_keys)
                        embedded_uids.update(batch_result.card_uids)
                        if batch_result.last_error:
                            last_error = batch_result.last_error
                        fail_suffix = f" failed={failed}" if failed else ""
                        err_suffix = f" last_error={last_error}" if last_error else ""
                        progress.update(embedded, extra=f"embedded={embedded}{fail_suffix}{err_suffix}")
                        if (
                            progress_every > 0
                            and embedded > 0
                            and (embedded // progress_every) > (prev_logged_bucket // progress_every)
                        ):
                            progress_data = _calculate_embed_progress(
                                embedded_so_far=embedded,
                                failed_so_far=failed,
                                total_pending=pending_chunks,
                                elapsed_seconds=time.monotonic() - t_start,
                            )
                            logger.info(
                                "embed_progress embedded=%d failed=%d rate=%.1f/s elapsed=%s eta=%s remaining=%d",
                                progress_data["embedded"],
                                progress_data["failed"],
                                progress_data["rate_per_second"],
                                _format_mss(float(progress_data["elapsed_seconds"])),
                                _format_mss(float(progress_data["eta_seconds"])),
                                progress_data["remaining"],
                            )
                            prev_logged_bucket = embedded

            try:
                with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
                    futures = {executor.submit(run_worker) for _ in range(max(1, concurrency))}
                    while futures:
                        done, futures = wait(futures, return_when=FIRST_COMPLETED)
                        for future in done:
                            batch_result = future.result()
                            if batch_result.last_error:
                                last_error = batch_result.last_error
            finally:
                fail_suffix = f" failed={failed}" if failed else ""
                progress.complete(embedded, extra=f"embedded={embedded}{fail_suffix}")
                with self._connect() as conn:
                    self._drop_embed_work_tables(conn)
                if should_rebuild_vector_index:
                    _log_rebuild_step(6, total_steps, "rebuild vector index")
                    with self._connect() as conn:
                        self._ensure_embeddings_vector_index(conn)
                        conn.commit()
                    _log_rebuild_step(6, total_steps, "rebuild vector index complete")
                else:
                    _log_rebuild_step(6, total_steps, "skip vector index rebuild (incremental mode)")
            completed_keys = list(dict.fromkeys([*reused_keys, *completed_new_keys]))
            failed_unique = list(dict.fromkeys(failed_keys))
            submitted_unique = list(dict.fromkeys(submitted_keys))
            if scoped:
                done_or_failed = set(completed_keys) | set(failed_unique)
                still_pending = [key for key in pending_keys if key not in done_or_failed]
            else:
                still_pending = []
            result: dict[str, Any] = {
                "provider": provider_name,
                "embedding_model": embedding_model,
                "embedding_version": embedding_version,
                "chunk_schema_version": CHUNK_SCHEMA_VERSION,
                "batch_size": batch_size,
                "write_batch_size": write_batch_size,
                "concurrency": concurrency,
                "embedded": embedded,
                "failed": failed,
                "reused": len(reused_keys),
                "selected": len(selected_keys) if scoped else total_chunks,
                "unscoped": not scoped,
                "selected_chunk_keys": list(selected_keys),
                "reused_chunk_keys": list(reused_keys),
                "submitted_chunk_keys": submitted_unique,
                "completed_chunk_keys": completed_keys,
                "failed_chunk_keys": failed_unique,
                "pending_chunk_keys": still_pending,
                "chunk_keys_by_uid": {uid: list(keys) for uid, keys in chunk_keys_by_uid.items()},
                "card_uids": sorted(embedded_uids),
            }
            if embedding_spec is not None:
                result["embedding_spec"] = embedding_spec.to_payload()
            if last_error:
                result["last_error"] = last_error
            return result
