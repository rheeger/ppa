"""Embedding pipeline mixin for PostgresArchiveIndex."""

from __future__ import annotations

import logging
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from threading import Lock
from typing import Any

from .features import build_context_prefix_for_embed_row
from .index_config import (CHUNK_SCHEMA_VERSION, EmbeddingBatchResult,
                           _vector_literal, embed_defer_vector_index,
                           get_embed_batch_size, get_embed_concurrency,
                           get_embed_max_retries, get_embed_progress_every,
                           get_embed_write_batch_size)
from .loader import _chunked, _log_rebuild_step, _RebuildProgressReporter

logger = logging.getLogger("ppa.embedder")


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
        total_chunks = int(total_row["count"])
        embedded_chunks = int(embedded_row["count"])
        return {
            "embedding_model": embedding_model,
            "embedding_version": embedding_version,
            "chunk_schema_version": CHUNK_SCHEMA_VERSION,
            "chunk_count": total_chunks,
            "embedded_chunk_count": embedded_chunks,
            "pending_chunk_count": max(total_chunks - embedded_chunks, 0),
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
            """
        )
        row = conn.execute(f"SELECT count(*) AS cnt FROM {self.schema}.card_embed_context").fetchone()
        count = int(row["cnt"]) if row else 0
        conn.commit()
        return count

    def _materialize_embed_queue(self, conn, *, embedding_model: str, embedding_version: int) -> int:
        """Build a work queue of chunk_keys that need embedding. Workers pop from this
        queue instead of scanning the full chunks table with LEFT JOIN on every batch.
        """
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
            WHERE e.chunk_key IS NULL
            ORDER BY c.rel_path, c.chunk_type, c.chunk_index
            """,
            (embedding_model, embedding_version),
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
                SELECT c.chunk_key, c.rel_path, c.chunk_type, c.chunk_index, c.content, c.token_count,
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
                SELECT c.chunk_key, c.rel_path, c.chunk_type, c.chunk_index, c.content, c.token_count
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
            result.claimed = len(batch)
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
                result.last_error = last_error
                return result
            if len(vectors) != len(batch):
                conn.rollback()
                result.failed = len(batch)
                result.last_error = "Embedding provider returned mismatched vector count"
                return result

            payload: list[tuple[str, str]] = []
            for row, vector in zip(batch, vectors, strict=True):
                if len(vector) != self.vector_dimension:
                    conn.rollback()
                    result.failed = len(batch)
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
            return result

    def embed_pending(
        self,
        *,
        provider: Any,
        embedding_model: str,
        embedding_version: int,
        limit: int = 20,
        include_context_prefix: bool = False,
    ) -> dict[str, int | str]:
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

            _log_rebuild_step(2, total_steps, "count embedding backlog")
            backlog_status = self.embedding_status(embedding_model=embedding_model, embedding_version=embedding_version)
            pending_chunks = int(backlog_status["pending_chunk_count"])
            total_chunks = int(backlog_status["chunk_count"])
            already_embedded = int(backlog_status["embedded_chunk_count"])
            _log_rebuild_step(
                2,
                total_steps,
                "count embedding backlog complete",
                f"total_chunks={total_chunks} already_embedded={already_embedded} pending={pending_chunks}",
            )
            if pending_chunks <= 0:
                _log_rebuild_step(6, total_steps, "nothing to embed")
                return {
                    "provider": provider_name,
                    "embedding_model": embedding_model,
                    "embedding_version": embedding_version,
                    "batch_size": batch_size,
                    "write_batch_size": write_batch_size,
                    "concurrency": concurrency,
                    "failed": 0,
                    "chunk_schema_version": CHUNK_SCHEMA_VERSION,
                    "embedded": 0,
                }

            remaining_limit = pending_chunks if limit <= 0 else min(limit, pending_chunks)
            target_total = remaining_limit
            embedded = 0
            failed = 0
            last_error = ""
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
                    if batch_result.last_error:
                        worker_result.last_error = batch_result.last_error
                    with progress_lock:
                        embedded += batch_result.embedded
                        failed += batch_result.failed
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
            result: dict[str, int | str] = {
                "provider": provider_name,
                "embedding_model": embedding_model,
                "embedding_version": embedding_version,
                "chunk_schema_version": CHUNK_SCHEMA_VERSION,
                "batch_size": batch_size,
                "write_batch_size": write_batch_size,
                "concurrency": concurrency,
                "embedded": embedded,
                "failed": failed,
            }
            if last_error:
                result["last_error"] = last_error
            return result
