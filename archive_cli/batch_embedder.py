"""OpenAI Batch API embedding path.

Submits, polls, and ingests async embedding jobs via OpenAI's Batch API
(``/v1/batches`` + ``/v1/embeddings``). Runs as a complement to the sync
``embed_pending`` path: both write to the same ``{schema}.embeddings`` table,
so partial progress made by either path is preserved.

State tables (created by ``SchemaDDLMixin._ensure_batch_embed_tables``):
  - ``{schema}.embed_batches`` — one row per OpenAI batch submitted
  - ``{schema}.embed_batch_requests`` — ``(openai_batch_id, custom_id) -> chunk_key``

Public orchestrators:
  - ``submit_batches`` — materialize ``card_embed_context``, claim pending
    chunks, render JSONL, upload, create batch, persist request map.
  - ``poll_batches`` — refresh status + counts for every in-flight batch.
  - ``ingest_completed_batches`` — download completed output files, upsert
    embeddings, mark batches as ingested.
  - ``batch_status`` — one-shot summary for CLI / MCP.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request

from .features import build_context_prefix_for_embed_row
from .index_config import (
    _ppa_env,
    _vector_literal,
    get_default_embedding_model,
    get_default_embedding_version,
    get_vector_dimension,
)

logger = logging.getLogger("ppa.batch_embedder")

DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_OPENAI_TIMEOUT_SECONDS = 120
DEFAULT_BATCH_REQUESTS = 50_000  # OpenAI Batch API cap for /v1/embeddings
DEFAULT_BATCH_ARTIFACT_DIR = "_artifacts/_embedding-runs/batches"
# OpenAI enforces a per-model enqueued-tokens cap (e.g. 100M for
# text-embedding-3-small). At ~250 tokens/chunk × 50k chunks/batch = ~12.5M
# tokens per batch, so roughly 8 batches fit. We keep a small safety margin.
DEFAULT_MAX_OUTSTANDING_BATCHES = 6

IN_FLIGHT_STATUSES = ("validating", "in_progress", "finalizing")
TERMINAL_STATUSES = ("completed", "failed", "expired", "cancelled")


# ---------------------------------------------------------------------------
# OpenAI HTTP client (urllib — no new deps)
# ---------------------------------------------------------------------------


def _resolve_api_key() -> str:
    key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not key or key.startswith("op://"):
        # Fall back to the sync path's resolver (supports Arnold/1Password).
        from .embedding_provider import _resolve_openai_api_key

        return _resolve_openai_api_key()
    return key


def _base_url() -> str:
    return (
        _ppa_env("PPA_OPENAI_BASE_URL")
        or os.environ.get("OPENAI_BASE_URL")
        or DEFAULT_OPENAI_BASE_URL
    ).rstrip("/")


def _timeout() -> int:
    raw = _ppa_env("PPA_OPENAI_TIMEOUT_SECONDS", default=str(DEFAULT_OPENAI_TIMEOUT_SECONDS))
    try:
        return max(int(float(raw)), 10)
    except ValueError:
        return DEFAULT_OPENAI_TIMEOUT_SECONDS


def _http_request(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    body: bytes | None = None,
    timeout: int | None = None,
) -> bytes:
    req = request.Request(url, data=body, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout or _timeout()) as resp:
            return resp.read()
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        raise RuntimeError(f"OpenAI {method} {url} failed: {exc.code} {detail}") from exc


def _http_download_to_file(
    url: str,
    dest_path: Path,
    *,
    headers: dict[str, str],
    timeout: int | None = None,
    max_retries: int = 5,
    chunk_bytes: int = 1 << 20,  # 1 MiB
) -> int:
    """Stream a GET response directly to ``dest_path`` with bounded retries.

    urllib's ``resp.read()`` buffers the whole body into memory and tends to
    raise ``IncompleteRead`` on multi-gigabyte downloads when the connection
    hiccups; ``shutil.copyfileobj`` can also return happily after a premature
    server-side FIN without the advertised ``Content-Length`` bytes. This
    helper streams in ``chunk_bytes`` pieces, validates the final on-disk
    size against ``Content-Length`` (when present), tears down the partial
    file on failure, and retries from scratch up to ``max_retries`` times.
    OpenAI's file-content endpoint doesn't support byte-range resumes, so
    restart-from-zero is the only correct recovery.
    """
    import http.client

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    t_eff = timeout or max(_timeout(), 600)
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        req = request.Request(url, headers=headers, method="GET")
        try:
            expected: int | None = None
            written = 0
            with request.urlopen(req, timeout=t_eff) as resp, dest_path.open("wb") as fh:
                raw_len = resp.headers.get("Content-Length")
                try:
                    expected = int(raw_len) if raw_len is not None else None
                except ValueError:
                    expected = None
                while True:
                    block = resp.read(chunk_bytes)
                    if not block:
                        break
                    fh.write(block)
                    written += len(block)
            if expected is not None and written != expected:
                raise ConnectionError(
                    f"short download: wrote {written} bytes, server advertised {expected}"
                )
            return written
        except (error.URLError, http.client.IncompleteRead, TimeoutError, ConnectionError) as exc:
            last_exc = exc
            try:
                dest_path.unlink()
            except FileNotFoundError:
                pass
            if attempt >= max_retries:
                break
            # Exponential backoff: 2s, 4s, 8s, 16s, 30s, 30s...
            time.sleep(min(2 * (2**attempt), 30))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
            raise RuntimeError(f"OpenAI GET {url} failed: {exc.code} {detail}") from exc
    raise RuntimeError(f"OpenAI GET {url} failed after {max_retries + 1} attempts: {last_exc}")


def _json_request(method: str, url: str, *, api_key: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    raw = _http_request(method, url, headers=headers, body=body)
    return json.loads(raw.decode("utf-8"))


def upload_input_file(local_path: Path, *, api_key: str, base_url: str) -> str:
    """Upload a JSONL file to OpenAI Files API with purpose=batch. Returns file id."""
    boundary = f"----ppa-batch-{uuid.uuid4().hex}"
    data = local_path.read_bytes()
    body = (
        f'--{boundary}\r\nContent-Disposition: form-data; name="purpose"\r\n\r\nbatch\r\n'
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{local_path.name}"\r\n'
        f"Content-Type: application/jsonl\r\n\r\n"
    ).encode("utf-8") + data + f"\r\n--{boundary}--\r\n".encode("utf-8")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": f"multipart/form-data; boundary={boundary}",
    }
    raw = _http_request("POST", f"{base_url}/files", headers=headers, body=body, timeout=600)
    payload = json.loads(raw.decode("utf-8"))
    file_id = str(payload.get("id", "")).strip()
    if not file_id:
        raise RuntimeError(f"OpenAI file upload returned no id: {payload}")
    return file_id


def create_batch(
    *,
    input_file_id: str,
    endpoint: str,
    api_key: str,
    base_url: str,
    metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "input_file_id": input_file_id,
        "endpoint": endpoint,
        "completion_window": "24h",
    }
    if metadata:
        payload["metadata"] = metadata
    return _json_request("POST", f"{base_url}/batches", api_key=api_key, payload=payload)


def retrieve_batch(*, openai_batch_id: str, api_key: str, base_url: str) -> dict[str, Any]:
    return _json_request("GET", f"{base_url}/batches/{openai_batch_id}", api_key=api_key)


def cancel_batch(*, openai_batch_id: str, api_key: str, base_url: str) -> dict[str, Any]:
    return _json_request("POST", f"{base_url}/batches/{openai_batch_id}/cancel", api_key=api_key)


def download_file(*, file_id: str, dest_path: Path, api_key: str, base_url: str) -> int:
    headers = {"Authorization": f"Bearer {api_key}"}
    return _http_download_to_file(
        f"{base_url}/files/{file_id}/content",
        dest_path,
        headers=headers,
        timeout=600,
        max_retries=3,
    )


# ---------------------------------------------------------------------------
# Internal: chunk selection + JSONL rendering
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PendingChunk:
    chunk_key: str
    content: str
    prefix: str  # already-rendered context prefix, "" if include_context_prefix=False


def _ensure_card_embed_context(conn, schema: str) -> None:
    """Materialize ``{schema}.card_embed_context`` if it doesn't exist yet.

    Mirrors ``EmbedderMixin._materialize_embed_context`` but is non-destructive:
    it only runs when the table is missing (after a sync ``embed_pending`` run
    finishes, that mixin drops the table — so batch submits need to rebuild it).
    """
    exists = conn.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = 'card_embed_context'
        """,
        (schema,),
    ).fetchone()
    if exists:
        return
    conn.execute(
        f"""
        CREATE TABLE {schema}.card_embed_context (
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
        INSERT INTO {schema}.card_embed_context
            (card_uid, card_type, summary, activity_at, sources_agg, people_agg, orgs_agg)
        SELECT
            card.uid,
            card.type,
            card.summary,
            card.activity_at,
            COALESCE(src.sources_agg, ''),
            COALESCE(ppl.people_agg, ''),
            COALESCE(org.orgs_agg, '')
        FROM {schema}.cards card
        LEFT JOIN LATERAL (
            SELECT string_agg(cs.source, '|' ORDER BY cs.source) AS sources_agg
            FROM {schema}.card_sources cs WHERE cs.card_uid = card.uid
        ) src ON true
        LEFT JOIN LATERAL (
            SELECT string_agg(cp.person, '|' ORDER BY cp.person) AS people_agg
            FROM {schema}.card_people cp WHERE cp.card_uid = card.uid
        ) ppl ON true
        LEFT JOIN LATERAL (
            SELECT string_agg(co.org, '|' ORDER BY co.org) AS orgs_agg
            FROM {schema}.card_orgs co WHERE co.card_uid = card.uid
        ) org ON true
        """
    )
    conn.commit()


def _claim_pending_chunks_for_batch(
    conn,
    *,
    schema: str,
    embedding_model: str,
    embedding_version: int,
    limit: int,
    include_context_prefix: bool,
) -> list[PendingChunk]:
    """Return ``limit`` chunks that are not yet embedded AND not in any in-flight batch."""
    if include_context_prefix:
        rows = conn.execute(
            f"""
            SELECT c.chunk_key, c.content,
                   ctx.card_type AS ctype,
                   ctx.summary,
                   ctx.activity_at,
                   ctx.sources_agg,
                   ctx.people_agg,
                   ctx.orgs_agg
            FROM {schema}.chunks c
            JOIN {schema}.card_embed_context ctx ON ctx.card_uid = c.card_uid
            LEFT JOIN {schema}.embeddings e
              ON e.chunk_key = c.chunk_key
              AND e.embedding_model = %s
              AND e.embedding_version = %s
            LEFT JOIN {schema}.embed_batch_requests br
              ON br.chunk_key = c.chunk_key
            LEFT JOIN {schema}.embed_batches b
              ON b.openai_batch_id = br.openai_batch_id
              AND b.embedding_model = %s
              AND b.embedding_version = %s
              AND b.ingested_at IS NULL
              AND b.status NOT IN ('failed', 'expired', 'cancelled')
            WHERE e.chunk_key IS NULL AND b.openai_batch_id IS NULL
            ORDER BY c.rel_path, c.chunk_type, c.chunk_index
            LIMIT %s
            """,
            (embedding_model, embedding_version, embedding_model, embedding_version, limit),
        ).fetchall()
        out: list[PendingChunk] = []
        for row in rows:
            row_dict = dict(row)
            prefix = build_context_prefix_for_embed_row(row_dict)
            out.append(
                PendingChunk(
                    chunk_key=str(row_dict["chunk_key"]),
                    content=str(row_dict.get("content", "")),
                    prefix=prefix,
                )
            )
        return out

    rows = conn.execute(
        f"""
        SELECT c.chunk_key, c.content
        FROM {schema}.chunks c
        LEFT JOIN {schema}.embeddings e
          ON e.chunk_key = c.chunk_key
          AND e.embedding_model = %s
          AND e.embedding_version = %s
        LEFT JOIN {schema}.embed_batch_requests br
          ON br.chunk_key = c.chunk_key
        LEFT JOIN {schema}.embed_batches b
          ON b.openai_batch_id = br.openai_batch_id
          AND b.embedding_model = %s
          AND b.embedding_version = %s
          AND b.ingested_at IS NULL
          AND b.status NOT IN ('failed', 'expired', 'cancelled')
        WHERE e.chunk_key IS NULL AND b.openai_batch_id IS NULL
        ORDER BY c.rel_path, c.chunk_type, c.chunk_index
        LIMIT %s
        """,
        (embedding_model, embedding_version, embedding_model, embedding_version, limit),
    ).fetchall()
    return [
        PendingChunk(
            chunk_key=str(r["chunk_key"] if isinstance(r, dict) else r[0]),
            content=str((r["content"] if isinstance(r, dict) else r[1]) or ""),
            prefix="",
        )
        for r in rows
    ]


def _render_batch_jsonl(
    chunks: list[PendingChunk],
    *,
    model: str,
    dimension: int,
    dest_path: Path,
) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with dest_path.open("w", encoding="utf-8") as fh:
        for idx, ch in enumerate(chunks):
            record = {
                "custom_id": f"r-{idx}",
                "method": "POST",
                "url": "/v1/embeddings",
                "body": {
                    "model": model,
                    "input": f"{ch.prefix}{ch.content}" if ch.prefix else ch.content,
                    "dimensions": dimension,
                },
            }
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _insert_batch_rows(
    conn,
    schema: str,
    *,
    openai_batch_id: str,
    embedding_model: str,
    embedding_version: int,
    input_file_id: str,
    status: str,
    request_count: int,
    input_jsonl_path: str,
    include_context_prefix: bool,
    chunks: list[PendingChunk],
) -> None:
    conn.execute(
        f"""
        INSERT INTO {schema}.embed_batches
          (openai_batch_id, embedding_model, embedding_version, input_file_id,
           status, request_count, input_jsonl_path, include_context_prefix)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            openai_batch_id,
            embedding_model,
            embedding_version,
            input_file_id,
            status,
            request_count,
            input_jsonl_path,
            include_context_prefix,
        ),
    )
    rows = [(openai_batch_id, f"r-{idx}", ch.chunk_key) for idx, ch in enumerate(chunks)]
    if not rows:
        return
    # Postgres wire-protocol caps parameters at 65535 per statement.
    # At 3 params/row, keep chunks at 20,000 rows (<60K params) to be safe.
    chunk_size = 20_000
    for start in range(0, len(rows), chunk_size):
        slice_rows = rows[start : start + chunk_size]
        values_sql = ",".join(["(%s, %s, %s)"] * len(slice_rows))
        params: list[Any] = []
        for batch_id, cid, ckey in slice_rows:
            params.extend([batch_id, cid, ckey])
        conn.execute(
            f"""
            INSERT INTO {schema}.embed_batch_requests (openai_batch_id, custom_id, chunk_key)
            VALUES {values_sql}
            """,
            params,
        )


# ---------------------------------------------------------------------------
# Public orchestrators (invoked from commands/batch_embed.py)
# ---------------------------------------------------------------------------


def submit_batches(
    *,
    index,
    logger_: logging.Logger,
    embedding_model: str = "",
    embedding_version: int = 0,
    max_batches: int = 0,
    requests_per_batch: int = DEFAULT_BATCH_REQUESTS,
    include_context_prefix: bool = True,
    artifact_dir: str | None = None,
) -> dict[str, Any]:
    """Submit up to ``max_batches`` OpenAI batches. Returns a summary dict.

    Each submitted batch covers up to ``requests_per_batch`` pending chunks.
    Pending chunks are those with no row in ``{schema}.embeddings`` for
    (model, version) AND not assigned to any still-active batch.
    """
    model = embedding_model.strip() or get_default_embedding_model()
    version = embedding_version or get_default_embedding_version()
    dimension = get_vector_dimension()
    requests_per_batch = max(1, min(requests_per_batch, DEFAULT_BATCH_REQUESTS))
    max_batches = max_batches if max_batches > 0 else 10_000
    art_dir = Path(artifact_dir or DEFAULT_BATCH_ARTIFACT_DIR)
    art_dir.mkdir(parents=True, exist_ok=True)
    max_outstanding_raw = _ppa_env("PPA_BATCH_MAX_OUTSTANDING", default=str(DEFAULT_MAX_OUTSTANDING_BATCHES))
    try:
        max_outstanding = max(1, int(max_outstanding_raw))
    except ValueError:
        max_outstanding = DEFAULT_MAX_OUTSTANDING_BATCHES

    api_key = _resolve_api_key()
    base_url = _base_url()

    index.ensure_ready()
    submitted: list[dict[str, Any]] = []
    total_requests = 0
    enqueue_full = False

    with index._connect() as conn:  # noqa: SLF001 — shared mixin hook
        index._ensure_batch_embed_tables(conn, ensure_indexes=True)  # noqa: SLF001
        if include_context_prefix:
            _ensure_card_embed_context(conn, index.schema)
        conn.commit()

    for batch_idx in range(max_batches):
        # Throttle: stop submitting when OpenAI's enqueued-tokens pool is near
        # full. Only ``validating`` and ``in_progress`` batches count against
        # the enqueue limit — once OpenAI moves a batch to ``finalizing`` or
        # ``completed`` it has already consumed its token budget and is no
        # longer blocking new submissions.
        with index._connect() as conn:  # noqa: SLF001
            outstanding_row = conn.execute(
                f"""
                SELECT COUNT(*) AS n FROM {index.schema}.embed_batches
                WHERE ingested_at IS NULL
                  AND status IN ('validating', 'in_progress')
                  AND embedding_model = %s
                  AND embedding_version = %s
                """,
                (model, version),
            ).fetchone()
        if outstanding_row is None:
            outstanding = 0
        elif isinstance(outstanding_row, dict):
            outstanding = int(outstanding_row.get("n", 0) or 0)
        else:
            outstanding = int(outstanding_row[0])
        if outstanding >= max_outstanding:
            logger_.info(
                "submit_batches_throttle_outstanding outstanding=%d max_outstanding=%d — stopping",
                outstanding,
                max_outstanding,
            )
            enqueue_full = True
            break

        with index._connect() as conn:  # noqa: SLF001
            pending = _claim_pending_chunks_for_batch(
                conn,
                schema=index.schema,
                embedding_model=model,
                embedding_version=version,
                limit=requests_per_batch,
                include_context_prefix=include_context_prefix,
            )
        if not pending:
            logger_.info(
                "submit_batches_no_more_pending submitted_batches=%d total_requests=%d",
                len(submitted),
                total_requests,
            )
            break

        input_name = f"batch-{int(time.time())}-{uuid.uuid4().hex[:8]}-in.jsonl"
        input_path = art_dir / input_name
        _render_batch_jsonl(pending, model=model, dimension=dimension, dest_path=input_path)
        size_mb = input_path.stat().st_size / (1024 * 1024)
        logger_.info(
            "submit_batches_rendered batch_slot=%d requests=%d path=%s size_mb=%.2f",
            batch_idx,
            len(pending),
            input_path,
            size_mb,
        )

        file_id = upload_input_file(input_path, api_key=api_key, base_url=base_url)
        logger_.info("submit_batches_uploaded file_id=%s", file_id)

        try:
            batch = create_batch(
                input_file_id=file_id,
                endpoint="/v1/embeddings",
                api_key=api_key,
                base_url=base_url,
                metadata={
                    "source": "ppa-embed-batch",
                    "embedding_model": model,
                    "embedding_version": str(version),
                },
            )
        except Exception as exc:
            logger_.error("submit_batches_create_error file_id=%s error=%s", file_id, exc)
            raise
        openai_batch_id = str(batch.get("id", "")).strip()
        status = str(batch.get("status", "")).strip() or "validating"
        if not openai_batch_id:
            raise RuntimeError(f"OpenAI batch create returned no id: {batch}")

        # OpenAI may create a batch with status=failed when the per-model
        # enqueued-tokens cap is hit (100M for text-embedding-3-small). In that
        # case we must NOT persist any mapping (those chunks should stay pending)
        # and we must stop submitting until in-flight batches drain.
        if status == "failed":
            errors = (batch.get("errors") or {}).get("data") or []
            codes = {str((e or {}).get("code", "")).strip() for e in errors}
            messages = [str((e or {}).get("message", "")).strip() for e in errors]
            if "token_limit_exceeded" in codes:
                logger_.warning(
                    "submit_batches_enqueue_full openai_batch_id=%s messages=%s "
                    "submitted=%d chunks_in_unpersisted_batch=%d — stopping cleanly (chunks stay pending)",
                    openai_batch_id,
                    messages,
                    len(submitted),
                    len(pending),
                )
                enqueue_full = True
                break
            # Some other failure — persist so the operator can inspect.
            logger_.warning(
                "submit_batches_create_failed openai_batch_id=%s messages=%s",
                openai_batch_id,
                messages,
            )

        with index._connect() as conn:  # noqa: SLF001
            _insert_batch_rows(
                conn,
                index.schema,
                openai_batch_id=openai_batch_id,
                embedding_model=model,
                embedding_version=version,
                input_file_id=file_id,
                status=status,
                request_count=len(pending),
                input_jsonl_path=str(input_path),
                include_context_prefix=include_context_prefix,
                chunks=pending,
            )
            conn.commit()

        logger_.info(
            "submit_batches_created openai_batch_id=%s status=%s requests=%d",
            openai_batch_id,
            status,
            len(pending),
        )
        submitted.append(
            {
                "openai_batch_id": openai_batch_id,
                "status": status,
                "request_count": len(pending),
                "input_file_id": file_id,
            }
        )
        total_requests += len(pending)

    return {
        "submitted_batches": len(submitted),
        "total_requests": total_requests,
        "batches": submitted,
        "embedding_model": model,
        "embedding_version": version,
        "stopped_reason": "enqueue_token_limit" if enqueue_full else "no_more_pending_or_max_batches",
    }


def poll_batches(*, index, logger_: logging.Logger, reclaim_token_limit_failures: bool = True) -> dict[str, Any]:
    """Refresh status / counts / output_file_id for every batch not yet ingested.

    When ``reclaim_token_limit_failures`` is True (default), batches that
    transitioned to ``failed`` because of OpenAI's ``token_limit_exceeded``
    enqueue cap are deleted from our DB so their chunks become pending again.
    These failures are normal backpressure — the batch ran no requests and
    consumed no tokens, so reclaiming is safe.
    """
    api_key = _resolve_api_key()
    base_url = _base_url()

    index.ensure_ready()
    with index._connect() as conn:  # noqa: SLF001
        index._ensure_batch_embed_tables(conn, ensure_indexes=True)  # noqa: SLF001
        rows = conn.execute(
            f"""
            SELECT openai_batch_id, status
            FROM {index.schema}.embed_batches
            WHERE ingested_at IS NULL
            ORDER BY created_at
            """
        ).fetchall()
    watch_ids = [str(r["openai_batch_id"] if isinstance(r, dict) else r[0]) for r in rows]

    refreshed: list[dict[str, Any]] = []
    reclaimed: list[str] = []
    for batch_id in watch_ids:
        try:
            payload = retrieve_batch(openai_batch_id=batch_id, api_key=api_key, base_url=base_url)
        except Exception as exc:
            logger_.warning("poll_batches_error batch=%s error=%s", batch_id, exc)
            continue
        status = str(payload.get("status", "")).strip()
        counts = payload.get("request_counts") or {}
        completed = int(counts.get("completed", 0) or 0)
        failed = int(counts.get("failed", 0) or 0)
        output_file_id = str(payload.get("output_file_id") or "").strip() or None
        error_file_id = str(payload.get("error_file_id") or "").strip() or None

        # Reclaim token-limit-exceeded failures: zero completed requests, zero
        # tokens charged; chunks should go back to pending.
        if reclaim_token_limit_failures and status == "failed" and completed == 0:
            errors = (payload.get("errors") or {}).get("data") or []
            codes = {str((e or {}).get("code", "")).strip() for e in errors}
            if "token_limit_exceeded" in codes:
                with index._connect() as conn:  # noqa: SLF001
                    conn.execute(
                        f"DELETE FROM {index.schema}.embed_batches WHERE openai_batch_id=%s",
                        (batch_id,),
                    )
                    conn.commit()
                reclaimed.append(batch_id)
                logger_.info("poll_batches_reclaimed_token_limit_failure batch=%s", batch_id)
                continue

        with index._connect() as conn:  # noqa: SLF001
            conn.execute(
                f"""
                UPDATE {index.schema}.embed_batches
                SET status=%s, completed_count=%s, failed_count=%s,
                    output_file_id=%s, error_file_id=%s, updated_at=NOW()
                WHERE openai_batch_id=%s
                """,
                (status, completed, failed, output_file_id, error_file_id, batch_id),
            )
            conn.commit()
        refreshed.append(
            {
                "openai_batch_id": batch_id,
                "status": status,
                "completed": completed,
                "failed": failed,
                "output_file_id": output_file_id,
                "error_file_id": error_file_id,
            }
        )
        logger_.info(
            "poll_batches_refreshed batch=%s status=%s completed=%d failed=%d",
            batch_id,
            status,
            completed,
            failed,
        )
    return {"polled": len(refreshed), "reclaimed_token_limit": len(reclaimed), "batches": refreshed}


def _ingest_one_batch(
    *,
    index,
    logger_: logging.Logger,
    info: dict[str, Any],
    api_key: str,
    base_url: str,
    art_dir: Path,
    write_batch_size: int,
) -> dict[str, Any]:
    """Download + parse + upsert one completed batch. Safe to call from a thread."""
    batch_id = str(info["openai_batch_id"])
    model = str(info["embedding_model"])
    version = int(info["embedding_version"])
    output_file_id = str(info["output_file_id"])
    error_file_id = (str(info["error_file_id"]) if info.get("error_file_id") else "") or None

    out_path = art_dir / f"{batch_id}-out.jsonl"
    t_start = time.monotonic()
    size = download_file(file_id=output_file_id, dest_path=out_path, api_key=api_key, base_url=base_url)
    logger_.info(
        "ingest_downloaded batch=%s bytes=%d elapsed=%.1fs path=%s",
        batch_id,
        size,
        time.monotonic() - t_start,
        out_path,
    )

    with index._connect() as conn:  # noqa: SLF001
        map_rows = conn.execute(
            f"""
            SELECT custom_id, chunk_key
            FROM {index.schema}.embed_batch_requests
            WHERE openai_batch_id = %s
            """,
            (batch_id,),
        ).fetchall()
    id_to_chunk = {
        str(r["custom_id"] if isinstance(r, dict) else r[0]): str(
            r["chunk_key"] if isinstance(r, dict) else r[1]
        )
        for r in map_rows
    }

    vectors_buffer: list[tuple[str, str]] = []
    written = 0
    failed_here = 0
    with out_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            custom_id = str(rec.get("custom_id", ""))
            chunk_key = id_to_chunk.get(custom_id)
            if chunk_key is None:
                failed_here += 1
                continue
            resp = rec.get("response") or {}
            body = resp.get("body") or {}
            data = body.get("data") or []
            if not isinstance(data, list) or not data:
                failed_here += 1
                continue
            emb = data[0].get("embedding")
            if not isinstance(emb, list):
                failed_here += 1
                continue
            vec = [float(v) for v in emb]
            vectors_buffer.append((chunk_key, _vector_literal(vec)))
            if len(vectors_buffer) >= write_batch_size:
                _upsert_embeddings_direct(
                    index,
                    rows=vectors_buffer,
                    embedding_model=model,
                    embedding_version=version,
                )
                written += len(vectors_buffer)
                vectors_buffer.clear()
    if vectors_buffer:
        _upsert_embeddings_direct(
            index,
            rows=vectors_buffer,
            embedding_model=model,
            embedding_version=version,
        )
        written += len(vectors_buffer)
        vectors_buffer.clear()

    error_count = 0
    if error_file_id:
        err_path = art_dir / f"{batch_id}-err.jsonl"
        try:
            download_file(
                file_id=error_file_id,
                dest_path=err_path,
                api_key=api_key,
                base_url=base_url,
            )
            with err_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if line.strip():
                        error_count += 1
        except Exception as exc:
            logger_.warning("ingest_error_file_download_failed batch=%s error=%s", batch_id, exc)

    with index._connect() as conn:  # noqa: SLF001
        conn.execute(
            f"""
            UPDATE {index.schema}.embed_batches
            SET ingested_at = NOW(), updated_at = NOW(),
                completed_count = %s, failed_count = GREATEST(failed_count, %s)
            WHERE openai_batch_id = %s
            """,
            (written, error_count + failed_here, batch_id),
        )
        conn.commit()

    logger_.info(
        "ingest_batch_complete batch=%s written=%d failed=%d elapsed=%.1fs",
        batch_id,
        written,
        failed_here + error_count,
        time.monotonic() - t_start,
    )
    return {
        "openai_batch_id": batch_id,
        "written": written,
        "failed": failed_here + error_count,
        "output_path": str(out_path),
    }


def ingest_completed_batches(
    *,
    index,
    logger_: logging.Logger,
    artifact_dir: str | None = None,
    write_batch_size: int = 500,
    workers: int = 1,
) -> dict[str, Any]:
    """Download output files for completed batches, upsert embeddings, mark ingested.

    When ``workers > 1`` downloads/parses run in parallel threads (each batch
    is ~1.5 GB download + parse, so threads help a lot — bottleneck is network).
    Thread-safe: each batch updates only its own row, and each writer uses its
    own short-lived connection.
    """
    api_key = _resolve_api_key()
    base_url = _base_url()
    art_dir = Path(artifact_dir or DEFAULT_BATCH_ARTIFACT_DIR)
    art_dir.mkdir(parents=True, exist_ok=True)

    index.ensure_ready()
    with index._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            f"""
            SELECT openai_batch_id, embedding_model, embedding_version, output_file_id, error_file_id
            FROM {index.schema}.embed_batches
            WHERE ingested_at IS NULL AND status = 'completed' AND output_file_id IS NOT NULL
            ORDER BY created_at
            """
        ).fetchall()
    candidates = [dict(r) for r in rows]

    ingested = 0
    total_written = 0
    total_failed = 0
    results: list[dict[str, Any]] = []
    workers = max(1, min(workers, len(candidates) or 1))

    if workers == 1 or len(candidates) <= 1:
        for info in candidates:
            try:
                res = _ingest_one_batch(
                    index=index,
                    logger_=logger_,
                    info=info,
                    api_key=api_key,
                    base_url=base_url,
                    art_dir=art_dir,
                    write_batch_size=write_batch_size,
                )
                results.append(res)
                ingested += 1
                total_written += int(res["written"])
                total_failed += int(res["failed"])
            except Exception as exc:
                logger_.error(
                    "ingest_batch_error batch=%s error=%s",
                    info.get("openai_batch_id"),
                    exc,
                )
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _ingest_one_batch,
                    index=index,
                    logger_=logger_,
                    info=info,
                    api_key=api_key,
                    base_url=base_url,
                    art_dir=art_dir,
                    write_batch_size=write_batch_size,
                ): info["openai_batch_id"]
                for info in candidates
            }
            for fut in as_completed(futures):
                batch_id = futures[fut]
                try:
                    res = fut.result()
                    results.append(res)
                    ingested += 1
                    total_written += int(res["written"])
                    total_failed += int(res["failed"])
                except Exception as exc:
                    logger_.error("ingest_batch_error batch=%s error=%s", batch_id, exc)

    return {
        "ingested_batches": ingested,
        "total_written": total_written,
        "total_failed": total_failed,
        "batches": results,
    }


def _upsert_embeddings_direct(
    index,
    *,
    rows: list[tuple[str, str]],
    embedding_model: str,
    embedding_version: int,
) -> None:
    """Upsert embeddings without using ``EmbedderMixin._bulk_upsert_embeddings``.

    That mixin method requires a connection arg; we manage our own here so we
    can commit per call (batches are large; we don't want one giant txn).
    """
    if not rows:
        return
    values_sql = ",".join(["(%s, %s, %s, %s::vector)"] * len(rows))
    params: list[Any] = []
    for chunk_key, vector_literal in rows:
        params.extend([chunk_key, embedding_model, embedding_version, vector_literal])
    with index._connect() as conn:  # noqa: SLF001
        conn.execute(
            f"""
            INSERT INTO {index.schema}.embeddings(chunk_key, embedding_model, embedding_version, embedding)
            VALUES {values_sql}
            ON CONFLICT (chunk_key, embedding_model, embedding_version)
            DO UPDATE SET embedding = EXCLUDED.embedding
            """,
            params,
        )
        conn.commit()


def batch_status(*, index, logger_: logging.Logger) -> dict[str, Any]:
    """Summary view for CLI / MCP — counts + per-status breakdown of all batches."""
    index.ensure_ready()
    with index._connect() as conn:  # noqa: SLF001
        index._ensure_batch_embed_tables(conn, ensure_indexes=True)  # noqa: SLF001
        status_rows = conn.execute(
            f"""
            SELECT status, COUNT(*) AS count, SUM(request_count) AS total_requests,
                   SUM(completed_count) AS total_completed, SUM(failed_count) AS total_failed
            FROM {index.schema}.embed_batches
            GROUP BY status ORDER BY status
            """
        ).fetchall()
        total_row = conn.execute(
            f"""
            SELECT COUNT(*) AS batches,
                   COALESCE(SUM(request_count),0) AS req,
                   COALESCE(SUM(completed_count),0) AS completed,
                   COALESCE(SUM(failed_count),0) AS failed,
                   COALESCE(SUM(CASE WHEN ingested_at IS NOT NULL THEN 1 ELSE 0 END),0) AS ingested
            FROM {index.schema}.embed_batches
            """
        ).fetchone()
        pending_row = conn.execute(
            f"""
            SELECT COUNT(*) AS pending_chunks
            FROM {index.schema}.chunks c
            LEFT JOIN {index.schema}.embeddings e ON e.chunk_key = c.chunk_key
            WHERE e.chunk_key IS NULL
            """
        ).fetchone()

    by_status = []
    for r in status_rows:
        d = dict(r)
        by_status.append(
            {
                "status": str(d["status"]),
                "count": int(d["count"]),
                "total_requests": int(d.get("total_requests") or 0),
                "total_completed": int(d.get("total_completed") or 0),
                "total_failed": int(d.get("total_failed") or 0),
            }
        )
    t = dict(total_row) if total_row else {}
    p = dict(pending_row) if pending_row else {}
    return {
        "batches_by_status": by_status,
        "totals": {
            "batches": int(t.get("batches", 0)),
            "requests": int(t.get("req", 0)),
            "completed": int(t.get("completed", 0)),
            "failed": int(t.get("failed", 0)),
            "ingested": int(t.get("ingested", 0)),
        },
        "pending_chunks_in_corpus": int(p.get("pending_chunks", 0)),
    }
