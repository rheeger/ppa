"""Data loading, rebuild orchestration, checkpoint management, and manifest I/O."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from itertools import islice
from typing import Any

from .index_config import (CHUNK_SCHEMA_VERSION, INDEX_SCHEMA_VERSION,
                           MANIFEST_SCHEMA_VERSION, PROJECTIONS_BY_LOAD_ORDER,
                           get_force_full_rebuild, get_rebuild_batch_size,
                           get_rebuild_commit_interval, get_rebuild_executor,
                           get_rebuild_progress_every, get_rebuild_resume,
                           get_rebuild_staging_mode, get_rebuild_verify_hash,
                           get_rebuild_workers, get_seed_frozen_enabled,
                           manifest_cache_disabled)
from .materializer import (_build_person_lookup, _materialize_row_batch,
                           _resolve_person_reference, build_target_field_index)
from .ppa_engine import ppa_engine
from .projections.base import ProjectionRowBuffer
from .projections.registry import (PROJECTION_REGISTRY,
                                   PROJECTION_REGISTRY_VERSION,
                                   TYPED_PROJECTIONS)
from .scanner import (CanonicalRow, NoteManifestRow,
                      _build_manifest_rows_from_canonical,
                      _classify_manifest_rebuild_delta,
                      _collect_canonical_rows, _row_sort_key)
from .vault_cache import VaultScanCache

logger = logging.getLogger("ppa.loader")

PERSON_ESCALATION_THRESHOLD = 5000


def _compute_run_id(vault_manifest_hash: str) -> str:
    """Deterministic run_id; schema bumps invalidate stale checkpoints."""
    components = (
        f"{vault_manifest_hash}:{INDEX_SCHEMA_VERSION}"
        f":{CHUNK_SCHEMA_VERSION}:{PROJECTION_REGISTRY_VERSION}"
    )
    return hashlib.sha256(components.encode()).hexdigest()[:16]


def _try_resume_checkpoint(conn: Any, schema: str, run_id: str) -> tuple[str | None, int]:
    """Return (last_committed_rel_path, loaded_card_count) when resuming; else (None, 0).

    loaded_card_count is the number of cards already flushed in sorted materialization order;
    the materialize loop skips that prefix of the ordered row list (see resume_skip_count).
    """
    row = conn.execute(
        f"""SELECT run_id, status, last_committed_rel_path, loaded_card_count
            FROM {schema}.rebuild_checkpoint WHERE id = 1""",
    ).fetchone()
    if row is None:
        return (None, 0)
    keys = ["run_id", "status", "last_committed_rel_path", "loaded_card_count"]
    r = row if isinstance(row, dict) else dict(zip(keys, row))
    if str(r.get("status") or "") == "in_progress" and str(r.get("run_id") or "") == run_id:
        path = str(r.get("last_committed_rel_path") or "")
        lc = int(r.get("loaded_card_count") or 0)
        return (path if path else None, lc)
    return (None, 0)


PROJECTION_COLUMNS_BY_TABLE = {
    projection.table_name: tuple(column.name for column in projection.columns) for projection in PROJECTION_REGISTRY
}
PROJECTION_COLUMNS_BY_TABLE["ingestion_log"] = ("card_uid", "action", "source_adapter", "batch_id")
PROJECTION_NAMES = tuple(projection.table_name for projection in PROJECTION_REGISTRY)
TYPED_PROJECTION_TABLES = tuple(projection.table_name for projection in TYPED_PROJECTIONS)


@dataclass(slots=True)
class RebuildRunResult:
    counts: dict[str, int]
    metrics: dict[str, Any]


@dataclass(slots=True)
class _RebuildFlushCaps:
    max_total_rows: int
    max_edges: int
    max_chunks: int
    max_bytes: int


DEFAULT_REBUILD_FLUSH_ROW_MULT = 120
DEFAULT_REBUILD_FLUSH_MAX_EDGES = 100_000
DEFAULT_REBUILD_FLUSH_MAX_CHUNKS = 50_000
DEFAULT_REBUILD_FLUSH_MAX_BYTES = 256 * 1024 * 1024


def get_rebuild_flush_caps(commit_interval: int) -> _RebuildFlushCaps:
    """Upper bounds for the load buffer before COPY flush (adaptive vs card-count-only)."""
    from .index_config import _ppa_env

    raw_total = _ppa_env("PPA_REBUILD_FLUSH_MAX_TOTAL_ROWS")
    if raw_total:
        try:
            max_total_rows = max(1, int(raw_total))
        except ValueError:
            max_total_rows = max(commit_interval * DEFAULT_REBUILD_FLUSH_ROW_MULT, 50_000)
    else:
        mult_raw = _ppa_env("PPA_REBUILD_FLUSH_ROW_MULT", default=str(DEFAULT_REBUILD_FLUSH_ROW_MULT))
        try:
            mult = max(1, int(mult_raw))
        except ValueError:
            mult = DEFAULT_REBUILD_FLUSH_ROW_MULT
        max_total_rows = max(commit_interval * mult, 50_000)

    raw_edges = _ppa_env("PPA_REBUILD_FLUSH_MAX_EDGES", default=str(DEFAULT_REBUILD_FLUSH_MAX_EDGES))
    try:
        max_edges = max(1, int(raw_edges))
    except ValueError:
        max_edges = DEFAULT_REBUILD_FLUSH_MAX_EDGES

    raw_chunks = _ppa_env("PPA_REBUILD_FLUSH_MAX_CHUNKS", default=str(DEFAULT_REBUILD_FLUSH_MAX_CHUNKS))
    try:
        max_chunks = max(1, int(raw_chunks))
    except ValueError:
        max_chunks = DEFAULT_REBUILD_FLUSH_MAX_CHUNKS

    raw_bytes = _ppa_env("PPA_REBUILD_FLUSH_MAX_BYTES", default=str(DEFAULT_REBUILD_FLUSH_MAX_BYTES))
    try:
        max_bytes = max(1, int(raw_bytes))
    except ValueError:
        max_bytes = DEFAULT_REBUILD_FLUSH_MAX_BYTES

    return _RebuildFlushCaps(
        max_total_rows=max_total_rows,
        max_edges=max_edges,
        max_chunks=max_chunks,
        max_bytes=max_bytes,
    )


def _estimate_projection_buffer_bytes(buffer: ProjectionRowBuffer) -> int:
    total = 0
    for table_name in PROJECTION_NAMES:
        for row in buffer.rows_for(table_name):
            for cell in row:
                if isinstance(cell, str):
                    total += len(cell.encode("utf-8"))
                elif isinstance(cell, (bytes, memoryview, bytearray)):
                    total += len(cell)
                else:
                    total += 24
    return total


def _load_buffer_total_row_count(buffer: ProjectionRowBuffer) -> int:
    return sum(len(buffer.rows_for(table_name)) for table_name in PROJECTION_NAMES)


def _load_buffer_should_flush(
    buffer: ProjectionRowBuffer,
    *,
    commit_interval: int,
    pending_bytes: int,
    caps: _RebuildFlushCaps,
) -> bool:
    if len(buffer.rows_for("cards")) >= commit_interval:
        return True
    if _load_buffer_total_row_count(buffer) >= caps.max_total_rows:
        return True
    if len(buffer.rows_for("edges")) >= caps.max_edges:
        return True
    if len(buffer.rows_for("chunks")) >= caps.max_chunks:
        return True
    if pending_bytes >= caps.max_bytes:
        return True
    return False


@dataclass(slots=True)
class _RebuildProgressReporter:
    step_number: int
    total_steps: int
    stage: str
    total_items: int
    progress_every: int
    started_at: float
    min_interval_seconds: float = 5.0
    last_log_at: float = field(default_factory=time.time)
    last_logged_count: int = 0

    def _should_log(self, count: int) -> bool:
        if count <= 0:
            return False
        if self.progress_every and count - self.last_logged_count >= self.progress_every:
            return True
        return (time.time() - self.last_log_at) >= self.min_interval_seconds

    def _emit(self, message: str, count: int) -> None:
        self.last_log_at = time.time()
        self.last_logged_count = count
        # All rebuild progress goes to stderr via logger — stdout is reserved for MCP JSON-RPC.
        logger.info("%s", message)

    def _progress_bar(self, count: int, width: int = 24) -> str:
        if self.total_items <= 0:
            return "[" + ("." * width) + "]"
        ratio = min(max(count / self.total_items, 0.0), 1.0)
        filled = int(ratio * width)
        return "[" + ("#" * filled) + ("." * (width - filled)) + "]"

    def update(self, count: int, *, extra: str = "") -> None:
        if not self._should_log(count):
            return
        total_label = self.total_items if self.total_items > 0 else "?"
        percent = (count / self.total_items * 100.0) if self.total_items > 0 else 0.0
        elapsed = time.time() - self.started_at
        rate = count / elapsed if elapsed > 0 else 0.0
        remaining = max(self.total_items - count, 0) if self.total_items > 0 else 0
        eta = (remaining / rate) if rate > 0 and self.total_items > 0 else 0.0
        suffix = f" {extra}" if extra else ""
        self._emit(
            f"[ppa] step {self.step_number}/{self.total_steps} {self.stage} "
            f"{self._progress_bar(count)} {count}/{total_label} ({percent:.1f}%) "
            f"elapsed={elapsed:.1f}s rate={rate:.1f}/s eta={eta:.1f}s{suffix}",
            count,
        )

    def complete(self, count: int, *, extra: str = "") -> None:
        total_label = self.total_items if self.total_items > 0 else count
        elapsed = time.time() - self.started_at
        suffix = f" {extra}" if extra else ""
        self._emit(
            f"[ppa] step {self.step_number}/{self.total_steps} {self.stage} "
            f"{self._progress_bar(total_label if isinstance(total_label, int) else count)} "
            f"complete {count}/{total_label} elapsed={elapsed:.1f}s{suffix}",
            count,
        )


def _log_rebuild_step(step_number: int, total_steps: int, title: str, detail: str = "") -> None:
    suffix = f" {detail}" if detail else ""
    logger.info("step %d/%d %s%s", step_number, total_steps, title, suffix)


def _chunked(iterable: Iterable[Any], size: int) -> Iterable[list[Any]]:
    iterator = iter(iterable)
    while True:
        batch = list(islice(iterator, size))
        if not batch:
            return
        yield batch


def _sanitize_copy_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value


class LoaderMixin:
    """Data loading, rebuild orchestration, and manifest management mixin."""

    schema: str
    vault: Any
    dsn: str
    vector_dimension: int

    def ensure_ready(self) -> None:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT value FROM {self.schema}.meta WHERE key = %s",
                ("schema_version",),
            ).fetchone()
            if row is not None:
                self._run_pending_migrations(conn)

    def bootstrap(self, *, force: bool = False) -> dict[str, str]:
        """Initialize the derived index schema.

        On a fresh install: creates every table from scratch.

        On an existing populated schema: by default REFUSES to run because
        ``recreate_typed=True`` would ``DROP TABLE … CASCADE`` for every
        typed projection (a large data loss event). Pass ``force=True`` to
        override (or set ``PPA_BOOTSTRAP_FORCE=1``). Added 2026-04-24 after
        the embedding-wipe incident; see Phase 6.5 plan for context.
        """
        force_env = os.environ.get("PPA_BOOTSTRAP_FORCE", "").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        force = force or force_env
        with self._connect() as conn:
            existing_card_count = 0
            try:
                row = conn.execute(
                    """
                    SELECT to_regclass(%s) IS NOT NULL AS exists
                    """,
                    (f"{self.schema}.cards",),
                ).fetchone()
                cards_exists = bool(row.get("exists")) if isinstance(row, dict) else bool(row)
                if cards_exists:
                    cnt_row = conn.execute(
                        f"SELECT COUNT(*) AS c FROM {self.schema}.cards"
                    ).fetchone()
                    if isinstance(cnt_row, dict):
                        existing_card_count = int(cnt_row.get("c") or 0)
                    elif cnt_row is not None:
                        existing_card_count = int(cnt_row[0])
            except Exception:
                # Schema may not exist yet (fresh install) or the fake-conn used
                # in unit tests may not support introspection. Treat as empty;
                # the real safeguard fires only when the COUNT actually returns >0.
                try:
                    conn.rollback()
                except Exception:
                    pass
                existing_card_count = 0
            if existing_card_count > 0 and not force:
                raise RuntimeError(
                    f"bootstrap refused: schema {self.schema!r} already has "
                    f"{existing_card_count:,} cards. ``recreate_typed=True`` "
                    f"would DROP TABLE … CASCADE on every typed projection. "
                    f"Pass force=True or PPA_BOOTSTRAP_FORCE=1 to override "
                    f"(typically you want ``rebuild-indexes`` instead)."
                )
            self._create_schema(conn, recreate_typed=True)
            conn.execute(
                f"""
                INSERT INTO {self.schema}.meta(key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                ("schema_version", str(INDEX_SCHEMA_VERSION)),
            )
            conn.execute(
                f"""
                INSERT INTO {self.schema}.meta(key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                ("chunk_schema_version", str(CHUNK_SCHEMA_VERSION)),
            )
            conn.execute(
                f"""
                INSERT INTO {self.schema}.meta(key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                ("projection_registry_version", str(PROJECTION_REGISTRY_VERSION)),
            )
            conn.commit()
        return {
            "backend": "postgres",
            "dsn": self.dsn,
            "schema": self.schema,
            "vector_dimension": str(self.vector_dimension),
        }

    def _upsert_meta(self, conn, values: dict[str, str]) -> None:
        for key, value in values.items():
            conn.execute(
                f"""
                INSERT INTO {self.schema}.meta(key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, value),
            )

    def _copy_rows(
        self,
        conn,
        table_name: str,
        rows: list[tuple[Any, ...]],
        *,
        dest_suffix: str = "",
    ) -> None:
        if not rows:
            return
        columns = ", ".join(PROJECTION_COLUMNS_BY_TABLE[table_name])
        target = f"{table_name}{dest_suffix}" if dest_suffix else table_name
        with conn.cursor() as cur:
            with cur.copy(f"COPY {self.schema}.{target} ({columns}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(tuple(_sanitize_copy_value(value) for value in row))

    def _flush_load_buffer(
        self,
        conn,
        buffer: ProjectionRowBuffer,
        *,
        dest_suffix: str = "",
    ) -> dict[str, int]:
        counts = {table_name: len(buffer.rows_for(table_name)) for table_name in PROJECTION_NAMES}
        for projection in PROJECTION_REGISTRY:
            self._copy_rows(
                conn,
                projection.table_name,
                buffer.rows_for(projection.table_name),
                dest_suffix=dest_suffix,
            )
        if buffer.ingestion_log_rows:
            self._copy_rows(
                conn,
                "ingestion_log",
                buffer.ingestion_log_rows,
                dest_suffix=dest_suffix,
            )
        return counts

    def _open_copy_connections(self, n: int) -> list:
        """Open N additional psycopg connections for parallel COPY."""
        import psycopg
        from psycopg.rows import dict_row

        conns = []
        for _ in range(n):
            c = psycopg.connect(
                self.dsn,
                row_factory=dict_row,
                connect_timeout=5,
                options="-c statement_timeout=300000",
            )
            conns.append(c)
        return conns

    def _flush_copy_buffer_parallel(
        self,
        copy_buf: Any,
        conns: list,
        dest_suffix: str = "",
    ) -> dict[str, int]:
        """Flush a CopyBuffer across multiple connections in parallel threads.

        Assigns each table to its own connection. When there are more tables than
        connections, smaller tables share a connection. This maximizes parallelism
        for the biggest tables (edges, chunks, cards).
        """
        all_tables = copy_buf.table_names()
        counts: dict[str, int] = {}
        errors: list[Exception] = []

        tasks: list[tuple[int, str]] = []
        for i, table_name in enumerate(all_tables):
            data = copy_buf.table_data(table_name)
            if data:
                tasks.append((i % len(conns), table_name))

        def _flush_one(conn_idx: int, table_name: str) -> tuple[str, int]:
            conn = conns[conn_idx]
            data = copy_buf.table_data(table_name)
            if not data:
                return table_name, 0
            n_rows = copy_buf.table_row_count(table_name)
            target = f"{table_name}{dest_suffix}" if dest_suffix else table_name
            columns_tuple = PROJECTION_COLUMNS_BY_TABLE.get(table_name)
            if not columns_tuple:
                return table_name, 0
            columns = ", ".join(columns_tuple)
            with conn.cursor() as cur:
                with cur.copy(
                    f"COPY {self.schema}.{target} ({columns}) FROM STDIN"
                ) as copy:
                    copy.write(data)
            return table_name, n_rows

        with ThreadPoolExecutor(max_workers=len(conns)) as executor:
            futures = []
            for conn_idx, table_name in tasks:
                futures.append(executor.submit(_flush_one, conn_idx, table_name))
            for f in futures:
                try:
                    tn, rc = f.result()
                    counts[tn] = rc
                except Exception as exc:
                    errors.append(exc)

        if errors:
            for c in conns:
                try:
                    c.rollback()
                except Exception:
                    pass
            raise errors[0]

        for c in conns:
            c.commit()

        return counts

    def _meta_dict(self, conn) -> dict[str, str]:
        rows = conn.execute(f"SELECT key, value FROM {self.schema}.meta").fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}

    def _load_note_manifest_map(self, conn) -> dict[str, NoteManifestRow]:
        try:
            rows = conn.execute(f"SELECT * FROM {self.schema}.note_manifest").fetchall()
        except Exception:
            return {}
        out: dict[str, NoteManifestRow] = {}
        for row in rows:
            out[str(row["rel_path"])] = NoteManifestRow(
                rel_path=str(row["rel_path"]),
                card_uid=str(row["card_uid"]),
                slug=str(row["slug"]),
                content_hash=str(row["content_hash"]),
                frontmatter_hash=str(row["frontmatter_hash"]),
                file_size=int(row["file_size"]),
                mtime_ns=int(row["mtime_ns"]),
                card_type=str(row["card_type"]),
                typed_projection=str(row.get("typed_projection") or ""),
                people_json=str(row.get("people_json") or "[]"),
                orgs_json=str(row.get("orgs_json") or "[]"),
                scan_version=int(row["scan_version"]),
                chunk_schema_version=int(row["chunk_schema_version"]),
                projection_registry_version=int(row["projection_registry_version"]),
                index_schema_version=int(row["index_schema_version"]),
            )
        return out

    def _replace_note_manifest(self, conn, entries: list[NoteManifestRow]) -> None:
        """Reconcile ``note_manifest`` to ``entries`` via UPSERT + targeted prune.

        Previous implementation was DELETE-then-INSERT, which left the table
        empty if the INSERT failed mid-way and forced the next rebuild to
        full-scan everything. Switched to UPSERT + ``DELETE WHERE rel_path
        NOT IN (...)`` so the manifest is always non-empty + consistent
        across partial failures.
        """
        if not entries:
            conn.execute(f"DELETE FROM {self.schema}.note_manifest")
            return
        sql = f"""
            INSERT INTO {self.schema}.note_manifest (
                rel_path, card_uid, slug, content_hash, frontmatter_hash, file_size, mtime_ns,
                card_type, typed_projection, people_json, orgs_json, scan_version,
                chunk_schema_version, projection_registry_version, index_schema_version
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (rel_path) DO UPDATE SET
                card_uid = EXCLUDED.card_uid,
                slug = EXCLUDED.slug,
                content_hash = EXCLUDED.content_hash,
                frontmatter_hash = EXCLUDED.frontmatter_hash,
                file_size = EXCLUDED.file_size,
                mtime_ns = EXCLUDED.mtime_ns,
                card_type = EXCLUDED.card_type,
                typed_projection = EXCLUDED.typed_projection,
                people_json = EXCLUDED.people_json,
                orgs_json = EXCLUDED.orgs_json,
                scan_version = EXCLUDED.scan_version,
                chunk_schema_version = EXCLUDED.chunk_schema_version,
                projection_registry_version = EXCLUDED.projection_registry_version,
                index_schema_version = EXCLUDED.index_schema_version
        """
        batch = [
            (
                e.rel_path,
                e.card_uid,
                e.slug,
                e.content_hash,
                e.frontmatter_hash,
                e.file_size,
                e.mtime_ns,
                e.card_type,
                e.typed_projection,
                e.people_json,
                e.orgs_json,
                e.scan_version,
                e.chunk_schema_version,
                e.projection_registry_version,
                e.index_schema_version,
            )
            for e in entries
        ]
        with conn.cursor() as cur:
            cur.executemany(sql, batch)
        live_paths = [e.rel_path for e in entries]
        conn.execute(
            f"DELETE FROM {self.schema}.note_manifest WHERE rel_path <> ALL(%s)",
            (live_paths,),
        )

    def _delete_manifest_paths(self, conn, rel_paths: set[str]) -> None:
        if not rel_paths:
            return
        conn.execute(
            f"DELETE FROM {self.schema}.note_manifest WHERE rel_path = ANY(%s)",
            (list(rel_paths),),
        )

    def _delete_derived_rows_for_uids(self, conn, uids: set[str]) -> None:
        if not uids:
            return
        uid_list = list(uids)
        # NOTE (post-Migration-004): we no longer cascade-delete embeddings
        # here. Embeddings are content-addressable by chunk_key. After we
        # delete + re-materialize chunks below, embeddings whose chunk_key
        # regenerates identically (i.e. content unchanged) are still valid.
        # Truly-orphaned embeddings (chunks deleted entirely) are pruned by
        # the explicit `ppa embed-gc --apply` step.
        conn.execute(f"DELETE FROM {self.schema}.chunks WHERE card_uid = ANY(%s)", (uid_list,))
        conn.execute(
            f"DELETE FROM {self.schema}.edges WHERE source_uid = ANY(%s) OR target_uid = ANY(%s)",
            (uid_list, uid_list),
        )
        for projection in TYPED_PROJECTIONS:
            conn.execute(
                f"DELETE FROM {self.schema}.{projection.table_name} WHERE card_uid = ANY(%s)",
                (uid_list,),
            )
        conn.execute(
            f"DELETE FROM {self.schema}.external_ids WHERE card_uid = ANY(%s)",
            (uid_list,),
        )
        conn.execute(f"DELETE FROM {self.schema}.card_orgs WHERE card_uid = ANY(%s)", (uid_list,))
        conn.execute(
            f"DELETE FROM {self.schema}.card_people WHERE card_uid = ANY(%s)",
            (uid_list,),
        )
        conn.execute(
            f"DELETE FROM {self.schema}.card_sources WHERE card_uid = ANY(%s)",
            (uid_list,),
        )
        conn.execute(f"DELETE FROM {self.schema}.cards WHERE uid = ANY(%s)", (uid_list,))

    def _delete_derived_rows_for_incremental(
        self,
        conn,
        *,
        materialize_uids: set[str],
        purge_uids: set[str],
    ) -> None:
        """Remove derived rows for incremental rebuild.

        Unlike full _delete_derived_rows_for_uids, incoming edges *to* a rebuilt card are kept:
        deleting edges WHERE target_uid IN materialize_uids would drop edges from unchanged
        cards pointing at a materialized target (e.g. attachment -> message) until those
        sources are also rebuilt.

        Post-Migration-004: embeddings are no longer cascade-deleted here. After
        chunks are re-materialized for the touched UIDs, any chunk whose content
        is unchanged regenerates with the same ``chunk_key`` and its existing
        embedding stays valid (we just save the OpenAI cost). Genuinely orphaned
        embeddings (chunks for purge_uids that disappear entirely) get cleaned
        up by ``ppa embed-gc --apply``.
        """
        all_uids = materialize_uids | purge_uids
        if not all_uids:
            return
        uid_list = list(all_uids)
        conn.execute(f"DELETE FROM {self.schema}.chunks WHERE card_uid = ANY(%s)", (uid_list,))
        conn.execute(f"DELETE FROM {self.schema}.edges WHERE source_uid = ANY(%s)", (uid_list,))
        if purge_uids:
            conn.execute(
                f"DELETE FROM {self.schema}.edges WHERE target_uid = ANY(%s)",
                (list(purge_uids),),
            )
        for projection in TYPED_PROJECTIONS:
            conn.execute(
                f"DELETE FROM {self.schema}.{projection.table_name} WHERE card_uid = ANY(%s)",
                (uid_list,),
            )
        conn.execute(
            f"DELETE FROM {self.schema}.external_ids WHERE card_uid = ANY(%s)",
            (uid_list,),
        )
        conn.execute(f"DELETE FROM {self.schema}.card_orgs WHERE card_uid = ANY(%s)", (uid_list,))
        conn.execute(
            f"DELETE FROM {self.schema}.card_people WHERE card_uid = ANY(%s)",
            (uid_list,),
        )
        conn.execute(
            f"DELETE FROM {self.schema}.card_sources WHERE card_uid = ANY(%s)",
            (uid_list,),
        )
        conn.execute(f"DELETE FROM {self.schema}.cards WHERE uid = ANY(%s)", (uid_list,))

    def _ensure_unlogged_stage_tables(self, conn) -> None:
        for projection in PROJECTIONS_BY_LOAD_ORDER:
            table = projection.table_name
            stage = f"{table}_stage"
            conn.execute(f"DROP TABLE IF EXISTS {self.schema}.{stage}")
            conn.execute(
                f"""
                CREATE UNLOGGED TABLE {self.schema}.{stage} (
                    LIKE {self.schema}.{table} INCLUDING DEFAULTS EXCLUDING CONSTRAINTS EXCLUDING STATISTICS
                )
                """
            )

    def _promote_unlogged_stages(self, conn) -> None:
        """Atomically swap each ``{table}`` for its ``{table}_stage``.

        The TRUNCATE+INSERT pair runs inside the rebuild's transaction; if
        the INSERT fails, the TRUNCATE rolls back, so the destination is
        never partially populated. Pre-Migration-004 a TRUNCATE on ``chunks``
        also wiped ``embeddings`` via the FK CASCADE; after 004 there is no
        FK from ``embeddings`` to ``chunks``, so a chunks rebuild preserves
        all chunk_keys whose content is unchanged and all matching embeddings
        remain valid.
        """
        for projection in PROJECTIONS_BY_LOAD_ORDER:
            table = projection.table_name
            stage = f"{table}_stage"
            conn.execute(f"TRUNCATE TABLE {self.schema}.{table}")
            conn.execute(f"INSERT INTO {self.schema}.{table} SELECT * FROM {self.schema}.{stage}")
            conn.execute(f"DROP TABLE IF EXISTS {self.schema}.{stage}")

    def _clear_rebuild_checkpoint(self, conn) -> None:
        conn.execute(
            f"""
            UPDATE {self.schema}.rebuild_checkpoint SET
                run_id = '',
                mode = 'full',
                last_committed_rel_path = '',
                last_committed_card_uid = '',
                loaded_card_count = 0,
                loaded_row_counts_json = '{{}}',
                loaded_bytes_estimate = 0,
                vault_manifest_hash = '',
                index_schema_version = 0,
                chunk_schema_version = 0,
                projection_registry_version = 0,
                manifest_schema_version = 0,
                duplicate_uid_rows_loaded = FALSE,
                status = '',
                updated_at = NOW()
            WHERE id = 1
            """
        )

    def _save_rebuild_checkpoint(
        self,
        conn,
        *,
        run_id: str,
        mode: str,
        last_rel_path: str,
        last_card_uid: str,
        loaded_cards: int,
        row_counts: dict[str, int],
        bytes_estimate: int,
        vault_fp: str,
        versions: tuple[int, int, int],
        dup_loaded: bool,
        status: str,
    ) -> None:
        conn.execute(
            f"""
            INSERT INTO {self.schema}.rebuild_checkpoint (
                id, run_id, mode, last_committed_rel_path, last_committed_card_uid,
                loaded_card_count, loaded_row_counts_json, loaded_bytes_estimate,
                vault_manifest_hash, index_schema_version, chunk_schema_version,
                projection_registry_version, manifest_schema_version, duplicate_uid_rows_loaded,
                status, updated_at
            ) VALUES (
                1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (id) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                mode = EXCLUDED.mode,
                last_committed_rel_path = EXCLUDED.last_committed_rel_path,
                last_committed_card_uid = EXCLUDED.last_committed_card_uid,
                loaded_card_count = EXCLUDED.loaded_card_count,
                loaded_row_counts_json = EXCLUDED.loaded_row_counts_json,
                loaded_bytes_estimate = EXCLUDED.loaded_bytes_estimate,
                vault_manifest_hash = EXCLUDED.vault_manifest_hash,
                index_schema_version = EXCLUDED.index_schema_version,
                chunk_schema_version = EXCLUDED.chunk_schema_version,
                projection_registry_version = EXCLUDED.projection_registry_version,
                manifest_schema_version = EXCLUDED.manifest_schema_version,
                duplicate_uid_rows_loaded = EXCLUDED.duplicate_uid_rows_loaded,
                status = EXCLUDED.status,
                updated_at = NOW()
            """,
            (
                run_id,
                mode,
                last_rel_path,
                last_card_uid,
                loaded_cards,
                json.dumps(row_counts),
                bytes_estimate,
                vault_fp,
                versions[0],
                versions[1],
                versions[2],
                MANIFEST_SCHEMA_VERSION,
                dup_loaded,
                status,
            ),
        )

    def _projection_table_counts(self, conn) -> dict[str, int]:
        out: dict[str, int] = {}
        for projection in PROJECTION_REGISTRY:
            row = conn.execute(f"SELECT COUNT(*) AS c FROM {self.schema}.{projection.table_name}").fetchone()
            out[projection.table_name] = int(row["c"] or 0)
        return out

    def _run_projection_materialize_loop(
        self,
        conn,
        *,
        rows_to_process: list[CanonicalRow],
        slug_map: dict[str, str],
        path_to_uid: dict[str, str],
        person_lookup: dict[str, str],
        target_field_index: dict[str, dict[str, str]],
        workers: int,
        batch_size: int,
        commit_interval: int,
        flush_caps: _RebuildFlushCaps,
        executor_kind: str,
        progress_every: int,
        dest_suffix: str,
        run_id: str,
        vault_fp: str,
        versions: tuple[int, int, int],
        write_checkpoint: bool,
        rebuild_mode: str,
        resume_after_rel_path: str | None = None,
        resume_skip_count: int = 0,
    ) -> tuple[dict[str, int], float, float, int]:
        counts = {projection.table_name: 0 for projection in PROJECTION_REGISTRY}
        materialize_seconds = 0.0
        load_seconds = 0.0
        committed_cards = 0
        load_buffer = ProjectionRowBuffer()
        load_buffer_pending_bytes = 0
        ordered = sorted(rows_to_process, key=lambda r: _row_sort_key(r.rel_path))
        if resume_skip_count > 0:
            skip = min(max(0, resume_skip_count), len(ordered))
            try:
                crow = conn.execute(f"SELECT COUNT(*) AS c FROM {self.schema}.cards").fetchone()
                db_n = int(crow["c"] if isinstance(crow, dict) else crow[0])
            except Exception:
                db_n = 0
            # Checkpoint loaded_card_count can lag SIGTERM (last flush committed more than last save).
            if db_n > skip:
                skip = min(len(ordered), db_n)
            rows_to_process = ordered[skip:]
        elif resume_after_rel_path:
            resume_key = _row_sort_key(resume_after_rel_path)
            rows_to_process = [r for r in ordered if _row_sort_key(r.rel_path) > resume_key]
        else:
            rows_to_process = ordered
        chunked_rows = _chunked(rows_to_process, batch_size)
        materialize_batches_total = max(1, (len(rows_to_process) + batch_size - 1) // batch_size)
        materialize_reporter = _RebuildProgressReporter(
            step_number=5,
            total_steps=6,
            stage="materialize",
            total_items=max(len(rows_to_process), 1),
            progress_every=progress_every,
            started_at=time.time(),
        )
        load_reporter = _RebuildProgressReporter(
            step_number=5,
            total_steps=6,
            stage="load",
            total_items=max(len(rows_to_process), 1),
            progress_every=progress_every,
            started_at=time.time(),
        )
        from .vault_cache import VaultScanCache

        _body_cache = None
        _use_all_rows = False
        _cache_sqlite = VaultScanCache.cache_path_for_vault(self.vault)
        if ppa_engine() == "rust":
            try:
                import archive_crate

                if _cache_sqlite.exists():
                    logger.info("Loading body cache from %s", _cache_sqlite)
                    _body_cache = archive_crate.BodyCache.load(str(_cache_sqlite))
                    logger.info("Body cache loaded: %d entries", len(_body_cache))
                logger.info("Using materialize_all_rows (single Rust call, maps converted once)")
                _all_batches: list[ProjectionRowBuffer] = archive_crate.materialize_all_rows(
                    rows_to_process,
                    str(self.vault),
                    slug_map,
                    path_to_uid,
                    person_lookup,
                    target_field_index,
                    run_id,
                    CHUNK_SCHEMA_VERSION,
                    body_cache=_body_cache,
                    batch_size=commit_interval,
                )
                _use_all_rows = True
                logger.info("materialize_all_rows complete: %d batches", len(_all_batches))
            except Exception as exc:
                logger.warning("materialize_all_rows failed (%s), falling back to Python loop", exc)
                import traceback
                traceback.print_exc()
                _use_all_rows = False

        if not _use_all_rows:
            materialize_fn = partial(
                _materialize_row_batch,
                vault_root=str(self.vault),
                slug_map=slug_map,
                path_to_uid=path_to_uid,
                person_lookup=person_lookup,
                target_field_index=target_field_index,
                batch_id=run_id,
                body_cache=_body_cache,
            )

            def _consume_materialized_batches() -> Iterable[ProjectionRowBuffer]:
                if workers <= 1 or executor_kind == "serial":
                    for batch_rows in chunked_rows:
                        yield materialize_fn(batch_rows)
                    return
                executor_cls = ProcessPoolExecutor if executor_kind == "process" else ThreadPoolExecutor
                map_kwargs: dict[str, Any] = {}
                if executor_kind == "process":
                    map_kwargs["chunksize"] = max(
                        1,
                        min(32, materialize_batches_total // max(workers * 4, 1) or 1),
                    )
                with executor_cls(max_workers=workers) as executor:
                    yield from executor.map(materialize_fn, chunked_rows, **map_kwargs)

            _all_batches = list(_consume_materialized_batches())

        _copy_conns: list = []
        if _use_all_rows:
            try:
                _copy_conns = self._open_copy_connections(4)
                logger.info("Opened %d parallel COPY connections", len(_copy_conns))
            except Exception as exc:
                logger.warning("Failed to open parallel connections, using sequential: %s", exc)

        try:
            wait_started_at = time.time()
            for batch_index, materialized in enumerate(_all_batches, start=1):
                materialize_seconds += time.time() - wait_started_at
                if _use_all_rows and hasattr(materialized, "table_names"):
                    batch_cards = materialized.card_count()
                    load_started_at = time.time()
                    if _copy_conns:
                        batch_counts = self._flush_copy_buffer_parallel(
                            materialized, _copy_conns, dest_suffix=dest_suffix,
                        )
                        for tn, rc in batch_counts.items():
                            counts[tn] = counts.get(tn, 0) + rc
                    else:
                        for table_name in materialized.table_names():
                            data = materialized.table_data(table_name)
                            if not data:
                                continue
                            n_rows = materialized.table_row_count(table_name)
                            counts[table_name] = counts.get(table_name, 0) + n_rows
                            target = f"{table_name}{dest_suffix}" if dest_suffix else table_name
                            columns = ", ".join(PROJECTION_COLUMNS_BY_TABLE.get(table_name, ()))
                            if not columns:
                                continue
                            with conn.cursor() as cur:
                                with cur.copy(f"COPY {self.schema}.{target} ({columns}) FROM STDIN") as copy:
                                    copy.write(data)
                        conn.commit()
                    committed_cards += batch_cards
                    load_elapsed = time.time() - load_started_at
                    load_seconds += load_elapsed
                    _log_rebuild_step(
                        5, 6, "load flush",
                        f"buffer_cards={batch_cards} edges={counts.get('edges', 0)} "
                        f"chunks={counts.get('chunks', 0)} elapsed={round(load_elapsed, 3)}s",
                    )
                    load_reporter.update(
                        committed_cards,
                        extra=f"edges={counts.get('edges', 0)} chunks={counts.get('chunks', 0)}",
                    )
                    self._upsert_meta(conn, {
                        "rebuild_stage": "loading",
                        "rebuild_loaded_cards": str(committed_cards),
                        "rebuild_loaded_edges": str(counts.get("edges", 0)),
                        "rebuild_loaded_chunks": str(counts.get("chunks", 0)),
                    })
                    conn.commit()
                    if write_checkpoint:
                        self._save_rebuild_checkpoint(
                            conn, run_id=run_id, mode=rebuild_mode,
                            last_rel_path="", last_card_uid="",
                            loaded_cards=committed_cards,
                            row_counts=dict(counts), bytes_estimate=0,
                            vault_fp=vault_fp, versions=versions,
                            dup_loaded=True, status="in_progress",
                        )
                        conn.commit()
                else:
                    load_buffer.extend(materialized)
                    load_buffer_pending_bytes += _estimate_projection_buffer_bytes(materialized)
                    for table_name, rows_for_table in materialized.rows_by_table.items():
                        counts[table_name] = counts.get(table_name, 0) + len(rows_for_table)
                    materialize_reporter.update(
                        counts["cards"],
                        extra=(
                            f"batch={batch_index} "
                            f"typed={sum(counts.get(table_name, 0) for table_name in TYPED_PROJECTION_TABLES)} "
                            f"edges={counts['edges']} chunks={counts['chunks']}"
                        ),
                    )
                    if _load_buffer_should_flush(
                        load_buffer,
                        commit_interval=commit_interval,
                        pending_bytes=load_buffer_pending_bytes,
                        caps=flush_caps,
                    ):
                        load_started_at = time.time()
                        buffer_rows_total = _load_buffer_total_row_count(load_buffer)
                        buffer_edges = len(load_buffer.rows_for("edges"))
                        buffer_chunks = len(load_buffer.rows_for("chunks"))
                        card_rows_before_flush = load_buffer.rows_for("cards")
                        last_rel = str(card_rows_before_flush[-1][1]) if card_rows_before_flush else ""
                        last_uid = str(card_rows_before_flush[-1][0]) if card_rows_before_flush else ""
                        flushed = self._flush_load_buffer(conn, load_buffer, dest_suffix=dest_suffix)
                        conn.commit()
                        load_elapsed = time.time() - load_started_at
                        load_seconds += load_elapsed
                        committed_cards += flushed["cards"]
                        _log_rebuild_step(
                            5, 6, "load flush",
                            (
                                f"buffer_cards={flushed['cards']} buffer_rows_total={buffer_rows_total} "
                                f"buffer_bytes_estimate={load_buffer_pending_bytes} edges={buffer_edges} "
                                f"chunks={buffer_chunks} elapsed={round(load_elapsed, 3)}s"
                            ),
                        )
                        load_reporter.update(
                            committed_cards,
                            extra=f"edges={counts['edges']} chunks={counts['chunks']}",
                        )
                        self._upsert_meta(conn, {
                            "rebuild_stage": "loading",
                            "rebuild_loaded_cards": str(committed_cards),
                            "rebuild_loaded_edges": str(counts["edges"]),
                            "rebuild_loaded_chunks": str(counts["chunks"]),
                        })
                        conn.commit()
                        if write_checkpoint:
                            self._save_rebuild_checkpoint(
                                conn, run_id=run_id, mode=rebuild_mode,
                                last_rel_path=last_rel, last_card_uid=last_uid,
                                loaded_cards=committed_cards,
                                row_counts=dict(counts),
                                bytes_estimate=load_buffer_pending_bytes,
                                vault_fp=vault_fp, versions=versions,
                                dup_loaded=True, status="in_progress",
                            )
                            conn.commit()
                        load_buffer.clear()
                        load_buffer_pending_bytes = 0
                wait_started_at = time.time()

            if any(load_buffer.rows_for(table_name) for table_name in PROJECTION_NAMES):
                load_started_at = time.time()
                buffer_rows_total = _load_buffer_total_row_count(load_buffer)
                buffer_edges = len(load_buffer.rows_for("edges"))
                buffer_chunks = len(load_buffer.rows_for("chunks"))
                final_card_rows = load_buffer.rows_for("cards")
                final_last_rel = str(final_card_rows[-1][1]) if final_card_rows else ""
                final_last_uid = str(final_card_rows[-1][0]) if final_card_rows else ""
                flushed = self._flush_load_buffer(conn, load_buffer, dest_suffix=dest_suffix)
                conn.commit()
                load_elapsed = time.time() - load_started_at
                load_seconds += load_elapsed
                committed_cards += flushed["cards"]
                _log_rebuild_step(
                    5,
                    6,
                    "load flush",
                    (
                        f"buffer_cards={flushed['cards']} buffer_rows_total={buffer_rows_total} "
                        f"buffer_bytes_estimate={load_buffer_pending_bytes} edges={buffer_edges} "
                        f"chunks={buffer_chunks} elapsed={round(load_elapsed, 3)}s final=1"
                    ),
                )
                load_buffer.clear()
                load_buffer_pending_bytes = 0
                load_reporter.update(
                    committed_cards,
                    extra=f"edges={counts['edges']} chunks={counts['chunks']}",
                )
                if write_checkpoint:
                    self._save_rebuild_checkpoint(
                        conn,
                        run_id=run_id,
                        mode=rebuild_mode,
                        last_rel_path=final_last_rel,
                        last_card_uid=final_last_uid,
                        loaded_cards=committed_cards,
                        row_counts=dict(counts),
                        bytes_estimate=0,
                        vault_fp=vault_fp,
                        versions=versions,
                        dup_loaded=True,
                        status="in_progress",
                    )
                    conn.commit()
        finally:
            for c in _copy_conns:
                try:
                    c.close()
                except Exception:
                    pass
            if _copy_conns:
                logger.info("Closed %d parallel COPY connections", len(_copy_conns))

        materialize_reporter.complete(
            counts["cards"],
            extra=(
                f"typed={sum(counts.get(table_name, 0) for table_name in TYPED_PROJECTION_TABLES)} "
                f"edges={counts['edges']} chunks={counts['chunks']}"
            ),
        )
        load_reporter.complete(
            committed_cards,
            extra=f"edges={counts['edges']} chunks={counts['chunks']}",
        )
        return counts, materialize_seconds, load_seconds, committed_cards

    def rebuild_with_metrics(
        self,
        *,
        workers: int | None = None,
        batch_size: int | None = None,
        commit_interval: int | None = None,
        progress_every: int | None = None,
        executor_kind: str | None = None,
        force_full: bool | None = None,
        disable_manifest_cache: bool | None = None,
        no_cache: bool | None = None,
    ) -> RebuildRunResult:
        if os.environ.get("PPA_FORBID_REBUILD", "").strip().lower() in {
            "1",
            "true",
            "yes",
        }:
            raise RuntimeError(
                "Rebuild is forbidden (PPA_FORBID_REBUILD=1). "
                "This is a safety guard for production databases. "
                "Unset PPA_FORBID_REBUILD to allow rebuilds."
            )
        workers = workers or get_rebuild_workers()
        batch_size = batch_size or get_rebuild_batch_size()
        commit_interval = commit_interval or get_rebuild_commit_interval()
        flush_caps = get_rebuild_flush_caps(commit_interval)
        progress_every = get_rebuild_progress_every() if progress_every is None else progress_every
        executor_kind = (executor_kind or get_rebuild_executor()).strip().lower()
        force_full = get_force_full_rebuild() if force_full is None else bool(force_full)
        disable_manifest = manifest_cache_disabled() if disable_manifest_cache is None else bool(disable_manifest_cache)
        staging_mode = get_rebuild_staging_mode()
        versions = (
            INDEX_SCHEMA_VERSION,
            CHUNK_SCHEMA_VERSION,
            PROJECTION_REGISTRY_VERSION,
        )

        no_cache_flag = getattr(self, "_no_cache", False) if no_cache is None else bool(no_cache)
        _vault_cache: VaultScanCache | None = VaultScanCache.build_or_load(
            self.vault,
            tier=2,
            workers=workers or 1,
            progress_every=progress_every,
            no_cache=no_cache_flag,
        )

        started_at = time.time()
        scan_started_at = time.time()
        (
            rows,
            slug_map,
            duplicate_uid_count,
            duplicate_uid_rows,
            vault_fingerprint,
            file_stats,
        ) = _collect_canonical_rows(
            self.vault,
            workers=workers,
            executor_kind=executor_kind,
            progress_every=progress_every,
            cache=_vault_cache,
        )
        scan_seconds = round(time.time() - scan_started_at, 6)
        run_id = _compute_run_id(vault_fingerprint)

        map_started_at = time.time()
        _log_rebuild_step(3, 6, "build lookup maps", f"rows={len(rows)}")
        path_to_uid = {row.rel_path: str(row.card.uid) for row in rows}
        person_lookup = _build_person_lookup(rows)
        target_field_index = build_target_field_index(rows)
        map_seconds = round(time.time() - map_started_at, 6)
        _log_rebuild_step(
            3,
            6,
            "build lookup maps complete",
            f"slug_map={len(slug_map)} person_lookup={len(person_lookup)} "
            f"target_field_keys={len(target_field_index)}",
        )

        rebuild_mode = "full"
        manifest_map: dict[str, NoteManifestRow] = {}
        manifest_counters: dict[str, int] = {}
        materialize_uids: set[str] = set()
        purge_uids: set[str] = set()
        deleted_paths: set[str] = set()

        with self._connect() as conn:
            conn.execute("SET statement_timeout = '300s'")
            conn.commit()
            self._create_schema(conn, recreate_typed=False, ensure_indexes=False)
            conn.commit()
            self._run_pending_migrations(conn)
            try:
                pre_emb_row = conn.execute(
                    f"SELECT COUNT(*) AS c FROM {self.schema}.embeddings"
                ).fetchone()
                pre_embedding_count = int(
                    pre_emb_row["c"] if isinstance(pre_emb_row, dict) else pre_emb_row[0]
                )
            except Exception:
                pre_embedding_count = 0
            if pre_embedding_count:
                logger.info(
                    "rebuild_embeddings_pre_count count=%d note=preserved_via_migration_004 "
                    "(re-pay_to_re-embed_only_if_chunk_content_changed)",
                    pre_embedding_count,
                )
            meta = self._meta_dict(conn)
            manifest_map = self._load_note_manifest_map(conn)
            incremental_ok = (
                not force_full
                and not disable_manifest
                and not duplicate_uid_count
                and bool(manifest_map)
                and int(meta.get("manifest_schema_version", 0)) == MANIFEST_SCHEMA_VERSION
                and int(meta.get("schema_version", 0)) == versions[0]
                and int(meta.get("chunk_schema_version", 0)) == versions[1]
                and int(meta.get("projection_registry_version", 0)) == versions[2]
            )
            if incremental_ok:
                verify_hash = get_rebuild_verify_hash()
                rebuild_mode, materialize_uids, purge_uids, manifest_counters = _classify_manifest_rebuild_delta(
                    rows,
                    manifest_by_path=manifest_map,
                    file_stats=file_stats,
                    versions=versions,
                    duplicate_uid_count=duplicate_uid_count,
                    verify_hash=verify_hash,
                    vault=self.vault if verify_hash else None,
                )
                deleted_paths = set(manifest_map.keys()) - {r.rel_path for r in rows}
                if rebuild_mode == "person_triggered":
                    changed_person_uids = {
                        str(row.card.uid) for row in rows if row.card.type == "person" and str(row.card.uid) in materialize_uids
                    }
                    path_by_person_uid = {str(r.card.uid): r.rel_path for r in rows if r.card.type == "person"}
                    affected_uids: set[str] = set()
                    for person_uid in changed_person_uids:
                        crows = conn.execute(
                            f"SELECT card_uid FROM {self.schema}.card_people WHERE person = %s",
                            (person_uid,),
                        ).fetchall()
                        for row in crows:
                            uid = row["card_uid"] if isinstance(row, dict) else row[0]
                            affected_uids.add(str(uid))
                    for person_uid in changed_person_uids:
                        person_rel = path_by_person_uid.get(person_uid)
                        if not person_rel:
                            continue
                        for row in rows:
                            if str(row.card.uid) == person_uid:
                                continue
                            for pref in getattr(row.card, "people", []):
                                resolved = _resolve_person_reference(person_lookup, pref)
                                if resolved == person_rel:
                                    affected_uids.add(str(row.card.uid))
                    materialize_uids |= affected_uids
                    if len(materialize_uids) > PERSON_ESCALATION_THRESHOLD:
                        rebuild_mode = "full"
                    else:
                        rebuild_mode = "incremental"
            seed_ok = False
            if rebuild_mode == "noop":
                stored_fp = meta.get("vault_manifest_hash", "")
                seed_ok = bool(
                    get_seed_frozen_enabled()
                    and self.schema.strip().lower() == "archive_seed"
                    and stored_fp
                    and stored_fp == vault_fingerprint
                    and int(meta.get("manifest_note_count", -1)) == len(rows)
                )
                if seed_ok:
                    _log_rebuild_step(
                        2,
                        6,
                        "fingerprint notes complete",
                        "mode=seed_frozen reused=all vault_unchanged=1",
                    )
                else:
                    _log_rebuild_step(
                        2,
                        6,
                        "fingerprint notes complete",
                        (
                            f"new={manifest_counters.get('new', 0)} changed={manifest_counters.get('changed', 0)} "
                            f"unchanged={manifest_counters.get('unchanged', 0)} deleted={manifest_counters.get('deleted', 0)}"
                        ),
                    )
                _log_rebuild_step(4, 6, "prepare schema", f"mode=noop schema={self.schema}")
                total_seconds = round(time.time() - started_at, 6)
                self._upsert_meta(
                    conn,
                    {
                        "rebuild_last_noop_at": str(started_at),
                        "rebuild_last_mode": "noop",
                        "vault_manifest_hash": vault_fingerprint,
                        "manifest_last_scan_seconds": str(scan_seconds),
                        "manifest_last_changed_count": "0",
                        "manifest_last_deleted_count": "0",
                        "manifest_last_reused_count": str(len(rows)),
                    },
                )
                conn.commit()
                db_counts = self._projection_table_counts(conn)
                db_counts["duplicate_uid_rows"] = len(duplicate_uid_rows)
                db_counts["duplicate_uids"] = duplicate_uid_count
                metrics = {
                    "scan_seconds": round(scan_seconds, 6),
                    "map_seconds": round(map_seconds, 6),
                    "materialize_seconds": 0.0,
                    "load_seconds": 0.0,
                    "total_seconds": total_seconds,
                    "workers": workers,
                    "executor": executor_kind,
                    "batch_size": batch_size,
                    "commit_interval": commit_interval,
                    "rebuild_mode": "seed_frozen" if seed_ok else "noop",
                    "rows_per_second": round(len(rows) / max(total_seconds, 0.001), 3),
                }
                return RebuildRunResult(counts=db_counts, metrics=metrics)

            if rebuild_mode == "incremental":
                _log_rebuild_step(
                    2,
                    6,
                    "fingerprint notes complete",
                    (
                        f"new={manifest_counters.get('new', 0)} changed={manifest_counters.get('changed', 0)} "
                        f"unchanged={manifest_counters.get('unchanged', 0)} deleted={manifest_counters.get('deleted', 0)}"
                    ),
                )
                _log_rebuild_step(4, 6, "prepare schema", f"mode=incremental schema={self.schema}")
                step4_started_at = time.time()
                self._create_schema(conn, recreate_typed=False, ensure_indexes=False)
                conn.commit()
                if materialize_uids or purge_uids:
                    self._delete_derived_rows_for_incremental(
                        conn,
                        materialize_uids=materialize_uids,
                        purge_uids=purge_uids,
                    )
                if deleted_paths:
                    self._delete_manifest_paths(conn, deleted_paths)
                conn.execute(f"TRUNCATE TABLE {self.schema}.duplicate_uid_rows")
                self._copy_rows(conn, "duplicate_uid_rows", duplicate_uid_rows)
                conn.commit()
                rows_to_process = [r for r in rows if str(r.card.uid) in materialize_uids]
                _log_rebuild_step(
                    5,
                    6,
                    "materialize and load derived rows",
                    (
                        f"mode=incremental changed_only=true affected_cards={len(rows_to_process)} "
                        f"batch_size={batch_size} executor={executor_kind}"
                    ),
                )
                counts, materialize_seconds, load_seconds, _committed = self._run_projection_materialize_loop(
                    conn,
                    rows_to_process=rows_to_process,
                    slug_map=slug_map,
                    path_to_uid=path_to_uid,
                    person_lookup=person_lookup,
                    target_field_index=target_field_index,
                    workers=workers,
                    batch_size=batch_size,
                    commit_interval=commit_interval,
                    flush_caps=flush_caps,
                    executor_kind=executor_kind,
                    progress_every=progress_every,
                    dest_suffix="",
                    run_id=run_id,
                    vault_fp=vault_fingerprint,
                    versions=versions,
                    write_checkpoint=False,
                    rebuild_mode="incremental",
                )
                ensure_indexes_started_at = time.time()
                _log_rebuild_step(6, 6, "ensure indexes", "ensure_indexes=1 recreate_typed=0")
                self._create_schema(conn, recreate_typed=False, ensure_indexes=True)
                conn.commit()
                _log_rebuild_step(
                    6,
                    6,
                    "ensure indexes complete",
                    f"elapsed={round(time.time() - ensure_indexes_started_at, 3)}s",
                )
                manifest_entries = _build_manifest_rows_from_canonical(
                    rows, self.vault, file_stats, versions, cache=_vault_cache
                )
                self._replace_note_manifest(conn, manifest_entries)
                conn.commit()
                self._log_embedding_preservation_summary(conn, pre_count=pre_embedding_count)
                final_counts = self._projection_table_counts(conn)
                final_counts["duplicate_uid_rows"] = len(duplicate_uid_rows)
                final_counts["duplicate_uids"] = duplicate_uid_count
                total_seconds = round(time.time() - started_at, 6)
                _log_rebuild_step(
                    6,
                    6,
                    "finalize manifest complete",
                    (f"reused={manifest_counters.get('unchanged', 0)} manifest_notes={len(manifest_entries)}"),
                )
                self._clear_meta_for_finalize(conn)
                self._upsert_meta(
                    conn,
                    {
                        "schema_version": str(INDEX_SCHEMA_VERSION),
                        "chunk_schema_version": str(CHUNK_SCHEMA_VERSION),
                        "projection_registry_version": str(PROJECTION_REGISTRY_VERSION),
                        "manifest_schema_version": str(MANIFEST_SCHEMA_VERSION),
                        "vault_manifest_hash": vault_fingerprint,
                        "manifest_note_count": str(len(rows)),
                        "manifest_last_scan_seconds": str(round(scan_seconds, 6)),
                        "manifest_last_changed_count": str(
                            manifest_counters.get("new", 0) + manifest_counters.get("changed", 0)
                        ),
                        "manifest_last_deleted_count": str(manifest_counters.get("deleted", 0)),
                        "manifest_last_reused_count": str(manifest_counters.get("unchanged", 0)),
                        "card_count": str(final_counts["cards"]),
                        "external_id_count": str(final_counts["external_ids"]),
                        "duplicate_uid_row_count": str(len(duplicate_uid_rows)),
                        "edge_count": str(final_counts["edges"]),
                        "chunk_count": str(final_counts["chunks"]),
                        "duplicate_uid_count": str(duplicate_uid_count),
                        "rebuild_scan_seconds": str(round(scan_seconds, 6)),
                        "rebuild_map_seconds": str(round(map_seconds, 6)),
                        "rebuild_materialize_seconds": str(round(materialize_seconds, 6)),
                        "rebuild_load_seconds": str(round(load_seconds, 6)),
                        "rebuild_total_seconds": str(total_seconds),
                        "rebuild_workers": str(workers),
                        "rebuild_executor": executor_kind,
                        "rebuild_batch_size": str(batch_size),
                        "rebuild_commit_interval": str(commit_interval),
                        "rebuild_last_mode": "incremental",
                        **{
                            f"{table_name}_count": str(final_counts.get(table_name, 0))
                            for table_name in PROJECTION_NAMES
                            if table_name
                            not in {
                                "cards",
                                "external_ids",
                                "duplicate_uid_rows",
                                "edges",
                                "chunks",
                            }
                        },
                    },
                )
                conn.commit()
                metrics = {
                    "scan_seconds": round(scan_seconds, 6),
                    "map_seconds": round(map_seconds, 6),
                    "materialize_seconds": round(materialize_seconds, 6),
                    "load_seconds": round(load_seconds, 6),
                    "total_seconds": total_seconds,
                    "workers": workers,
                    "executor": executor_kind,
                    "batch_size": batch_size,
                    "commit_interval": commit_interval,
                    "rebuild_mode": "incremental",
                    "rows_per_second": round(len(rows_to_process) / max(total_seconds, 0.001), 3),
                }
                return RebuildRunResult(counts=final_counts, metrics=metrics)

            _log_rebuild_step(
                2,
                6,
                "fingerprint notes complete",
                f"new={len(rows)} changed=0 unchanged=0 deleted=0 mode=full_scan",
            )
            resume_after_full: str | None = None
            resume_loaded_cards = 0
            resuming_full = False
            if get_rebuild_resume():
                resume_after_full, resume_loaded_cards = _try_resume_checkpoint(conn, self.schema, run_id)
                resuming_full = resume_loaded_cards > 0 or bool(resume_after_full)
                if resume_after_full:
                    logger.info(
                        "Resuming rebuild from checkpoint after %s loaded_cards=%s",
                        resume_after_full,
                        resume_loaded_cards,
                    )
                elif resume_loaded_cards > 0:
                    logger.info("Resuming rebuild from checkpoint loaded_cards=%s", resume_loaded_cards)

            dest_suffix = ""
            use_unlogged_stage = staging_mode == "unlogged"
            if use_unlogged_stage and not resuming_full:
                _log_rebuild_step(4, 6, "prepare schema staging", "mode=unlogged_stage")

            _log_rebuild_step(4, 6, "prepare schema", f"mode=full schema={self.schema} resume={resuming_full}")
            step4_started_at = time.time()
            create_tables_started_at = time.time()
            if not resuming_full:
                _log_rebuild_step(
                    4,
                    6,
                    "prepare schema create tables",
                    "ensure_indexes=0 recreate_typed=1",
                )
                self._create_schema(conn, recreate_typed=True, ensure_indexes=False)
                conn.commit()
                _log_rebuild_step(
                    4,
                    6,
                    "prepare schema create tables complete",
                    f"elapsed={round(time.time() - create_tables_started_at, 3)}s",
                )
                self._clear_rebuild_checkpoint(conn)
                conn.commit()
                clear_started_at = time.time()
                _log_rebuild_step(
                    4,
                    6,
                    "prepare schema clear projections",
                    "truncate existing derived tables",
                )
                self._clear(conn)
                conn.commit()
                _log_rebuild_step(
                    4,
                    6,
                    "prepare schema clear projections complete",
                    f"elapsed={round(time.time() - clear_started_at, 3)}s",
                )
            else:
                self._create_schema(conn, recreate_typed=False, ensure_indexes=False)
                conn.commit()
            if use_unlogged_stage and not resuming_full:
                self._ensure_unlogged_stage_tables(conn)
                conn.commit()
                dest_suffix = "_stage"

            duplicate_rows_started_at = time.time()
            _log_rebuild_step(
                4,
                6,
                "prepare schema load duplicate uid rows",
                f"rows={len(duplicate_uid_rows)}",
            )
            if resuming_full:
                conn.execute(f"TRUNCATE TABLE {self.schema}.duplicate_uid_rows")
            self._copy_rows(conn, "duplicate_uid_rows", duplicate_uid_rows)
            conn.commit()
            _log_rebuild_step(
                4,
                6,
                "prepare schema load duplicate uid rows complete",
                f"rows={len(duplicate_uid_rows)} elapsed={round(time.time() - duplicate_rows_started_at, 3)}s",
            )
            _log_rebuild_step(
                4,
                6,
                "prepare schema complete",
                f"duplicate_uid_rows={len(duplicate_uid_rows)} elapsed={round(time.time() - step4_started_at, 3)}s",
            )
            self._upsert_meta(
                conn,
                {
                    "rebuild_stage": "scan_complete",
                    "rebuild_started_at": str(started_at),
                    "rebuild_scan_seconds": str(scan_seconds),
                    "rebuild_workers": str(workers),
                    "rebuild_executor": executor_kind,
                    "rebuild_batch_size": str(batch_size),
                    "rebuild_commit_interval": str(commit_interval),
                },
            )
            conn.commit()
            if not resuming_full:
                dropped = self._drop_indexes_for_bulk_load(conn)
                if dropped:
                    _log_rebuild_step(4, 6, "drop indexes for bulk load", f"dropped={dropped}")
            _log_rebuild_step(
                5,
                6,
                "materialize and load derived rows",
                (
                    f"rows={len(rows)} batch_size={batch_size} commit_interval={commit_interval} "
                    f"executor={executor_kind} staging={staging_mode} "
                    f"flush_max_total_rows={flush_caps.max_total_rows} "
                    f"flush_max_edges={flush_caps.max_edges} flush_max_chunks={flush_caps.max_chunks} "
                    f"flush_max_bytes={flush_caps.max_bytes}"
                ),
            )
            counts, materialize_seconds, load_seconds, _committed = self._run_projection_materialize_loop(
                conn,
                rows_to_process=rows,
                slug_map=slug_map,
                path_to_uid=path_to_uid,
                person_lookup=person_lookup,
                target_field_index=target_field_index,
                workers=workers,
                batch_size=batch_size,
                commit_interval=commit_interval,
                flush_caps=flush_caps,
                executor_kind=executor_kind,
                progress_every=progress_every,
                dest_suffix=dest_suffix,
                run_id=run_id,
                vault_fp=vault_fingerprint,
                versions=versions,
                write_checkpoint=True,
                rebuild_mode="full",
                resume_after_rel_path=resume_after_full,
                resume_skip_count=resume_loaded_cards,
            )
            counts["duplicate_uid_rows"] = len(duplicate_uid_rows)
            counts["duplicate_uids"] = duplicate_uid_count
            if use_unlogged_stage:
                promote_started = time.time()
                _log_rebuild_step(5, 6, "promote unlogged stage tables", "finalize=1")
                self._promote_unlogged_stages(conn)
                conn.commit()
                _log_rebuild_step(
                    5,
                    6,
                    "promote unlogged stage tables complete",
                    f"elapsed={round(time.time() - promote_started, 3)}s",
                )
            ensure_indexes_started_at = time.time()
            _log_rebuild_step(6, 6, "ensure indexes", "parallel=4 maintenance_work_mem=1GB")
            try:
                idx_elapsed = self._create_indexes_parallel(n_connections=4)
                _log_rebuild_step(
                    6, 6, "ensure indexes complete (parallel)",
                    f"elapsed={round(idx_elapsed, 3)}s",
                )
            except Exception as exc:
                logger.warning("Parallel index creation failed (%s), falling back to sequential", exc)
                self._create_schema(conn, recreate_typed=False, ensure_indexes=True)
                conn.commit()
                _log_rebuild_step(
                    6, 6, "ensure indexes complete (sequential fallback)",
                    f"elapsed={round(time.time() - ensure_indexes_started_at, 3)}s",
                )
            total_seconds = round(time.time() - started_at, 6)
            manifest_entries = _build_manifest_rows_from_canonical(
                rows, self.vault, file_stats, versions, cache=_vault_cache
            )
            self._replace_note_manifest(conn, manifest_entries)
            conn.commit()
            _log_rebuild_step(
                6,
                6,
                "finalize manifest complete",
                f"reused=0 manifest_notes={len(manifest_entries)}",
            )
            self._clear_meta_for_finalize(conn)
            self._upsert_meta(
                conn,
                {
                    "schema_version": str(INDEX_SCHEMA_VERSION),
                    "chunk_schema_version": str(CHUNK_SCHEMA_VERSION),
                    "projection_registry_version": str(PROJECTION_REGISTRY_VERSION),
                    "manifest_schema_version": str(MANIFEST_SCHEMA_VERSION),
                    "vault_manifest_hash": vault_fingerprint,
                    "manifest_note_count": str(len(rows)),
                    "manifest_last_scan_seconds": str(round(scan_seconds, 6)),
                    "manifest_last_changed_count": str(len(rows)),
                    "manifest_last_deleted_count": "0",
                    "manifest_last_reused_count": "0",
                    "card_count": str(counts["cards"]),
                    "external_id_count": str(counts["external_ids"]),
                    "duplicate_uid_row_count": str(len(duplicate_uid_rows)),
                    "edge_count": str(counts["edges"]),
                    "chunk_count": str(counts["chunks"]),
                    "duplicate_uid_count": str(duplicate_uid_count),
                    "rebuild_scan_seconds": str(round(scan_seconds, 6)),
                    "rebuild_map_seconds": str(round(map_seconds, 6)),
                    "rebuild_materialize_seconds": str(round(materialize_seconds, 6)),
                    "rebuild_load_seconds": str(round(load_seconds, 6)),
                    "rebuild_total_seconds": str(total_seconds),
                    "rebuild_workers": str(workers),
                    "rebuild_executor": executor_kind,
                    "rebuild_batch_size": str(batch_size),
                    "rebuild_commit_interval": str(commit_interval),
                    "rebuild_last_mode": "full",
                    **{
                        f"{table_name}_count": str(counts.get(table_name, 0))
                        for table_name in PROJECTION_NAMES
                        if table_name
                        not in {
                            "cards",
                            "external_ids",
                            "duplicate_uid_rows",
                            "edges",
                            "chunks",
                        }
                    },
                },
            )
            conn.commit()
            self._clear_rebuild_checkpoint(conn)
            conn.commit()
            _log_rebuild_step(
                6,
                6,
                "finalize rebuild metadata complete",
                f"cards={counts['cards']} edges={counts['edges']} chunks={counts['chunks']}",
            )
            self._log_embedding_preservation_summary(conn, pre_count=pre_embedding_count)

        metrics = {
            "scan_seconds": round(scan_seconds, 6),
            "map_seconds": round(map_seconds, 6),
            "materialize_seconds": round(materialize_seconds, 6),
            "load_seconds": round(load_seconds, 6),
            "total_seconds": round(time.time() - started_at, 6),
            "workers": workers,
            "executor": executor_kind,
            "batch_size": batch_size,
            "commit_interval": commit_interval,
            "rebuild_mode": "full",
            "rows_per_second": round(counts["cards"] / max(time.time() - started_at, 0.001), 3),
        }
        return RebuildRunResult(counts=counts, metrics=metrics)

    def _log_embedding_preservation_summary(self, conn, *, pre_count: int) -> None:
        """Emit a clear post-rebuild summary of embedding preservation + orphans.

        After Migration 004 chunks DROP/CASCADE no longer wipes embeddings, so
        ``post_count`` equals ``pre_count`` (modulo concurrent embed runs).
        ``orphan_count`` is the number of embeddings whose ``chunk_key`` is no
        longer present in ``chunks`` — those are safe to remove via
        ``ppa embed-gc --apply``. Until that runs they cost only disk.
        """
        try:
            post_row = conn.execute(
                f"SELECT COUNT(*) AS c FROM {self.schema}.embeddings"
            ).fetchone()
            post_count = int(
                post_row["c"] if isinstance(post_row, dict) else post_row[0]
            )
            orphan_row = conn.execute(
                f"""
                SELECT COUNT(*) AS c FROM {self.schema}.embeddings e
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.schema}.chunks c WHERE c.chunk_key = e.chunk_key
                )
                """
            ).fetchone()
            orphan_count = int(
                orphan_row["c"] if isinstance(orphan_row, dict) else orphan_row[0]
            )
        except Exception as exc:
            logger.warning("rebuild_embeddings_summary_query_failed error=%s", exc)
            return
        valid = max(post_count - orphan_count, 0)
        logger.info(
            "rebuild_embeddings_summary pre=%d post=%d valid_after_rebuild=%d "
            "orphan=%d gc_with=`ppa embed-gc --apply`",
            pre_count,
            post_count,
            valid,
            orphan_count,
        )
        if pre_count > 0 and post_count == 0:
            logger.error(
                "rebuild_embeddings_wiped pre=%d post=0 — Migration 004 should "
                "prevent this; investigate the rebuild path before re-embedding",
                pre_count,
            )
        elif orphan_count > 0 and pre_count > 0 and orphan_count > pre_count * 0.05:
            logger.warning(
                "rebuild_embeddings_significant_orphans orphan=%d (>%d%% of pre=%d) — "
                "expected if many cards' content changed; otherwise inspect chunk_key derivation",
                orphan_count,
                int(orphan_count / pre_count * 100),
                pre_count,
            )

    def _clear_meta_for_finalize(self, conn) -> None:
        """No-op kept for backward compatibility.

        Previously ``DELETE FROM meta`` then ``_upsert_meta(...)`` re-populated
        it; if the upsert failed, ``meta`` was left empty (broke ``index-status``,
        rebuild resume, schema-version detection, MCP startup). The upsert is
        now sufficient on its own: ``ON CONFLICT (key) DO UPDATE`` handles every
        key the rebuild writes, and any leftover keys are stale-but-harmless. To
        explicitly prune keys not in a known set, callers should do that
        themselves via a targeted ``DELETE WHERE key <> ALL(%s)`` instead.
        """
        return None

    def rebuild(self) -> dict[str, int]:
        result = self.rebuild_with_metrics()
        return result.counts
