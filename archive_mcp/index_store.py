"""Derived archive index storage and query helpers.

Thin coordinator module. Functionality is distributed across:

- index_config: shared constants, env getters, utility functions
- chunk_builders: type-aware chunk building for each card type
- scanner: vault scanning, canonical row building, manifest diffing
- materializer: row materialization, edge building, person lookup
- loader: progress reporting, data loading utilities, LoaderMixin
- schema_ddl: SchemaDDLMixin (DDL, table creation)
- index_query: QueryMixin (search, query, graph)
- embedder: EmbedderMixin (embedding pipeline)

PostgresArchiveIndex remains the public API. All previously-public symbols
are re-exported for backward compatibility.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

from .chunk_builders import (  # noqa: F401 — re-exported for backward compat
    _build_calendar_event_chunks,
    _build_chunks,
    _build_document_chunks,
    _build_email_message_chunks,
    _build_email_thread_chunks,
    _build_git_commit_chunks,
    _build_git_message_chunks,
    _build_git_repository_chunks,
    _build_git_thread_chunks,
    _build_imessage_thread_chunks,
    _build_meeting_transcript_chunks,
    _build_person_chunks,
    _chunk_hash,
    _colon_speaker_turns,
    _format_labeled_block,
    _markdown_heading_sections,
    _meeting_transcript_focus_section,
    _otter_pipe_turns,
    _rolling_text_windows,
    _split_paragraphs,
    _split_text_chunks,
    _token_count,
)
from .chunking import render_chunks_for_card  # noqa: F401
from .embedder import EmbedderMixin
from .explain import projection_explain_payload  # noqa: F401
from .features import (
    TIMELINE_FIELDS,  # noqa: F401
)
from .index_config import (  # noqa: F401 — re-exported for backward compat
    CARD_TYPE_PRIORS,
    CHUNK_SCHEMA_VERSION,
    DEFAULT_CHUNK_CHAR_LIMIT,
    DEFAULT_EMBED_BATCH_SIZE,
    DEFAULT_EMBED_CONCURRENCY,
    DEFAULT_EMBED_MAX_RETRIES,
    DEFAULT_EMBED_PROGRESS_EVERY,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_EMBEDDING_VERSION,
    DEFAULT_POSTGRES_SCHEMA,
    DEFAULT_REBUILD_BATCH_SIZE,
    DEFAULT_REBUILD_COMMIT_INTERVAL,
    DEFAULT_REBUILD_EXECUTOR,
    DEFAULT_REBUILD_FLUSH_MAX_BYTES,
    DEFAULT_REBUILD_FLUSH_MAX_CHUNKS,
    DEFAULT_REBUILD_FLUSH_MAX_EDGES,
    DEFAULT_REBUILD_FLUSH_ROW_MULT,
    DEFAULT_REBUILD_PROGRESS_EVERY,
    DEFAULT_REBUILD_WORKERS,
    DEFAULT_VECTOR_DIMENSION,
    EDGE_RULE_BY_CARD_TYPE,
    HASH_SUFFIX_RE,
    INDEX_SCHEMA_VERSION,
    LOW_CONFIDENCE_FIELDS,
    MANIFEST_SCHEMA_VERSION,
    PROJECTIONS_BY_LOAD_ORDER,
    SCAN_MANIFEST_VERSION,
    VECTOR_CANDIDATE_MULTIPLIER,
    EmbeddingBatchResult,
    _activity_date,
    _apply_recency_boost,
    _card_type_prior,
    _coerce_source_fields,
    _field_provenance_bonus,
    _field_provenance_label,
    _format_row,
    _vector_literal,
    embed_defer_vector_index,
    get_chunk_char_limit,
    get_default_embedding_model,
    get_default_embedding_version,
    get_embed_batch_size,
    get_embed_concurrency,
    get_embed_max_retries,
    get_embed_progress_every,
    get_embed_write_batch_size,
    get_force_full_rebuild,
    get_index_dsn,
    get_index_schema,
    get_rebuild_batch_size,
    get_rebuild_commit_interval,
    get_rebuild_executor,
    get_rebuild_progress_every,
    get_rebuild_resume,
    get_rebuild_staging_mode,
    get_rebuild_workers,
    get_seed_frozen_enabled,
    get_vector_dimension,
    manifest_cache_disabled,
)
from .index_query import QueryMixin
from .loader import (  # noqa: F401 — re-exported for backward compat
    PROJECTION_COLUMNS_BY_TABLE,
    PROJECTION_NAMES,
    TYPED_PROJECTION_TABLES,
    LoaderMixin,
    RebuildRunResult,
    _chunked,
    _estimate_projection_buffer_bytes,
    _load_buffer_should_flush,
    _load_buffer_total_row_count,
    _log_rebuild_step,
    _RebuildFlushCaps,
    _RebuildProgressReporter,
    _sanitize_copy_value,
    get_rebuild_flush_caps,
)
from .materializer import (  # noqa: F401 — re-exported for backward compat  # noqa: F401 — re-exported for backward compat  # noqa: F401 — re-exported for backward compat
    EXTERNAL_ID_TARGET_PREFIX,
    _append_edge,
    _body_wikilinks,
    _build_edges,
    _build_person_lookup,
    _build_search_text,
    _chunk_key,
    _clean_text,
    _coerce_string_list,
    _dedupe_rows,
    _iter_string_values,
    _materialize_row,
    _materialize_row_batch,
    _normalize_exact_text,
    _resolve_person_reference,
    _resolve_slug,
    _slug_from_wikilink,
    _synthetic_external_id_path,
    _wikilinks_from_frontmatter,
)
from .materializer import _content_hash as _content_hash  # noqa: F401
from .materializer import _normalize_slug as _normalize_slug  # noqa: F401
from .projections.base import ProjectionRowBuffer  # noqa: F401
from .projections.registry import CHUNK_RULE_SPECS  # noqa: F401
from .scanner import (  # noqa: F401 — re-exported for backward compat
    CanonicalRow,
    NoteManifestRow,
    _build_manifest_rows_from_canonical,
    _canonical_row_from_rel_path,
    _classify_manifest_rebuild_delta,
    _collect_canonical_rows,
    _frontmatter_hash_stable,
    _iter_canonical_rows,
    _note_manifest_row_from_materialized,
    _people_orgs_json_for_card,
    _register_slug,
    _row_sort_key,
    _vault_paths_and_fingerprint,
)
from .schema_ddl import SchemaDDLMixin

logger = logging.getLogger("ppa.index_store")

_PHASE2_STANDALONE_FUNCTIONS_EXTRACTED = True


class BaseArchiveIndex:
    """Base derived index interface."""

    def __init__(self, vault: Path):
        self.vault = Path(vault)

    @property
    def location(self) -> str:
        raise NotImplementedError

    def ensure_ready(self) -> None:
        raise NotImplementedError

    def rebuild(self) -> dict[str, int]:
        raise NotImplementedError

    def bootstrap(self) -> dict[str, str]:
        raise NotImplementedError

    def status(self) -> dict[str, str]:
        raise NotImplementedError

    def read_path_for_uid(self, uid: str) -> str | None:
        raise NotImplementedError

    def person_path(self, name: str) -> str | None:
        raise NotImplementedError

    def query_cards(
        self,
        *,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        org_filter: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def timeline(self, *, start_date: str = "", end_date: str = "", limit: int = 20) -> list[dict[str, Any]]:
        raise NotImplementedError

    def stats(self) -> tuple[int, list[dict[str, Any]], list[dict[str, Any]]]:
        raise NotImplementedError

    def graph(self, note_path: str, hops: int = 2) -> dict[str, list[str]] | None:
        raise NotImplementedError

    def search(
        self,
        query: str,
        limit: int = 20,
        *,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def embedding_status(self, *, embedding_model: str, embedding_version: int) -> dict[str, int | str]:
        raise NotImplementedError

    def embedding_backlog(
        self,
        *,
        embedding_model: str,
        embedding_version: int,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def embed_pending(
        self,
        *,
        provider: Any,
        embedding_model: str,
        embedding_version: int,
        limit: int = 20,
        include_context_prefix: bool = False,
    ) -> dict[str, int | str]:
        raise NotImplementedError

    def vector_search(
        self,
        *,
        query_vector: list[float],
        embedding_model: str,
        embedding_version: int,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def hybrid_search(
        self,
        *,
        query: str,
        query_vector: list[float],
        embedding_model: str,
        embedding_version: int,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def duplicate_uid_rows(self, *, limit: int = 20) -> list[dict[str, Any]]:
        raise NotImplementedError


class PostgresArchiveIndex(SchemaDDLMixin, EmbedderMixin, QueryMixin, LoaderMixin, BaseArchiveIndex):
    """Postgres-backed derived index for ppa.

    Concrete implementation composing mixin modules:
    - SchemaDDLMixin: DDL, table creation, index management
    - EmbedderMixin: embedding pipeline
    - QueryMixin: search, query, graph traversal
    - LoaderMixin: rebuild orchestration, data loading, manifest management
    """

    def __init__(self, vault: Path, dsn: str | None = None):
        super().__init__(vault)
        self.dsn = (dsn or get_index_dsn()).strip()
        self.schema = get_index_schema()
        self.vector_dimension = get_vector_dimension()
        # Serialize embed_pending: concurrent calls share DDL (DROP/CREATE embed_queue).
        self._embed_pending_lock = threading.Lock()

    @property
    def location(self) -> str:
        return self.dsn

    def _connect(self):
        """Open a short-lived Postgres connection with safety timeouts.

        ``connect_timeout`` prevents indefinite hangs when the DB or tunnel is unreachable.
        ``statement_timeout`` prevents runaway queries from blocking the MCP server.
        Both matter for Arnold (remote DB over SSH tunnel) where connectivity can drop
        mid-session without notice.
        """
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("psycopg is required for Postgres archive indexes") from exc
        from .index_config import get_connect_timeout, get_statement_timeout_ms

        timeout_ms = get_statement_timeout_ms()
        conn_timeout = get_connect_timeout()
        dsn_preview = self.dsn[:40] if len(self.dsn) > 40 else self.dsn
        logger.debug(
            "pg_connect dsn=%s connect_timeout=%d statement_timeout_ms=%d",
            dsn_preview,
            conn_timeout,
            timeout_ms,
        )
        return psycopg.connect(
            self.dsn,
            row_factory=dict_row,
            connect_timeout=conn_timeout,
            options=f"-c statement_timeout={timeout_ms}",
        )


def get_archive_index(vault: Path) -> BaseArchiveIndex:
    dsn = get_index_dsn()
    if not dsn:
        raise RuntimeError("PPA_INDEX_DSN is required")
    return PostgresArchiveIndex(vault, dsn=dsn)
