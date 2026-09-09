"""Shared constants, environment config getters, and utility functions.

Extracted from index_store.py to break circular imports between the
coordinator (index_store) and its mixin modules (loader, schema_ddl,
embedder, index_query).
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from archive_vault.schema import DETERMINISTIC_ONLY, LLM_ELIGIBLE

from .projections.registry import EDGE_RULE_SPECS, PROJECTION_REGISTRY


def _ppa_env(canonical: str, default: str = "") -> str:
    """Read a PPA env var. No alias fallback — consumers must use PPA_* names."""
    value = os.environ.get(canonical, "").strip()
    return value if value else default


# ---------------------------------------------------------------------------
# Schema / chunk / manifest version constants
# ---------------------------------------------------------------------------

INDEX_SCHEMA_VERSION = 9
CHUNK_SCHEMA_VERSION = 7
MANIFEST_SCHEMA_VERSION = 2
SCAN_MANIFEST_VERSION = 1
DEFAULT_POSTGRES_SCHEMA = "ppa"
DEFAULT_VECTOR_DIMENSION = 1536
DEFAULT_CHUNK_CHAR_LIMIT = 1200
DEFAULT_BURST_TOKEN_LIMIT = 800
DEFAULT_BURST_CHAT_GAP_SECONDS = 300
BURST_ALGORITHM_VERSION = "p01b1-burst-1"
DEFAULT_RRF_K = 60
DEFAULT_DIVERSITY_CAP = 2
DEFAULT_DIVERSITY_WINDOW = 10
DEFAULT_RARE_TOKEN_WEIGHT = 0.0
DEFAULT_CURRENT_OPS_HALF_LIFE_DAYS = 30
DEFAULT_RERANK_TOP_N = 30
DEFAULT_RERANK_TIMEOUT_MS = 2000
DEFAULT_RRF_EXACT_WEIGHT = 2.0
DEFAULT_RRF_LEXICAL_WEIGHT = 1.0
DEFAULT_RRF_VECTOR_WEIGHT = 1.0
DEFAULT_RRF_GRAPH_WEIGHT = 0.25
MULTI_EVENT_CARD_TYPES = frozenset(
    {
        "calendar_event",
        "meal_order",
        "grocery_order",
        "ride",
        "flight",
        "accommodation",
        "car_rental",
        "purchase",
        "shipment",
        "event_ticket",
        "payroll",
    }
)
DEFAULT_EMBEDDING_MODEL = "default-embedding-model"
DEFAULT_EMBEDDING_VERSION = 1
DEFAULT_EMBED_BATCH_SIZE = 32
DEFAULT_EMBED_MAX_RETRIES = 3
DEFAULT_EMBED_CONCURRENCY = 4
DEFAULT_EMBED_PROGRESS_EVERY = 0
DEFAULT_GMAIL_API_WORKERS = 24
DEFAULT_GITHUB_STAGE_WORKERS = 2
DEFAULT_REBUILD_WORKERS = max(os.cpu_count() or 4, 1)
DEFAULT_REBUILD_BATCH_SIZE = 1000
DEFAULT_REBUILD_COMMIT_INTERVAL = 5000
DEFAULT_REBUILD_PROGRESS_EVERY = 10000
DEFAULT_SERVING_EXPORT_BATCH = 2000
DEFAULT_REBUILD_EXECUTOR = "thread"
DEFAULT_REBUILD_FLUSH_ROW_MULT = 120
DEFAULT_REBUILD_FLUSH_MAX_EDGES = 100_000
DEFAULT_REBUILD_FLUSH_MAX_CHUNKS = 50_000
DEFAULT_REBUILD_FLUSH_MAX_BYTES = 256 * 1024 * 1024
VECTOR_CANDIDATE_MULTIPLIER = 8
HASH_SUFFIX_RE = re.compile(r"-[0-9a-f]{8}$")
LOW_CONFIDENCE_FIELDS = frozenset({"summary", "thread_summary", "description"})
CARD_TYPE_PRIORS = {
    "person": 0.14,
    "calendar_event": 0.12,
    "meeting_transcript": 0.11,
    "email_thread": 0.1,
    "git_repository": 0.1,
    "git_thread": 0.09,
    "email_message": 0.08,
    "imessage_thread": 0.08,
    "git_commit": 0.08,
    "document": 0.07,
    "beeper_thread": 0.07,
    "imessage_message": 0.06,
    "git_message": 0.06,
    "medical_record": 0.05,
    "vaccination": 0.05,
    "beeper_message": 0.05,
    "finance": 0.04,
    "media_asset": 0.04,
    "email_attachment": 0.03,
    "imessage_attachment": 0.03,
    "beeper_attachment": 0.03,
    "meal_order": 0.06,
    "grocery_order": 0.06,
    "ride": 0.06,
    "flight": 0.07,
    "accommodation": 0.07,
    "car_rental": 0.06,
    "purchase": 0.06,
    "shipment": 0.05,
    "subscription": 0.06,
    "event_ticket": 0.07,
    "payroll": 0.05,
    "place": 0.08,
    "organization": 0.08,
    "knowledge": 0.03,
    "observation": 0.03,
}

PROJECTIONS_BY_LOAD_ORDER = tuple(sorted(PROJECTION_REGISTRY, key=lambda projection: projection.load_order))
EDGE_RULE_BY_CARD_TYPE = {spec.card_type: spec for spec in EDGE_RULE_SPECS}


# ---------------------------------------------------------------------------
# Environment config getters
# ---------------------------------------------------------------------------


def _ppa_env_int(canonical: str, default: int) -> int:
    raw = _ppa_env(canonical, default=str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def _ppa_env_bool(canonical: str) -> bool:
    return _ppa_env(canonical).lower() in {"1", "true", "yes", "on"}


def _bound_instance():
    from archive_engine.config import current_instance_config

    return current_instance_config()


def get_index_dsn() -> str:
    bound = _bound_instance()
    if bound is not None:
        from archive_engine.config import current_secret_values

        return current_secret_values().get("index_dsn") or ""
    return _ppa_env("PPA_INDEX_DSN")


def get_index_schema() -> str:
    bound = _bound_instance()
    if bound is not None:
        return bound.storage.index_schema
    return _ppa_env("PPA_INDEX_SCHEMA", default=DEFAULT_POSTGRES_SCHEMA)


def get_default_timezone() -> str:
    return _ppa_env("PPA_DEFAULT_TIMEZONE", default="UTC")


def get_vector_dimension() -> int:
    bound = _bound_instance()
    if bound is not None:
        return bound.embeddings.dimension if bound.embeddings.dimension > 0 else DEFAULT_VECTOR_DIMENSION
    v = _ppa_env_int("PPA_VECTOR_DIMENSION", default=DEFAULT_VECTOR_DIMENSION)
    return v if v > 0 else DEFAULT_VECTOR_DIMENSION


def get_statement_timeout_ms() -> int:
    """Postgres ``statement_timeout`` in milliseconds. Prevents runaway queries."""
    v = _ppa_env_int("PPA_STATEMENT_TIMEOUT_MS", default=30000)
    return max(v, 1000)


def get_connect_timeout() -> int:
    """Postgres ``connect_timeout`` in seconds. Prevents indefinite connection hangs."""
    v = _ppa_env_int("PPA_CONNECT_TIMEOUT", default=5)
    return max(v, 1)


def get_chunk_char_limit() -> int:
    v = _ppa_env_int("PPA_CHUNK_CHAR_LIMIT", default=DEFAULT_CHUNK_CHAR_LIMIT)
    return v if v > 0 else DEFAULT_CHUNK_CHAR_LIMIT


def get_burst_token_limit() -> int:
    v = _ppa_env_int("PPA_BURST_TOKEN_LIMIT", default=DEFAULT_BURST_TOKEN_LIMIT)
    return v if v > 0 else DEFAULT_BURST_TOKEN_LIMIT


def get_burst_chat_gap_seconds() -> int:
    v = _ppa_env_int("PPA_BURST_CHAT_GAP_SECONDS", default=DEFAULT_BURST_CHAT_GAP_SECONDS)
    return v if v > 0 else DEFAULT_BURST_CHAT_GAP_SECONDS


def _ppa_env_float(canonical: str, default: float) -> float:
    raw = _ppa_env(canonical, default=str(default))
    try:
        return float(raw)
    except ValueError:
        return default


def get_rrf_k() -> int:
    v = _ppa_env_int("PPA_RRF_K", default=DEFAULT_RRF_K)
    return v if v > 0 else DEFAULT_RRF_K


def get_rrf_channel_weight(channel: str) -> float:
    defaults = {
        "exact": DEFAULT_RRF_EXACT_WEIGHT,
        "lexical": DEFAULT_RRF_LEXICAL_WEIGHT,
        "vector": DEFAULT_RRF_VECTOR_WEIGHT,
        "graph": DEFAULT_RRF_GRAPH_WEIGHT,
    }
    key = {
        "exact": "PPA_RRF_EXACT_WEIGHT",
        "lexical": "PPA_RRF_LEXICAL_WEIGHT",
        "vector": "PPA_RRF_VECTOR_WEIGHT",
        "graph": "PPA_RRF_GRAPH_WEIGHT",
    }[channel]
    v = _ppa_env_float(key, defaults[channel])
    return v if v >= 0 else defaults[channel]


def get_diversity_cap() -> int:
    v = _ppa_env_int("PPA_DIVERSITY_CAP", default=DEFAULT_DIVERSITY_CAP)
    return v if v >= 0 else DEFAULT_DIVERSITY_CAP


def get_diversity_window() -> int:
    v = _ppa_env_int("PPA_DIVERSITY_WINDOW", default=DEFAULT_DIVERSITY_WINDOW)
    return v if v > 0 else DEFAULT_DIVERSITY_WINDOW


def get_rare_token_weight() -> float:
    v = _ppa_env_float("PPA_RARE_TOKEN_WEIGHT", DEFAULT_RARE_TOKEN_WEIGHT)
    return v if v >= 0 else DEFAULT_RARE_TOKEN_WEIGHT


def get_ranking_profile() -> str:
    value = _ppa_env("PPA_RANKING_PROFILE", default="default").strip().lower()
    return value if value else "default"


def get_current_ops_half_life_days() -> float:
    v = _ppa_env_float("PPA_CURRENT_OPS_HALF_LIFE_DAYS", float(DEFAULT_CURRENT_OPS_HALF_LIFE_DAYS))
    return v if v > 0 else float(DEFAULT_CURRENT_OPS_HALF_LIFE_DAYS)


def get_rerank_top_n() -> int:
    v = _ppa_env_int("PPA_RERANK_TOP_N", default=DEFAULT_RERANK_TOP_N)
    return v if v > 0 else DEFAULT_RERANK_TOP_N


def get_rerank_timeout_ms() -> int:
    v = _ppa_env_int("PPA_RERANK_TIMEOUT_MS", default=DEFAULT_RERANK_TIMEOUT_MS)
    return v if v > 0 else DEFAULT_RERANK_TIMEOUT_MS


def get_default_embedding_model() -> str:
    bound = _bound_instance()
    if bound is not None:
        return bound.embeddings.model
    return _ppa_env("PPA_EMBEDDING_MODEL", default=DEFAULT_EMBEDDING_MODEL)


def get_default_embedding_version() -> int:
    bound = _bound_instance()
    if bound is not None:
        try:
            version = int(bound.embeddings.model_revision)
        except (TypeError, ValueError):
            version = DEFAULT_EMBEDDING_VERSION
        return version if version > 0 else DEFAULT_EMBEDDING_VERSION
    v = _ppa_env_int("PPA_EMBEDDING_VERSION", default=DEFAULT_EMBEDDING_VERSION)
    return v if v > 0 else DEFAULT_EMBEDDING_VERSION


def get_embed_batch_size() -> int:
    v = _ppa_env_int("PPA_EMBED_BATCH_SIZE", default=DEFAULT_EMBED_BATCH_SIZE)
    return v if v > 0 else DEFAULT_EMBED_BATCH_SIZE


def get_embed_max_retries() -> int:
    v = _ppa_env_int("PPA_EMBED_MAX_RETRIES", default=DEFAULT_EMBED_MAX_RETRIES)
    return v if v >= 0 else DEFAULT_EMBED_MAX_RETRIES


def get_embed_concurrency() -> int:
    v = _ppa_env_int("PPA_EMBED_CONCURRENCY", default=DEFAULT_EMBED_CONCURRENCY)
    return max(v, 1)


def get_gmail_api_workers() -> int:
    """Provider-bound Gmail HTTP workers. Default 24 (CardEnrichmentRunner-class I/O)."""
    v = _ppa_env_int("PPA_GMAIL_API_WORKERS", default=DEFAULT_GMAIL_API_WORKERS)
    return max(v, 1)


def get_github_stage_workers() -> int:
    """Parallel ``gh`` workers for GitHub stage extract. Default 2 (rate-limit safe)."""
    v = _ppa_env_int("PPA_GITHUB_STAGE_WORKERS", default=DEFAULT_GITHUB_STAGE_WORKERS)
    return max(v, 1)


def get_anydoc_extract_cache_path() -> Path:
    """SHA-256 extract reuse cache (documents + attachments). Machine-wide SQLite."""
    raw = _ppa_env("PPA_ANYDOC_EXTRACT_CACHE")
    if raw:
        return Path(raw)
    return Path.home() / ".ppa" / "anydoc-extract-cache.sqlite"


def get_file_identity_db_path() -> Path:
    """Source-byte SHA → card UID index for same-file duplicate links."""
    raw = _ppa_env("PPA_FILE_IDENTITY_DB")
    if raw:
        return Path(raw)
    return Path.home() / ".ppa" / "file-identity.sqlite"


def get_embed_write_batch_size() -> int:
    raw = _ppa_env("PPA_EMBED_WRITE_BATCH_SIZE")
    if not raw:
        return get_embed_batch_size()
    try:
        return max(int(raw), 1)
    except ValueError:
        return get_embed_batch_size()


def get_embed_progress_every() -> int:
    v = _ppa_env_int("PPA_EMBED_PROGRESS_EVERY", default=DEFAULT_EMBED_PROGRESS_EVERY)
    return max(v, 0)


def embed_defer_vector_index() -> bool:
    return _ppa_env_bool("PPA_EMBED_DEFER_VECTOR_INDEX")


def get_ivfflat_lists() -> int | None:
    """Optional IVFFlat ``lists`` override. None means auto-calculate from row count."""
    raw = _ppa_env("PPA_IVFFLAT_LISTS")
    if not raw:
        return None
    try:
        v = int(raw)
        return v if v > 0 else None
    except ValueError:
        return None


def get_rebuild_workers() -> int:
    v = _ppa_env_int("PPA_REBUILD_WORKERS", default=DEFAULT_REBUILD_WORKERS)
    return max(v, 1)


def get_rebuild_batch_size() -> int:
    v = _ppa_env_int("PPA_REBUILD_BATCH_SIZE", default=DEFAULT_REBUILD_BATCH_SIZE)
    return max(v, 1)


def get_rebuild_commit_interval() -> int:
    v = _ppa_env_int("PPA_REBUILD_COMMIT_INTERVAL", default=DEFAULT_REBUILD_COMMIT_INTERVAL)
    return max(v, 1)


def get_rebuild_progress_every() -> int:
    v = _ppa_env_int("PPA_REBUILD_PROGRESS_EVERY", default=DEFAULT_REBUILD_PROGRESS_EVERY)
    return max(v, 0)


def get_rebuild_executor() -> str:
    value = _ppa_env("PPA_REBUILD_EXECUTOR", default=DEFAULT_REBUILD_EXECUTOR).lower()
    return value if value in {"thread", "process", "serial"} else DEFAULT_REBUILD_EXECUTOR


def get_rebuild_staging_mode() -> str:
    value = _ppa_env("PPA_REBUILD_STAGING_MODE", default="direct").lower()
    return value if value in {"direct", "unlogged"} else "direct"


def get_force_full_rebuild() -> bool:
    return _ppa_env_bool("PPA_FORCE_FULL_REBUILD")


def manifest_cache_disabled() -> bool:
    return _ppa_env_bool("PPA_DISABLE_MANIFEST_CACHE")


def get_seed_frozen_enabled() -> bool:
    return _ppa_env_bool("PPA_SEED_FROZEN")


def get_rebuild_resume() -> bool:
    return _ppa_env_bool("PPA_REBUILD_RESUME")


def get_rebuild_verify_hash() -> bool:
    """When true, incremental classification compares on-disk content_hash to manifest."""
    return _ppa_env_bool("PPA_REBUILD_VERIFY_HASH")


def get_primary_user_uid() -> str:
    return _ppa_env("PPA_PRIMARY_USER_UID", default="")


def get_seed_links_enabled() -> bool:
    return _ppa_env_bool("PPA_SEED_LINKS_ENABLED")


DEFAULT_CONTEXT_PRECEDING = 1
DEFAULT_CONTEXT_FOLLOWING = 1
DEFAULT_CONTEXT_MAX_TOKENS_PER_HIT = 2000
DEFAULT_CONTEXT_MAX_TOKENS_TOTAL = 8000
DEFAULT_GRAPH_MAX_DEPTH = 1
DEFAULT_GRAPH_MAX_PUBLIC_DEPTH = 2
DEFAULT_GRAPH_MAX_NODES = 256
DEFAULT_GRAPH_MAX_EDGES = 512
DEFAULT_GRAPH_MAX_ELAPSED_MS = 250


def get_context_preceding() -> int:
    return max(_ppa_env_int("PPA_CONTEXT_PRECEDING", DEFAULT_CONTEXT_PRECEDING), 0)


def get_context_following() -> int:
    return max(_ppa_env_int("PPA_CONTEXT_FOLLOWING", DEFAULT_CONTEXT_FOLLOWING), 0)


def get_context_max_tokens_per_hit() -> int:
    return max(_ppa_env_int("PPA_CONTEXT_MAX_TOKENS_PER_HIT", DEFAULT_CONTEXT_MAX_TOKENS_PER_HIT), 1)


def get_context_max_tokens_total() -> int:
    return max(_ppa_env_int("PPA_CONTEXT_MAX_TOKENS_TOTAL", DEFAULT_CONTEXT_MAX_TOKENS_TOTAL), 1)


def get_graph_max_depth() -> int:
    value = _ppa_env_int("PPA_GRAPH_MAX_DEPTH", DEFAULT_GRAPH_MAX_DEPTH)
    return min(max(value, 1), DEFAULT_GRAPH_MAX_PUBLIC_DEPTH)


def get_graph_max_nodes() -> int:
    return max(_ppa_env_int("PPA_GRAPH_MAX_NODES", DEFAULT_GRAPH_MAX_NODES), 1)


def get_graph_max_edges() -> int:
    return max(_ppa_env_int("PPA_GRAPH_MAX_EDGES", DEFAULT_GRAPH_MAX_EDGES), 1)


def get_graph_max_elapsed_ms() -> int:
    return max(_ppa_env_int("PPA_GRAPH_MAX_ELAPSED_MS", DEFAULT_GRAPH_MAX_ELAPSED_MS), 0)


def get_serving_index_path(vault: Path | None = None) -> Path:
    raw = _ppa_env("PPA_SERVING_INDEX_PATH")
    if raw:
        return Path(raw)
    bound = _bound_instance()
    if vault is not None:
        return Path(vault) / "_meta" / "rust-search-index"
    if bound is not None:
        return Path(bound.storage.serving_index_path)
    root = Path(_ppa_env("PPA_PATH", default="."))
    return root / "_meta" / "rust-search-index"


def get_serving_index_max_rss_mb() -> int:
    return max(_ppa_env_int("PPA_SERVING_INDEX_MAX_RSS_MB", default=8192), 256)


def get_serving_nlist() -> int | None:
    """Optional serving IVF list count. None means ``sqrt(N)`` clamped to 4096."""
    raw = _ppa_env("PPA_SERVING_NLIST")
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def get_serving_nprobe() -> int:
    """Selective probe count. Default 32; never silently becomes ``nlist`` at scale."""
    return max(_ppa_env_int("PPA_SERVING_NPROBE", default=32), 1)


def get_serving_train_sample() -> int:
    return max(_ppa_env_int("PPA_SERVING_TRAIN_SAMPLE", default=100_000), 1)


def get_serving_train_iters() -> int:
    return max(_ppa_env_int("PPA_SERVING_TRAIN_ITERS", default=25), 1)


def get_serving_train_seed() -> int:
    return _ppa_env_int("PPA_SERVING_TRAIN_SEED", default=20260906)


def get_serving_candidate_budget() -> int:
    return max(_ppa_env_int("PPA_SERVING_CANDIDATE_BUDGET", default=4096), 1)


def get_serving_train_memory_mb() -> int:
    return max(_ppa_env_int("PPA_SERVING_TRAIN_MEMORY_MB", default=get_serving_index_max_rss_mb()), 64)


def get_serving_export_batch_size() -> int:
    """Client fetch size for warehouse embedding export. Keep bounded so 4M+ vectors are not fetchall'd."""
    return max(_ppa_env_int("PPA_SERVING_EXPORT_BATCH", default=DEFAULT_SERVING_EXPORT_BATCH), 100)


def _ppa_env_float(canonical: str, default: float) -> float:
    raw = _ppa_env(canonical, default=str(default))
    try:
        return float(raw)
    except ValueError:
        return default


def get_publication_max_chain_depth() -> int:
    """Bounded parent walk before an explicit compaction rebuild."""
    return max(_ppa_env_int("PPA_PUBLICATION_MAX_CHAIN_DEPTH", default=8), 1)


def get_publication_delta_ratio() -> float:
    """Delta/live-vector ratio that triggers explicit compaction."""
    return max(_ppa_env_float("PPA_PUBLICATION_DELTA_RATIO", default=0.5), 0.01)


def get_publication_disk_budget_mb() -> int:
    """Hard ceiling for one publication candidate. ``0`` fails closed in tests."""
    raw = _ppa_env("PPA_PUBLICATION_DISK_BUDGET_MB")
    if raw == "0":
        return 0
    return max(_ppa_env_int("PPA_PUBLICATION_DISK_BUDGET_MB", default=1_000_000), 1)


def get_publication_lease_stale_seconds() -> int:
    return max(_ppa_env_int("PPA_PUBLICATION_LEASE_STALE_SECONDS", default=30), 1)


def get_publication_min_embed_coverage() -> float:
    """Minimum embeddings/chunks ratio for a non-hash full publish. ``0`` disables."""
    raw = _ppa_env("PPA_PUBLICATION_MIN_EMBED_COVERAGE")
    if raw == "0":
        return 0.0
    return min(max(_ppa_env_float("PPA_PUBLICATION_MIN_EMBED_COVERAGE", default=0.9), 0.0), 1.0)


def get_publication_min_embed_chunks() -> int:
    """Skip the coverage gate below this chunk count (fixtures / empty gens)."""
    return max(_ppa_env_int("PPA_PUBLICATION_MIN_EMBED_CHUNKS", default=1000), 0)


def get_prior_chunk_schema_versions() -> tuple[int, ...]:
    """Versioned hash recipes that may still hold leftover embedding keys."""
    raw = _ppa_env("PPA_PRIOR_CHUNK_SCHEMA_VERSIONS")
    if raw:
        return tuple(int(part) for part in raw.split(",") if part.strip())
    return (6, 5, 4)


def get_embed_reuse_batch_size() -> int:
    """Pending chunk_keys per reuse INSERT. Keep this small; toast copies are huge."""
    return max(_ppa_env_int("PPA_EMBED_REUSE_BATCH_SIZE", default=2000), 1)


def get_embed_gc_batch_size() -> int:
    """Leftover embedding rows per DELETE batch."""
    return max(_ppa_env_int("PPA_EMBED_GC_BATCH_SIZE", default=2000), 1)


def get_warehouse_min_free_gb() -> int:
    """Stop warehouse writes when the vault volume has less than this many GiB free."""
    return max(_ppa_env_int("PPA_WAREHOUSE_MIN_FREE_GB", default=40), 0)


def get_query_embed_cache_path(vault: Path | None = None) -> Path:
    raw = _ppa_env("PPA_QUERY_EMBED_CACHE_PATH")
    if raw:
        return Path(raw)
    bound = _bound_instance()
    if bound is not None:
        bound_root = Path(bound.storage.vault_path).expanduser().resolve()
        if vault is None or Path(vault).expanduser().resolve() == bound_root:
            return Path(bound.storage.query_embed_cache_path)
    root = Path(vault) if vault is not None else Path(_ppa_env("PPA_PATH", default="."))
    return root / "_meta" / "query-embed-cache.sqlite"


def get_query_embed_cache_ram_entries() -> int:
    return max(_ppa_env_int("PPA_QUERY_EMBED_CACHE_RAM_ENTRIES", default=2048), 0)


def get_query_embed_cache_max_rows() -> int:
    return max(_ppa_env_int("PPA_QUERY_EMBED_CACHE_MAX_ROWS", default=50000), 1)


def get_query_embed_cache_max_age_days() -> int:
    return max(_ppa_env_int("PPA_QUERY_EMBED_CACHE_MAX_AGE_DAYS", default=30), 1)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class EmbeddingBatchResult:
    claimed: int = 0
    embedded: int = 0
    failed: int = 0
    last_error: str = ""
    claimed_keys: list[str] = field(default_factory=list)
    embedded_keys: list[str] = field(default_factory=list)
    failed_keys: list[str] = field(default_factory=list)
    card_uids: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Utility functions (used by mixin modules and the index_store coordinator)
# ---------------------------------------------------------------------------


def _format_row(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    raise TypeError(f"Unsupported row type: {type(row)!r}")


def _field_provenance_label(source_fields: list[str]) -> str:
    field_set = {field for field in source_fields if field}
    if not field_set:
        return "unknown"
    if "body" in field_set:
        return "deterministic"
    if field_set.issubset(DETERMINISTIC_ONLY):
        return "deterministic"
    if field_set & LLM_ELIGIBLE:
        return "llm_derived"
    return "mixed"


def _field_provenance_bonus(source_fields: list[str]) -> float:
    label = _field_provenance_label(source_fields)
    if label == "deterministic":
        return 0.08
    if label == "mixed":
        return 0.04
    if label == "llm_derived":
        return 0.01
    return 0.0


def _card_type_prior(card_type: str) -> float:
    return CARD_TYPE_PRIORS.get(card_type, 0.02)


def _format_activity_at(value: Any) -> str:
    """Format activity_at for display (TIMESTAMPTZ from psycopg or legacy string)."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value).strip()


def _activity_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    raw = str(value).strip()
    return raw[:10] if raw else ""


def _apply_recency_boost(rows: list[dict[str, Any]], *, key_name: str) -> None:
    dated = [row for row in rows if _format_activity_at(row.get("activity_at")).strip()]
    if not dated:
        return
    ordered = sorted(
        dated,
        key=lambda row: (_format_activity_at(row.get("activity_at")), str(row.get("rel_path", ""))),
        reverse=True,
    )
    total = max(len(ordered) - 1, 1)
    for index, row in enumerate(ordered):
        row[key_name] = round((1.0 - (index / total)) * 0.06, 6)


def _coerce_source_fields(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [value] if value.strip() else []
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item).strip()]
    return []


def _vector_literal(values: list[float]) -> str:
    return "[" + ",".join(f"{value:.8f}" for value in values) + "]"
