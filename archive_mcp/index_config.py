"""Shared constants, environment config getters, and utility functions.

Extracted from index_store.py to break circular imports between the
coordinator (index_store) and its mixin modules (loader, schema_ddl,
embedder, index_query).
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from hfa.schema import DETERMINISTIC_ONLY, LLM_ELIGIBLE

from .projections.registry import EDGE_RULE_SPECS, PROJECTION_REGISTRY


def _ppa_env(canonical: str, default: str = "") -> str:
    """Read a PPA env var. No alias fallback — consumers must use PPA_* names."""
    value = os.environ.get(canonical, "").strip()
    return value if value else default


# ---------------------------------------------------------------------------
# Schema / chunk / manifest version constants
# ---------------------------------------------------------------------------

INDEX_SCHEMA_VERSION = 9
CHUNK_SCHEMA_VERSION = 5
MANIFEST_SCHEMA_VERSION = 2
SCAN_MANIFEST_VERSION = 1
DEFAULT_POSTGRES_SCHEMA = "archive_mcp"
DEFAULT_VECTOR_DIMENSION = 1536
DEFAULT_CHUNK_CHAR_LIMIT = 1200
DEFAULT_EMBEDDING_MODEL = "default-embedding-model"
DEFAULT_EMBEDDING_VERSION = 1
DEFAULT_EMBED_BATCH_SIZE = 32
DEFAULT_EMBED_MAX_RETRIES = 3
DEFAULT_EMBED_CONCURRENCY = 4
DEFAULT_EMBED_PROGRESS_EVERY = 0
DEFAULT_REBUILD_WORKERS = max(os.cpu_count() or 4, 1)
DEFAULT_REBUILD_BATCH_SIZE = 1000
DEFAULT_REBUILD_COMMIT_INTERVAL = 5000
DEFAULT_REBUILD_PROGRESS_EVERY = 10000
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


def get_index_dsn() -> str:
    return _ppa_env("PPA_INDEX_DSN")


def get_index_schema() -> str:
    return _ppa_env("PPA_INDEX_SCHEMA", default=DEFAULT_POSTGRES_SCHEMA)


def get_default_timezone() -> str:
    return _ppa_env("PPA_DEFAULT_TIMEZONE", default="UTC")


def get_vector_dimension() -> int:
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


def get_default_embedding_model() -> str:
    return _ppa_env("PPA_EMBEDDING_MODEL", default=DEFAULT_EMBEDDING_MODEL)


def get_default_embedding_version() -> int:
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


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class EmbeddingBatchResult:
    claimed: int = 0
    embedded: int = 0
    failed: int = 0
    last_error: str = ""


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
