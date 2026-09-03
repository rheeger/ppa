"""Persistent query-embedding cache (InferenceCache-shaped).

Does not store document embeddings. Does not skip vector kNN — a cache hit
only avoids re-embedding the query string.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import struct
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("ppa.query_embed_cache")

SCHEMA_VERSION = "1"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS query_embed_cache (
    cache_key TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    version INTEGER NOT NULL,
    provider TEXT NOT NULL,
    dimension INTEGER NOT NULL,
    vector BLOB NOT NULL,
    created_at TEXT NOT NULL,
    hit_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_query_embed_created ON query_embed_cache(created_at);
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_query_text(text: str) -> str:
    return " ".join((text or "").casefold().split())


def query_embed_cache_key(
    text: str,
    *,
    model: str,
    version: int,
    provider: str,
    dimension: int,
) -> str:
    raw = f"{normalize_query_text(text)}\0{model}\0{version}\0{provider}\0{dimension}\0{SCHEMA_VERSION}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _pack_vector(values: list[float]) -> bytes:
    return struct.pack(f"<{len(values)}f", *values)


def _unpack_vector(blob: bytes, dimension: int) -> list[float]:
    if len(blob) != dimension * 4:
        raise ValueError(f"vector blob length {len(blob)} != {dimension * 4}")
    return list(struct.unpack(f"<{dimension}f", blob))


@dataclass(frozen=True)
class QueryEmbedSpec:
    model: str
    version: int
    provider: str
    dimension: int


class QueryEmbedCache:
    """SHA-keyed SQLite WAL cache with a process LRU in front."""

    def __init__(
        self,
        db_path: Path | str,
        *,
        ram_entries: int = 2048,
        _skip_init: bool = False,
    ):
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._ram_cap = max(int(ram_entries), 0)
        self._lru: OrderedDict[str, list[float]] = OrderedDict()
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._conn = sqlite3.connect(str(self._path), timeout=30.0, check_same_thread=False)
        if not _skip_init:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _lru_get(self, key: str) -> list[float] | None:
        if self._ram_cap <= 0:
            return None
        vec = self._lru.get(key)
        if vec is None:
            return None
        self._lru.move_to_end(key)
        return list(vec)

    def _lru_put(self, key: str, vector: list[float]) -> None:
        if self._ram_cap <= 0:
            return
        self._lru[key] = list(vector)
        self._lru.move_to_end(key)
        while len(self._lru) > self._ram_cap:
            self._lru.popitem(last=False)

    def get(self, text: str, spec: QueryEmbedSpec) -> list[float] | None:
        key = query_embed_cache_key(
            text,
            model=spec.model,
            version=spec.version,
            provider=spec.provider,
            dimension=spec.dimension,
        )
        with self._lock:
            ram = self._lru_get(key)
            if ram is not None:
                self._hits += 1
                return ram
            row = self._conn.execute(
                "SELECT vector, dimension FROM query_embed_cache WHERE cache_key = ?",
                (key,),
            ).fetchone()
            if row is None:
                self._misses += 1
                return None
            vector = _unpack_vector(row[0], int(row[1]))
            self._conn.execute(
                "UPDATE query_embed_cache SET hit_count = hit_count + 1 WHERE cache_key = ?",
                (key,),
            )
            self._conn.commit()
            self._lru_put(key, vector)
            self._hits += 1
            return vector

    def put(self, text: str, spec: QueryEmbedSpec, vector: list[float]) -> str:
        if len(vector) != spec.dimension:
            raise ValueError(f"vector length {len(vector)} != dimension {spec.dimension}")
        key = query_embed_cache_key(
            text,
            model=spec.model,
            version=spec.version,
            provider=spec.provider,
            dimension=spec.dimension,
        )
        blob = _pack_vector(vector)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO query_embed_cache
                    (cache_key, model, version, provider, dimension, vector, created_at, hit_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(cache_key) DO UPDATE SET
                    vector = excluded.vector,
                    created_at = excluded.created_at
                """,
                (key, spec.model, spec.version, spec.provider, spec.dimension, blob, _utc_now_iso()),
            )
            self._conn.commit()
            self._lru_put(key, vector)
        return key

    def evict(self, *, max_rows: int, max_age_days: int) -> dict[str, int]:
        """Maintain-only eviction. Deletes oldest rows past max_rows or max_age_days."""
        deleted_age = 0
        deleted_cap = 0
        cutoff = time.time() - max(int(max_age_days), 0) * 86400
        cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).replace(microsecond=0).isoformat().replace(
            "+00:00", "Z"
        )
        with self._lock:
            cur = self._conn.execute("DELETE FROM query_embed_cache WHERE created_at < ?", (cutoff_iso,))
            deleted_age = int(cur.rowcount or 0)
            count = int(self._conn.execute("SELECT COUNT(*) FROM query_embed_cache").fetchone()[0] or 0)
            overflow = max(count - max(int(max_rows), 0), 0)
            if overflow > 0:
                cur = self._conn.execute(
                    """
                    DELETE FROM query_embed_cache WHERE cache_key IN (
                        SELECT cache_key FROM query_embed_cache
                        ORDER BY created_at ASC
                        LIMIT ?
                    )
                    """,
                    (overflow,),
                )
                deleted_cap = int(cur.rowcount or 0)
            self._conn.commit()
            self._lru.clear()
        logger.info("query_embed_cache evict deleted_age=%s deleted_cap=%s", deleted_age, deleted_cap)
        return {"deleted_age": deleted_age, "deleted_cap": deleted_cap}

    def stats(self) -> dict[str, Any]:
        with self._lock:
            rows = int(self._conn.execute("SELECT COUNT(*) FROM query_embed_cache").fetchone()[0] or 0)
            return {
                "path": str(self._path),
                "rows": rows,
                "hits": self._hits,
                "misses": self._misses,
                "ram_entries": len(self._lru),
                "schema_version": SCHEMA_VERSION,
            }
