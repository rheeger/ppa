"""Durable SHA-256 cache for document/attachment extracts.

Keyed by **source file bytes** (SHA-256). A hit copies markdown + text_source
and skips local convert and Firecrawl hosted OCR. Documents and email
attachments share one machine-wide SQLite file.

Default path: ``~/.ppa/anydoc-extract-cache.sqlite``
Override: ``PPA_ANYDOC_EXTRACT_CACHE``.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Iterable

log = logging.getLogger("ppa.extract_cache")

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SCHEMA = """
CREATE TABLE IF NOT EXISTS extracts (
    sha256 TEXT PRIMARY KEY,
    markdown TEXT NOT NULL,
    text_source TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inflight (
    sha256 TEXT PRIMARY KEY,
    started_at TEXT NOT NULL
);
"""
_SEED_TYPES = ("document", "email_attachment")
_REUSABLE_SOURCES = frozenset(
    {"anydoc", "anydoc_hosted", "html2text", "markitdown", "plain"}
)
_REUSABLE_STATUS = "content_extracted"


@dataclass(frozen=True)
class CachedExtract:
    sha256: str
    markdown: str
    text_source: str
    status: str
    created_at: str = ""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def is_sha256(value: str) -> bool:
    return bool(_SHA256_RE.fullmatch((value or "").strip().lower()))


def default_extract_cache_path() -> Path:
    from archive_cli.index_config import get_anydoc_extract_cache_path

    return get_anydoc_extract_cache_path()


class ExtractCache:
    """WAL SQLite cache: source sha256 → markdown + text_source.

    Hosted OCR is serialized per SHA: ``inflight()`` is a process-wide lock
    plus a sqlite ``inflight`` row so waiters re-check WAL after the owner
    writes through. Never two concurrent hosted calls for the same bytes.
    """

    def __init__(self, db_path: Path | str) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path), timeout=120.0, check_same_thread=False)
        self._lock = threading.RLock()
        self._sha_locks_guard = threading.Lock()
        self._sha_locks: dict[str, threading.RLock] = {}
        self.hits = 0
        self.misses = 0
        self.puts = 0
        self.inflight_waits = 0
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=120000")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def _lock_for_sha(self, sha256: str) -> threading.RLock:
        with self._sha_locks_guard:
            lock = self._sha_locks.get(sha256)
            if lock is None:
                lock = threading.RLock()
                self._sha_locks[sha256] = lock
            return lock

    def _claim_inflight_row(self, sha256: str) -> None:
        now = _utc_now_iso()
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO inflight (sha256, started_at) VALUES (?, ?)",
                (sha256, now),
            )
            self._conn.commit()

    def _release_inflight_row(self, sha256: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM inflight WHERE sha256 = ?", (sha256,))
            self._conn.commit()

    @contextmanager
    def inflight(self, sha256: str) -> Iterator[CachedExtract | None]:
        """Hold the per-SHA lock. Yields a cache hit if one appeared while waiting.

        Callers must treat a yielded ``CachedExtract`` as done — do not call
        hosted OCR. On a miss, do the work and ``put`` before exiting.
        """

        key = (sha256 or "").strip().lower()
        if not is_sha256(key):
            yield None
            return
        lock = self._lock_for_sha(key)
        waited = not lock.acquire(blocking=False)
        if waited:
            self.inflight_waits += 1
            lock.acquire()
        try:
            hit = self.get(key)
            if hit is not None:
                yield hit
                return
            self._claim_inflight_row(key)
            try:
                yield None
            finally:
                self._release_inflight_row(key)
        finally:
            lock.release()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> ExtractCache:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def get(self, sha256: str) -> CachedExtract | None:
        key = (sha256 or "").strip().lower()
        if not is_sha256(key):
            self.misses += 1
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT sha256, markdown, text_source, status, created_at FROM extracts WHERE sha256 = ?",
                (key,),
            ).fetchone()
        if not row or not str(row[1] or "").strip():
            self.misses += 1
            return None
        self.hits += 1
        return CachedExtract(
            sha256=str(row[0]),
            markdown=str(row[1]),
            text_source=str(row[2] or ""),
            status=str(row[3] or _REUSABLE_STATUS),
            created_at=str(row[4] or ""),
        )

    def put(
        self,
        sha256: str,
        markdown: str,
        text_source: str,
        status: str = _REUSABLE_STATUS,
    ) -> bool:
        key = (sha256 or "").strip().lower()
        text = (markdown or "").strip()
        source = (text_source or "").strip()
        st = (status or "").strip() or _REUSABLE_STATUS
        if not is_sha256(key) or not text or st != _REUSABLE_STATUS:
            return False
        if source and source not in _REUSABLE_SOURCES:
            return False
        now = _utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO extracts (sha256, markdown, text_source, status, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(sha256) DO UPDATE SET
                    markdown = excluded.markdown,
                    text_source = excluded.text_source,
                    status = excluded.status,
                    created_at = excluded.created_at
                """,
                (key, text, source or "anydoc", st, now),
            )
            self._conn.commit()
        self.puts += 1
        return True

    def put_if_absent(
        self,
        sha256: str,
        markdown: str,
        text_source: str,
        status: str = _REUSABLE_STATUS,
    ) -> bool:
        key = (sha256 or "").strip().lower()
        if not is_sha256(key):
            return False
        with self._lock:
            row = self._conn.execute("SELECT 1 FROM extracts WHERE sha256 = ?", (key,)).fetchone()
        if row:
            return False
        return self.put(sha256, markdown, text_source, status)

    def stats(self) -> dict[str, int]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "puts": self.puts,
            "inflight_waits": self.inflight_waits,
        }


_CACHE: ExtractCache | None = None
_CACHE_PATH: Path | None = None
_CACHE_LOCK = threading.Lock()


def get_extract_cache() -> ExtractCache:
    global _CACHE, _CACHE_PATH
    path = default_extract_cache_path()
    with _CACHE_LOCK:
        if _CACHE is not None and _CACHE_PATH == path:
            return _CACHE
        if _CACHE is not None:
            _CACHE.close()
        _CACHE = ExtractCache(path)
        _CACHE_PATH = path
        log.debug("extract-cache open path=%s", path)
        return _CACHE


def reset_extract_cache_for_tests() -> None:
    """Close the process singleton so tests can point at a temp file."""

    global _CACHE, _CACHE_PATH
    with _CACHE_LOCK:
        if _CACHE is not None:
            _CACHE.close()
        _CACHE = None
        _CACHE_PATH = None


def _card_shas(fm: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for key in ("content_sha", "extracted_text_sha"):
        raw = str(fm.get(key) or "").strip().lower()
        if raw in seen or not is_sha256(raw):
            continue
        seen.add(raw)
        out.append(raw)
    return out


def seed_card(cache: ExtractCache, fm: dict[str, Any], body: str) -> int:
    """Store one vault card's extract under content_sha / extracted_text_sha."""

    status = str(fm.get("extraction_status") or "").strip()
    source = str(fm.get("text_source") or "").strip()
    text = (body or "").strip()
    if status != _REUSABLE_STATUS or not text:
        return 0
    if source and source not in _REUSABLE_SOURCES:
        return 0
    n = 0
    for sha in _card_shas(fm):
        if cache.put_if_absent(sha, text, source or "anydoc", status):
            n += 1
    return n


def seed_from_scan_cache(scan_cache: Any, *, types: Iterable[str] = _SEED_TYPES) -> int:
    """Copy existing vault extracts into the machine cache (one SQL body read each)."""

    cache = get_extract_cache()
    seeded = 0
    by_type = scan_cache.rel_paths_by_type()
    for card_type in types:
        for rel_path in by_type.get(card_type) or []:
            fm = scan_cache.frontmatter_for_rel_path(rel_path) or {}
            if str(fm.get("extraction_status") or "").strip() != _REUSABLE_STATUS:
                continue
            if not _card_shas(fm):
                continue
            try:
                body = scan_cache.body_for_rel_path(rel_path)
            except (OSError, ValueError):
                continue
            seeded += seed_card(cache, fm, body)
    if seeded:
        log.info("extract-cache seeded rows=%s path=%s", seeded, cache._path)
    return seeded
