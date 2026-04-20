"""SQLite-backed vault scan cache — avoid re-reading every note on large vaults."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import sqlite3
import threading
import time
import warnings
import zlib
from pathlib import Path
from typing import Any, Iterator

from archive_vault.schema import validate_card_permissive
from archive_vault.vault import extract_wikilinks, iter_note_paths, read_note_file, read_note_frontmatter_file

logger = logging.getLogger("ppa.vault_cache")

# ---------------------------------------------------------------------------
# Cache version — auto-derived from source files that determine row shape.
#
# Any change to parsing, hashing, or field-extraction logic triggers a full
# cache rebuild on next load, preventing stale rows from surviving incremental
# updates.  The set of files below covers both the Python and Rust code paths.
# ---------------------------------------------------------------------------

_CACHE_CODE_FINGERPRINT_FILES = (
    # Python: parsing, hashing, row population
    "archive_cli/vault_cache.py",
    # Python: card model, permissive validation
    "archive_vault/schema.py",
    # Python: note reading, wikilink extraction
    "archive_vault/vault.py",
    # Rust: row build (tier1/tier2), provenance, wikilinks
    "archive_crate/src/cache_build.rs",
    # Rust: JSON serialization, frontmatter hash, content hash
    "archive_crate/src/json_stable.rs",
    # Rust: card field extraction (uid, type, people, orgs, etc.)
    "archive_crate/src/materializer/card_fields.rs",
    "archive_crate/src/materializer/card_field_keys.rs",
    # Rust: raw content hash
    "archive_crate/src/hasher.rs",
)


def _compute_cache_code_version() -> str:
    """SHA-256 fingerprint of source files that determine cache row shape.

    Returns the first 16 hex chars — short enough for readable meta values,
    long enough to be collision-free in practice.
    """
    repo_root = Path(__file__).resolve().parent.parent
    h = hashlib.sha256()
    for rel in _CACHE_CODE_FINGERPRINT_FILES:
        p = repo_root / rel
        try:
            h.update(p.read_bytes())
        except FileNotFoundError:
            h.update(rel.encode())
    return h.hexdigest()[:16]


CACHE_VERSION = _compute_cache_code_version()
CACHE_FILENAME = "vault-scan-cache.sqlite3"
ZLIB_LEVEL = 6
BATCH_COMMIT_EVERY = 50_000


def _format_mins_secs(seconds: float) -> str:
    if not math.isfinite(seconds) or seconds < 0:
        return "?"
    total = int(round(seconds))
    m, s = divmod(total, 60)
    return f"{m}:{s:02d}"


def _frontmatter_hash_stable(frontmatter: dict[str, Any]) -> str:
    sanitized = json.loads(json.dumps(frontmatter, sort_keys=True, default=str).replace("\\u0000", ""))
    payload = json.dumps(sanitized, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _content_hash(frontmatter: dict[str, Any], body: str) -> str:
    sanitized_frontmatter = json.loads(json.dumps(frontmatter, sort_keys=True, default=str).replace("\\u0000", ""))
    payload = json.dumps(sanitized_frontmatter, sort_keys=True, default=str) + "\n" + body.replace("\x00", "")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _compute_vault_fingerprint(
    vault: Path, rel_paths: list[str]
) -> tuple[dict[str, tuple[int, int]], str]:
    """Walk-only fingerprint: stat each file, hash sorted path+mtime+size (matches scanner._vault_paths_and_fingerprint)."""
    lines: list[str] = []
    stats: dict[str, tuple[int, int]] = {}
    for rel_path in sorted(rel_paths):
        target = vault / rel_path
        try:
            st = target.stat()
        except OSError:
            continue
        mtime_ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))
        size = int(st.st_size)
        stats[rel_path] = (mtime_ns, size)
        lines.append(f"{rel_path}\t{mtime_ns}\t{size}")
    fingerprint = hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()
    return stats, fingerprint


def _compute_fingerprint_with_paths(
    vault: Path,
) -> tuple[list[str], dict[str, tuple[int, int]], str]:
    """Walk + stat + fingerprint via Rust (rayon-parallelised) when available, else Python fallback."""
    from archive_cli.ppa_engine import ppa_engine

    if ppa_engine() == "rust":
        try:
            import archive_crate

            rel_paths_py, stats_dict, fp = archive_crate.vault_fingerprint_with_paths(str(vault))
            stats: dict[str, tuple[int, int]] = {}
            for k, v in stats_dict.items():
                stats[k] = (int(v[0]), int(v[1]))
            return list(rel_paths_py), stats, fp
        except ImportError:
            pass
        except Exception as exc:
            warnings.warn(
                f"PPA: falling back to Python for fingerprint — archive_crate error: {exc}",
                stacklevel=2,
            )

    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]
    stats_py, fp = _compute_vault_fingerprint(vault, rel_paths)
    return rel_paths, stats_py, fp


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cache_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notes (
            rel_path TEXT PRIMARY KEY,
            uid TEXT NOT NULL DEFAULT '',
            card_type TEXT NOT NULL DEFAULT '',
            slug TEXT NOT NULL DEFAULT '',
            mtime_ns INTEGER NOT NULL DEFAULT 0,
            file_size INTEGER NOT NULL DEFAULT 0,
            frontmatter_json TEXT NOT NULL DEFAULT '{}',
            frontmatter_hash TEXT NOT NULL DEFAULT '',
            body_compressed BLOB,
            content_hash TEXT,
            wikilinks_json TEXT,
            raw_content_sha256 TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notes_uid ON notes(uid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notes_card_type ON notes(card_type)")
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(notes)")}
    if "raw_content_sha256" not in cols:
        conn.execute("ALTER TABLE notes ADD COLUMN raw_content_sha256 TEXT")
    conn.commit()


def _meta_get(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM cache_meta WHERE key = ?", (key,)).fetchone()
    return str(row[0]) if row else None


def _meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO cache_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def _infer_tier_from_notes(conn: sqlite3.Connection) -> int:
    """Return 2 if tier-2 rows exist (body indexed), else 1."""

    row = conn.execute(
        "SELECT COUNT(*) FROM notes WHERE body_compressed IS NOT NULL"
    ).fetchone()
    if row and int(row[0]) > 0:
        return 2
    return 1


def _repair_cache_meta_if_needed(
    conn: sqlite3.Connection,
    *,
    stored_fp: str | None,
    fp: str,
    stored_ver: str | None,
    stored_tier: str | None,
) -> tuple[str | None, str | None]:
    """Backfill ``cache_version`` / ``tier`` when fingerprint matches but meta is incomplete.

    Some caches only had ``vault_fingerprint`` stamped (e.g. fingerprint refresh) or older
    builders omitted keys — without these, :meth:`VaultScanCache.build_or_load` would miss
    despite a multi-GB valid ``notes`` table.
    """

    if stored_fp != fp:
        return stored_ver, stored_tier
    n_notes = conn.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
    if int(n_notes) == 0:
        return stored_ver, stored_tier
    needs = (
        stored_ver is None
        or stored_tier is None
        or stored_ver != str(CACHE_VERSION)
    )
    if not needs:
        return stored_ver, stored_tier
    inferred = _infer_tier_from_notes(conn)
    _meta_set(conn, "cache_version", str(CACHE_VERSION))
    _meta_set(conn, "tier", str(inferred))
    conn.commit()
    logger.info(
        "vault-cache repaired cache_meta (fingerprint already matched; inferred tier=%s)",
        inferred,
    )
    return str(CACHE_VERSION), str(inferred)


def _try_build_vault_cache_disk_with_rust(
    vault: Path,
    cache_path: Path,
    tier: int,
    expected_fp: str,
    expected_note_count: int,
    *,
    incremental: bool = False,
) -> bool:
    """If ``PPA_ENGINE=rust`` and ``archive_crate`` is available, build the on-disk cache via Rust.

    When ``incremental=True`` and the cache file already exists, uses
    ``build_vault_cache_incremental`` to only re-parse notes whose ``mtime_ns`` or
    ``file_size`` changed on disk. Falls back to full build when incremental is unavailable.

    Returns True when the SQLite file at ``cache_path`` was produced by Rust and matches
    ``expected_fp``. On failure, attempts to remove ``cache_path`` and returns False so callers
    can fall back to Python ``_populate_db``.
    """

    from archive_cli.ppa_engine import use_rust_vault_cache_disk_build

    if not use_rust_vault_cache_disk_build():
        return False
    try:
        import archive_crate
    except ImportError as e:
        warnings.warn(
            f"PPA: falling back to Python for vault cache disk build — archive_crate not available: {e}",
            stacklevel=2,
        )
        logger.info("vault-cache PPA_ENGINE=rust but archive_crate is not installed; using Python fill")
        return False

    use_incremental = incremental and cache_path.exists() and hasattr(archive_crate, "build_vault_cache_incremental")

    if not use_incremental:
        # Full build: verify fingerprint parity first
        try:
            _, fp_r = archive_crate.vault_fingerprint(str(vault))
            if fp_r != expected_fp:
                logger.warning(
                    "vault-cache rust fingerprint mismatch (walk parity); using Python fill fp_py=%s fp_rust=%s",
                    expected_fp,
                    fp_r,
                )
                return False
        except Exception as exc:
            logger.warning("vault-cache rust fingerprint check failed: %s", exc)
            return False

    try:
        if use_incremental:
            out = archive_crate.build_vault_cache_incremental(str(vault), str(cache_path), tier, CACHE_VERSION)
            rebuilt = int(out.get("rebuilt", 0))
            skipped = int(out.get("skipped", 0))
            deleted = int(out.get("deleted", 0))
            unchanged = int(out.get("unchanged", 0))
            n = int(out.get("note_count", 0))
            if out.get("fingerprint") != expected_fp:
                logger.warning(
                    "vault-cache rust incremental fingerprint mismatch; falling back to full build"
                )
                # Fall through to full build below
                use_incremental = False
            else:
                if skipped > 0:
                    logger.info(
                        "vault-cache rust incremental skipped %d notes with parse errors", skipped
                    )
                logger.info(
                    "vault-cache incremental build via rust tier=%d rebuilt=%d deleted=%d unchanged=%d total=%d",
                    tier, rebuilt, deleted, unchanged, n,
                )
                return True

        if not use_incremental:
            for suffix in ("", "-wal", "-shm"):
                p = Path(str(cache_path) + suffix)
                if p.exists():
                    p.unlink()
            out = archive_crate.build_vault_cache(str(vault), str(cache_path), tier, CACHE_VERSION)
            if out.get("fingerprint") != expected_fp:
                logger.warning("vault-cache rust build fingerprint mismatch; using Python fill")
                try:
                    cache_path.unlink(missing_ok=True)
                except OSError:
                    pass
                return False
            n = int(out.get("note_count", 0))
            skipped = int(out.get("skipped", 0))
            if n + skipped != expected_note_count:
                logger.warning(
                    "vault-cache rust note_count mismatch (expected=%d got=%d skipped=%d); using Python fill",
                    expected_note_count,
                    n,
                    skipped,
                )
                try:
                    cache_path.unlink(missing_ok=True)
                except OSError:
                    pass
                return False
            if skipped > 0:
                logger.info(
                    "vault-cache rust skipped %d notes with parse errors (inserted=%d)", skipped, n
                )
            logger.info("vault-cache disk build used rust engine tier=%d notes=%d", tier, n)
            return True
    except Exception as exc:
        logger.warning("vault-cache rust build failed: %s", exc)
        try:
            if not use_incremental:
                cache_path.unlink(missing_ok=True)
        except OSError:
            pass
        return False
    return False


class VaultScanCache:
    """SQLite-backed vault scan cache (tier 1: frontmatter; tier 2: + body, wikilinks, content_hash)."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        tier: int,
        vault_fingerprint: str,
        *,
        cache_hit: bool = False,
    ):
        self._conn = conn
        self._tier = tier
        self._vault_fingerprint = vault_fingerprint
        self._cache_hit = cache_hit
        # sqlite3 connections are not thread-safe; parallel enrich-cards workers must serialize reads.
        self._lock = threading.RLock()

    @property
    def is_cache_hit(self) -> bool:
        """True when rows were loaded from an on-disk cache without rebuilding."""
        return self._cache_hit

    @staticmethod
    def cache_path_for_vault(vault: Path) -> Path:
        return vault / "_meta" / CACHE_FILENAME

    @classmethod
    def build_or_load(
        cls,
        vault: Path,
        *,
        tier: int = 1,
        workers: int = 1,
        progress_every: int = 5000,
        no_cache: bool = False,
    ) -> VaultScanCache:
        _ = workers  # reserved for future parallel reads
        vault = Path(vault).resolve()
        t0 = time.monotonic()
        rel_paths, stats, fp = _compute_fingerprint_with_paths(vault)
        fp_elapsed = time.monotonic() - t0
        logger.info("vault-cache fingerprint_check vault=%s elapsed=%.2fs", vault, fp_elapsed)

        if no_cache:
            logger.info("vault-cache miss reason=no_cache building tier=%d notes=%d", tier, len(rel_paths))
            return cls._build_fresh(
                vault,
                rel_paths,
                stats,
                fp,
                tier,
                persist_path=None,
                progress_every=progress_every,
                cache_hit=False,
            )

        cache_path = cls.cache_path_for_vault(vault)
        miss_reason = "fingerprint_changed"
        prior_stored_fp: str | None = None
        if cache_path.exists():
            try:
                conn = sqlite3.connect(str(cache_path), timeout=60.0, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                stored_fp = _meta_get(conn, "vault_fingerprint")
                prior_stored_fp = stored_fp
                stored_tier = _meta_get(conn, "tier")
                stored_ver = _meta_get(conn, "cache_version")
                stored_ver, stored_tier = _repair_cache_meta_if_needed(
                    conn,
                    stored_fp=stored_fp,
                    fp=fp,
                    stored_ver=stored_ver,
                    stored_tier=stored_tier,
                )
                if (
                    stored_fp == fp
                    and stored_ver == str(CACHE_VERSION)
                    and stored_tier is not None
                    and int(stored_tier) >= tier
                ):
                    note_count = conn.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
                    load_elapsed = time.monotonic() - t0 - fp_elapsed
                    logger.info(
                        "vault-cache hit tier=%s notes=%d elapsed=%.2fs",
                        stored_tier,
                        note_count,
                        load_elapsed,
                    )
                    _init_db(conn)
                    return cls(conn, int(stored_tier), fp, cache_hit=True)
                if stored_ver != str(CACHE_VERSION):
                    miss_reason = "version_changed"
                elif stored_fp == fp and stored_tier is not None and int(stored_tier) < tier:
                    miss_reason = "tier_upgrade"
                conn.close()
            except sqlite3.OperationalError as exc:
                logger.warning("vault-cache miss reason=open_failed err=%s", exc)
                miss_reason = "open_failed"
        else:
            miss_reason = "file_not_found"

        if (
            prior_stored_fp
            and prior_stored_fp != fp
            and miss_reason == "fingerprint_changed"
        ):
            logger.info(
                "vault-cache fingerprint differ: stored=%s… computed=%s… "
                "(note mtimes/sizes changed since cache was built — e.g. vault writes)",
                prior_stored_fp[:16],
                fp[:16],
            )

        use_incremental = miss_reason == "fingerprint_changed" and cache_path.exists()
        logger.info(
            "vault-cache miss reason=%s building tier=%d notes=%d incremental=%s",
            miss_reason,
            tier,
            len(rel_paths),
            use_incremental,
        )
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            note_count_expected = len(rel_paths)
            if _try_build_vault_cache_disk_with_rust(
                vault, cache_path, tier, fp, note_count_expected,
                incremental=use_incremental,
            ):
                conn = sqlite3.connect(str(cache_path), timeout=120.0, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                _init_db(conn)
                result = cls(conn, tier, fp, cache_hit=False)
            else:
                conn = sqlite3.connect(str(cache_path), timeout=120.0, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                _init_db(conn)
                result = cls._populate_db(
                    conn,
                    vault,
                    rel_paths,
                    stats,
                    fp,
                    tier,
                    progress_every=progress_every,
                    cache_hit=False,
                    incremental=use_incremental,
                )
                conn.commit()
            try:
                sz = cache_path.stat().st_size / (1024 * 1024)
                logger.info(
                    "vault-cache build_complete tier=%d notes=%d file_size_mb=%.1f",
                    tier,
                    result.note_count(),
                    sz,
                )
            except OSError:
                logger.info("vault-cache build_complete tier=%d notes=%d", tier, result.note_count())
            return result
        except (OSError, PermissionError, sqlite3.OperationalError) as exc:
            logger.warning(
                "vault-cache WARNING cache_write_failed reason=%s falling_back_to_in_memory",
                exc,
            )
            return cls._build_fresh(
                vault,
                rel_paths,
                stats,
                fp,
                tier,
                persist_path=None,
                progress_every=progress_every,
                cache_hit=False,
            )

    @classmethod
    def _build_fresh(
        cls,
        vault: Path,
        rel_paths: list[str],
        stats: dict[str, tuple[int, int]],
        fp: str,
        tier: int,
        *,
        persist_path: Path | None,
        progress_every: int,
        cache_hit: bool = False,
    ) -> VaultScanCache:
        if persist_path is None:
            conn = sqlite3.connect(":memory:", check_same_thread=False)
        else:
            conn = sqlite3.connect(str(persist_path), timeout=120.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        _init_db(conn)
        return cls._populate_db(
            conn,
            vault,
            rel_paths,
            stats,
            fp,
            tier,
            progress_every=progress_every,
            cache_hit=cache_hit,
        )

    @classmethod
    def _populate_db(
        cls,
        conn: sqlite3.Connection,
        vault: Path,
        rel_paths: list[str],
        stats: dict[str, tuple[int, int]],
        fp: str,
        tier: int,
        *,
        progress_every: int,
        cache_hit: bool = False,
        incremental: bool = False,
    ) -> VaultScanCache:
        t_build = time.monotonic()

        if incremental:
            cached_stats: dict[str, tuple[int, int]] = {}
            try:
                for row in conn.execute("SELECT rel_path, mtime_ns, file_size FROM notes"):
                    cached_stats[str(row[0])] = (int(row[1]), int(row[2]))
            except Exception:
                cached_stats = {}

            if cached_stats:
                disk_paths = set(stats.keys())
                cached_paths = set(cached_stats.keys())
                deleted_paths = cached_paths - disk_paths
                paths_to_build: list[str] = []
                unchanged_count = 0
                for rp in rel_paths:
                    cached = cached_stats.get(rp)
                    if cached is not None:
                        disk_mt, disk_sz = stats.get(rp, (0, 0))
                        if disk_mt == cached[0] and disk_sz == cached[1]:
                            unchanged_count += 1
                            continue
                    paths_to_build.append(rp)

                if deleted_paths:
                    for chunk_start in range(0, len(deleted_paths), 500):
                        chunk = list(deleted_paths)[chunk_start:chunk_start + 500]
                        placeholders = ",".join("?" for _ in chunk)
                        conn.execute(
                            f"DELETE FROM notes WHERE rel_path IN ({placeholders})",
                            chunk,
                        )
                    conn.commit()

                logger.info(
                    "vault-cache incremental delta: rebuild=%d deleted=%d unchanged=%d",
                    len(paths_to_build),
                    len(deleted_paths),
                    unchanged_count,
                )
            else:
                paths_to_build = rel_paths
                unchanged_count = 0
        else:
            conn.execute("DELETE FROM notes")
            paths_to_build = rel_paths
            unchanged_count = 0

        conn.execute("DELETE FROM cache_meta")

        n_total = len(paths_to_build)
        batch: list[tuple[Any, ...]] = []
        inserted = 0

        def flush_batch() -> None:
            nonlocal inserted
            if not batch:
                return
            conn.executemany(
                """
                INSERT OR REPLACE INTO notes (
                    rel_path, uid, card_type, slug, mtime_ns, file_size,
                    frontmatter_json, frontmatter_hash, body_compressed, content_hash, wikilinks_json,
                    raw_content_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            inserted += len(batch)
            batch.clear()
            if inserted % BATCH_COMMIT_EVERY == 0:
                conn.commit()

        for i, rel_path in enumerate(paths_to_build, start=1):
            if progress_every > 0 and i % progress_every == 0:
                elapsed = time.monotonic() - t_build
                rate = i / elapsed if elapsed > 0 else 0.0
                remaining = n_total - i
                eta_sec = remaining / rate if rate > 0 else float("nan")
                pct = 100.0 * i / n_total if n_total else 100.0
                logger.info(
                    "vault-cache build notes_read=%d/%d (%.1f%%) elapsed=%s eta_remaining=%s rate_notes_per_s=%.0f",
                    i,
                    n_total,
                    pct,
                    _format_mins_secs(elapsed),
                    _format_mins_secs(eta_sec),
                    rate,
                )

            mtime_ns, file_size = stats.get(rel_path, (0, 0))
            if tier >= 2:
                note = read_note_file(vault / rel_path, vault_root=vault)
                fm = note.frontmatter
                card = validate_card_permissive(fm)
                uid = str(card.uid).strip()
                ctype = str(card.type or "")
                slug = Path(rel_path).stem
                fm_json = json.dumps(fm, sort_keys=True, default=str)
                fm_hash = _frontmatter_hash_stable(fm)
                body_b = zlib.compress(note.body.encode("utf-8"), level=ZLIB_LEVEL)
                ch = _content_hash(fm, note.body)
                wikis = json.dumps(list(extract_wikilinks(note.body)))
                raw_hex = hashlib.sha256(note.content.encode("utf-8")).hexdigest()
                batch.append(
                    (rel_path, uid, ctype, slug, mtime_ns, file_size, fm_json, fm_hash, body_b, ch, wikis, raw_hex)
                )
            else:
                note = read_note_frontmatter_file(vault / rel_path, vault_root=vault)
                fm = note.frontmatter
                card = validate_card_permissive(fm)
                uid = str(card.uid).strip()
                ctype = str(card.type or "")
                slug = Path(rel_path).stem
                fm_json = json.dumps(fm, sort_keys=True, default=str)
                fm_hash = _frontmatter_hash_stable(fm)
                batch.append(
                    (rel_path, uid, ctype, slug, mtime_ns, file_size, fm_json, fm_hash, None, None, None, None)
                )

            if len(batch) >= 1000:
                flush_batch()

        flush_batch()
        conn.commit()

        total_notes = unchanged_count + inserted
        _meta_set(conn, "vault_fingerprint", fp)
        _meta_set(conn, "tier", str(tier))
        _meta_set(conn, "cache_version", str(CACHE_VERSION))
        _meta_set(conn, "generated_at", str(time.time()))
        _meta_set(conn, "note_count", str(total_notes))
        conn.commit()

        build_elapsed = time.monotonic() - t_build
        if incremental and unchanged_count > 0:
            logger.info(
                "vault-cache incremental_build_complete tier=%d rebuilt=%d unchanged=%d total=%d elapsed=%s",
                tier, inserted, unchanged_count, total_notes, _format_mins_secs(build_elapsed),
            )
        else:
            logger.info(
                "vault-cache build_complete tier=%d notes=%d elapsed=%s",
                tier, total_notes, _format_mins_secs(build_elapsed),
            )
        return cls(conn, tier, fp, cache_hit=cache_hit)

    def note_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) FROM notes").fetchone()
        return int(row[0]) if row else 0

    def tier(self) -> int:
        return self._tier

    def vault_fingerprint(self) -> str:
        return self._vault_fingerprint

    def uid_to_rel_path(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT uid, rel_path FROM notes WHERE uid != ''"
            ).fetchall()
        out: dict[str, str] = {}
        for uid, rp in rows:
            out[str(uid)] = str(rp)
        return out

    def rel_path_to_uid(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT rel_path, uid FROM notes WHERE uid != ''"
            ).fetchall()
        out: dict[str, str] = {}
        for rp, uid in rows:
            out[str(rp)] = str(uid)
        return out

    def rel_path_for_slug(self, slug: str) -> str | None:
        """Resolve a note filename stem (Obsidian wikilink target) to ``rel_path``."""

        s = (slug or "").strip()
        if not s:
            return None
        variants = [s, s.replace(" ", "-"), s.replace(" ", "_")]
        seen: set[str] = set()
        with self._lock:
            for v in variants:
                if not v or v in seen:
                    continue
                seen.add(v)
                row = self._conn.execute(
                    "SELECT rel_path FROM notes WHERE slug = ? LIMIT 1",
                    (v,),
                ).fetchone()
                if row:
                    return str(row[0])
        return None

    def rel_paths_by_type(self) -> dict[str, list[str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT card_type, rel_path FROM notes WHERE card_type != '' ORDER BY card_type, rel_path"
            ).fetchall()
        out: dict[str, list[str]] = {}
        for ctype, rp in rows:
            ct = str(ctype)
            out.setdefault(ct, []).append(str(rp))
        return out

    def frontmatter_for_rel_path(self, rel_path: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT frontmatter_json FROM notes WHERE rel_path = ?", (rel_path,)
            ).fetchone()
        if not row:
            raise KeyError(rel_path)
        return json.loads(row[0])

    def all_frontmatters(self) -> Iterator[tuple[str, dict[str, Any]]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT rel_path, frontmatter_json FROM notes ORDER BY rel_path"
            ).fetchall()
        for rp, fj in rows:
            yield str(rp), json.loads(fj)

    def all_rel_paths(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute("SELECT rel_path FROM notes ORDER BY rel_path").fetchall()
        return [str(r[0]) for r in rows]

    def slice_lookup_tables(
        self,
        *,
        progress_every: int = 100_000,
    ) -> tuple[
        dict[str, list[str]],   # by_type: card_type → [rel_path, ...]
        dict[str, str],         # rel_by_uid: uid → rel_path
        dict[str, str],         # uid_by_path: rel_path → uid
        dict[str, str],         # uid_by_stem: stem/summary/alias → uid
        dict[str, dict[str, Any]],  # frontmatter_by_uid: uid → parsed frontmatter dict
    ]:
        """Build all lookup tables for slice-seed in a single cursor pass.

        Returns string-typed paths (not Path objects) for speed — callers
        wrap in Path() only when needed for filesystem ops.
        """
        by_type: dict[str, list[str]] = {}
        rel_by_uid: dict[str, str] = {}
        uid_by_path: dict[str, str] = {}
        uid_by_stem: dict[str, str] = {}
        frontmatter_by_uid: dict[str, dict[str, Any]] = {}

        with self._lock:
            cursor = self._conn.execute(
                "SELECT uid, rel_path, card_type, frontmatter_json FROM notes WHERE uid != ''"
            ).fetchall()
        count = 0
        t0 = time.monotonic()
        for uid_raw, rp_raw, ct_raw, fj_raw in cursor:
            uid = str(uid_raw)
            rp = str(rp_raw)
            ct = str(ct_raw)
            count += 1

            rel_by_uid[uid] = rp
            uid_by_path[rp] = uid

            if ct:
                by_type.setdefault(ct, []).append(rp)

            stem = Path(rp).stem.strip()
            if stem:
                uid_by_stem.setdefault(stem, uid)
                norm = stem.replace(" ", "-").lower()
                uid_by_stem.setdefault(norm, uid)

            fm: dict[str, Any] = {}
            if fj_raw:
                fm = json.loads(fj_raw)
                frontmatter_by_uid[uid] = fm

            summary = str(fm.get("summary", "") or "").strip()
            if summary:
                uid_by_stem.setdefault(summary, uid)
                uid_by_stem.setdefault(summary.replace(" ", "-").lower(), uid)

            if ct == "person":
                for alias in fm.get("aliases", []) or []:
                    alias_text = str(alias).strip()
                    if alias_text:
                        uid_by_stem.setdefault(alias_text, uid)
                        uid_by_stem.setdefault(alias_text.replace(" ", "-").lower(), uid)
                for email in fm.get("emails", []) or []:
                    email_text = str(email).strip()
                    if email_text:
                        uid_by_stem.setdefault(email_text, uid)

            if progress_every > 0 and count % progress_every == 0:
                elapsed = time.monotonic() - t0
                logger.info(
                    "vault-cache slice_lookup rows=%d elapsed=%.1fs rate=%.0f/s",
                    count, elapsed, count / elapsed if elapsed > 0 else 0,
                )

        elapsed = time.monotonic() - t0
        logger.info(
            "vault-cache slice_lookup_complete rows=%d uid_by_stem=%d frontmatters=%d elapsed=%.1fs",
            count, len(uid_by_stem), len(frontmatter_by_uid), elapsed,
        )
        return by_type, rel_by_uid, uid_by_path, uid_by_stem, frontmatter_by_uid

    def body_for_rel_path(self, rel_path: str) -> str:
        with self._lock:
            row = self._conn.execute(
                "SELECT body_compressed FROM notes WHERE rel_path = ?", (rel_path,)
            ).fetchone()
        if not row or row[0] is None:
            raise ValueError("tier 2 required for body access")
        return zlib.decompress(row[0]).decode("utf-8")

    def wikilinks_for_rel_path(self, rel_path: str) -> list[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT wikilinks_json FROM notes WHERE rel_path = ?", (rel_path,)
            ).fetchone()
        if not row or row[0] is None:
            raise ValueError("tier 2 required for wikilinks access")
        return json.loads(row[0])

    def content_hash_for_rel_path(self, rel_path: str) -> str:
        with self._lock:
            row = self._conn.execute(
                "SELECT content_hash FROM notes WHERE rel_path = ?", (rel_path,)
            ).fetchone()
        if not row or row[0] is None:
            raise ValueError("tier 2 required for content_hash access")
        return str(row[0])

    def raw_content_sha256_for_rel_path(self, rel_path: str) -> str:
        """SHA-256 hex of full UTF-8 file bytes (matches seed_links catalog sketch hash)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT raw_content_sha256 FROM notes WHERE rel_path = ?", (rel_path,)
            ).fetchone()
        if not row or row[0] is None:
            raise ValueError("tier 2 required for raw_content_sha256 access")
        return str(row[0])

    def all_wikilinks(self) -> Iterator[tuple[str, list[str]]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT rel_path, wikilinks_json FROM notes WHERE wikilinks_json IS NOT NULL ORDER BY rel_path"
            ).fetchall()
        for rp, wj in rows:
            yield str(rp), json.loads(wj)

    def all_bodies(self) -> Iterator[tuple[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT rel_path, body_compressed FROM notes WHERE body_compressed IS NOT NULL ORDER BY rel_path"
            ).fetchall()
        for rp, blob in rows:
            yield str(rp), zlib.decompress(blob).decode("utf-8")

    def file_stats(self) -> dict[str, tuple[int, int]]:
        with self._lock:
            rows = self._conn.execute("SELECT rel_path, mtime_ns, file_size FROM notes").fetchall()
        out: dict[str, tuple[int, int]] = {}
        for rp, mtime_ns, fsize in rows:
            out[str(rp)] = (int(mtime_ns), int(fsize))
        return out

    def all_stems(self) -> set[str]:
        with self._lock:
            rows = self._conn.execute("SELECT rel_path FROM notes").fetchall()
        return {Path(str(r[0])).stem for r in rows}

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def refresh_stored_vault_fingerprint(vault: Path | str) -> bool:
    """Update ``vault_fingerprint`` in ``vault-scan-cache.sqlite3`` to match the vault *now*.

    The scan cache rows may still reflect an older scan; callers that read updated fields via
    ``read_note()`` (as match resolution does for writes) are fine. This re-stamps the walk
    fingerprint so :meth:`VaultScanCache.build_or_load` can **hit** on the next run without
    any per-note work.

    With incremental cache rebuilds now supported, a fingerprint mismatch no longer triggers
    a full rebuild — only changed notes are re-parsed. This function remains useful as a
    cheaper alternative when callers know the stale rows are acceptable (e.g. enrichment
    steps that read from disk, not cache).

    Returns True if the cache file existed and was updated.
    """

    vault = Path(vault).resolve()
    cache_path = VaultScanCache.cache_path_for_vault(vault)
    if not cache_path.exists():
        return False
    t0 = time.monotonic()
    _, _, fp = _compute_fingerprint_with_paths(vault)
    conn = sqlite3.connect(str(cache_path), timeout=120.0)
    try:
        inferred = _infer_tier_from_notes(conn)
        _meta_set(conn, "vault_fingerprint", fp)
        _meta_set(conn, "cache_version", str(CACHE_VERSION))
        _meta_set(conn, "tier", str(inferred))
        conn.commit()
    finally:
        conn.close()
    logger.info(
        "vault-cache stored fingerprint refreshed in %.1fs (next build_or_load can hit without full rebuild)",
        time.monotonic() - t0,
    )
    return True
