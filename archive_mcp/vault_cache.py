"""SQLite-backed vault scan cache — avoid re-reading every note on large vaults."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import sqlite3
import time
import zlib
from pathlib import Path
from typing import Any, Iterator

from hfa.schema import validate_card_permissive
from hfa.vault import extract_wikilinks, iter_note_paths, read_note_file, read_note_frontmatter_file

logger = logging.getLogger("ppa.vault_cache")

CACHE_VERSION = 1
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
        rel_paths = [p.as_posix() for p in iter_note_paths(vault)]
        stats, fp = _compute_vault_fingerprint(vault, rel_paths)
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
        if cache_path.exists():
            try:
                conn = sqlite3.connect(str(cache_path), timeout=60.0)
                conn.row_factory = sqlite3.Row
                stored_fp = _meta_get(conn, "vault_fingerprint")
                stored_tier = _meta_get(conn, "tier")
                stored_ver = _meta_get(conn, "cache_version")
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
                if stored_fp == fp and stored_ver == str(CACHE_VERSION) and stored_tier is not None:
                    if int(stored_tier) < tier:
                        miss_reason = "tier_upgrade"
                conn.close()
            except sqlite3.OperationalError as exc:
                logger.warning("vault-cache miss reason=open_failed err=%s", exc)
                miss_reason = "open_failed"
        else:
            miss_reason = "file_not_found"

        logger.info(
            "vault-cache miss reason=%s building tier=%d notes=%d",
            miss_reason,
            tier,
            len(rel_paths),
        )
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(cache_path), timeout=120.0)
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
            conn = sqlite3.connect(":memory:")
        else:
            conn = sqlite3.connect(str(persist_path), timeout=120.0)
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
    ) -> VaultScanCache:
        conn.execute("DELETE FROM notes")
        conn.execute("DELETE FROM cache_meta")
        n_total = len(rel_paths)
        t_build = time.monotonic()
        batch: list[tuple[Any, ...]] = []
        inserted = 0

        def flush_batch() -> None:
            nonlocal inserted
            if not batch:
                return
            conn.executemany(
                """
                INSERT INTO notes (
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

        for i, rel_path in enumerate(rel_paths, start=1):
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

        _meta_set(conn, "vault_fingerprint", fp)
        _meta_set(conn, "tier", str(tier))
        _meta_set(conn, "cache_version", str(CACHE_VERSION))
        _meta_set(conn, "generated_at", str(time.time()))
        _meta_set(conn, "note_count", str(inserted))
        conn.commit()

        build_elapsed = time.monotonic() - t_build
        logger.info(
            "vault-cache build_complete tier=%d notes=%d elapsed=%s",
            tier,
            inserted,
            _format_mins_secs(build_elapsed),
        )
        return cls(conn, tier, fp, cache_hit=cache_hit)

    def note_count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM notes").fetchone()
        return int(row[0]) if row else 0

    def tier(self) -> int:
        return self._tier

    def vault_fingerprint(self) -> str:
        return self._vault_fingerprint

    def uid_to_rel_path(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for uid, rp in self._conn.execute(
            "SELECT uid, rel_path FROM notes WHERE uid != ''"
        ).fetchall():
            out[str(uid)] = str(rp)
        return out

    def rel_path_to_uid(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for rp, uid in self._conn.execute(
            "SELECT rel_path, uid FROM notes WHERE uid != ''"
        ).fetchall():
            out[str(rp)] = str(uid)
        return out

    def rel_paths_by_type(self) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for ctype, rp in self._conn.execute(
            "SELECT card_type, rel_path FROM notes WHERE card_type != '' ORDER BY card_type, rel_path"
        ).fetchall():
            ct = str(ctype)
            out.setdefault(ct, []).append(str(rp))
        return out

    def frontmatter_for_rel_path(self, rel_path: str) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT frontmatter_json FROM notes WHERE rel_path = ?", (rel_path,)
        ).fetchone()
        if not row:
            raise KeyError(rel_path)
        return json.loads(row[0])

    def all_frontmatters(self) -> Iterator[tuple[str, dict[str, Any]]]:
        for rp, fj in self._conn.execute("SELECT rel_path, frontmatter_json FROM notes ORDER BY rel_path"):
            yield str(rp), json.loads(fj)

    def all_rel_paths(self) -> list[str]:
        return [str(r[0]) for r in self._conn.execute("SELECT rel_path FROM notes ORDER BY rel_path").fetchall()]

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

        cursor = self._conn.execute(
            "SELECT uid, rel_path, card_type, frontmatter_json FROM notes WHERE uid != ''"
        )
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
        row = self._conn.execute(
            "SELECT body_compressed FROM notes WHERE rel_path = ?", (rel_path,)
        ).fetchone()
        if not row or row[0] is None:
            raise ValueError("tier 2 required for body access")
        return zlib.decompress(row[0]).decode("utf-8")

    def wikilinks_for_rel_path(self, rel_path: str) -> list[str]:
        row = self._conn.execute(
            "SELECT wikilinks_json FROM notes WHERE rel_path = ?", (rel_path,)
        ).fetchone()
        if not row or row[0] is None:
            raise ValueError("tier 2 required for wikilinks access")
        return json.loads(row[0])

    def content_hash_for_rel_path(self, rel_path: str) -> str:
        row = self._conn.execute(
            "SELECT content_hash FROM notes WHERE rel_path = ?", (rel_path,)
        ).fetchone()
        if not row or row[0] is None:
            raise ValueError("tier 2 required for content_hash access")
        return str(row[0])

    def raw_content_sha256_for_rel_path(self, rel_path: str) -> str:
        """SHA-256 hex of full UTF-8 file bytes (matches seed_links catalog sketch hash)."""
        row = self._conn.execute(
            "SELECT raw_content_sha256 FROM notes WHERE rel_path = ?", (rel_path,)
        ).fetchone()
        if not row or row[0] is None:
            raise ValueError("tier 2 required for raw_content_sha256 access")
        return str(row[0])

    def all_wikilinks(self) -> Iterator[tuple[str, list[str]]]:
        for rp, wj in self._conn.execute(
            "SELECT rel_path, wikilinks_json FROM notes WHERE wikilinks_json IS NOT NULL ORDER BY rel_path"
        ):
            yield str(rp), json.loads(wj)

    def all_bodies(self) -> Iterator[tuple[str, str]]:
        for rp, blob in self._conn.execute(
            "SELECT rel_path, body_compressed FROM notes WHERE body_compressed IS NOT NULL ORDER BY rel_path"
        ):
            yield str(rp), zlib.decompress(blob).decode("utf-8")

    def file_stats(self) -> dict[str, tuple[int, int]]:
        out: dict[str, tuple[int, int]] = {}
        for rp, mtime_ns, fsize in self._conn.execute(
            "SELECT rel_path, mtime_ns, file_size FROM notes"
        ):
            out[str(rp)] = (int(mtime_ns), int(fsize))
        return out

    def all_stems(self) -> set[str]:
        return {Path(str(r[0])).stem for r in self._conn.execute("SELECT rel_path FROM notes")}

    def close(self) -> None:
        self._conn.close()
