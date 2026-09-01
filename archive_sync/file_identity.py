"""Source-byte identity for documents and email attachments.

Hash is SHA-256 of the **source file bytes** (not card markdown). Same hash on
two or more kept cards gets bidirectional ``duplicates: [[uid]]`` wikilinks.

Used by file-library ingest, Gmail attachment apply, extract writers, and
``link-file-duplicates``.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from archive_sync.document_extract import bytes_sha256, is_lockfile
from archive_sync.extract_cache import get_extract_cache, is_sha256
from archive_vault.provenance import ProvenanceEntry, merge_provenance
from archive_vault.schema import validate_card_strict
from archive_vault.vault import read_note, write_card

log = logging.getLogger("ppa.file_identity")

FILE_IDENTITY_TYPES = frozenset({"document", "email_attachment"})
_SCHEMA = """
CREATE TABLE IF NOT EXISTS file_cards (
    sha256 TEXT NOT NULL,
    uid TEXT NOT NULL,
    rel_path TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (sha256, uid)
);
CREATE INDEX IF NOT EXISTS file_cards_uid ON file_cards(uid);
"""


def _fmt_elapsed(seconds: float) -> str:
    total = int(round(max(0.0, seconds)))
    m, s = divmod(total, 60)
    return f"{m}:{s:02d}"


def _log_progress(prefix: str, i: int, n: int, t0: float, extra: str = "") -> None:
    elapsed = time.monotonic() - t0
    rate = i / elapsed if elapsed > 0 else 0.0
    remain = (n - i) / rate if rate > 0 else 0.0
    pct = (100.0 * i / n) if n else 100.0
    log.info(
        "%s %s/%s (%.1f%%) elapsed=%s eta_remaining=%s rate_per_s=%.2f %s",
        prefix,
        i,
        n,
        pct,
        _fmt_elapsed(elapsed),
        _fmt_elapsed(remain),
        rate,
        extra,
    )


def wikilink_uid(uid: str) -> str:
    cleaned = str(uid or "").strip().strip("[]")
    return f"[[{cleaned}]]" if cleaned else ""


def uid_from_wikilink(value: str) -> str:
    return str(value or "").strip().strip("[]")


def source_sha_from_frontmatter(fm: dict[str, Any]) -> str:
    """Reuse ``content_sha`` or extract-cache / ``extracted_text_sha`` when valid."""

    for key in ("content_sha", "extracted_text_sha"):
        raw = str(fm.get(key) or "").strip().lower()
        if is_sha256(raw):
            return raw
    return ""


def resolve_source_path(vault: Path, fm: dict[str, Any]) -> Path | None:
    """Document ROOTS path or seed ``Attachments/{uid}/`` file."""

    card_type = str(fm.get("type") or "").strip()
    if card_type == "document":
        from archive_sync.llm_enrichment.document_text_extractor import resolve_source_file

        return resolve_source_file(str(fm.get("library_root") or ""), str(fm.get("relative_path") or ""))
    if card_type == "email_attachment":
        from archive_sync.attachment_text import resolve_local_attachment

        uid = str(fm.get("uid") or "").strip()
        filename = str(fm.get("filename") or "").strip()
        if not uid or is_lockfile(filename):
            return None
        return resolve_local_attachment(vault, uid, filename)
    return None


def hash_paths_sha256(paths: list[str]) -> dict[str, str]:
    """SHA-256 source bytes. Rust+rayon when available; hashlib fallback."""

    if not paths:
        return {}
    try:
        import archive_crate

        rows = archive_crate.hash_paths_sha256(paths)
        return {str(path): str(sha) for path, sha in rows if path and sha}
    except Exception as exc:
        log.warning("file-identity rust hash_paths fallback err=%s", exc)
    out: dict[str, str] = {}
    for path in paths:
        try:
            out[path] = bytes_sha256(Path(path).read_bytes())
        except OSError:
            continue
    return out


def default_identity_db_path() -> Path:
    from archive_cli.index_config import get_file_identity_db_path

    return get_file_identity_db_path()


class FileIdentityIndex:
    """WAL SQLite: source sha256 → card UIDs. Single connection + RLock."""

    def __init__(self, db_path: Path | str | None = None) -> None:
        self._path = Path(db_path) if db_path is not None else default_identity_db_path()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path), timeout=120.0, check_same_thread=False)
        self._lock = threading.RLock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=120000")
            self._conn.executescript(_SCHEMA)
            cols = {row[1] for row in self._conn.execute("PRAGMA table_info(file_cards)").fetchall()}
            if "rel_path" not in cols:
                self._conn.execute("ALTER TABLE file_cards ADD COLUMN rel_path TEXT NOT NULL DEFAULT ''")
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def uids_for_sha(self, sha256: str) -> list[str]:
        key = (sha256 or "").strip().lower()
        if not is_sha256(key):
            return []
        with self._lock:
            rows = self._conn.execute(
                "SELECT uid FROM file_cards WHERE sha256 = ? ORDER BY uid", (key,)
            ).fetchall()
        return [str(row[0]) for row in rows if row[0]]

    def rows_for_sha(self, sha256: str) -> list[tuple[str, str]]:
        key = (sha256 or "").strip().lower()
        if not is_sha256(key):
            return []
        with self._lock:
            rows = self._conn.execute(
                "SELECT uid, rel_path FROM file_cards WHERE sha256 = ? ORDER BY uid", (key,)
            ).fetchall()
        return [(str(uid), str(rel)) for uid, rel in rows if uid]

    def put(self, sha256: str, uid: str, rel_path: str = "") -> None:
        key = (sha256 or "").strip().lower()
        card = str(uid or "").strip()
        rel = str(rel_path or "").strip()
        if not is_sha256(key) or not card:
            return
        with self._lock:
            self._conn.execute(
                "INSERT INTO file_cards (sha256, uid, rel_path) VALUES (?, ?, ?) "
                "ON CONFLICT(sha256, uid) DO UPDATE SET rel_path = excluded.rel_path",
                (key, card, rel),
            )
            self._conn.commit()

    def upsert_many(self, triples: Iterable[tuple[str, str, str]]) -> int:
        """Bulk upsert. One commit. Incremental maintain must not put()+commit per row."""

        rows = []
        for sha, uid, rel_path in triples:
            key = str(sha or "").strip().lower()
            card = str(uid or "").strip()
            rel = str(rel_path or "").strip()
            if is_sha256(key) and card:
                rows.append((key, card, rel))
        if not rows:
            return 0
        with self._lock:
            self._conn.executemany(
                "INSERT INTO file_cards (sha256, uid, rel_path) VALUES (?, ?, ?) "
                "ON CONFLICT(sha256, uid) DO UPDATE SET rel_path = excluded.rel_path",
                rows,
            )
            self._conn.commit()
        return len(rows)

    def replace_all(self, triples: Iterable[tuple[str, str, str]]) -> int:
        rows = []
        for sha, uid, rel_path in triples:
            key = str(sha or "").strip().lower()
            card = str(uid or "").strip()
            rel = str(rel_path or "").strip()
            if is_sha256(key) and card:
                rows.append((key, card, rel))
        with self._lock:
            self._conn.execute("DELETE FROM file_cards")
            if rows:
                self._conn.executemany(
                    "INSERT OR IGNORE INTO file_cards (sha256, uid, rel_path) VALUES (?, ?, ?)",
                    rows,
                )
            self._conn.commit()
        return len(rows)

    def drop_uids(self, uids: Iterable[str]) -> None:
        wanted = [str(uid).strip() for uid in uids if str(uid).strip()]
        if not wanted:
            return
        with self._lock:
            self._conn.executemany("DELETE FROM file_cards WHERE uid = ?", [(uid,) for uid in wanted])
            self._conn.commit()


def merge_duplicate_links(existing: list[str], peer_uids: Iterable[str], *, self_uid: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    self_uid = str(self_uid or "").strip()
    for raw in list(existing) + [wikilink_uid(uid) for uid in peer_uids]:
        uid = uid_from_wikilink(raw)
        if not uid or uid == self_uid or uid in seen:
            continue
        seen.add(uid)
        out.append(wikilink_uid(uid))
    return out


def _write_identity_fields(
    vault: Path,
    rel_path: str,
    *,
    content_sha: str,
    duplicates: list[str],
    dry_run: bool,
) -> bool:
    fm, body, existing_prov = read_note(vault, rel_path)
    current_sha = source_sha_from_frontmatter(fm)
    current_dups = [wikilink_uid(uid_from_wikilink(x)) for x in (fm.get("duplicates") or []) if uid_from_wikilink(x)]
    want_dups = merge_duplicate_links(duplicates, [], self_uid=str(fm.get("uid") or ""))
    if current_sha == content_sha and current_dups == want_dups:
        return False
    if dry_run:
        return True
    field_updates: dict[str, Any] = {"content_sha": content_sha, "duplicates": want_dups}
    if str(fm.get("type") or "") == "email_attachment" and not str(fm.get("extracted_text_sha") or "").strip():
        field_updates["extracted_text_sha"] = content_sha
    merged = {**fm, **field_updates}
    card = validate_card_strict(merged)
    today = datetime.now(timezone.utc).date().isoformat()
    incoming = {
        key: ProvenanceEntry(
            source="file_identity",
            date=today,
            method="deterministic",
            model="sha256",
            input_hash=content_sha[:16],
        )
        for key in field_updates
    }
    write_card(vault, rel_path, card, body, merge_provenance(existing_prov, incoming))
    return True


def register_ingested_file(
    vault: Path,
    *,
    uid: str,
    rel_path: str,
    sha256: str,
    dry_run: bool = False,
    identity: FileIdentityIndex | None = None,
) -> list[str]:
    """Record *uid* under *sha256* and wikilink existing peers both ways."""

    sha = (sha256 or "").strip().lower()
    uid = str(uid or "").strip()
    if not is_sha256(sha) or not uid:
        return []
    own = identity or FileIdentityIndex()
    own.put(sha, uid, rel_path)
    rows = own.rows_for_sha(sha)
    peers = [peer_uid for peer_uid, _ in rows if peer_uid != uid]
    if not peers:
        _write_identity_fields(vault, rel_path, content_sha=sha, duplicates=[], dry_run=dry_run)
        return []
    _write_identity_fields(vault, rel_path, content_sha=sha, duplicates=[wikilink_uid(p) for p in peers], dry_run=dry_run)
    for peer_uid, peer_rel in rows:
        if peer_uid == uid or not peer_rel:
            continue
        _write_identity_fields(
            vault,
            peer_rel,
            content_sha=sha,
            duplicates=[wikilink_uid(p) for p, _ in rows if p != peer_uid],
            dry_run=dry_run,
        )
    return peers


def run_file_duplicate_linking(
    vault: Path,
    *,
    dry_run: bool = False,
    identity_db: Path | None = None,
    incremental: bool = False,
    uid_allowlist: set[str] | None = None,
    exclude_uids: set[str] | None = None,
) -> dict[str, Any]:
    """Hash kept document + email_attachment cards and write bidirectional duplicate links.

    ``incremental=True`` (maintain) upserts scanned rows and never ``replace_all``
    the identity index. It only writes cards that share a sha and are missing
    ``duplicates`` links — unique files are not stamped (avoids vault-wide dirty).
    Full CLI rebuilds still replace and may stamp ``content_sha`` on unique files.
    ``uid_allowlist`` limits writes to those UIDs plus same-sha peers.
    ``exclude_uids`` skips cards (purged junk still in a stale cache).
    """

    from archive_cli.vault_cache import VaultScanCache

    vault = Path(vault).resolve()
    allow = {str(uid).strip() for uid in (uid_allowlist or set()) if str(uid).strip()}
    exclude = {str(uid).strip() for uid in (exclude_uids or set()) if str(uid).strip()}
    log.info(
        "link-file-duplicates start vault=%s dry_run=%s incremental=%s allowlist=%s exclude=%s",
        vault,
        dry_run,
        incremental,
        len(allow),
        len(exclude),
    )
    scan_cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    seeded = 0
    try:
        seeded = get_extract_cache().stats().get("hits", 0)
    except Exception:
        seeded = 0

    cards: list[tuple[str, dict[str, Any]]] = []
    for card_type in ("document", "email_attachment"):
        for rel in scan_cache.rel_paths_by_type().get(card_type) or []:
            fm = scan_cache.frontmatter_for_rel_path(rel) or {}
            uid = str(fm.get("uid") or Path(rel).stem).strip()
            if not uid or uid in exclude:
                continue
            cards.append((rel, {**fm, "uid": uid, "type": card_type}))

    reused = 0
    missing_paths: list[str] = []
    path_to_rels: dict[str, list[str]] = defaultdict(list)
    sha_by_rel: dict[str, str] = {}
    t0 = time.monotonic()
    for i, (rel, fm) in enumerate(cards, start=1):
        sha = source_sha_from_frontmatter(fm)
        if sha:
            sha_by_rel[rel] = sha
            reused += 1
        else:
            src = resolve_source_path(vault, fm)
            if src is not None:
                key = str(src)
                missing_paths.append(key)
                path_to_rels[key].append(rel)
        if i == 1 or i % 5000 == 0 or i == len(cards):
            _log_progress("link-file-duplicates scan", i, len(cards), t0, extra=f"reused={reused}")

    unique_missing = list(dict.fromkeys(missing_paths))
    log.info("link-file-duplicates hash-missing paths=%s reused=%s", len(unique_missing), reused)
    computed = 0
    if unique_missing:
        hashed = hash_paths_sha256(unique_missing)
        computed = len(hashed)
        for path, sha in hashed.items():
            for rel in path_to_rels.get(path, []):
                sha_by_rel[rel] = sha

    by_sha: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for rel, fm in cards:
        sha = sha_by_rel.get(rel, "")
        if sha:
            by_sha[sha].append((rel, fm))

    groups = {sha: members for sha, members in by_sha.items() if len(members) >= 2}
    identity = FileIdentityIndex(identity_db)
    triples = [(sha, fm["uid"], rel) for sha, members in by_sha.items() for rel, fm in members]
    if incremental:
        identity.upsert_many(triples)
    else:
        identity.replace_all(triples)

    if allow:
        allow_shas = {sha for sha, members in by_sha.items() if any(fm["uid"] in allow for _, fm in members)}
        groups = {sha: members for sha, members in groups.items() if sha in allow_shas}

    dirty: list[str] = []
    cards_linked = 0
    t1 = time.monotonic()
    group_items = list(groups.items())
    for i, (sha, members) in enumerate(group_items, start=1):
        uids = [fm["uid"] for _, fm in members]
        for rel, fm in members:
            peers = [wikilink_uid(uid) for uid in uids if uid != fm["uid"]]
            changed = _write_identity_fields(vault, rel, content_sha=sha, duplicates=peers, dry_run=dry_run)
            if changed:
                dirty.append(fm["uid"])
                cards_linked += 1
        if i == 1 or i % 25 == 0 or i == len(group_items):
            _log_progress("link-file-duplicates write", i, len(group_items), t1, extra=f"sha={sha[:12]}")

    sha_only: list[tuple[str, dict[str, Any]]] = []
    if not incremental:
        sha_only = [
            (rel, fm)
            for rel, fm in cards
            if sha_by_rel.get(rel)
            and rel not in {r for members in groups.values() for r, _ in members}
            and (not allow or fm["uid"] in allow)
        ]
        t2 = time.monotonic()
        for i, (rel, fm) in enumerate(sha_only, start=1):
            sha = sha_by_rel[rel]
            if _write_identity_fields(vault, rel, content_sha=sha, duplicates=[], dry_run=dry_run):
                dirty.append(fm["uid"])
            if i == 1 or i % 5000 == 0 or i == len(sha_only):
                _log_progress("link-file-duplicates stamp-sha", i, max(1, len(sha_only)), t2)

    log.info(
        "link-file-duplicates done cards=%s reused=%s computed=%s groups=%s linked=%s dirty=%s cache_hits_at_start=%s",
        len(cards),
        reused,
        computed,
        len(groups),
        cards_linked,
        len(set(dirty)),
        seeded,
    )
    return {
        "vault": str(vault),
        "dry_run": dry_run,
        "cards_scanned": len(cards),
        "hashes_reused": reused,
        "hashes_computed": computed,
        "groups": len(groups),
        "cards_linked": cards_linked,
        "dirty_uids": sorted(set(dirty)),
    }
