"""Canonical change journal — durable mutation spine under ``_meta``.

Publication is the first consumer. Named cursor slots also exist for warehouse,
vectors, graph, seed-link, enrichment, and future claims. The journal is not a
search-only dirty log.
"""

from __future__ import annotations

import contextvars
import fcntl
import hashlib
import json
import logging
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from archive_engine.contracts import ChangeBatch, ChangeRecord
from archive_engine.errors import IncompatibleStateError
from archive_vault.paths import PathEscapeError, atomic_write_contained, normalize_vault_rel, unlink_contained

logger = logging.getLogger("ppa.journal")

JOURNAL_SCHEMA_VERSION = 1
JOURNAL_REL_PATH = "_meta/change-journal.sqlite3"
JOURNAL_LOCK_REL = "_meta/change-journal.lock"
STAGED_DIR_REL = "_meta/change-journal/staged"

CONSUMER_PUBLICATION = "publication"
CONSUMER_WAREHOUSE = "warehouse"
CONSUMER_VECTORS = "vectors"
CONSUMER_GRAPH = "graph"
CONSUMER_SEED_LINK = "seed-link"
CONSUMER_ENRICHMENT = "enrichment"
CONSUMER_CLAIMS = "claims"

NAMED_CONSUMERS: tuple[str, ...] = (
    CONSUMER_PUBLICATION,
    CONSUMER_WAREHOUSE,
    CONSUMER_VECTORS,
    CONSUMER_GRAPH,
    CONSUMER_SEED_LINK,
    CONSUMER_ENRICHMENT,
    CONSUMER_CLAIMS,
)

OPERATION_CREATE = "create"
OPERATION_UPDATE = "update"
OPERATION_DELETE = "delete"
OPERATION_EMBED = "embed"
OPERATION_LEGACY_DIRTY = "legacy_dirty"

OPERATIONS = frozenset({OPERATION_CREATE, OPERATION_UPDATE, OPERATION_DELETE, OPERATION_EMBED, OPERATION_LEGACY_DIRTY})

STATE_PREPARED = "prepared"
STATE_COMMITTED = "committed"
STATE_CONFLICT = "conflict"
STATE_ABORTED = "aborted"

CHECKPOINT_REASON = "p02_change_journal"

_MUTATION_CONTEXT: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar(
    "ppa_journal_mutation_context",
    default={},
)


class JournalFault(RuntimeError):
    """Test-injected crash at a named protocol boundary."""

    def __init__(self, point: str):
        super().__init__(point)
        self.point = point


class RevisionConflict(IncompatibleStateError):
    """A writer observed a different on-disk revision than it prepared against."""


class JournalConflict(IncompatibleStateError):
    """Prepared state does not match the file and cannot be marked successful."""


@dataclass(frozen=True, slots=True)
class FaultHook:
    """Raise :class:`JournalFault` when ``fail_at`` matches a protocol point."""

    fail_at: str = ""

    def check(self, point: str) -> None:
        if self.fail_at and self.fail_at == point:
            raise JournalFault(point)


@dataclass(frozen=True, slots=True)
class ConsumerCursor:
    consumer_name: str
    high_watermark: int
    gaps: tuple[int, ...]
    acked_sequences: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    mutation_id: str
    sequence: int
    action: str
    state: str
    detail: str = ""


def content_revision(data: bytes | None) -> str:
    if not data:
        return ""
    return hashlib.sha256(data).hexdigest()


def file_revision(path: Path) -> str:
    if not path.is_file():
        return ""
    return content_revision(path.read_bytes())


def idempotency_key(uid: str, operation: str, before_revision: str, after_revision: str) -> str:
    material = f"{uid}\n{operation}\n{before_revision}\n{after_revision}".encode()
    return hashlib.sha256(material).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@contextmanager
def mutation_context(*, source: str = "", account: str = "", run_id: str = "") -> Iterator[None]:
    """Adapter write-seam context copied onto the next journaled mutation."""

    token = _MUTATION_CONTEXT.set(
        {
            "source": str(source or ""),
            "account": str(account or ""),
            "run_id": str(run_id or ""),
        }
    )
    try:
        yield
    finally:
        _MUTATION_CONTEXT.reset(token)


def current_mutation_context() -> dict[str, str]:
    return dict(_MUTATION_CONTEXT.get())


class ChangeJournal:
    """SQLite WAL journal for one vault. Mutations are serialized locally."""

    def __init__(
        self,
        vault: str | Path,
        *,
        archive_id: str | None = None,
        fault: FaultHook | None = None,
    ) -> None:
        self.vault = Path(vault).expanduser().resolve()
        if not self.vault.is_dir():
            raise IncompatibleStateError(f"vault root is not a directory: {self.vault}")
        self.fault = fault or FaultHook()
        self._lock = threading.RLock()
        self._lock_depth = 0
        self._lock_fd: int | None = None
        self._conn: sqlite3.Connection | None = None
        self._archive_id = (archive_id or os.environ.get("PPA_ARCHIVE_ID", "") or "").strip()
        self._ensure_layout()
        self._open()

    @property
    def journal_path(self) -> Path:
        return self.vault / JOURNAL_REL_PATH

    @property
    def archive_id(self) -> str:
        return self._archive_id

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> ChangeJournal:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def apply_mutation(
        self,
        *,
        uid: str,
        rel_path: str,
        operation: str,
        content: bytes | None = None,
        source: str = "",
        account: str = "",
        run_id: str = "",
        mutation_id: str | None = None,
        expected_before: str | None = None,
    ) -> ChangeRecord:
        """Prepare, replace/delete through the canonical writer, then commit."""

        uid = str(uid or "").strip()
        if not uid:
            raise IncompatibleStateError("uid is required")
        if operation not in OPERATIONS:
            raise IncompatibleStateError(f"unsupported operation: {operation}")
        rel = str(normalize_vault_rel(rel_path)) if rel_path else ""
        if operation not in {OPERATION_EMBED, OPERATION_LEGACY_DIRTY} and not rel:
            raise IncompatibleStateError("rel_path is required")
        if rel:
            normalize_vault_rel(rel)

        ctx = current_mutation_context()
        source = ctx.get("source") or source
        account = ctx.get("account") or account
        run_id = ctx.get("run_id") or run_id

        with self._exclusive():
            target = self.vault / rel if rel else None
            before = file_revision(target) if target is not None else ""
            if operation == OPERATION_DELETE:
                after = ""
            elif operation in {OPERATION_EMBED, OPERATION_LEGACY_DIRTY} and content is None:
                after = before
            else:
                after = content_revision(content)
            key = idempotency_key(uid, operation, before, after)
            existing = self._row_by_idempotency(key)
            if existing is not None:
                if existing["state"] == STATE_COMMITTED:
                    return self._record_from_row(existing)
                if existing["state"] == STATE_PREPARED:
                    return self._resume_prepared(existing, content=content)
                raise JournalConflict(f"mutation {existing['mutation_id']} is {existing['state']}")

            if expected_before is not None and expected_before != before:
                raise RevisionConflict(f"uid={uid} expected {expected_before or '∅'} got {before or '∅'}")
            if operation not in {OPERATION_EMBED, OPERATION_LEGACY_DIRTY} and target is not None:
                live = file_revision(target)
                if live != before:
                    raise RevisionConflict(f"uid={uid} expected {before or '∅'} got {live or '∅'}")

            if before == after and operation not in {OPERATION_DELETE, OPERATION_LEGACY_DIRTY, OPERATION_EMBED}:
                latest = self._latest_committed_for_uid(uid)
                if latest is not None and latest["after_revision"] == after:
                    return self._record_from_row(latest)

            mid = (mutation_id or uuid.uuid4().hex).strip() or uuid.uuid4().hex
            sequence = self._next_sequence()
            staged_rel = f"{STAGED_DIR_REL}/{mid}"
            now = _utc_now()
            self._conn_req().execute(
                """
                INSERT INTO mutations (
                    mutation_id, sequence, uid, rel_path, operation,
                    before_revision, after_revision, source, account, run_id,
                    state, staged_path, staged_content_hash, tombstone_json,
                    created_at, committed_at, idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
                """,
                (
                    mid,
                    sequence,
                    uid,
                    rel,
                    operation,
                    before,
                    after,
                    source,
                    account,
                    run_id,
                    STATE_PREPARED,
                    staged_rel if content is not None or operation == OPERATION_DELETE else "",
                    after,
                    json.dumps(
                        {"uid": uid, "rel_path": rel, "before_revision": before, "deleted_at": now}
                        if operation == OPERATION_DELETE
                        else {},
                        sort_keys=True,
                    ),
                    now,
                    key,
                ),
            )
            self._conn_req().commit()
            self.fault.check("after_prepare")
            row = self._row_by_id(mid)
            assert row is not None
            return self._finish_prepared(row, content=content)

    def reconcile(self) -> list[ReconcileResult]:
        """Complete, resume, or conflict pending prepared records. Never invent success."""

        results: list[ReconcileResult] = []
        with self._exclusive():
            rows = (
                self._conn_req()
                .execute(
                    "SELECT * FROM mutations WHERE state = ? ORDER BY sequence",
                    (STATE_PREPARED,),
                )
                .fetchall()
            )
            for row in rows:
                results.append(self._reconcile_row(row))
        return results

    def detect_external_edits(self, uid_to_rel: Mapping[str, str]) -> list[dict[str, str]]:
        """Report committed UIDs whose files no longer match ``after_revision``.

        Callers supply a cache-built map. This method does not walk the vault.
        """

        reports: list[dict[str, str]] = []
        with self._exclusive():
            for uid, rel_path in uid_to_rel.items():
                latest = self._latest_committed_for_uid(str(uid))
                if latest is None:
                    continue
                try:
                    rel = str(normalize_vault_rel(str(rel_path)))
                except PathEscapeError:
                    reports.append(
                        {
                            "uid": str(uid),
                            "rel_path": str(rel_path),
                            "status": "path_escape",
                            "expected": str(latest["after_revision"]),
                        }
                    )
                    continue
                live = file_revision(self.vault / rel)
                expected = str(latest["after_revision"] or "")
                if live != expected:
                    reports.append(
                        {
                            "uid": str(uid),
                            "rel_path": rel,
                            "status": "external_edit",
                            "expected": expected,
                            "actual": live,
                        }
                    )
        return reports

    def import_legacy_dirty(
        self,
        uids: list[str] | None,
        *,
        reason: str = "legacy_dirty",
        rel_paths: Mapping[str, str] | None = None,
    ) -> list[ChangeRecord]:
        """Auditable conversion of legacy DIRTY UIDs. Existing mutations are skipped."""

        imported: list[ChangeRecord] = []
        rel_paths = rel_paths or {}
        for raw in uids or []:
            uid = str(raw).strip()
            if not uid:
                continue
            with self._exclusive():
                if self._latest_for_uid(uid) is not None:
                    continue
                rel = str(rel_paths.get(uid) or "")
                if rel:
                    try:
                        rel = str(normalize_vault_rel(rel))
                    except PathEscapeError:
                        rel = ""
                imported.append(
                    self.apply_mutation(
                        uid=uid,
                        rel_path=rel,
                        operation=OPERATION_LEGACY_DIRTY,
                        content=None,
                        source=reason,
                    )
                )
        return imported

    def consume(self, consumer_name: str, *, limit: int = 100) -> ChangeBatch:
        name = self._require_consumer(consumer_name)
        cursor = self.consumer_cursor(name)
        with self._exclusive():
            pending_gaps = list(cursor.gaps)
            rows = (
                self._conn_req()
                .execute(
                    """
                SELECT * FROM mutations
                WHERE state = ? AND (sequence > ? OR sequence IN ({placeholders}))
                ORDER BY sequence
                LIMIT ?
                """.format(placeholders=",".join("?" for _ in pending_gaps) or "NULL"),
                    (STATE_COMMITTED, cursor.high_watermark, *pending_gaps, max(1, int(limit))),
                )
                .fetchall()
                if pending_gaps
                else self._conn_req()
                .execute(
                    """
                SELECT * FROM mutations
                WHERE state = ? AND sequence > ?
                ORDER BY sequence
                LIMIT ?
                """,
                    (STATE_COMMITTED, cursor.high_watermark, max(1, int(limit))),
                )
                .fetchall()
            )
            records = tuple(self._record_from_row(row) for row in rows)
            captured = records[-1].sequence if records else cursor.high_watermark
            return ChangeBatch(
                high_watermark=captured,
                consumer_name=name,
                records=records,
                record_ref=f"journal:{self.archive_id}:{cursor.high_watermark}:{captured}",
            )

    def acknowledge(
        self,
        consumer_name: str,
        batch: ChangeBatch,
        *,
        acked_sequences: list[int] | None = None,
        gap_sequences: list[int] | None = None,
        gap_reason: str = "pending",
    ) -> ConsumerCursor:
        """Ack sequences for one consumer. Never advances a different consumer."""

        name = self._require_consumer(consumer_name)
        if batch.consumer_name != name:
            raise IncompatibleStateError("ChangeBatch.consumer_name does not match acknowledge target")
        requested = set(acked_sequences if acked_sequences is not None else [r.sequence for r in batch.records])
        gaps = set(gap_sequences or ())
        requested -= gaps
        now = _utc_now()
        with self._exclusive():
            for record in batch.records:
                if record.sequence in requested:
                    self._conn_req().execute(
                        """
                        INSERT INTO consumer_acks (consumer_name, sequence, mutation_id, acked_at)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(consumer_name, sequence) DO NOTHING
                        """,
                        (name, record.sequence, record.mutation_id, now),
                    )
                    self._conn_req().execute(
                        "DELETE FROM consumer_gaps WHERE consumer_name = ? AND sequence = ?",
                        (name, record.sequence),
                    )
                elif record.sequence in gaps:
                    self._conn_req().execute(
                        """
                        INSERT INTO consumer_gaps (consumer_name, sequence, mutation_id, reason, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(consumer_name, sequence) DO UPDATE SET reason = excluded.reason
                        """,
                        (name, record.sequence, record.mutation_id, gap_reason, now),
                    )
            if batch.high_watermark > 0 and not batch.records and not requested and not gaps:
                # watermark-only replay ack is ignored unless sequences are named
                pass
            watermark = self._recompute_watermark(name)
            self._conn_req().execute(
                """
                UPDATE consumer_cursors
                SET high_watermark = ?, updated_at = ?
                WHERE consumer_name = ?
                """,
                (watermark, now, name),
            )
            self._conn_req().commit()
        return self.consumer_cursor(name)

    def consumer_cursor(self, consumer_name: str) -> ConsumerCursor:
        name = self._require_consumer(consumer_name)
        with self._exclusive():
            row = (
                self._conn_req()
                .execute(
                    "SELECT high_watermark FROM consumer_cursors WHERE consumer_name = ?",
                    (name,),
                )
                .fetchone()
            )
            gaps = tuple(
                int(item["sequence"])
                for item in self._conn_req().execute(
                    "SELECT sequence FROM consumer_gaps WHERE consumer_name = ? ORDER BY sequence",
                    (name,),
                )
            )
            acked = tuple(
                int(item["sequence"])
                for item in self._conn_req().execute(
                    "SELECT sequence FROM consumer_acks WHERE consumer_name = ? ORDER BY sequence",
                    (name,),
                )
            )
        return ConsumerCursor(
            consumer_name=name,
            high_watermark=int(row["high_watermark"]) if row else 0,
            gaps=gaps,
            acked_sequences=acked,
        )

    def consumer_cursors(self) -> dict[str, ConsumerCursor]:
        return {name: self.consumer_cursor(name) for name in NAMED_CONSUMERS}

    def committed_records(self, *, after_sequence: int = 0, limit: int = 1000) -> list[ChangeRecord]:
        with self._exclusive():
            rows = (
                self._conn_req()
                .execute(
                    """
                SELECT * FROM mutations
                WHERE state = ? AND sequence > ?
                ORDER BY sequence
                LIMIT ?
                """,
                    (STATE_COMMITTED, int(after_sequence), max(1, int(limit))),
                )
                .fetchall()
            )
        return [self._record_from_row(row) for row in rows]

    def mutation_by_id(self, mutation_id: str) -> ChangeRecord | None:
        with self._exclusive():
            row = self._row_by_id(mutation_id)
        return None if row is None else self._record_from_row(row)

    def checkpoint(self) -> dict[str, Any]:
        """P07-adoptable recovery checkpoint. Durable committed mutations only."""

        with self._exclusive():
            high = (
                self._conn_req()
                .execute(
                    "SELECT COALESCE(MAX(sequence), 0) AS n FROM mutations WHERE state = ?",
                    (STATE_COMMITTED,),
                )
                .fetchone()
            )
            prepared = (
                self._conn_req()
                .execute(
                    "SELECT COUNT(*) AS n FROM mutations WHERE state = ?",
                    (STATE_PREPARED,),
                )
                .fetchone()
            )
        consumers = {
            name: {"high_watermark": cursor.high_watermark, "gaps": list(cursor.gaps)}
            for name, cursor in self.consumer_cursors().items()
        }
        value = {
            "archive_id": self.archive_id,
            "schema_version": JOURNAL_SCHEMA_VERSION,
            "high_watermark": int(high["n"]) if high else 0,
            "prepared_count": int(prepared["n"]) if prepared else 0,
            "journal_rel_path": JOURNAL_REL_PATH,
            "consumers": consumers,
        }
        return {
            "status": "available",
            "reason": CHECKPOINT_REASON,
            "value": value,
        }

    def archive_identity_binding(self) -> dict[str, Any]:
        return {
            "status": "available",
            "reason": CHECKPOINT_REASON,
            "value": self.archive_id,
        }

    def import_dirty_file(self, dirty_path: Path) -> list[ChangeRecord]:
        if not dirty_path.is_file():
            return []
        uids: list[str] = []
        reason = "legacy_dirty"
        for raw in dirty_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            reason = str(rec.get("reason") or "legacy_dirty")
            for uid in rec.get("uids") or []:
                text = str(uid).strip()
                if text:
                    uids.append(text)
        return self.import_legacy_dirty(uids, reason=reason)

    def _ensure_layout(self) -> None:
        normalize_vault_rel(JOURNAL_REL_PATH)
        (self.vault / "_meta" / "change-journal" / "staged").mkdir(parents=True, exist_ok=True)

    def _open(self) -> None:
        path = self.journal_path
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        self._conn = conn
        self._create_schema()
        stored = self._meta("archive_id")
        if self._archive_id:
            self._set_meta("archive_id", self._archive_id)
        elif stored:
            self._archive_id = stored
        else:
            material = str(self.vault.resolve()).encode()
            self._archive_id = hashlib.sha256(material).hexdigest()
            self._set_meta("archive_id", self._archive_id)
        self._set_meta("schema_version", str(JOURNAL_SCHEMA_VERSION))
        self._seed_consumers()
        conn.commit()

    def _create_schema(self) -> None:
        conn = self._conn_req()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS journal_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mutations (
                mutation_id TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL UNIQUE,
                uid TEXT NOT NULL,
                rel_path TEXT NOT NULL,
                operation TEXT NOT NULL,
                before_revision TEXT NOT NULL,
                after_revision TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT '',
                account TEXT NOT NULL DEFAULT '',
                run_id TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL,
                staged_path TEXT NOT NULL DEFAULT '',
                staged_content_hash TEXT NOT NULL DEFAULT '',
                tombstone_json TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                committed_at TEXT,
                idempotency_key TEXT NOT NULL UNIQUE
            );
            CREATE INDEX IF NOT EXISTS idx_mutations_uid_state ON mutations(uid, state);
            CREATE INDEX IF NOT EXISTS idx_mutations_state_seq ON mutations(state, sequence);
            CREATE TABLE IF NOT EXISTS consumer_cursors (
                consumer_name TEXT PRIMARY KEY,
                high_watermark INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS consumer_acks (
                consumer_name TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                mutation_id TEXT NOT NULL,
                acked_at TEXT NOT NULL,
                PRIMARY KEY (consumer_name, sequence)
            );
            CREATE TABLE IF NOT EXISTS consumer_gaps (
                consumer_name TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                mutation_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (consumer_name, sequence)
            );
            """
        )

    def _seed_consumers(self) -> None:
        now = _utc_now()
        for name in NAMED_CONSUMERS:
            self._conn_req().execute(
                """
                INSERT INTO consumer_cursors (consumer_name, high_watermark, updated_at)
                VALUES (?, 0, ?)
                ON CONFLICT(consumer_name) DO NOTHING
                """,
                (name, now),
            )

    def _meta(self, key: str) -> str:
        row = self._conn_req().execute("SELECT value FROM journal_meta WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row else ""

    def _set_meta(self, key: str, value: str) -> None:
        self._conn_req().execute(
            "INSERT INTO journal_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def _conn_req(self) -> sqlite3.Connection:
        if self._conn is None:
            raise IncompatibleStateError("journal is closed")
        return self._conn

    @contextmanager
    def _exclusive(self) -> Iterator[None]:
        with self._lock:
            if self._lock_depth == 0:
                lock_path = self.vault / JOURNAL_LOCK_REL
                lock_path.parent.mkdir(parents=True, exist_ok=True)
                fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
                fcntl.flock(fd, fcntl.LOCK_EX)
                self._lock_fd = fd
            self._lock_depth += 1
            try:
                yield
            finally:
                self._lock_depth -= 1
                if self._lock_depth == 0 and self._lock_fd is not None:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                    os.close(self._lock_fd)
                    self._lock_fd = None

    def _next_sequence(self) -> int:
        row = self._conn_req().execute("SELECT COALESCE(MAX(sequence), 0) AS n FROM mutations").fetchone()
        return int(row["n"]) + 1

    def _row_by_id(self, mutation_id: str) -> sqlite3.Row | None:
        return self._conn_req().execute("SELECT * FROM mutations WHERE mutation_id = ?", (mutation_id,)).fetchone()

    def _row_by_idempotency(self, key: str) -> sqlite3.Row | None:
        return self._conn_req().execute("SELECT * FROM mutations WHERE idempotency_key = ?", (key,)).fetchone()

    def _latest_for_uid(self, uid: str) -> sqlite3.Row | None:
        return (
            self._conn_req()
            .execute(
                "SELECT * FROM mutations WHERE uid = ? ORDER BY sequence DESC LIMIT 1",
                (uid,),
            )
            .fetchone()
        )

    def _latest_committed_for_uid(self, uid: str) -> sqlite3.Row | None:
        return (
            self._conn_req()
            .execute(
                "SELECT * FROM mutations WHERE uid = ? AND state = ? ORDER BY sequence DESC LIMIT 1",
                (uid, STATE_COMMITTED),
            )
            .fetchone()
        )

    def _record_from_row(self, row: sqlite3.Row) -> ChangeRecord:
        return ChangeRecord(
            archive_id=self.archive_id,
            sequence=int(row["sequence"]),
            mutation_id=str(row["mutation_id"]),
            uid=str(row["uid"]),
            operation=str(row["operation"]),
            before_revision=str(row["before_revision"] or ""),
            after_revision=str(row["after_revision"] or ""),
            source=str(row["source"] or ""),
            account=str(row["account"] or ""),
            run_id=str(row["run_id"] or ""),
            committed=str(row["state"]) == STATE_COMMITTED,
        )

    def _require_consumer(self, consumer_name: str) -> str:
        name = str(consumer_name or "").strip()
        if name not in NAMED_CONSUMERS:
            raise IncompatibleStateError(f"unknown consumer: {consumer_name!r}")
        return name

    def _recompute_watermark(self, consumer_name: str) -> int:
        acked = {
            int(row["sequence"])
            for row in self._conn_req().execute(
                "SELECT sequence FROM consumer_acks WHERE consumer_name = ?",
                (consumer_name,),
            )
        }
        watermark = 0
        while (watermark + 1) in acked:
            watermark += 1
        return watermark

    def _finish_prepared(self, row: sqlite3.Row, *, content: bytes | None) -> ChangeRecord:
        operation = str(row["operation"])
        rel = str(row["rel_path"] or "")
        staged_rel = str(row["staged_path"] or "")
        if operation == OPERATION_LEGACY_DIRTY or operation == OPERATION_EMBED:
            return self._mark_committed(row)
        if operation == OPERATION_DELETE:
            if staged_rel and content:
                atomic_write_contained(self.vault, staged_rel, content)
                self.fault.check("after_stage")
            if rel:
                unlink_contained(self.vault, rel)
            self.fault.check("after_replace")
            return self._mark_committed(row)
        if content is None:
            raise JournalConflict(f"prepared {row['mutation_id']} has no staged content")
        if not staged_rel:
            staged_rel = f"{STAGED_DIR_REL}/{row['mutation_id']}"
        atomic_write_contained(self.vault, staged_rel, content)
        self.fault.check("after_stage")
        atomic_write_contained(self.vault, rel, content)
        self.fault.check("after_replace")
        record = self._mark_committed(row)
        try:
            unlink_contained(self.vault, staged_rel)
        except OSError:
            logger.debug("staged cleanup failed mutation=%s", row["mutation_id"], exc_info=True)
        return record

    def _resume_prepared(self, row: sqlite3.Row, *, content: bytes | None) -> ChangeRecord:
        self._reconcile_row(row, content=content)
        return self._record_after_reconcile(str(row["mutation_id"]))

    def _record_after_reconcile(self, mutation_id: str) -> ChangeRecord:
        row = self._row_by_id(str(mutation_id))
        if row is None:
            raise JournalConflict(f"mutation {mutation_id} disappeared during resume")
        if row["state"] == STATE_CONFLICT:
            raise JournalConflict(f"mutation {mutation_id} is in conflict")
        return self._record_from_row(row)

    def _reconcile_row(self, row: sqlite3.Row, *, content: bytes | None = None) -> ReconcileResult:
        mid = str(row["mutation_id"])
        rel = str(row["rel_path"] or "")
        operation = str(row["operation"])
        before = str(row["before_revision"] or "")
        after = str(row["after_revision"] or "")
        staged_rel = str(row["staged_path"] or "")
        target = self.vault / rel if rel else None
        live = file_revision(target) if target is not None else ""
        staged_path = self.vault / staged_rel if staged_rel else None
        staged_bytes = content
        if staged_bytes is None and staged_path is not None and staged_path.is_file():
            staged_bytes = staged_path.read_bytes()

        if operation in {OPERATION_EMBED, OPERATION_LEGACY_DIRTY}:
            self._mark_committed(row)
            return ReconcileResult(mid, int(row["sequence"]), "commit", STATE_COMMITTED, "journal-only")

        if live == after and (operation != OPERATION_DELETE or not live):
            if operation == OPERATION_DELETE and live:
                self._mark_conflict(row, "delete target still present")
                return ReconcileResult(mid, int(row["sequence"]), "conflict", STATE_CONFLICT, "delete target present")
            self._mark_committed(row)
            return ReconcileResult(mid, int(row["sequence"]), "commit", STATE_COMMITTED, "file matches after_revision")

        if operation == OPERATION_DELETE and live == "":
            self._mark_committed(row)
            return ReconcileResult(mid, int(row["sequence"]), "commit", STATE_COMMITTED, "delete already applied")

        if live == before and staged_bytes is not None:
            self._finish_prepared(row, content=staged_bytes)
            refreshed = self._row_by_id(mid)
            state = str(refreshed["state"]) if refreshed else STATE_CONFLICT
            return ReconcileResult(mid, int(row["sequence"]), "resume", state, "replaced from staged")

        if live == before and staged_bytes is None:
            return ReconcileResult(
                mid,
                int(row["sequence"]),
                "incomplete_prepare",
                STATE_PREPARED,
                "prepared; file unchanged; no staged content",
            )

        self._mark_conflict(row, f"live={live or '∅'} before={before or '∅'} after={after or '∅'}")
        return ReconcileResult(mid, int(row["sequence"]), "conflict", STATE_CONFLICT, "unknown file state")

    def _mark_committed(self, row: sqlite3.Row) -> ChangeRecord:
        now = _utc_now()
        self._conn_req().execute(
            "UPDATE mutations SET state = ?, committed_at = ? WHERE mutation_id = ? AND state = ?",
            (STATE_COMMITTED, now, row["mutation_id"], STATE_PREPARED),
        )
        self._conn_req().commit()
        self.fault.check("after_commit")
        refreshed = self._row_by_id(str(row["mutation_id"]))
        assert refreshed is not None
        record = self._record_from_row(refreshed)
        logger.info(
            "journal commit mutation=%s sequence=%s uid=%s operation=%s",
            record.mutation_id,
            record.sequence,
            record.uid,
            record.operation,
        )
        return record

    def _mark_conflict(self, row: sqlite3.Row, detail: str) -> None:
        self._conn_req().execute(
            "UPDATE mutations SET state = ? WHERE mutation_id = ? AND state = ?",
            (STATE_CONFLICT, row["mutation_id"], STATE_PREPARED),
        )
        self._conn_req().commit()
        logger.error("journal conflict mutation=%s detail=%s", row["mutation_id"], detail)


def open_journal(vault: str | Path, **kwargs: Any) -> ChangeJournal:
    return ChangeJournal(vault, **kwargs)
