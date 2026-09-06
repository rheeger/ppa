"""Durable processor state and run history."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_engine.contracts import EmbeddingSpec, OutputReceipt, OutputRevision

from .batch import ProcessorRunReport
from .constants import (
    LEASE_SECONDS_DEFAULT,
    RECEIPT_STATUS_RUNNING,
    RECEIPT_STATUS_SUPERSEDED,
    RUN_STATUS_SUCCESS,
    SUCCESS_RECEIPT_STATUSES,
    output_receipt_status,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_ts(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat()
    return str(value).strip() or None


def ensure_processor_tables(conn: Any, schema: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.processor_state (
            processor_key TEXT PRIMARY KEY,
            processor_version TEXT NOT NULL DEFAULT '',
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            last_success_at TIMESTAMPTZ,
            last_attempt_at TIMESTAMPTZ,
            last_error TEXT NOT NULL DEFAULT '',
            pending_count INT NOT NULL DEFAULT 0,
            stale_count INT NOT NULL DEFAULT 0,
            failed_count INT NOT NULL DEFAULT 0,
            last_run_id TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.processor_runs (
            run_id TEXT PRIMARY KEY,
            processor_key TEXT NOT NULL,
            processor_version TEXT NOT NULL DEFAULT '',
            archive_instance TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            input_count INT NOT NULL DEFAULT 0,
            dirty_count INT NOT NULL DEFAULT 0,
            stale_count INT NOT NULL DEFAULT 0,
            skipped_count INT NOT NULL DEFAULT 0,
            output_count INT NOT NULL DEFAULT 0,
            skip_reasons JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            stale_reasons JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            engine_mode TEXT NOT NULL DEFAULT '',
            ladder_gate TEXT NOT NULL DEFAULT '',
            decision_run_id TEXT NOT NULL DEFAULT '',
            errors JSONB NOT NULL DEFAULT '[]'::jsonb,
            warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ,
            artifact_paths JSONB NOT NULL DEFAULT '{{}}'::jsonb
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.processor_input_state (
            processor_key TEXT NOT NULL,
            input_uid TEXT NOT NULL,
            input_hash TEXT NOT NULL DEFAULT '',
            input_corpus_state TEXT NOT NULL DEFAULT 'active',
            processor_version TEXT NOT NULL DEFAULT '',
            output_identity TEXT NOT NULL DEFAULT '',
            output_uids JSONB NOT NULL DEFAULT '[]'::jsonb,
            status TEXT NOT NULL DEFAULT 'pending',
            skip_reason TEXT NOT NULL DEFAULT '',
            error TEXT NOT NULL DEFAULT '',
            last_run_id TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (processor_key, input_uid)
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_processor_runs_key_started
        ON {schema}.processor_runs(processor_key, started_at DESC)
        """
    )


def ensure_processor_receipt_tables(conn: Any, schema: str) -> None:
    """Revision receipts + per-input head pointer (P03-A)."""

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.processor_receipts (
            processor_key TEXT NOT NULL,
            processor_version TEXT NOT NULL,
            input_uid TEXT NOT NULL,
            input_revision TEXT NOT NULL,
            dependency_receipt_digest TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            contract_status TEXT NOT NULL DEFAULT '',
            outputs JSONB NOT NULL DEFAULT '[]'::jsonb,
            chunk_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
            embedding_spec JSONB,
            error_reason TEXT NOT NULL DEFAULT '',
            dependency_reason TEXT NOT NULL DEFAULT '',
            receipt_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            lease_owner TEXT NOT NULL DEFAULT '',
            lease_expires_at TIMESTAMPTZ,
            last_run_id TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (
                processor_key,
                processor_version,
                input_uid,
                input_revision,
                dependency_receipt_digest
            )
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.processor_receipt_heads (
            processor_key TEXT NOT NULL,
            input_uid TEXT NOT NULL,
            processor_version TEXT NOT NULL DEFAULT '',
            input_revision TEXT NOT NULL,
            dependency_receipt_digest TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (processor_key, input_uid)
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_processor_receipts_uid
        ON {schema}.processor_receipts(input_uid, processor_key)
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_processor_receipts_status
        ON {schema}.processor_receipts(status)
        """
    )


@dataclass
class ProcessorStateRecord:
    processor_key: str
    processor_version: str = ""
    enabled: bool = True
    last_success_at: str | None = None
    last_attempt_at: str | None = None
    last_error: str = ""
    pending_count: int = 0
    stale_count: int = 0
    failed_count: int = 0
    last_run_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "processor_key": self.processor_key,
            "processor_version": self.processor_version,
            "enabled": self.enabled,
            "last_success_at": self.last_success_at,
            "last_attempt_at": self.last_attempt_at,
            "last_error": self.last_error,
            "pending_count": self.pending_count,
            "stale_count": self.stale_count,
            "failed_count": self.failed_count,
            "last_run_id": self.last_run_id,
        }


@dataclass
class ProcessorInputStateRecord:
    processor_key: str
    input_uid: str
    input_hash: str = ""
    input_corpus_state: str = "active"
    processor_version: str = ""
    output_identity: str = ""
    output_uids: list[str] = field(default_factory=list)
    status: str = "pending"
    skip_reason: str = ""
    error: str = ""
    last_run_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "processor_key": self.processor_key,
            "input_uid": self.input_uid,
            "input_hash": self.input_hash,
            "input_corpus_state": self.input_corpus_state,
            "processor_version": self.processor_version,
            "output_identity": self.output_identity,
            "output_uids": list(self.output_uids),
            "status": self.status,
            "skip_reason": self.skip_reason,
            "error": self.error,
            "last_run_id": self.last_run_id,
        }


@dataclass
class StoredReceipt:
    """Durable receipt row plus lease / head metadata."""

    receipt: OutputReceipt
    scheduler_status: str = ""
    lease_owner: str = ""
    lease_expires_at: str | None = None
    last_run_id: str = ""
    updated_at: str | None = None
    dependency_receipt_digest: str = ""

    def to_meta(self) -> dict[str, Any]:
        return {
            "receipt": self.receipt.to_payload(),
            "scheduler_status": self.scheduler_status or self.receipt.status,
            "lease_owner": self.lease_owner,
            "lease_expires_at": self.lease_expires_at,
            "last_run_id": self.last_run_id,
            "updated_at": self.updated_at,
            "dependency_receipt_digest": self.dependency_receipt_digest or "",
        }


def _parse_outputs(raw: Any) -> tuple[OutputRevision, ...]:
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, list):
        return ()
    out: list[OutputRevision] = []
    for item in raw:
        if isinstance(item, dict):
            uid = str(item.get("uid") or "")
            revision = str(item.get("revision") or "")
            if uid and revision:
                out.append(OutputRevision(uid=uid, revision=revision))
        elif isinstance(item, str) and item:
            out.append(OutputRevision(uid=item, revision=""))
    return tuple(out)


def _parse_chunk_keys(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, list):
        return ()
    return tuple(str(item) for item in raw if str(item))


def _parse_embedding_spec(raw: Any) -> EmbeddingSpec | None:
    if raw in (None, "", {}):
        return None
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, dict):
        return None
    return EmbeddingSpec.from_payload(raw)


def _row_to_stored_receipt(row: Any) -> StoredReceipt:
    if isinstance(row, dict):
        data = row
    else:
        keys = (
            "processor_key",
            "processor_version",
            "input_uid",
            "input_revision",
            "dependency_receipt_digest",
            "status",
            "contract_status",
            "outputs",
            "chunk_keys",
            "embedding_spec",
            "error_reason",
            "dependency_reason",
            "receipt_payload",
            "lease_owner",
            "lease_expires_at",
            "last_run_id",
            "updated_at",
        )
        data = dict(zip(keys, row, strict=False))
    payload = data.get("receipt_payload") or {}
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    scheduler_status = str(data.get("status") or "")
    if isinstance(payload, dict) and payload.get("processor"):
        receipt = OutputReceipt.from_payload(payload)
    else:
        receipt = OutputReceipt(
            processor=str(data.get("processor_key") or ""),
            processor_version=str(data.get("processor_version") or ""),
            input_uid=str(data.get("input_uid") or ""),
            input_revision=str(data.get("input_revision") or ""),
            status=str(data.get("contract_status") or output_receipt_status(scheduler_status)),  # type: ignore[arg-type]
            outputs=_parse_outputs(data.get("outputs")),
            chunk_keys=_parse_chunk_keys(data.get("chunk_keys")),
            embedding_spec=_parse_embedding_spec(data.get("embedding_spec")),
            error_reason=str(data.get("error_reason") or ""),
            dependency_reason=str(data.get("dependency_reason") or ""),
        )
    return StoredReceipt(
        receipt=receipt,
        scheduler_status=scheduler_status or receipt.status,
        lease_owner=str(data.get("lease_owner") or ""),
        lease_expires_at=_format_ts(data.get("lease_expires_at")),
        last_run_id=str(data.get("last_run_id") or ""),
        updated_at=_format_ts(data.get("updated_at")),
        dependency_receipt_digest=str(data.get("dependency_receipt_digest") or ""),
    )


def _row_to_state(row: Any) -> ProcessorStateRecord:
    if isinstance(row, dict):
        data = row
    else:
        keys = (
            "processor_key",
            "processor_version",
            "enabled",
            "last_success_at",
            "last_attempt_at",
            "last_error",
            "pending_count",
            "stale_count",
            "failed_count",
            "last_run_id",
        )
        data = dict(zip(keys, row, strict=False))
    return ProcessorStateRecord(
        processor_key=str(data.get("processor_key") or ""),
        processor_version=str(data.get("processor_version") or ""),
        enabled=bool(data.get("enabled", True)),
        last_success_at=_format_ts(data.get("last_success_at")),
        last_attempt_at=_format_ts(data.get("last_attempt_at")),
        last_error=str(data.get("last_error") or ""),
        pending_count=int(data.get("pending_count") or 0),
        stale_count=int(data.get("stale_count") or 0),
        failed_count=int(data.get("failed_count") or 0),
        last_run_id=str(data.get("last_run_id") or ""),
    )


def _row_to_input_state(row: Any) -> ProcessorInputStateRecord:
    if isinstance(row, dict):
        data = row
    else:
        keys = (
            "processor_key",
            "input_uid",
            "input_hash",
            "input_corpus_state",
            "processor_version",
            "output_identity",
            "output_uids",
            "status",
            "skip_reason",
            "error",
            "last_run_id",
        )
        data = dict(zip(keys, row, strict=False))
    output_uids = data.get("output_uids") or []
    if isinstance(output_uids, str):
        output_uids = json.loads(output_uids)
    return ProcessorInputStateRecord(
        processor_key=str(data.get("processor_key") or ""),
        input_uid=str(data.get("input_uid") or ""),
        input_hash=str(data.get("input_hash") or ""),
        input_corpus_state=str(data.get("input_corpus_state") or "active"),
        processor_version=str(data.get("processor_version") or ""),
        output_identity=str(data.get("output_identity") or ""),
        output_uids=list(output_uids) if isinstance(output_uids, list) else [],
        status=str(data.get("status") or "pending"),
        skip_reason=str(data.get("skip_reason") or ""),
        error=str(data.get("error") or ""),
        last_run_id=str(data.get("last_run_id") or ""),
    )


class ProcessorStateStore:
    """Postgres-backed store with optional vault _meta JSON fallback."""

    def __init__(
        self,
        conn: Any | None = None,
        schema: str = "ppa",
        *,
        meta_path: Path | None = None,
    ) -> None:
        self._conn = conn
        self._schema = schema
        self._meta_path = meta_path

    def ensure_tables(self) -> None:
        if self._conn is not None:
            ensure_processor_tables(self._conn, self._schema)
            ensure_processor_receipt_tables(self._conn, self._schema)

    def list_state(self) -> list[ProcessorStateRecord]:
        if self._conn is not None:
            self.ensure_tables()
            rows = self._conn.execute(f"SELECT * FROM {self._schema}.processor_state ORDER BY processor_key").fetchall()
            return [_row_to_state(r) for r in rows]
        return [_row_to_state(item) for item in self._load_meta().get("state", [])]

    def get_state(self, processor_key: str) -> ProcessorStateRecord | None:
        if self._conn is not None:
            self.ensure_tables()
            row = self._conn.execute(
                f"SELECT * FROM {self._schema}.processor_state WHERE processor_key = %s",
                (processor_key,),
            ).fetchone()
            return _row_to_state(row) if row else None
        for item in self._load_meta().get("state", []):
            if str(item.get("processor_key")) == processor_key:
                return _row_to_state(item)
        return None

    def upsert_state(self, record: ProcessorStateRecord) -> ProcessorStateRecord:
        if self._conn is not None:
            self.ensure_tables()
            self._conn.execute(
                f"""
                INSERT INTO {self._schema}.processor_state (
                    processor_key, processor_version, enabled,
                    last_success_at, last_attempt_at, last_error,
                    pending_count, stale_count, failed_count, last_run_id, updated_at
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (processor_key) DO UPDATE SET
                    processor_version = EXCLUDED.processor_version,
                    enabled = EXCLUDED.enabled,
                    last_success_at = EXCLUDED.last_success_at,
                    last_attempt_at = EXCLUDED.last_attempt_at,
                    last_error = EXCLUDED.last_error,
                    pending_count = EXCLUDED.pending_count,
                    stale_count = EXCLUDED.stale_count,
                    failed_count = EXCLUDED.failed_count,
                    last_run_id = EXCLUDED.last_run_id,
                    updated_at = NOW()
                """,
                (
                    record.processor_key,
                    record.processor_version,
                    record.enabled,
                    record.last_success_at,
                    record.last_attempt_at,
                    record.last_error,
                    record.pending_count,
                    record.stale_count,
                    record.failed_count,
                    record.last_run_id,
                ),
            )
            return record
        meta = self._load_meta()
        states = [s for s in meta.get("state", []) if s.get("processor_key") != record.processor_key]
        states.append(record.to_dict())
        meta["state"] = states
        self._save_meta(meta)
        return record

    def get_input_state(self, processor_key: str, input_uid: str) -> ProcessorInputStateRecord | None:
        rows = self.get_input_states_for_uids([input_uid]).get(str(input_uid).strip())
        if not rows:
            return None
        return rows.get(processor_key)

    def get_input_states_for_uids(
        self,
        uids: list[str],
    ) -> dict[str, dict[str, ProcessorInputStateRecord]]:
        """One query: ``uid -> {processor_key: record}`` for the given UIDs."""

        wanted = [str(uid).strip() for uid in uids if str(uid).strip()]
        out: dict[str, dict[str, ProcessorInputStateRecord]] = {}
        if not wanted:
            return out
        if self._conn is not None:
            self.ensure_tables()
            chunk_size = 500
            for i in range(0, len(wanted), chunk_size):
                chunk = wanted[i : i + chunk_size]
                rows = self._conn.execute(
                    f"""
                    SELECT * FROM {self._schema}.processor_input_state
                    WHERE input_uid = ANY(%s)
                    """,
                    (chunk,),
                ).fetchall()
                for row in rows:
                    rec = _row_to_input_state(row)
                    out.setdefault(rec.input_uid, {})[rec.processor_key] = rec
            return out
        for item in self._load_meta().get("input_state", []):
            uid = str(item.get("input_uid") or "").strip()
            if uid not in wanted:
                continue
            rec = _row_to_input_state(item)
            out.setdefault(uid, {})[rec.processor_key] = rec
        return out

    def upsert_input_state(self, record: ProcessorInputStateRecord) -> ProcessorInputStateRecord:
        if self._conn is not None:
            self.ensure_tables()
            self._conn.execute(
                f"""
                INSERT INTO {self._schema}.processor_input_state (
                    processor_key, input_uid, input_hash, input_corpus_state,
                    processor_version, output_identity, output_uids,
                    status, skip_reason, error, last_run_id, updated_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s::jsonb,
                    %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (processor_key, input_uid) DO UPDATE SET
                    input_hash = EXCLUDED.input_hash,
                    input_corpus_state = EXCLUDED.input_corpus_state,
                    processor_version = EXCLUDED.processor_version,
                    output_identity = EXCLUDED.output_identity,
                    output_uids = EXCLUDED.output_uids,
                    status = EXCLUDED.status,
                    skip_reason = EXCLUDED.skip_reason,
                    error = EXCLUDED.error,
                    last_run_id = EXCLUDED.last_run_id,
                    updated_at = NOW()
                """,
                (
                    record.processor_key,
                    record.input_uid,
                    record.input_hash,
                    record.input_corpus_state,
                    record.processor_version,
                    record.output_identity,
                    json.dumps(record.output_uids),
                    record.status,
                    record.skip_reason,
                    record.error,
                    record.last_run_id,
                ),
            )
            return record
        meta = self._load_meta()
        items = [
            i
            for i in meta.get("input_state", [])
            if not (i.get("processor_key") == record.processor_key and i.get("input_uid") == record.input_uid)
        ]
        items.append(record.to_dict())
        meta["input_state"] = items[-5000:]
        self._save_meta(meta)
        return record

    def record_run(self, report: ProcessorRunReport) -> None:
        completed = report.completed_at or _format_ts(_utc_now())
        if self._conn is not None:
            self.ensure_tables()
            self._conn.execute(
                f"""
                INSERT INTO {self._schema}.processor_runs (
                    run_id, processor_key, processor_version, archive_instance, status,
                    input_count, dirty_count, stale_count, skipped_count, output_count,
                    skip_reasons, stale_reasons, engine_mode, ladder_gate, decision_run_id,
                    errors, warnings, started_at, completed_at, artifact_paths
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s, %s, %s::jsonb
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    stale_count = EXCLUDED.stale_count,
                    skipped_count = EXCLUDED.skipped_count,
                    output_count = EXCLUDED.output_count,
                    skip_reasons = EXCLUDED.skip_reasons,
                    stale_reasons = EXCLUDED.stale_reasons,
                    completed_at = EXCLUDED.completed_at,
                    artifact_paths = EXCLUDED.artifact_paths
                """,
                (
                    report.run_id,
                    report.processor_key,
                    report.processor_version,
                    report.archive_instance,
                    report.status,
                    report.input_count,
                    report.dirty_count,
                    report.stale_count,
                    report.skipped_count,
                    report.output_count,
                    json.dumps(report.skip_reasons),
                    json.dumps(report.stale_reasons),
                    report.engine_mode,
                    report.ladder_gate,
                    report.decision_run_id,
                    json.dumps(report.errors),
                    json.dumps(report.warnings),
                    report.started_at,
                    completed,
                    json.dumps(report.artifact_paths),
                ),
            )
        else:
            meta = self._load_meta()
            runs = [r for r in meta.get("runs", []) if r.get("run_id") != report.run_id]
            runs.append(report.to_dict())
            meta["runs"] = runs[-200:]
            self._save_meta(meta)

        state = self.get_state(report.processor_key) or ProcessorStateRecord(
            processor_key=report.processor_key,
            processor_version=report.processor_version,
        )
        now = _format_ts(_utc_now())
        state.last_attempt_at = now
        state.last_run_id = report.run_id
        state.processor_version = report.processor_version or state.processor_version
        state.pending_count = report.plan.pending_count
        state.stale_count = report.stale_count
        if report.status in (RUN_STATUS_SUCCESS, "partial"):
            state.last_success_at = now
            state.last_error = ""
        else:
            state.last_error = "; ".join(report.errors) or report.status
        self.upsert_state(state)

    def _receipt_pk(
        self,
        processor_key: str,
        processor_version: str,
        input_uid: str,
        input_revision: str,
        digest: str,
    ) -> tuple[str, str, str, str, str]:
        return (
            str(processor_key),
            str(processor_version),
            str(input_uid),
            str(input_revision),
            str(digest or ""),
        )

    def get_stored_receipt(
        self,
        processor_key: str,
        processor_version: str,
        input_uid: str,
        input_revision: str,
        digest: str = "",
    ) -> StoredReceipt | None:
        pk = self._receipt_pk(processor_key, processor_version, input_uid, input_revision, digest)
        if self._conn is not None:
            self.ensure_tables()
            row = self._conn.execute(
                f"""
                SELECT processor_key, processor_version, input_uid, input_revision,
                       dependency_receipt_digest, status, contract_status, outputs,
                       chunk_keys, embedding_spec, error_reason, dependency_reason,
                       receipt_payload, lease_owner, lease_expires_at, last_run_id, updated_at
                FROM {self._schema}.processor_receipts
                WHERE processor_key = %s AND processor_version = %s AND input_uid = %s
                  AND input_revision = %s AND dependency_receipt_digest = %s
                """,
                pk,
            ).fetchone()
            return _row_to_stored_receipt(row) if row else None
        for item in self._load_meta().get("receipts", []):
            receipt_raw = item.get("receipt") or {}
            if (
                str(receipt_raw.get("processor")) == pk[0]
                and str(receipt_raw.get("processor_version")) == pk[1]
                and str(receipt_raw.get("input_uid")) == pk[2]
                and str(receipt_raw.get("input_revision")) == pk[3]
                and str(item.get("dependency_receipt_digest") or "") == pk[4]
            ):
                return StoredReceipt(
                    receipt=OutputReceipt.from_payload(receipt_raw),
                    scheduler_status=str(item.get("scheduler_status") or ""),
                    lease_owner=str(item.get("lease_owner") or ""),
                    lease_expires_at=_format_ts(item.get("lease_expires_at")),
                    last_run_id=str(item.get("last_run_id") or ""),
                    updated_at=_format_ts(item.get("updated_at")),
                    dependency_receipt_digest=str(item.get("dependency_receipt_digest") or ""),
                )
        return None

    def get_head_receipt(self, processor_key: str, input_uid: str) -> StoredReceipt | None:
        if self._conn is not None:
            self.ensure_tables()
            head = self._conn.execute(
                f"""
                SELECT processor_version, input_revision, dependency_receipt_digest
                FROM {self._schema}.processor_receipt_heads
                WHERE processor_key = %s AND input_uid = %s
                """,
                (processor_key, input_uid),
            ).fetchone()
            if not head:
                return None
            if isinstance(head, dict):
                version = str(head.get("processor_version") or "")
                revision = str(head.get("input_revision") or "")
                digest = str(head.get("dependency_receipt_digest") or "")
            else:
                version, revision, digest = str(head[0] or ""), str(head[1] or ""), str(head[2] or "")
            return self.get_stored_receipt(processor_key, version, input_uid, revision, digest)
        for item in self._load_meta().get("receipt_heads", []):
            if str(item.get("processor_key")) == processor_key and str(item.get("input_uid")) == input_uid:
                return self.get_stored_receipt(
                    processor_key,
                    str(item.get("processor_version") or ""),
                    input_uid,
                    str(item.get("input_revision") or ""),
                    str(item.get("dependency_receipt_digest") or ""),
                )
        return None

    def get_receipts_for_uids(self, uids: list[str]) -> dict[str, dict[str, StoredReceipt]]:
        """Latest head receipt per ``(uid, processor_key)``."""

        wanted = [str(uid).strip() for uid in uids if str(uid).strip()]
        out: dict[str, dict[str, StoredReceipt]] = {}
        if not wanted:
            return out
        if self._conn is not None:
            self.ensure_tables()
            chunk_size = 500
            for i in range(0, len(wanted), chunk_size):
                chunk = wanted[i : i + chunk_size]
                rows = self._conn.execute(
                    f"""
                    SELECT h.processor_key, r.processor_version, r.input_uid, r.input_revision,
                           r.dependency_receipt_digest, r.status, r.contract_status, r.outputs,
                           r.chunk_keys, r.embedding_spec, r.error_reason, r.dependency_reason,
                           r.receipt_payload, r.lease_owner, r.lease_expires_at, r.last_run_id,
                           r.updated_at
                    FROM {self._schema}.processor_receipt_heads h
                    JOIN {self._schema}.processor_receipts r
                      ON r.processor_key = h.processor_key
                     AND r.input_uid = h.input_uid
                     AND r.processor_version = h.processor_version
                     AND r.input_revision = h.input_revision
                     AND r.dependency_receipt_digest = h.dependency_receipt_digest
                    WHERE h.input_uid = ANY(%s)
                    """,
                    (chunk,),
                ).fetchall()
                for row in rows:
                    stored = _row_to_stored_receipt(row)
                    out.setdefault(stored.receipt.input_uid, {})[stored.receipt.processor] = stored
            return out
        heads = {
            (str(item.get("processor_key")), str(item.get("input_uid"))): item
            for item in self._load_meta().get("receipt_heads", [])
        }
        for uid in wanted:
            for (key, head_uid), item in heads.items():
                if head_uid != uid:
                    continue
                stored = self.get_stored_receipt(
                    key,
                    str(item.get("processor_version") or ""),
                    uid,
                    str(item.get("input_revision") or ""),
                    str(item.get("dependency_receipt_digest") or ""),
                )
                if stored is not None:
                    out.setdefault(uid, {})[key] = stored
        return out

    def list_receipts(self, input_uid: str | None = None) -> list[StoredReceipt]:
        if self._conn is not None:
            self.ensure_tables()
            if input_uid:
                rows = self._conn.execute(
                    f"""
                    SELECT processor_key, processor_version, input_uid, input_revision,
                           dependency_receipt_digest, status, contract_status, outputs,
                           chunk_keys, embedding_spec, error_reason, dependency_reason,
                           receipt_payload, lease_owner, lease_expires_at, last_run_id, updated_at
                    FROM {self._schema}.processor_receipts
                    WHERE input_uid = %s
                    ORDER BY updated_at
                    """,
                    (input_uid,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    f"""
                    SELECT processor_key, processor_version, input_uid, input_revision,
                           dependency_receipt_digest, status, contract_status, outputs,
                           chunk_keys, embedding_spec, error_reason, dependency_reason,
                           receipt_payload, lease_owner, lease_expires_at, last_run_id, updated_at
                    FROM {self._schema}.processor_receipts
                    ORDER BY updated_at
                    """
                ).fetchall()
            return [_row_to_stored_receipt(row) for row in rows]
        items = []
        for item in self._load_meta().get("receipts", []):
            receipt_raw = item.get("receipt") or {}
            if input_uid and str(receipt_raw.get("input_uid")) != input_uid:
                continue
            items.append(
                StoredReceipt(
                    receipt=OutputReceipt.from_payload(receipt_raw),
                    scheduler_status=str(item.get("scheduler_status") or ""),
                    lease_owner=str(item.get("lease_owner") or ""),
                    lease_expires_at=_format_ts(item.get("lease_expires_at")),
                    last_run_id=str(item.get("last_run_id") or ""),
                    updated_at=_format_ts(item.get("updated_at")),
                    dependency_receipt_digest=str(item.get("dependency_receipt_digest") or ""),
                )
            )
        return items

    def _write_receipt_row(self, stored: StoredReceipt) -> None:
        receipt = stored.receipt
        digest = stored.dependency_receipt_digest
        payload = json.dumps(receipt.to_payload())
        outputs = json.dumps([item.to_payload() for item in receipt.outputs])
        chunk_keys = json.dumps(list(receipt.chunk_keys))
        spec = json.dumps(receipt.embedding_spec.to_payload()) if receipt.embedding_spec else None
        if self._conn is not None:
            self.ensure_tables()
            self._conn.execute(
                f"""
                INSERT INTO {self._schema}.processor_receipts (
                    processor_key, processor_version, input_uid, input_revision,
                    dependency_receipt_digest, status, contract_status, outputs,
                    chunk_keys, embedding_spec, error_reason, dependency_reason,
                    receipt_payload, lease_owner, lease_expires_at, last_run_id, updated_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s::jsonb,
                    %s::jsonb, %s::jsonb, %s, %s,
                    %s::jsonb, %s, %s, %s, NOW()
                )
                ON CONFLICT (
                    processor_key, processor_version, input_uid, input_revision,
                    dependency_receipt_digest
                ) DO UPDATE SET
                    status = EXCLUDED.status,
                    contract_status = EXCLUDED.contract_status,
                    outputs = EXCLUDED.outputs,
                    chunk_keys = EXCLUDED.chunk_keys,
                    embedding_spec = EXCLUDED.embedding_spec,
                    error_reason = EXCLUDED.error_reason,
                    dependency_reason = EXCLUDED.dependency_reason,
                    receipt_payload = EXCLUDED.receipt_payload,
                    lease_owner = EXCLUDED.lease_owner,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    last_run_id = EXCLUDED.last_run_id,
                    updated_at = NOW()
                """,
                (
                    receipt.processor,
                    receipt.processor_version,
                    receipt.input_uid,
                    receipt.input_revision,
                    digest,
                    stored.scheduler_status,
                    receipt.status,
                    outputs,
                    chunk_keys,
                    spec,
                    receipt.error_reason,
                    receipt.dependency_reason,
                    payload,
                    stored.lease_owner,
                    stored.lease_expires_at,
                    stored.last_run_id,
                ),
            )
            return
        meta = self._load_meta()
        receipts = [
            item
            for item in meta.get("receipts", [])
            if not (
                str((item.get("receipt") or {}).get("processor")) == receipt.processor
                and str((item.get("receipt") or {}).get("processor_version")) == receipt.processor_version
                and str((item.get("receipt") or {}).get("input_uid")) == receipt.input_uid
                and str((item.get("receipt") or {}).get("input_revision")) == receipt.input_revision
                and str(item.get("dependency_receipt_digest") or "") == digest
            )
        ]
        stored.updated_at = _format_ts(_utc_now())
        receipts.append(stored.to_meta())
        meta["receipts"] = receipts[-5000:]
        self._save_meta(meta)

    def _cas_head(
        self,
        *,
        processor_key: str,
        input_uid: str,
        processor_version: str,
        input_revision: str,
        digest: str,
        advance: bool,
    ) -> bool:
        """Compare-and-swap the current revision pointer.

        ``advance=True`` lets a newer input revision take the head (new work).
        ``advance=False`` succeeds only when the head already names this revision
        — an old worker cannot complete after the head moved.
        """

        if self._conn is not None:
            self.ensure_tables()
            if advance:
                self._conn.execute(
                    f"""
                    INSERT INTO {self._schema}.processor_receipt_heads (
                        processor_key, input_uid, processor_version, input_revision,
                        dependency_receipt_digest, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (processor_key, input_uid) DO UPDATE SET
                        processor_version = EXCLUDED.processor_version,
                        input_revision = EXCLUDED.input_revision,
                        dependency_receipt_digest = EXCLUDED.dependency_receipt_digest,
                        updated_at = NOW()
                    """,
                    (processor_key, input_uid, processor_version, input_revision, digest),
                )
                return True
            cur = self._conn.execute(
                f"""
                UPDATE {self._schema}.processor_receipt_heads
                SET processor_version = %s,
                    dependency_receipt_digest = %s,
                    updated_at = NOW()
                WHERE processor_key = %s AND input_uid = %s AND input_revision = %s
                """,
                (processor_version, digest, processor_key, input_uid, input_revision),
            )
            return bool(getattr(cur, "rowcount", 0))
        meta = self._load_meta()
        heads = list(meta.get("receipt_heads", []))
        current = next(
            (
                item
                for item in heads
                if item.get("processor_key") == processor_key and item.get("input_uid") == input_uid
            ),
            None,
        )
        if current is None:
            heads.append(
                {
                    "processor_key": processor_key,
                    "input_uid": input_uid,
                    "processor_version": processor_version,
                    "input_revision": input_revision,
                    "dependency_receipt_digest": digest,
                    "updated_at": _format_ts(_utc_now()),
                }
            )
            meta["receipt_heads"] = heads
            self._save_meta(meta)
            return True
        if advance or str(current.get("input_revision") or "") == input_revision:
            current["processor_version"] = processor_version
            current["input_revision"] = input_revision
            current["dependency_receipt_digest"] = digest
            current["updated_at"] = _format_ts(_utc_now())
            meta["receipt_heads"] = heads
            self._save_meta(meta)
            return True
        return False

    def acquire_lease(
        self,
        *,
        processor_key: str,
        processor_version: str,
        input_uid: str,
        input_revision: str,
        digest: str,
        owner: str,
        lease_seconds: int = LEASE_SECONDS_DEFAULT,
    ) -> bool:
        existing = self.get_stored_receipt(processor_key, processor_version, input_uid, input_revision, digest)
        if existing is not None and existing.scheduler_status in SUCCESS_RECEIPT_STATUSES:
            return False
        if (
            existing is not None
            and existing.scheduler_status == RECEIPT_STATUS_RUNNING
            and existing.lease_owner
            and existing.lease_owner != owner
        ):
            expiry = existing.lease_expires_at
            if expiry:
                try:
                    exp_dt = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
                except ValueError:
                    exp_dt = None
                if exp_dt is not None and exp_dt > _utc_now():
                    return False
        if not self._cas_head(
            processor_key=processor_key,
            input_uid=input_uid,
            processor_version=processor_version,
            input_revision=input_revision,
            digest=digest,
            advance=True,
        ):
            return False
        expires = _utc_now()
        from datetime import timedelta

        expires = expires + timedelta(seconds=int(lease_seconds))
        placeholder = OutputReceipt(
            processor=processor_key,
            processor_version=processor_version,
            input_uid=input_uid,
            input_revision=input_revision,
            status="pending",
        )
        self._write_receipt_row(
            StoredReceipt(
                receipt=placeholder,
                scheduler_status=RECEIPT_STATUS_RUNNING,
                lease_owner=owner,
                lease_expires_at=_format_ts(expires),
                last_run_id=owner,
                dependency_receipt_digest=digest,
            )
        )
        return True

    def commit_receipt(
        self,
        receipt: OutputReceipt,
        *,
        scheduler_status: str,
        digest: str,
        run_id: str,
        lease_owner: str = "",
    ) -> bool:
        """Persist a receipt only if this revision still owns the head pointer."""

        if not self._cas_head(
            processor_key=receipt.processor,
            input_uid=receipt.input_uid,
            processor_version=receipt.processor_version,
            input_revision=receipt.input_revision,
            digest=digest,
            advance=False,
        ):
            superseded = OutputReceipt(
                processor=receipt.processor,
                processor_version=receipt.processor_version,
                input_uid=receipt.input_uid,
                input_revision=receipt.input_revision,
                status="skipped",
                outputs=receipt.outputs,
                chunk_keys=receipt.chunk_keys,
                embedding_spec=receipt.embedding_spec,
                error_reason=receipt.error_reason or "superseded_by_newer_revision",
                dependency_reason=receipt.dependency_reason,
            )
            self._write_receipt_row(
                StoredReceipt(
                    receipt=superseded,
                    scheduler_status=RECEIPT_STATUS_SUPERSEDED,
                    lease_owner=lease_owner,
                    last_run_id=run_id,
                    dependency_receipt_digest=digest,
                )
            )
            return False
        self._write_receipt_row(
            StoredReceipt(
                receipt=receipt,
                scheduler_status=scheduler_status,
                lease_owner=lease_owner,
                last_run_id=run_id,
                dependency_receipt_digest=digest,
            )
        )
        return True

    def get_last_run(self, processor_key: str) -> dict[str, Any] | None:
        if self._conn is not None:
            self.ensure_tables()
            row = self._conn.execute(
                f"""
                SELECT * FROM {self._schema}.processor_runs
                WHERE processor_key = %s
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (processor_key,),
            ).fetchone()
            if not row:
                return None
            if isinstance(row, dict):
                return dict(row)
            return dict(zip(row.keys(), row, strict=False)) if hasattr(row, "keys") else None
        runs = [r for r in self._load_meta().get("runs", []) if r.get("processor_key") == processor_key]
        return runs[-1] if runs else None

    def _load_meta(self) -> dict[str, Any]:
        if self._meta_path is None or not self._meta_path.is_file():
            return {"state": [], "runs": [], "input_state": [], "receipts": [], "receipt_heads": []}
        try:
            data = json.loads(self._meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"state": [], "runs": [], "input_state": [], "receipts": [], "receipt_heads": []}
        if not isinstance(data, dict):
            return {"state": [], "runs": [], "input_state": [], "receipts": [], "receipt_heads": []}
        data.setdefault("receipts", [])
        data.setdefault("receipt_heads", [])
        return data

    def _save_meta(self, data: dict[str, Any]) -> None:
        if self._meta_path is None:
            return
        self._meta_path.parent.mkdir(parents=True, exist_ok=True)
        self._meta_path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
