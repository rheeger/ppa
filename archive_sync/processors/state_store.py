"""Durable processor state and run history."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .batch import ProcessorRunReport
from .constants import RUN_STATUS_SUCCESS


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
        if self._conn is not None:
            self.ensure_tables()
            row = self._conn.execute(
                f"""
                SELECT * FROM {self._schema}.processor_input_state
                WHERE processor_key = %s AND input_uid = %s
                """,
                (processor_key, input_uid),
            ).fetchone()
            return _row_to_input_state(row) if row else None
        for item in self._load_meta().get("input_state", []):
            if item.get("processor_key") == processor_key and item.get("input_uid") == input_uid:
                return _row_to_input_state(item)
        return None

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
            return {"state": [], "runs": [], "input_state": []}
        try:
            data = json.loads(self._meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"state": [], "runs": [], "input_state": []}
        return data if isinstance(data, dict) else {"state": [], "runs": [], "input_state": []}

    def _save_meta(self, data: dict[str, Any]) -> None:
        if self._meta_path is None:
            return
        self._meta_path.parent.mkdir(parents=True, exist_ok=True)
        self._meta_path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
