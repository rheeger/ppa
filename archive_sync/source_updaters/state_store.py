"""Durable source updater state and run history."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .batch import SourceUpdaterBatchSummary, SourceUpdaterRunReport
from .constants import RUN_STATUS_SUCCESS, STALENESS_NEVER_SYNCED
from .staleness import compute_staleness_state


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_ts(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat()
    return str(value).strip() or None


def ensure_source_updater_tables(conn: Any, schema: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.source_updater_state (
            source_key TEXT PRIMARY KEY,
            source_type TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            staleness_state TEXT NOT NULL DEFAULT '{STALENESS_NEVER_SYNCED}',
            cursor_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            last_success_at TIMESTAMPTZ,
            last_attempt_at TIMESTAMPTZ,
            last_error_at TIMESTAMPTZ,
            last_error TEXT NOT NULL DEFAULT '',
            last_run_id TEXT NOT NULL DEFAULT '',
            last_batch_summary JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            adapter_version TEXT NOT NULL DEFAULT '',
            policy_version TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.source_updater_runs (
            run_id TEXT PRIMARY KEY,
            source_key TEXT NOT NULL,
            source_type TEXT NOT NULL,
            archive_instance TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            cursor_before JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            cursor_after JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            observed INT NOT NULL DEFAULT 0,
            unchanged INT NOT NULL DEFAULT 0,
            promoted INT NOT NULL DEFAULT 0,
            suppressed INT NOT NULL DEFAULT 0,
            quarantined INT NOT NULL DEFAULT 0,
            updated INT NOT NULL DEFAULT 0,
            deleted_or_tombstoned INT NOT NULL DEFAULT 0,
            dirty_card_uids_count INT NOT NULL DEFAULT 0,
            errors JSONB NOT NULL DEFAULT '[]'::jsonb,
            warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ,
            artifact_paths JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            engine_mode TEXT NOT NULL DEFAULT '',
            ladder_gate TEXT NOT NULL DEFAULT '',
            decision_run_id TEXT NOT NULL DEFAULT '',
            adapter_version TEXT NOT NULL DEFAULT '',
            policy_version TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_source_updater_runs_source_started
        ON {schema}.source_updater_runs(source_key, started_at DESC)
        """
    )


@dataclass
class SourceUpdaterStateRecord:
    source_key: str
    source_type: str
    enabled: bool = True
    staleness_state: str = STALENESS_NEVER_SYNCED
    cursor_payload: dict[str, Any] = field(default_factory=dict)
    last_success_at: str | None = None
    last_attempt_at: str | None = None
    last_error_at: str | None = None
    last_error: str = ""
    last_run_id: str = ""
    last_batch_summary: dict[str, Any] = field(default_factory=dict)
    adapter_version: str = ""
    policy_version: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_key": self.source_key,
            "source_type": self.source_type,
            "enabled": self.enabled,
            "staleness_state": self.staleness_state,
            "cursor_payload": dict(self.cursor_payload),
            "last_success_at": self.last_success_at,
            "last_attempt_at": self.last_attempt_at,
            "last_error_at": self.last_error_at,
            "last_error": self.last_error,
            "last_run_id": self.last_run_id,
            "last_batch_summary": dict(self.last_batch_summary),
            "adapter_version": self.adapter_version,
            "policy_version": self.policy_version,
        }


def _row_to_state(row: Any) -> SourceUpdaterStateRecord:
    if isinstance(row, dict):
        data = row
    else:
        keys = (
            "source_key",
            "source_type",
            "enabled",
            "staleness_state",
            "cursor_payload",
            "last_success_at",
            "last_attempt_at",
            "last_error_at",
            "last_error",
            "last_run_id",
            "last_batch_summary",
            "adapter_version",
            "policy_version",
        )
        data = dict(zip(keys, row, strict=False))
    payload = data.get("cursor_payload") or {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    summary = data.get("last_batch_summary") or {}
    if isinstance(summary, str):
        summary = json.loads(summary)
    return SourceUpdaterStateRecord(
        source_key=str(data.get("source_key") or ""),
        source_type=str(data.get("source_type") or ""),
        enabled=bool(data.get("enabled", True)),
        staleness_state=str(data.get("staleness_state") or STALENESS_NEVER_SYNCED),
        cursor_payload=dict(payload) if isinstance(payload, dict) else {},
        last_success_at=_format_ts(data.get("last_success_at")),
        last_attempt_at=_format_ts(data.get("last_attempt_at")),
        last_error_at=_format_ts(data.get("last_error_at")),
        last_error=str(data.get("last_error") or ""),
        last_run_id=str(data.get("last_run_id") or ""),
        last_batch_summary=dict(summary) if isinstance(summary, dict) else {},
        adapter_version=str(data.get("adapter_version") or ""),
        policy_version=str(data.get("policy_version") or ""),
    )


class SourceUpdaterStateStore:
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
            ensure_source_updater_tables(self._conn, self._schema)

    def list_state(self) -> list[SourceUpdaterStateRecord]:
        if self._conn is not None:
            self.ensure_tables()
            rows = self._conn.execute(
                f"SELECT * FROM {self._schema}.source_updater_state ORDER BY source_key"
            ).fetchall()
            return [_row_to_state(r) for r in rows]
        return [_row_to_state(item) for item in self._load_meta().get("state", [])]

    def get_state(self, source_key: str) -> SourceUpdaterStateRecord | None:
        if self._conn is not None:
            self.ensure_tables()
            row = self._conn.execute(
                f"SELECT * FROM {self._schema}.source_updater_state WHERE source_key = %s",
                (source_key,),
            ).fetchone()
            return _row_to_state(row) if row else None
        for item in self._load_meta().get("state", []):
            if str(item.get("source_key")) == source_key:
                return _row_to_state(item)
        return None

    def upsert_state(
        self,
        record: SourceUpdaterStateRecord,
        *,
        last_run_status: str = "",
    ) -> SourceUpdaterStateRecord:
        record.staleness_state = compute_staleness_state(
            last_success_at=record.last_success_at,
            last_attempt_at=record.last_attempt_at,
            last_error=record.last_error,
            last_run_status=last_run_status,
            enabled=record.enabled,
        )
        if self._conn is not None:
            self.ensure_tables()
            self._conn.execute(
                f"""
                INSERT INTO {self._schema}.source_updater_state (
                    source_key, source_type, enabled, staleness_state, cursor_payload,
                    last_success_at, last_attempt_at, last_error_at, last_error,
                    last_run_id, last_batch_summary, adapter_version, policy_version, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s::jsonb,
                    %s, %s, %s, %s,
                    %s, %s::jsonb, %s, %s, NOW()
                )
                ON CONFLICT (source_key) DO UPDATE SET
                    source_type = EXCLUDED.source_type,
                    enabled = EXCLUDED.enabled,
                    staleness_state = EXCLUDED.staleness_state,
                    cursor_payload = EXCLUDED.cursor_payload,
                    last_success_at = EXCLUDED.last_success_at,
                    last_attempt_at = EXCLUDED.last_attempt_at,
                    last_error_at = EXCLUDED.last_error_at,
                    last_error = EXCLUDED.last_error,
                    last_run_id = EXCLUDED.last_run_id,
                    last_batch_summary = EXCLUDED.last_batch_summary,
                    adapter_version = EXCLUDED.adapter_version,
                    policy_version = EXCLUDED.policy_version,
                    updated_at = NOW()
                """,
                (
                    record.source_key,
                    record.source_type,
                    record.enabled,
                    record.staleness_state,
                    json.dumps(record.cursor_payload),
                    record.last_success_at,
                    record.last_attempt_at,
                    record.last_error_at,
                    record.last_error,
                    record.last_run_id,
                    json.dumps(record.last_batch_summary),
                    record.adapter_version,
                    record.policy_version,
                ),
            )
            return record
        meta = self._load_meta()
        states = [s for s in meta.get("state", []) if s.get("source_key") != record.source_key]
        states.append(record.to_dict())
        meta["state"] = states
        self._save_meta(meta)
        return record

    def record_run(self, report: SourceUpdaterRunReport) -> None:
        completed = report.completed_at or _format_ts(_utc_now())
        if self._conn is not None:
            self.ensure_tables()
            self._conn.execute(
                f"""
                INSERT INTO {self._schema}.source_updater_runs (
                    run_id, source_key, source_type, archive_instance, status,
                    cursor_before, cursor_after,
                    observed, unchanged, promoted, suppressed, quarantined,
                    updated, deleted_or_tombstoned, dirty_card_uids_count,
                    errors, warnings, started_at, completed_at,
                    artifact_paths, engine_mode, ladder_gate,
                    decision_run_id, adapter_version, policy_version
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s, %s,
                    %s::jsonb, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    cursor_after = EXCLUDED.cursor_after,
                    observed = EXCLUDED.observed,
                    unchanged = EXCLUDED.unchanged,
                    promoted = EXCLUDED.promoted,
                    suppressed = EXCLUDED.suppressed,
                    quarantined = EXCLUDED.quarantined,
                    updated = EXCLUDED.updated,
                    deleted_or_tombstoned = EXCLUDED.deleted_or_tombstoned,
                    dirty_card_uids_count = EXCLUDED.dirty_card_uids_count,
                    errors = EXCLUDED.errors,
                    warnings = EXCLUDED.warnings,
                    completed_at = EXCLUDED.completed_at,
                    artifact_paths = EXCLUDED.artifact_paths
                """,
                (
                    report.run_id,
                    report.source_key,
                    report.source_type,
                    report.archive_instance,
                    report.status,
                    json.dumps(report.cursor_before),
                    json.dumps(report.cursor_after),
                    report.batch.observed,
                    report.batch.unchanged,
                    report.batch.promoted,
                    report.batch.suppressed,
                    report.batch.quarantined,
                    report.batch.updated,
                    report.batch.deleted_or_tombstoned,
                    report.batch.dirty_card_uids_count,
                    json.dumps(report.errors),
                    json.dumps(report.warnings),
                    report.started_at,
                    completed,
                    json.dumps(report.artifact_paths),
                    report.engine_mode,
                    report.ladder_gate,
                    report.decision_run_id,
                    report.adapter_version,
                    report.policy_version,
                ),
            )
        else:
            meta = self._load_meta()
            runs = [r for r in meta.get("runs", []) if r.get("run_id") != report.run_id]
            runs.append(report.to_dict())
            meta["runs"] = runs[-200:]
            self._save_meta(meta)

        state = self.get_state(report.source_key) or SourceUpdaterStateRecord(
            source_key=report.source_key,
            source_type=report.source_type,
        )
        now = _format_ts(_utc_now())
        state.last_attempt_at = now
        state.last_run_id = report.run_id
        state.last_batch_summary = report.batch.to_dict()
        state.cursor_payload = dict(report.cursor_after)
        state.adapter_version = report.adapter_version or state.adapter_version
        state.policy_version = report.policy_version or state.policy_version
        if report.status in (RUN_STATUS_SUCCESS, "partial"):
            state.last_success_at = now
            state.last_error = ""
            state.last_error_at = None
        else:
            state.last_error = "; ".join(report.errors) or report.status
            state.last_error_at = now
        self.upsert_state(state, last_run_status=report.status)

    def get_last_run(self, source_key: str) -> dict[str, Any] | None:
        if self._conn is not None:
            self.ensure_tables()
            row = self._conn.execute(
                f"""
                SELECT * FROM {self._schema}.source_updater_runs
                WHERE source_key = %s
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (source_key,),
            ).fetchone()
            if not row:
                return None
            if isinstance(row, dict):
                return dict(row)
            return dict(zip(row.keys(), row, strict=False)) if hasattr(row, "keys") else None
        runs = [r for r in self._load_meta().get("runs", []) if r.get("source_key") == source_key]
        return runs[-1] if runs else None

    def _load_meta(self) -> dict[str, Any]:
        if self._meta_path is None or not self._meta_path.is_file():
            return {"state": [], "runs": []}
        try:
            data = json.loads(self._meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"state": [], "runs": []}
        return data if isinstance(data, dict) else {"state": [], "runs": []}

    def _save_meta(self, data: dict[str, Any]) -> None:
        if self._meta_path is None:
            return
        self._meta_path.parent.mkdir(parents=True, exist_ok=True)
        self._meta_path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def record_isolated_source_results(
    store: SourceUpdaterStateStore,
    reports: list[SourceUpdaterRunReport],
) -> list[str]:
    """Record each source run independently; failures on one do not block others."""

    errors: list[str] = []
    for report in reports:
        try:
            store.record_run(report)
        except Exception as exc:
            errors.append(f"{report.source_key}: {exc}")
    return errors
