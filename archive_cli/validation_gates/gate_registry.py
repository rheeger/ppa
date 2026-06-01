"""Durable v2.5 gate-run registry backed by Postgres."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .constants import (
    GATE_RUN_STATUS_PASSED,
    GATE_RUN_STATUS_PENDING,
    META_LAST_GATE_NAME,
    META_LAST_GATE_RUN_ID,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _row_value(row: Any, key: str) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


@dataclass(frozen=True, slots=True)
class GateRunRecord:
    run_id: str
    gate: str
    archive_instance: str
    vault_path: str
    index_schema: str
    engine_mode: str
    policy_version: str
    input_hash: str
    status: str
    reviewed: bool
    approved: bool
    report_path: str
    summary_path: str
    error: str
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    applied_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        def _iso(value: datetime | None) -> str | None:
            return value.isoformat() if value is not None else None

        return {
            "run_id": self.run_id,
            "gate": self.gate,
            "archive_instance": self.archive_instance,
            "vault_path": self.vault_path,
            "index_schema": self.index_schema,
            "engine_mode": self.engine_mode,
            "policy_version": self.policy_version,
            "input_hash": self.input_hash,
            "status": self.status,
            "reviewed": self.reviewed,
            "approved": self.approved,
            "report_path": self.report_path,
            "summary_path": self.summary_path,
            "error": self.error,
            "created_at": _iso(self.created_at),
            "started_at": _iso(self.started_at),
            "completed_at": _iso(self.completed_at),
            "applied_at": _iso(self.applied_at),
        }


def ensure_gate_runs_table(conn: Any, schema: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.gate_runs (
            run_id TEXT PRIMARY KEY,
            gate TEXT NOT NULL,
            archive_instance TEXT NOT NULL,
            vault_path TEXT NOT NULL DEFAULT '',
            index_schema TEXT NOT NULL DEFAULT '',
            engine_mode TEXT NOT NULL DEFAULT 'rust',
            policy_version TEXT NOT NULL DEFAULT '',
            input_hash TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            reviewed BOOLEAN NOT NULL DEFAULT FALSE,
            approved BOOLEAN NOT NULL DEFAULT FALSE,
            report_path TEXT NOT NULL DEFAULT '',
            summary_path TEXT NOT NULL DEFAULT '',
            error TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            applied_at TIMESTAMPTZ
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_gate_runs_gate_instance ON {schema}.gate_runs(gate, archive_instance, created_at DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_gate_runs_status ON {schema}.gate_runs(status, gate)"
    )


def _record_from_row(row: Any) -> GateRunRecord:
    return GateRunRecord(
        run_id=str(_row_value(row, "run_id") or ""),
        gate=str(_row_value(row, "gate") or ""),
        archive_instance=str(_row_value(row, "archive_instance") or ""),
        vault_path=str(_row_value(row, "vault_path") or ""),
        index_schema=str(_row_value(row, "index_schema") or ""),
        engine_mode=str(_row_value(row, "engine_mode") or ""),
        policy_version=str(_row_value(row, "policy_version") or ""),
        input_hash=str(_row_value(row, "input_hash") or ""),
        status=str(_row_value(row, "status") or ""),
        reviewed=bool(_row_value(row, "reviewed")),
        approved=bool(_row_value(row, "approved")),
        report_path=str(_row_value(row, "report_path") or ""),
        summary_path=str(_row_value(row, "summary_path") or ""),
        error=str(_row_value(row, "error") or ""),
        created_at=_row_value(row, "created_at"),
        started_at=_row_value(row, "started_at"),
        completed_at=_row_value(row, "completed_at"),
        applied_at=_row_value(row, "applied_at"),
    )


class GateRegistry:
    """Query and mutate v2.5 gate evidence in Postgres."""

    def __init__(self, conn: Any, schema: str):
        self.conn = conn
        self.schema = schema

    def ensure_table(self) -> None:
        ensure_gate_runs_table(self.conn, self.schema)

    def create_run(
        self,
        *,
        gate: str,
        archive_instance: str,
        vault_path: str,
        index_schema: str,
        engine_mode: str,
        policy_version: str = "",
        input_hash: str = "",
        run_id: str | None = None,
        status: str = GATE_RUN_STATUS_PENDING,
        reviewed: bool = False,
        approved: bool = False,
        report_path: str = "",
        summary_path: str = "",
    ) -> GateRunRecord:
        self.ensure_table()
        rid = run_id or f"gate-{uuid.uuid4().hex[:12]}"
        self.conn.execute(
            f"""
            INSERT INTO {self.schema}.gate_runs (
                run_id, gate, archive_instance, vault_path, index_schema,
                engine_mode, policy_version, input_hash, status, reviewed, approved,
                report_path, summary_path, started_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                rid,
                gate,
                archive_instance,
                vault_path,
                index_schema,
                engine_mode,
                policy_version,
                input_hash,
                status,
                reviewed,
                approved,
                report_path,
                summary_path,
            ),
        )
        self.conn.commit()
        row = self.get_run(rid)
        assert row is not None
        return row

    def complete_run(
        self,
        run_id: str,
        *,
        status: str,
        reviewed: bool | None = None,
        approved: bool | None = None,
        report_path: str = "",
        summary_path: str = "",
        error: str = "",
        applied: bool = False,
    ) -> GateRunRecord | None:
        self.ensure_table()
        sets = ["status = %s", "completed_at = NOW()", "error = %s"]
        params: list[Any] = [status, error]
        if reviewed is not None:
            sets.append("reviewed = %s")
            params.append(reviewed)
        if approved is not None:
            sets.append("approved = %s")
            params.append(approved)
        if report_path:
            sets.append("report_path = %s")
            params.append(report_path)
        if summary_path:
            sets.append("summary_path = %s")
            params.append(summary_path)
        if applied:
            sets.append("applied_at = NOW()")
        params.append(run_id)
        self.conn.execute(
            f"UPDATE {self.schema}.gate_runs SET {', '.join(sets)} WHERE run_id = %s",
            tuple(params),
        )
        self.conn.commit()
        run = self.get_run(run_id)
        if status == GATE_RUN_STATUS_PASSED and run is not None:
            self._set_meta_watermark(run_id=run_id, gate=run.gate)
        return run

    def get_run(self, run_id: str) -> GateRunRecord | None:
        self.ensure_table()
        row = self.conn.execute(
            f"""
            SELECT run_id, gate, archive_instance, vault_path, index_schema,
                   engine_mode, policy_version, input_hash, status, reviewed, approved,
                   report_path, summary_path, error, created_at, started_at, completed_at, applied_at
            FROM {self.schema}.gate_runs
            WHERE run_id = %s
            """,
            (run_id,),
        ).fetchone()
        return _record_from_row(row) if row else None

    def latest_passed(self, *, gate: str, archive_instance: str) -> GateRunRecord | None:
        self.ensure_table()
        row = self.conn.execute(
            f"""
            SELECT run_id, gate, archive_instance, vault_path, index_schema,
                   engine_mode, policy_version, input_hash, status, reviewed, approved,
                   report_path, summary_path, error, created_at, started_at, completed_at, applied_at
            FROM {self.schema}.gate_runs
            WHERE gate = %s AND archive_instance = %s AND status = %s
            ORDER BY completed_at DESC NULLS LAST, created_at DESC
            LIMIT 1
            """,
            (gate, archive_instance, GATE_RUN_STATUS_PASSED),
        ).fetchone()
        return _record_from_row(row) if row else None

    def has_passed_gate(self, *, gate: str, archive_instance: str) -> bool:
        return self.latest_passed(gate=gate, archive_instance=archive_instance) is not None

    def list_runs(self, *, archive_instance: str | None = None, limit: int = 50) -> list[GateRunRecord]:
        self.ensure_table()
        if archive_instance:
            rows = self.conn.execute(
                f"""
                SELECT run_id, gate, archive_instance, vault_path, index_schema,
                       engine_mode, policy_version, input_hash, status, reviewed, approved,
                       report_path, summary_path, error, created_at, started_at, completed_at, applied_at
                FROM {self.schema}.gate_runs
                WHERE archive_instance = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (archive_instance, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                f"""
                SELECT run_id, gate, archive_instance, vault_path, index_schema,
                       engine_mode, policy_version, input_hash, status, reviewed, approved,
                       report_path, summary_path, error, created_at, started_at, completed_at, applied_at
                FROM {self.schema}.gate_runs
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [_record_from_row(row) for row in rows]

    def _set_meta_watermark(self, *, run_id: str, gate: str) -> None:
        self.conn.execute(
            f"""
            INSERT INTO {self.schema}.meta (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            (META_LAST_GATE_RUN_ID, run_id),
        )
        self.conn.execute(
            f"""
            INSERT INTO {self.schema}.meta (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            (META_LAST_GATE_NAME, gate),
        )
        self.conn.commit()
