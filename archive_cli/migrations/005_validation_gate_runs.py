"""Migration 005: validation ladder gate-run registry."""

from __future__ import annotations

from archive_cli.validation_gates.gate_registry import ensure_gate_runs_table

VERSION = 5
NAME = "validation_gate_runs"


def upgrade(conn, schema: str) -> None:
    ensure_gate_runs_table(conn, schema)


def downgrade(conn, schema: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {schema}.gate_runs")
