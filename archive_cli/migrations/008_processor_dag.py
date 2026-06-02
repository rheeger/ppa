"""Migration 008: processor state and run history (Section E)."""

from __future__ import annotations

from archive_sync.processors.state_store import ensure_processor_tables

VERSION = 8
NAME = "processor_dag"


def upgrade(conn, schema: str) -> None:
    ensure_processor_tables(conn, schema)


def downgrade(conn, schema: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {schema}.processor_input_state")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.processor_runs")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.processor_state")
