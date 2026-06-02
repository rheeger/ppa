"""Migration 007: source updater state and run history (Section D)."""

from __future__ import annotations

from archive_sync.source_updaters.state_store import ensure_source_updater_tables

VERSION = 7
NAME = "source_updater_contract"


def upgrade(conn, schema: str) -> None:
    ensure_source_updater_tables(conn, schema)


def downgrade(conn, schema: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {schema}.source_updater_runs")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.source_updater_state")
