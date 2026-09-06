"""Migration 009: warehouse consumer checkpoints for the change journal."""

from __future__ import annotations

from archive_cli.change_consumers import ensure_change_consumer_tables

VERSION = 9
NAME = "change_consumers"


def upgrade(conn, schema: str) -> None:
    ensure_change_consumer_tables(conn, schema)


def downgrade(conn, schema: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {schema}.change_consumers")
