"""Warehouse-side consumer cursor tables (migration 009 + fresh bootstrap)."""

from __future__ import annotations

from archive_vault.change_journal import NAMED_CONSUMERS


def ensure_change_consumer_tables(conn, schema: str) -> None:
    """Create per-consumer warehouse cursors. Idempotent for bootstrap + migrate."""

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.change_consumers (
            consumer_name TEXT PRIMARY KEY,
            high_watermark BIGINT NOT NULL DEFAULT 0,
            snapshot_ref TEXT NOT NULL DEFAULT '',
            gaps_json TEXT NOT NULL DEFAULT '[]',
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    for name in NAMED_CONSUMERS:
        conn.execute(
            f"""
            INSERT INTO {schema}.change_consumers (consumer_name)
            VALUES (%s)
            ON CONFLICT (consumer_name) DO NOTHING
            """,
            (name,),
        )
