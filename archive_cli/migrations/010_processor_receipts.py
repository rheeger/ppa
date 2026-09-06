"""Migration 010: revision-specific processor receipts (P03-A)."""

from __future__ import annotations

from archive_sync.processors.state_store import ensure_processor_receipt_tables

VERSION = 10
NAME = "processor_receipts"


def upgrade(conn, schema: str) -> None:
    ensure_processor_receipt_tables(conn, schema)


def downgrade(conn, schema: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {schema}.processor_receipt_heads")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.processor_receipts")
