"""Migration 003: semantic linker support — embedding_score on link_decisions."""

from __future__ import annotations

VERSION = 3
NAME = "semantic_linker"


def upgrade(conn, schema: str) -> None:
    conn.execute(
        f"""
        ALTER TABLE {schema}.link_decisions
        ADD COLUMN IF NOT EXISTS embedding_score DOUBLE PRECISION NOT NULL DEFAULT 0
        """
    )


def downgrade(conn, schema: str) -> None:
    conn.execute(
        f"""
        ALTER TABLE {schema}.link_decisions
        DROP COLUMN IF EXISTS embedding_score
        """
    )
