"""Migration 011: persist content identity on embeddings for reuse after rematerialize."""

from __future__ import annotations

VERSION = 11
NAME = "embedding_content_identity"


def upgrade(conn, schema: str) -> None:
    conn.execute("SET statement_timeout = 0")
    conn.execute(
        f"""
        ALTER TABLE {schema}.embeddings
            ADD COLUMN IF NOT EXISTS content_hash TEXT NOT NULL DEFAULT '',
            ADD COLUMN IF NOT EXISTS card_uid TEXT NOT NULL DEFAULT '',
            ADD COLUMN IF NOT EXISTS chunk_type TEXT NOT NULL DEFAULT '',
            ADD COLUMN IF NOT EXISTS chunk_index INTEGER NOT NULL DEFAULT 0
        """
    )
    conn.execute(
        f"""
        UPDATE {schema}.embeddings e
        SET
            content_hash = c.content_hash,
            card_uid = c.card_uid,
            chunk_type = c.chunk_type,
            chunk_index = c.chunk_index
        FROM {schema}.chunks c
        WHERE e.chunk_key = c.chunk_key
          AND (e.content_hash = '' OR e.card_uid = '')
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_embeddings_content_hash
        ON {schema}.embeddings (content_hash, embedding_model, embedding_version)
        WHERE content_hash <> ''
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_embeddings_slot
        ON {schema}.embeddings (card_uid, chunk_type, chunk_index)
        WHERE card_uid <> ''
        """
    )


def downgrade(conn, schema: str) -> None:
    conn.execute(f"DROP INDEX IF EXISTS {schema}.idx_embeddings_slot")
    conn.execute(f"DROP INDEX IF EXISTS {schema}.idx_embeddings_content_hash")
    conn.execute(
        f"""
        ALTER TABLE {schema}.embeddings
            DROP COLUMN IF EXISTS chunk_index,
            DROP COLUMN IF EXISTS chunk_type,
            DROP COLUMN IF EXISTS card_uid,
            DROP COLUMN IF EXISTS content_hash
        """
    )
