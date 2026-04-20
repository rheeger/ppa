"""Migration 004: decouple embeddings from chunks lifecycle.

Embeddings are content-addressable (``chunk_key`` is a stable hash of
``(card_uid, chunk_type, chunk_index, content_hash)`` derived from the chunk's
content). Coupling them to ``chunks`` row-lifecycle via a CASCADE FK meant
every full rebuild had to re-pay OpenAI to regenerate millions of vectors
that were already valid. After this migration, dropping or rebuilding chunks
no longer touches embeddings — orphaned embedding rows are pruned by the
explicit ``ppa embed-gc`` step, which the operator runs on demand.

This migration is safe to apply multiple times; it only drops the FK if it
exists.
"""

from __future__ import annotations

VERSION = 4
NAME = "embeddings_decouple_from_chunks"


def upgrade(conn, schema: str) -> None:
    rows = conn.execute(
        """
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = (%s::regclass)
          AND contype = 'f'
        """,
        (f"{schema}.embeddings",),
    ).fetchall()
    for row in rows:
        name = row["conname"] if isinstance(row, dict) else row[0]
        conn.execute(f'ALTER TABLE {schema}.embeddings DROP CONSTRAINT IF EXISTS "{name}"')


def downgrade(conn, schema: str) -> None:
    """Restore the FK. ``ON DELETE NO ACTION`` is intentional — re-adding the
    historical ``CASCADE`` would silently re-introduce the embedding-wipe
    behavior that this migration was created to prevent.
    """
    conn.execute(
        f"""
        ALTER TABLE {schema}.embeddings
        ADD CONSTRAINT embeddings_chunk_key_fkey
        FOREIGN KEY (chunk_key) REFERENCES {schema}.chunks(chunk_key) ON DELETE NO ACTION
        """
    )
