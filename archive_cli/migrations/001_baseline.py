"""Baseline migration: marks the existing schema (INDEX_SCHEMA_VERSION=8,
CHUNK_SCHEMA_VERSION=4, PROJECTION_REGISTRY_VERSION=1) as the migration
starting point.

For fresh databases, _create_schema() builds everything and this migration
is marked as applied without running.  For existing databases (like Arnold
production), the schema already matches so the upgrade is a no-op verification
that the expected tables exist.
"""

VERSION = 1
NAME = "baseline_v8_chunk4_proj1"


def upgrade(conn, schema: str) -> None:
    """Verify core tables exist. No DDL changes — the schema is already at
    the level this baseline represents."""
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name IN ('cards', 'edges', 'chunks', 'embeddings', 'meta')
        """,
        (schema,),
    ).fetchone()
    count = row["cnt"] if isinstance(row, dict) else row[0]
    if count < 5:
        raise RuntimeError(
            f"Baseline migration expects at least 5 core tables in schema '{schema}', "
            f"found {count}. Run _create_schema() first for fresh databases."
        )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.note_manifest (
            rel_path TEXT PRIMARY KEY,
            content_hash TEXT NOT NULL,
            schema_version INTEGER NOT NULL DEFAULT 0,
            chunk_version INTEGER NOT NULL DEFAULT 0
        )
        """
    )


def downgrade(conn, schema: str) -> None:
    pass
