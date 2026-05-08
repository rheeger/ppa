"""Convert activity_at from TEXT to TIMESTAMPTZ, add activity_end_at,
quality columns, and infrastructure tables."""

from __future__ import annotations

VERSION = 2
NAME = "activity_at_timestamptz_and_infra"


def _activity_at_column_type(conn, schema: str) -> str | None:
    row = conn.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = 'cards' AND column_name = 'activity_at'
        """,
        (schema,),
    ).fetchone()
    if row is None:
        return None
    return str(row["data_type"] if isinstance(row, dict) else row[0])


def upgrade(conn, schema: str) -> None:
    dt = _activity_at_column_type(conn, schema)
    if dt == "timestamp with time zone":
        _ensure_cards_columns(conn, schema)
        _ensure_infra_tables(conn, schema)
        return

    if dt != "text" and dt is not None:
        raise RuntimeError(f"Unexpected cards.activity_at type {dt!r}; cannot migrate automatically.")

    conn.execute(f"ALTER TABLE {schema}.cards ADD COLUMN IF NOT EXISTS activity_at_tz TIMESTAMPTZ")
    conn.execute(
        f"""
        UPDATE {schema}.cards SET activity_at_tz = CASE
            WHEN activity_at::text ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}T' THEN activity_at::TIMESTAMPTZ
            WHEN activity_at::text ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN (activity_at::text || 'T00:00:00')::TIMESTAMPTZ
            ELSE NULL
        END
        """
    )
    conn.execute(f"ALTER TABLE {schema}.cards DROP COLUMN activity_at")
    conn.execute(f"ALTER TABLE {schema}.cards RENAME COLUMN activity_at_tz TO activity_at")

    _ensure_cards_columns(conn, schema)

    _ensure_infra_tables(conn, schema)


def _ensure_cards_columns(conn, schema: str) -> None:
    conn.execute(f"ALTER TABLE {schema}.cards ALTER COLUMN activity_at DROP NOT NULL")
    conn.execute(f"ALTER TABLE {schema}.cards ADD COLUMN IF NOT EXISTS activity_end_at TIMESTAMPTZ")

    conn.execute(
        f"ALTER TABLE {schema}.cards ADD COLUMN IF NOT EXISTS quality_score DOUBLE PRECISION NOT NULL DEFAULT 0.0"
    )
    conn.execute(f"ALTER TABLE {schema}.cards ADD COLUMN IF NOT EXISTS quality_flags TEXT[] NOT NULL DEFAULT '{{}}'")
    conn.execute(f"ALTER TABLE {schema}.cards ADD COLUMN IF NOT EXISTS enrichment_version INTEGER NOT NULL DEFAULT 0")
    conn.execute(f"ALTER TABLE {schema}.cards ADD COLUMN IF NOT EXISTS enrichment_status TEXT NOT NULL DEFAULT 'none'")
    conn.execute(f"ALTER TABLE {schema}.cards ADD COLUMN IF NOT EXISTS last_enriched_at TIMESTAMPTZ")

    conn.execute(f"DROP INDEX IF EXISTS {schema}.idx_cards_activity_at")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_cards_activity_at_uid ON {schema}.cards(activity_at, uid)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_cards_activity_end_at ON {schema}.cards(activity_end_at)")


def _ensure_infra_tables(conn, schema: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.ingestion_log (
            id BIGSERIAL PRIMARY KEY,
            card_uid TEXT NOT NULL,
            action TEXT NOT NULL,
            source_adapter TEXT NOT NULL,
            batch_id TEXT NOT NULL DEFAULT '',
            logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_ingestion_log_logged_at ON {schema}.ingestion_log(logged_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_ingestion_log_card_uid ON {schema}.ingestion_log(card_uid)")

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.enrichment_queue (
            id BIGSERIAL PRIMARY KEY,
            card_uid TEXT NOT NULL,
            task_type TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 100,
            status TEXT NOT NULL DEFAULT 'pending',
            queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            claimed_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            error_message TEXT DEFAULT '',
            attempts INTEGER DEFAULT 0
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_eq_status_priority ON {schema}.enrichment_queue(status, priority)")

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.retrieval_gaps (
            id BIGSERIAL PRIMARY KEY,
            query_text TEXT NOT NULL,
            gap_type TEXT NOT NULL,
            detail TEXT NOT NULL DEFAULT '',
            card_uid TEXT DEFAULT '',
            detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            resolved BOOLEAN NOT NULL DEFAULT FALSE,
            resolved_at TIMESTAMPTZ
        )
        """
    )


def downgrade(conn, schema: str) -> None:
    pass
