"""Migration 006: email corpus state + decision history for Section B apply/rollback."""

from __future__ import annotations

from archive_cli.corpus_hygiene.state_store import ensure_corpus_hygiene_tables

VERSION = 6
NAME = "email_corpus_state"


def upgrade(conn, schema: str) -> None:
    ensure_corpus_hygiene_tables(conn, schema)


def downgrade(conn, schema: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {schema}.email_corpus_decisions")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.card_corpus_state")
