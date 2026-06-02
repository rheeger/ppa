"""Durable corpus state store for email hygiene apply/rollback (strategy 2)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

from .decisions import EmailCorpusDecisionRecord

CORPUS_STATE_ACTIVE = "active"
CORPUS_STATE_SUPPRESSED = "suppressed"
CORPUS_STATE_QUARANTINE = "quarantine"
NON_ACTIVE_CORPUS_STATES = frozenset({CORPUS_STATE_SUPPRESSED, CORPUS_STATE_QUARANTINE})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_corpus_hygiene_tables(conn: Any, schema: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.card_corpus_state (
            card_uid TEXT PRIMARY KEY,
            corpus_state TEXT NOT NULL DEFAULT 'active',
            decision_run_id TEXT NOT NULL DEFAULT '',
            previous_corpus_state TEXT NOT NULL DEFAULT 'active',
            policy_version TEXT NOT NULL DEFAULT '',
            applied_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_card_corpus_state_decision
        ON {schema}.card_corpus_state(decision_run_id)
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_card_corpus_state_state
        ON {schema}.card_corpus_state(corpus_state)
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.email_corpus_decisions (
            decision_run_id TEXT NOT NULL,
            thread_uid TEXT NOT NULL,
            gmail_thread_id TEXT NOT NULL DEFAULT '',
            corpus_decision TEXT NOT NULL,
            processor_decision TEXT NOT NULL,
            classification_source TEXT NOT NULL DEFAULT '',
            policy_version TEXT NOT NULL DEFAULT '',
            previous_corpus_state TEXT NOT NULL DEFAULT 'active',
            decision_reason TEXT NOT NULL DEFAULT '',
            decision_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            applied_at TIMESTAMPTZ,
            PRIMARY KEY (decision_run_id, thread_uid)
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_email_corpus_decisions_gmail
        ON {schema}.email_corpus_decisions(gmail_thread_id)
        """
    )


def corpus_state_table_exists(conn: Any, schema: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = 'card_corpus_state'
        LIMIT 1
        """,
        (schema,),
    ).fetchone()
    return row is not None


def active_corpus_sql_filter(*, schema: str, card_alias: str) -> str:
    """SQL fragment: include card only when corpus_state is active or unset."""

    return f"""
        NOT EXISTS (
            SELECT 1 FROM {schema}.card_corpus_state cs
            WHERE cs.card_uid = {card_alias}.uid
              AND cs.corpus_state IN ('suppressed', 'quarantine')
        )
    """


def get_card_corpus_state(conn: Any, schema: str, card_uid: str) -> str:
    row = conn.execute(
        f"SELECT corpus_state FROM {schema}.card_corpus_state WHERE card_uid = %s",
        (card_uid,),
    ).fetchone()
    if row is None:
        return CORPUS_STATE_ACTIVE
    return str(row["corpus_state"] if isinstance(row, dict) else row[0])


def is_card_retrieval_active(conn: Any, schema: str, card_uid: str) -> bool:
    if not corpus_state_table_exists(conn, schema):
        return True
    return get_card_corpus_state(conn, schema, card_uid) == CORPUS_STATE_ACTIVE


def card_uids_for_decision(record: EmailCorpusDecisionRecord) -> list[str]:
    uids: list[str] = []
    for uid in (record.thread_uid, *record.message_uids, *record.attachment_uids):
        if uid and uid not in uids:
            uids.append(uid)
    return uids


@dataclass
class ApplyCounts:
    cards_updated: int = 0
    threads_applied: int = 0
    by_corpus_state: dict[str, int] | None = None


def apply_decision_records(
    conn: Any,
    schema: str,
    records: list[EmailCorpusDecisionRecord],
    *,
    decision_run_id: str,
) -> ApplyCounts:
    ensure_corpus_hygiene_tables(conn, schema)
    counts = ApplyCounts(by_corpus_state={})
    now = _utc_now()

    for record in records:
        if record.decision_run_id != decision_run_id:
            continue
        target_state = record.corpus_decision
        for card_uid in card_uids_for_decision(record):
            previous = get_card_corpus_state(conn, schema, card_uid)
            conn.execute(
                f"""
                INSERT INTO {schema}.card_corpus_state (
                    card_uid, corpus_state, decision_run_id, previous_corpus_state,
                    policy_version, applied_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (card_uid) DO UPDATE SET
                    previous_corpus_state = EXCLUDED.previous_corpus_state,
                    corpus_state = EXCLUDED.corpus_state,
                    decision_run_id = EXCLUDED.decision_run_id,
                    policy_version = EXCLUDED.policy_version,
                    applied_at = EXCLUDED.applied_at,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    card_uid,
                    target_state,
                    decision_run_id,
                    previous,
                    record.policy_version or EMAIL_PROMOTION_POLICY_VERSION,
                    now,
                    now,
                ),
            )
            counts.cards_updated += 1
            assert counts.by_corpus_state is not None
            counts.by_corpus_state[target_state] = counts.by_corpus_state.get(target_state, 0) + 1

        conn.execute(
            f"""
            INSERT INTO {schema}.email_corpus_decisions (
                decision_run_id, thread_uid, gmail_thread_id, corpus_decision,
                processor_decision, classification_source, policy_version,
                previous_corpus_state, decision_reason, decision_payload, applied_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (decision_run_id, thread_uid) DO UPDATE SET
                corpus_decision = EXCLUDED.corpus_decision,
                processor_decision = EXCLUDED.processor_decision,
                classification_source = EXCLUDED.classification_source,
                policy_version = EXCLUDED.policy_version,
                previous_corpus_state = EXCLUDED.previous_corpus_state,
                decision_reason = EXCLUDED.decision_reason,
                decision_payload = EXCLUDED.decision_payload,
                applied_at = EXCLUDED.applied_at
            """,
            (
                decision_run_id,
                record.thread_uid,
                record.gmail_thread_id,
                record.corpus_decision,
                record.processor_decision,
                record.classification_source,
                record.policy_version,
                record.previous_corpus_state,
                record.decision_reason,
                json.dumps(record.to_dict(), sort_keys=True),
                now,
            ),
        )
        counts.threads_applied += 1

    conn.commit()
    return counts


@dataclass
class RollbackCounts:
    cards_restored: int = 0
    threads_restored: int = 0


def rollback_decision_run(conn: Any, schema: str, decision_run_id: str) -> RollbackCounts:
    ensure_corpus_hygiene_tables(conn, schema)
    counts = RollbackCounts()
    now = _utc_now()

    rows = conn.execute(
        f"""
        SELECT card_uid, previous_corpus_state
        FROM {schema}.card_corpus_state
        WHERE decision_run_id = %s
        """,
        (decision_run_id,),
    ).fetchall()

    for row in rows:
        card_uid = str(row["card_uid"] if isinstance(row, dict) else row[0])
        previous = str(row["previous_corpus_state"] if isinstance(row, dict) else row[1])
        if previous == CORPUS_STATE_ACTIVE:
            conn.execute(
                f"DELETE FROM {schema}.card_corpus_state WHERE card_uid = %s",
                (card_uid,),
            )
        else:
            conn.execute(
                f"""
                UPDATE {schema}.card_corpus_state
                SET corpus_state = %s,
                    decision_run_id = %s,
                    updated_at = %s
                WHERE card_uid = %s
                """,
                (previous, f"{decision_run_id}:rollback", now, card_uid),
            )
        counts.cards_restored += 1

    thread_rows = conn.execute(
        f"""
        SELECT COUNT(*) AS n FROM {schema}.email_corpus_decisions
        WHERE decision_run_id = %s
        """,
        (decision_run_id,),
    ).fetchone()
    counts.threads_restored = int(
        thread_rows["n"] if isinstance(thread_rows, dict) else thread_rows[0]
    )
    conn.commit()
    return counts
