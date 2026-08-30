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
REMOVAL_CORPUS_STATES = frozenset({CORPUS_STATE_SUPPRESSED})
QUARANTINE_RETRIEVAL_WEIGHT = 0.35


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


def normalize_corpus_state(value: Any) -> str:
    cleaned = str(value or "").strip()
    if cleaned == CORPUS_STATE_QUARANTINE:
        return CORPUS_STATE_QUARANTINE
    if cleaned == CORPUS_STATE_SUPPRESSED:
        return CORPUS_STATE_SUPPRESSED
    return CORPUS_STATE_ACTIVE


def retrieval_weight_for_corpus_state(state: Any) -> float:
    if normalize_corpus_state(state) == CORPUS_STATE_QUARANTINE:
        return QUARANTINE_RETRIEVAL_WEIGHT
    return 1.0


def annotate_retrieval_corpus_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Stamp ``corpus_state`` and ``retrieval_weight`` onto a search row."""

    state = normalize_corpus_state(row.get("corpus_state"))
    row["corpus_state"] = state
    row["retrieval_weight"] = retrieval_weight_for_corpus_state(state)
    return row


def corpus_state_sql_expr(*, schema: str, card_alias: str) -> str:
    """COALESCE subquery: card_corpus_state row, else ``active``."""

    return (
        f"COALESCE((SELECT cs.corpus_state FROM {schema}.card_corpus_state cs "
        f"WHERE cs.card_uid = {card_alias}.uid), '{CORPUS_STATE_ACTIVE}')"
    )


def active_corpus_sql_filter(*, schema: str, card_alias: str) -> str:
    """SQL fragment: hide suppressed cards. Quarantine stays retrievable."""

    return f"""
        NOT EXISTS (
            SELECT 1 FROM {schema}.card_corpus_state cs
            WHERE cs.card_uid = {card_alias}.uid
              AND cs.corpus_state = '{CORPUS_STATE_SUPPRESSED}'
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
    """True unless the card is suppressed. Quarantine remains retrievable."""

    if not corpus_state_table_exists(conn, schema):
        return True
    return get_card_corpus_state(conn, schema, card_uid) != CORPUS_STATE_SUPPRESSED


def card_uids_for_decision(record: EmailCorpusDecisionRecord) -> list[str]:
    """Thread + message + attachment + derived UIDs for one record (set-deduped)."""

    seen: set[str] = set()
    uids: list[str] = []
    for uid in (
        record.thread_uid,
        *record.message_uids,
        *record.attachment_uids,
        *record.derived_uids,
    ):
        if uid and uid not in seen:
            seen.add(uid)
            uids.append(uid)
    return uids


def _uids_for_corpus_states(
    records: list[EmailCorpusDecisionRecord],
    states: frozenset[str],
) -> list[str]:
    seen: set[str] = set()
    uids: list[str] = []
    for record in records:
        if record.corpus_decision not in states:
            continue
        for uid in card_uids_for_decision(record):
            if uid and uid not in seen:
                seen.add(uid)
                uids.append(uid)
    return uids


def all_card_uids_for_records(records: list[EmailCorpusDecisionRecord]) -> list[str]:
    """All card UIDs across *records* (set-deduped, first-seen order)."""

    seen: set[str] = set()
    uids: list[str] = []
    for record in records:
        for uid in card_uids_for_decision(record):
            if uid and uid not in seen:
                seen.add(uid)
                uids.append(uid)
    return uids


def removal_uids_for_records(records: list[EmailCorpusDecisionRecord]) -> list[str]:
    """UIDs to vault-remove / purge: suppressed only. Quarantine stays on disk."""

    return _uids_for_corpus_states(records, REMOVAL_CORPUS_STATES)


def quarantine_uids_for_records(records: list[EmailCorpusDecisionRecord]) -> list[str]:
    """UIDs belonging to quarantine decisions (thread + message + derived + attach)."""

    return _uids_for_corpus_states(records, frozenset({CORPUS_STATE_QUARANTINE}))


def rel_paths_for_card_uids(conn: Any, schema: str, card_uids: list[str]) -> dict[str, str]:
    """Resolve note paths from the index ``cards`` table (no vault walk)."""

    if not card_uids:
        return {}
    rows = conn.execute(
        f"SELECT uid AS card_uid, rel_path FROM {schema}.cards WHERE uid = ANY(%s)",
        (list(card_uids),),
    ).fetchall()
    out: dict[str, str] = {}
    for row in rows:
        uid = str(row["card_uid"] if isinstance(row, dict) else row[0])
        rel = str(row["rel_path"] if isinstance(row, dict) else row[1])
        if uid and rel:
            out[uid] = rel
    return out


def purge_card_uids(conn: Any, schema: str, card_uids: list[str]) -> int:
    """Incrementally delete cards, chunks, embeddings, and projections for *card_uids*.

    Never escalates to a full rebuild and never rebuilds IVFFlat.
    """

    uid_list = [uid for uid in dict.fromkeys(card_uids) if uid]
    if not uid_list:
        return 0
    conn.execute(
        f"""
        DELETE FROM {schema}.embeddings
        WHERE chunk_key IN (
            SELECT chunk_key FROM {schema}.chunks WHERE card_uid = ANY(%s)
        )
        """,
        (uid_list,),
    )
    conn.execute(f"DELETE FROM {schema}.chunks WHERE card_uid = ANY(%s)", (uid_list,))
    conn.execute(
        f"DELETE FROM {schema}.edges WHERE source_uid = ANY(%s) OR target_uid = ANY(%s)",
        (uid_list, uid_list),
    )
    from archive_cli.projections.registry import TYPED_PROJECTIONS

    for projection in TYPED_PROJECTIONS:
        conn.execute(
            f"DELETE FROM {schema}.{projection.table_name} WHERE card_uid = ANY(%s)",
            (uid_list,),
        )
    conn.execute(f"DELETE FROM {schema}.external_ids WHERE card_uid = ANY(%s)", (uid_list,))
    conn.execute(f"DELETE FROM {schema}.card_orgs WHERE card_uid = ANY(%s)", (uid_list,))
    conn.execute(f"DELETE FROM {schema}.card_people WHERE card_uid = ANY(%s)", (uid_list,))
    conn.execute(f"DELETE FROM {schema}.card_sources WHERE card_uid = ANY(%s)", (uid_list,))
    conn.execute(f"DELETE FROM {schema}.note_manifest WHERE card_uid = ANY(%s)", (uid_list,))
    conn.execute(f"DELETE FROM {schema}.cards WHERE uid = ANY(%s)", (uid_list,))
    conn.commit()
    return len(uid_list)


@dataclass
class ApplyCounts:
    cards_updated: int = 0
    threads_applied: int = 0
    by_corpus_state: dict[str, int] | None = None
    files_deleted: int = 0
    uids_purged: int = 0
    ledger_records_appended: int = 0
    rollback_kit_files: int = 0


def _bulk_corpus_states(conn: Any, schema: str, card_uids: list[str]) -> dict[str, str]:
    """One SELECT for existing corpus states (no per-UID round trip)."""

    if not card_uids:
        return {}
    rows = conn.execute(
        f"""
        SELECT card_uid, corpus_state
        FROM {schema}.card_corpus_state
        WHERE card_uid = ANY(%s)
        """,
        (card_uids,),
    ).fetchall()
    out: dict[str, str] = {}
    for row in rows:
        uid = str(row["card_uid"] if isinstance(row, dict) else row[0])
        state = str(row["corpus_state"] if isinstance(row, dict) else row[1])
        out[uid] = state
    return out


def _copy_rows(conn: Any, table_name: str, columns: tuple[str, ...], rows: list[tuple[Any, ...]]) -> None:
    """COPY FROM STDIN into an already-created (temp) table."""

    if not rows:
        return
    col_sql = ", ".join(columns)
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table_name} ({col_sql}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)


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

    matching = [record for record in records if record.decision_run_id == decision_run_id]
    if not matching:
        conn.commit()
        return counts

    # Last write wins for a card_uid that appears on multiple threads.
    card_by_uid: dict[str, tuple[str, str]] = {}
    for record in matching:
        target_state = record.corpus_decision
        policy = record.policy_version or EMAIL_PROMOTION_POLICY_VERSION
        for card_uid in card_uids_for_decision(record):
            card_by_uid[card_uid] = (target_state, policy)

    previous_by_uid = _bulk_corpus_states(conn, schema, list(card_by_uid))
    conn.execute(
        """
        CREATE TEMP TABLE _ccs_stage (
            card_uid TEXT PRIMARY KEY,
            corpus_state TEXT NOT NULL,
            decision_run_id TEXT NOT NULL,
            previous_corpus_state TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            applied_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ
        ) ON COMMIT DROP
        """
    )
    ccs_rows = [
        (
            card_uid,
            target_state,
            decision_run_id,
            previous_by_uid.get(card_uid, CORPUS_STATE_ACTIVE),
            policy,
            now,
            now,
        )
        for card_uid, (target_state, policy) in card_by_uid.items()
    ]
    _copy_rows(
        conn,
        "_ccs_stage",
        (
            "card_uid",
            "corpus_state",
            "decision_run_id",
            "previous_corpus_state",
            "policy_version",
            "applied_at",
            "updated_at",
        ),
        ccs_rows,
    )
    conn.execute(
        f"""
        INSERT INTO {schema}.card_corpus_state (
            card_uid, corpus_state, decision_run_id, previous_corpus_state,
            policy_version, applied_at, updated_at
        )
        SELECT
            card_uid, corpus_state, decision_run_id, previous_corpus_state,
            policy_version, applied_at, updated_at
        FROM _ccs_stage
        ON CONFLICT (card_uid) DO UPDATE SET
            previous_corpus_state = EXCLUDED.previous_corpus_state,
            corpus_state = EXCLUDED.corpus_state,
            decision_run_id = EXCLUDED.decision_run_id,
            policy_version = EXCLUDED.policy_version,
            applied_at = EXCLUDED.applied_at,
            updated_at = EXCLUDED.updated_at
        """
    )
    counts.cards_updated = len(ccs_rows)
    assert counts.by_corpus_state is not None
    for _uid, (target_state, _policy) in card_by_uid.items():
        counts.by_corpus_state[target_state] = counts.by_corpus_state.get(target_state, 0) + 1

    conn.execute(
        """
        CREATE TEMP TABLE _ecd_stage (
            decision_run_id TEXT NOT NULL,
            thread_uid TEXT NOT NULL,
            gmail_thread_id TEXT NOT NULL,
            corpus_decision TEXT NOT NULL,
            processor_decision TEXT NOT NULL,
            classification_source TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            previous_corpus_state TEXT NOT NULL,
            decision_reason TEXT NOT NULL,
            decision_payload TEXT NOT NULL,
            applied_at TIMESTAMPTZ,
            PRIMARY KEY (decision_run_id, thread_uid)
        ) ON COMMIT DROP
        """
    )
    ecd_rows = [
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
        )
        for record in matching
    ]
    _copy_rows(
        conn,
        "_ecd_stage",
        (
            "decision_run_id",
            "thread_uid",
            "gmail_thread_id",
            "corpus_decision",
            "processor_decision",
            "classification_source",
            "policy_version",
            "previous_corpus_state",
            "decision_reason",
            "decision_payload",
            "applied_at",
        ),
        ecd_rows,
    )
    conn.execute(
        f"""
        INSERT INTO {schema}.email_corpus_decisions (
            decision_run_id, thread_uid, gmail_thread_id, corpus_decision,
            processor_decision, classification_source, policy_version,
            previous_corpus_state, decision_reason, decision_payload, applied_at
        )
        SELECT
            decision_run_id, thread_uid, gmail_thread_id, corpus_decision,
            processor_decision, classification_source, policy_version,
            previous_corpus_state, decision_reason, decision_payload::jsonb, applied_at
        FROM _ecd_stage
        ON CONFLICT (decision_run_id, thread_uid) DO UPDATE SET
            corpus_decision = EXCLUDED.corpus_decision,
            processor_decision = EXCLUDED.processor_decision,
            classification_source = EXCLUDED.classification_source,
            policy_version = EXCLUDED.policy_version,
            previous_corpus_state = EXCLUDED.previous_corpus_state,
            decision_reason = EXCLUDED.decision_reason,
            decision_payload = EXCLUDED.decision_payload,
            applied_at = EXCLUDED.applied_at
        """
    )
    counts.threads_applied = len(ecd_rows)

    conn.commit()
    return counts


@dataclass
class RollbackCounts:
    cards_restored: int = 0
    threads_restored: int = 0
    kit_files_restored: int = 0


def rollback_decision_run(conn: Any, schema: str, decision_run_id: str) -> RollbackCounts:
    ensure_corpus_hygiene_tables(conn, schema)
    counts = RollbackCounts()
    now = _utc_now()

    count_row = conn.execute(
        f"""
        SELECT COUNT(*) AS n FROM {schema}.card_corpus_state
        WHERE decision_run_id = %s
        """,
        (decision_run_id,),
    ).fetchone()
    counts.cards_restored = int(count_row["n"] if isinstance(count_row, dict) else count_row[0])

    conn.execute(
        f"""
        UPDATE {schema}.card_corpus_state
        SET corpus_state = previous_corpus_state,
            decision_run_id = %s,
            updated_at = %s
        WHERE decision_run_id = %s
          AND previous_corpus_state <> %s
        """,
        (f"{decision_run_id}:rollback", now, decision_run_id, CORPUS_STATE_ACTIVE),
    )
    conn.execute(
        f"""
        DELETE FROM {schema}.card_corpus_state
        WHERE decision_run_id = %s
          AND previous_corpus_state = %s
        """,
        (decision_run_id, CORPUS_STATE_ACTIVE),
    )

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
