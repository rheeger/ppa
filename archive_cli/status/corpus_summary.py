"""Read-only corpus hygiene summaries from Section B stores."""

from __future__ import annotations

from typing import Any

from archive_cli.corpus_hygiene.constants import CLASSIFICATION_SOURCES
from archive_cli.corpus_hygiene.state_store import (
    CORPUS_STATE_ACTIVE,
    CORPUS_STATE_QUARANTINE,
    CORPUS_STATE_SUPPRESSED,
    corpus_state_table_exists,
)
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION


def _row_count(row: Any) -> int:
    if row is None:
        return 0
    if isinstance(row, dict):
        return int(row.get("n") or row.get("count") or 0)
    return int(row[0] or 0)


def query_corpus_summary(conn: Any, schema: str) -> dict[str, Any]:
    """Aggregate card_corpus_state and email_corpus_decisions without mutation."""

    if not corpus_state_table_exists(conn, schema):
        return {
            "table_exists": False,
            "active": {},
            "suppressed": {},
            "quarantine": {},
            "by_corpus_state": {},
            "email_threads_evaluated": 0,
            "last_apply_decision_run_id": "",
            "last_apply_at": None,
        }

    state_rows = conn.execute(
        f"""
        SELECT corpus_state, COUNT(*) AS n
        FROM {schema}.card_corpus_state
        GROUP BY corpus_state
        """
    ).fetchall()
    by_state: dict[str, int] = {}
    for row in state_rows:
        state = str(row["corpus_state"] if isinstance(row, dict) else row[0])
        count = int(row["n"] if isinstance(row, dict) else row[1])
        by_state[state] = count

    decision_summary = conn.execute(
        f"""
        SELECT decision_run_id,
               COUNT(*) AS thread_count,
               MAX(applied_at) AS last_applied_at
        FROM {schema}.email_corpus_decisions
        GROUP BY decision_run_id
        ORDER BY MAX(applied_at) DESC NULLS LAST
        LIMIT 1
        """
    ).fetchone()

    last_apply_run_id = ""
    last_apply_at = None
    email_threads = 0
    if decision_summary is not None:
        last_apply_run_id = str(
            decision_summary["decision_run_id"]
            if isinstance(decision_summary, dict)
            else decision_summary[0]
        )
        email_threads = int(
            decision_summary["thread_count"]
            if isinstance(decision_summary, dict)
            else decision_summary[1]
        )
        last_apply_at = (
            decision_summary["last_applied_at"]
            if isinstance(decision_summary, dict)
            else decision_summary[2]
        )

    reason_rows = conn.execute(
        f"""
        SELECT corpus_decision, decision_reason, COUNT(*) AS n
        FROM {schema}.email_corpus_decisions
        GROUP BY corpus_decision, decision_reason
        """
    ).fetchall()
    suppression_reasons: dict[str, int] = {}
    quarantine_reasons: dict[str, int] = {}
    for row in reason_rows:
        corpus_decision = str(row["corpus_decision"] if isinstance(row, dict) else row[0])
        reason = str(row["decision_reason"] if isinstance(row, dict) else row[1])
        count = int(row["n"] if isinstance(row, dict) else row[2])
        if corpus_decision == CORPUS_STATE_SUPPRESSED:
            suppression_reasons[reason] = count
        elif corpus_decision == CORPUS_STATE_QUARANTINE:
            quarantine_reasons[reason] = count

    return {
        "table_exists": True,
        "active": {"cards": by_state.get(CORPUS_STATE_ACTIVE, 0)},
        "suppressed": {"cards": by_state.get(CORPUS_STATE_SUPPRESSED, 0), "by_reason": suppression_reasons},
        "quarantine": {"cards": by_state.get(CORPUS_STATE_QUARANTINE, 0), "by_reason": quarantine_reasons},
        "by_corpus_state": by_state,
        "email_threads_evaluated": email_threads,
        "last_apply_decision_run_id": last_apply_run_id,
        "last_apply_at": str(last_apply_at) if last_apply_at else None,
    }


def query_email_hygiene_summary(conn: Any, schema: str) -> dict[str, Any]:
    """Email hygiene fields for Section F status."""

    corpus = query_corpus_summary(conn, schema)
    classification_sources = {source: 0 for source in CLASSIFICATION_SOURCES}
    if corpus_state_table_exists(conn, schema):
        source_rows = conn.execute(
            f"""
            SELECT classification_source, COUNT(*) AS n
            FROM {schema}.email_corpus_decisions
            GROUP BY classification_source
            """
        ).fetchall()
        for row in source_rows:
            source = str(row["classification_source"] if isinstance(row, dict) else row[0] or "missing")
            count = int(row["n"] if isinstance(row, dict) else row[1])
            classification_sources[source] = classification_sources.get(source, 0) + count

    total_threads = int(corpus.get("email_threads_evaluated") or 0)
    classified = sum(
        count
        for source, count in classification_sources.items()
        if source not in ("missing", "new_llm") and count > 0
    )
    coverage = round(classified / total_threads, 4) if total_threads else 0.0

    return {
        "policy_version": EMAIL_PROMOTION_POLICY_VERSION,
        "classification_coverage": coverage,
        "total_threads_evaluated": total_threads,
        "classification_source_counts": classification_sources,
        "unclassified_thread_count": classification_sources.get("missing", 0),
        "last_corpus_hygiene_decision_run_id": corpus.get("last_apply_decision_run_id") or "",
        "last_corpus_hygiene_apply_at": corpus.get("last_apply_at"),
        "corpus_counts": corpus.get("by_corpus_state") or {},
    }


def rollback_decision_run_ids(conn: Any, schema: str, *, limit: int = 5) -> list[str]:
    if not corpus_state_table_exists(conn, schema):
        return []
    rows = conn.execute(
        f"""
        SELECT DISTINCT decision_run_id
        FROM {schema}.card_corpus_state
        WHERE decision_run_id <> ''
        ORDER BY decision_run_id DESC
        LIMIT %s
        """,
        (limit,),
    ).fetchall()
    out: list[str] = []
    for row in rows:
        run_id = str(row["decision_run_id"] if isinstance(row, dict) else row[0])
        if run_id and ":rollback" not in run_id:
            out.append(run_id)
    return out
