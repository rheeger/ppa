"""Suppressed-email visibility checks for v3 readiness (read-only)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from archive_cli.corpus_hygiene.state_store import (
    CORPUS_STATE_QUARANTINE,
    CORPUS_STATE_SUPPRESSED,
    corpus_state_table_exists,
    is_card_retrieval_active,
)


def _table_exists(conn: Any, schema: str, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (schema, table_name),
    ).fetchone()
    return row is not None


@dataclass
class SuppressionVisibilityResult:
    ok: bool
    suppressed_count: int = 0
    quarantine_count: int = 0
    retrieval_violations: list[str] = field(default_factory=list)
    enrichment_queue_violations: int = 0
    link_job_violations: int = 0
    skipped: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "suppressed_count": self.suppressed_count,
            "quarantine_count": self.quarantine_count,
            "retrieval_violations": list(self.retrieval_violations),
            "enrichment_queue_violations": self.enrichment_queue_violations,
            "link_job_violations": self.link_job_violations,
            "skipped": self.skipped,
        }


def evaluate_suppression_visibility(conn: Any, schema: str, *, sample_limit: int = 200) -> SuppressionVisibilityResult:
    """Fail when suppressed cards remain retrieval-active or queued for downstream work.

    Quarantine is retrievable by design; it is still a violation if it sits on
    the enrichment or linker queues.
    """

    if not corpus_state_table_exists(conn, schema):
        return SuppressionVisibilityResult(ok=True, skipped="card_corpus_state_missing")

    rows = conn.execute(
        f"""
        SELECT card_uid, corpus_state
        FROM {schema}.card_corpus_state
        WHERE corpus_state IN (%s, %s)
        LIMIT %s
        """,
        (CORPUS_STATE_SUPPRESSED, CORPUS_STATE_QUARANTINE, sample_limit),
    ).fetchall()

    suppressed = 0
    quarantine = 0
    violations: list[str] = []
    for row in rows:
        card_uid = str(row["card_uid"] if isinstance(row, dict) else row[0])
        state = str(row["corpus_state"] if isinstance(row, dict) else row[1])
        if state == CORPUS_STATE_SUPPRESSED:
            suppressed += 1
            if is_card_retrieval_active(conn, schema, card_uid):
                violations.append(card_uid)
        else:
            quarantine += 1

    enrichment_violations = 0
    if _table_exists(conn, schema, "enrichment_queue") and (suppressed or quarantine):
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM {schema}.enrichment_queue eq
            INNER JOIN {schema}.card_corpus_state cs ON cs.card_uid = eq.card_uid
            WHERE eq.status = 'pending'
              AND cs.corpus_state IN (%s, %s)
            """,
            (CORPUS_STATE_SUPPRESSED, CORPUS_STATE_QUARANTINE),
        ).fetchone()
        enrichment_violations = int(row["n"] if isinstance(row, dict) else row[0] or 0)

    link_violations = 0
    if _table_exists(conn, schema, "link_jobs") and (suppressed or quarantine):
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM {schema}.link_jobs lj
            INNER JOIN {schema}.card_corpus_state cs ON cs.card_uid = lj.source_card_uid
            WHERE lj.status IN ('pending', 'running')
              AND cs.corpus_state IN (%s, %s)
            """,
            (CORPUS_STATE_SUPPRESSED, CORPUS_STATE_QUARANTINE),
        ).fetchone()
        link_violations = int(row["n"] if isinstance(row, dict) else row[0] or 0)

    ok = not violations and enrichment_violations == 0 and link_violations == 0
    return SuppressionVisibilityResult(
        ok=ok,
        suppressed_count=suppressed,
        quarantine_count=quarantine,
        retrieval_violations=violations,
        enrichment_queue_violations=enrichment_violations,
        link_job_violations=link_violations,
    )
