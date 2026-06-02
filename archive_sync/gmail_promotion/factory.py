"""Construct promotion gate from adapter kwargs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from archive_cli.corpus_hygiene.classification_reuse import open_classify_index
from .classification_resolve import GmailClassificationResolver, load_card_classifications_from_index_rows
from .gate import GmailPromotionGate
from .ledger import DbPromotionLedger, FilePromotionLedger, PromotionLedger, default_ledger_path
from .metrics import GmailPromotionBatchMetrics


def build_promotion_gate(
    vault_path: str,
    *,
    account_email: str,
    decision_run_id: str,
    conn: Any | None = None,
    schema: str = "",
    card_classification_rows: list[dict[str, Any]] | None = None,
    allow_new_llm: bool = False,
    metrics: GmailPromotionBatchMetrics | None = None,
    fail_on_missing_classification: bool = False,
) -> GmailPromotionGate:
    ledger: PromotionLedger
    if conn is not None and schema and decision_run_id:
        ledger = DbPromotionLedger(conn, schema, decision_run_id=decision_run_id)
    else:
        ledger = FilePromotionLedger(default_ledger_path(vault_path))

    corpus_records = ledger.all_decisions()
    from .classification_resolve import corpus_decisions_index

    classify_index = open_classify_index(Path(vault_path))
    card_map = load_card_classifications_from_index_rows(card_classification_rows or [])
    resolver = GmailClassificationResolver(
        corpus_decisions=corpus_decisions_index(corpus_records),
        card_classifications=card_map,
        classify_index=classify_index,
        allow_new_llm=allow_new_llm,
    )
    return GmailPromotionGate(
        ledger=ledger,
        resolver=resolver,
        decision_run_id=decision_run_id,
        metrics=metrics,
        fail_on_missing_classification=fail_on_missing_classification,
    )
