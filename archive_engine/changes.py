"""Change-journal consumption and P07 checkpoint hook (P02-A).

Import ``ChangeRecord`` / ``ChangeBatch`` from ``archive_engine.contracts``.
This module does not redefine those records.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from archive_engine.contracts import ChangeBatch, ChangeRecord
from archive_vault.change_journal import (
    CHECKPOINT_REASON,
    CONSUMER_CLAIMS,
    CONSUMER_ENRICHMENT,
    CONSUMER_GRAPH,
    CONSUMER_PUBLICATION,
    CONSUMER_SEED_LINK,
    CONSUMER_VECTORS,
    CONSUMER_WAREHOUSE,
    JOURNAL_SCHEMA_VERSION,
    NAMED_CONSUMERS,
    ChangeJournal,
    ConsumerCursor,
    FaultHook,
    JournalFault,
    RevisionConflict,
    open_journal,
)

__all__ = [
    "CHECKPOINT_REASON",
    "CONSUMER_CLAIMS",
    "CONSUMER_ENRICHMENT",
    "CONSUMER_GRAPH",
    "CONSUMER_PUBLICATION",
    "CONSUMER_SEED_LINK",
    "CONSUMER_VECTORS",
    "CONSUMER_WAREHOUSE",
    "JOURNAL_SCHEMA_VERSION",
    "NAMED_CONSUMERS",
    "ChangeBatch",
    "ChangeJournal",
    "ChangeRecord",
    "ConsumerCursor",
    "FaultHook",
    "JournalFault",
    "RevisionConflict",
    "acknowledge_batch",
    "consume_batch",
    "named_consumers",
    "open_change_journal",
    "recovery_checkpoint_binding",
]


def named_consumers() -> tuple[str, ...]:
    """Reserved event-spine consumer names. Publication is first, not exclusive."""

    return NAMED_CONSUMERS


def open_change_journal(vault: str | Path, **kwargs: Any) -> ChangeJournal:
    return open_journal(vault, **kwargs)


def consume_batch(journal: ChangeJournal, consumer_name: str, *, limit: int = 100) -> ChangeBatch:
    return journal.consume(consumer_name, limit=limit)


def acknowledge_batch(
    journal: ChangeJournal,
    batch: ChangeBatch,
    *,
    acked_sequences: list[int] | None = None,
    gap_sequences: list[int] | None = None,
    gap_reason: str = "pending",
) -> ConsumerCursor:
    return journal.acknowledge(
        batch.consumer_name,
        batch,
        acked_sequences=acked_sequences,
        gap_sequences=gap_sequences,
        gap_reason=gap_reason,
    )


def recovery_checkpoint_binding(vault: str | Path, *, archive_id: str | None = None) -> dict[str, Any]:
    """P07-adoptable ``archive_id`` / ``checkpoint`` bindings.

    P07-A left both fields unavailable. Callers replace ``unavailable_binding()``
    with this hook after integrating the journal. Bindings use the same
    ``{status, reason, value}`` envelope.
    """

    with open_journal(vault, archive_id=archive_id) as journal:
        return {
            "archive_id": journal.archive_identity_binding(),
            "checkpoint": journal.checkpoint(),
        }
