"""Change-journal consumption and P07 checkpoint hook (P02-A).

Import ``ChangeRecord`` / ``ChangeBatch`` from ``archive_engine.contracts``.
This module does not redefine those records.
"""

from __future__ import annotations

from dataclasses import asdict
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
    OPERATION_DELETE,
    OPERATION_EMBED,
    OPERATION_UPDATE,
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
    "acknowledge_materialized",
    "consume_batch",
    "emit_embed_completion",
    "named_consumers",
    "open_change_journal",
    "recovery_checkpoint_binding",
    "request_reconciliation",
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


def emit_embed_completion(
    vault: str | Path,
    uids: list[str] | None,
    *,
    rel_paths: dict[str, str] | None = None,
    source: str = "embedder",
) -> list[ChangeRecord]:
    """Journal embedding-only completion. Does not rewrite card files."""

    records: list[ChangeRecord] = []
    paths = rel_paths or {}
    with ChangeJournal(vault) as journal:
        for raw in uids or []:
            uid = str(raw or "").strip()
            if not uid:
                continue
            records.append(
                journal.apply_mutation(
                    uid=uid,
                    rel_path=str(paths.get(uid) or ""),
                    operation=OPERATION_EMBED,
                    content=None,
                    source=source,
                )
            )
    return records


def acknowledge_materialized(
    vault: str | Path,
    *,
    uids: list[str] | None = None,
    limit: int = 10_000,
) -> ConsumerCursor:
    """Ack warehouse sequences for a completed materialization. Other consumers stay put."""

    wanted = {str(uid).strip() for uid in (uids or []) if str(uid).strip()}
    with ChangeJournal(vault) as journal:
        batch = journal.consume(CONSUMER_WAREHOUSE, limit=max(1, int(limit)))
        if wanted:
            acked = [record.sequence for record in batch.records if record.uid in wanted]
            gaps = [record.sequence for record in batch.records if record.uid not in wanted]
            return journal.acknowledge(
                CONSUMER_WAREHOUSE,
                batch,
                acked_sequences=acked,
                gap_sequences=gaps,
                gap_reason="not_in_allowlist",
            )
        return journal.acknowledge(CONSUMER_WAREHOUSE, batch)


def request_reconciliation(
    vault: str | Path,
    *,
    uid_to_rel: dict[str, str] | None = None,
    reason: str = "external_edit",
) -> dict[str, Any]:
    """Replay prepared rows and journal provided external edits. Never walks the vault."""

    imported: list[ChangeRecord] = []
    with ChangeJournal(vault) as journal:
        prepared = [asdict(item) for item in journal.reconcile()]
        edits = journal.detect_external_edits(uid_to_rel or {})
        root = Path(vault)
        for edit in edits:
            rel = str(edit.get("rel_path") or "")
            uid = str(edit.get("uid") or "")
            if not uid:
                continue
            path = root / rel if rel else None
            if path is not None and path.is_file():
                imported.append(
                    journal.apply_mutation(
                        uid=uid,
                        rel_path=rel,
                        operation=OPERATION_UPDATE,
                        content=path.read_bytes(),
                        source=reason,
                    )
                )
            else:
                imported.append(
                    journal.apply_mutation(
                        uid=uid,
                        rel_path=rel,
                        operation=OPERATION_DELETE,
                        source=reason,
                    )
                )
    return {
        "reason": reason,
        "reconcile": prepared,
        "external_edits": edits,
        "imported": [record.sequence for record in imported],
        "bounded": True,
    }
