"""Changes since a journal checkpoint.

Source mutations, derived-only refreshes, corrections, and proposed links stay
distinguishable. Deletes return tombstones without card bodies. The cursor is
sequence-based and snapshot-bound.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from archive_engine.analytics import reject_advice
from archive_engine.contracts import UNKNOWN, AccessContext, ChangeRecord
from archive_engine.errors import CursorInvalidError
from archive_engine.query_cursor import QueryCursor, decode_cursor

WORKFLOW = "changes_since"
CURSOR_VERSION = "p10c.1"
REFRESH_OPERATIONS = frozenset({"embed", "legacy_dirty"})
CASE_CHANGES = "p10c-changes-since"


@dataclass(frozen=True)
class ChangeEvent:
    sequence: int
    uid: str
    operation: str
    before_revision: str
    after_revision: str
    source: str
    method: str
    evidence_kind: str
    role: str
    mutation_id: str
    committed: bool
    tombstone: bool = False

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "sequence": self.sequence,
            "uid": self.uid,
            "operation": self.operation,
            "before_revision": self.before_revision,
            "after_revision": self.after_revision,
            "source": self.source,
            "method": self.method,
            "evidence_kind": self.evidence_kind,
            "role": self.role,
            "mutation_id": self.mutation_id,
            "committed": self.committed,
            "tombstone": self.tombstone,
        }
        reject_advice(payload)
        return payload


@dataclass(frozen=True)
class ChangesPage:
    rows: tuple[ChangeEvent, ...]
    next_cursor: str
    checkpoint: int
    snapshot_id: str
    complete: bool
    truncated: bool
    excluded_uids: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "workflow": WORKFLOW,
            "case_id": CASE_CHANGES,
            "rows": [row.to_payload() for row in self.rows],
            "next_cursor": self.next_cursor,
            "checkpoint": self.checkpoint,
            "snapshot_id": self.snapshot_id,
            "complete": self.complete,
            "truncated": self.truncated,
            "excluded_uids": list(self.excluded_uids),
            "coverage": "eligible_stored",
        }


def _encode_cursor(*, snapshot_id: str, sequence: int) -> str:
    cursor = QueryCursor(
        version=1,
        archive_id=snapshot_id or "changes",
        snapshot=snapshot_id,
        policy_fingerprint="changes",
        predicate_fingerprint=CURSOR_VERSION,
        order_field="sequence",
        order_direction="asc",
        last_order_value=str(sequence),
        last_uid=str(sequence),
    )
    return cursor.encode()


def _decode_cursor(cursor: str, *, snapshot_id: str) -> int:
    parsed = decode_cursor(cursor)
    if parsed.predicate_fingerprint != CURSOR_VERSION or parsed.order_field != "sequence":
        raise CursorInvalidError("changes cursor is not sequence-bound")
    if parsed.snapshot != snapshot_id:
        raise CursorInvalidError("cursor snapshot is stale; restart the query")
    try:
        return int(parsed.last_order_value or parsed.last_uid or 0)
    except ValueError as exc:
        raise CursorInvalidError("changes cursor sequence is invalid") from exc


def _classify(
    record: ChangeRecord,
    *,
    decisions_by_uid: Mapping[str, Sequence[Mapping[str, Any]]],
) -> ChangeEvent:
    source = str(record.source or "")
    operation = str(record.operation or "")
    decisions = list(decisions_by_uid.get(record.uid) or [])
    if operation in REFRESH_OPERATIONS:
        role = "derived_refresh"
        kind = "derived"
        method = operation
    elif source.startswith("decision:") or any(
        str(item.get("kind") or "") in {"field_override", "correction"} for item in decisions
    ):
        role = "corrected"
        kind = "derived"
        method = "decision"
        if operation == "update":
            operation = "correct"
    elif operation == "merge" or any(str(item.get("kind") or "") == "identity_merge" for item in decisions):
        role = "merged"
        kind = "derived"
        method = "decision"
        operation = "merge"
    elif any(str(item.get("kind") or "") == "proposed_link" for item in decisions):
        role = "proposed"
        kind = "proposed_link"
        method = str(decisions[0].get("method") or UNKNOWN)
    elif operation == "delete":
        role = "deleted"
        kind = "source_reported"
        method = source or "source"
    elif operation == "create":
        role = "created"
        kind = "source_reported"
        method = source or "source"
    else:
        role = "updated"
        kind = "source_reported"
        method = source or "source"
    return ChangeEvent(
        sequence=record.sequence,
        uid=record.uid,
        operation=operation,
        before_revision=record.before_revision,
        after_revision=record.after_revision,
        source=source,
        method=method,
        evidence_kind=kind,
        role=role,
        mutation_id=record.mutation_id,
        committed=record.committed,
        tombstone=operation == "delete",
    )


def changes_since(
    records: Sequence[ChangeRecord],
    *,
    after_sequence: int = 0,
    snapshot_id: str = "journal",
    cursor: str = "",
    page_size: int = 50,
    decisions: Sequence[Mapping[str, Any]] = (),
    access: AccessContext | None = None,
    denied_uids: Sequence[str] = (),
    life_events_only: bool = True,
) -> ChangesPage:
    """Page committed mutations after a checkpoint. Denied UIDs keep no body."""

    if cursor:
        after_sequence = _decode_cursor(cursor, snapshot_id=snapshot_id)
    denied = {uid for uid in denied_uids if uid}
    if access is not None and access.deny:
        denied.update(record.uid for record in records)
    decisions_by_uid: dict[str, list[Mapping[str, Any]]] = {}
    for item in decisions:
        uid = str(item.get("uid") or "").strip()
        if uid:
            decisions_by_uid.setdefault(uid, []).append(item)

    events: list[ChangeEvent] = []
    excluded: list[str] = []
    for record in sorted(records, key=lambda item: (item.sequence, item.uid)):
        if not record.committed or record.sequence <= after_sequence:
            continue
        event = _classify(record, decisions_by_uid=decisions_by_uid)
        if life_events_only and event.role == "derived_refresh":
            excluded.append(event.uid)
            continue
        if event.uid in denied:
            excluded.append(event.uid)
            if event.tombstone:
                events.append(
                    ChangeEvent(
                        sequence=event.sequence,
                        uid=event.uid,
                        operation="delete",
                        before_revision="",
                        after_revision="",
                        source="",
                        method="tombstone",
                        evidence_kind="unknown",
                        role="deleted",
                        mutation_id=event.mutation_id,
                        committed=True,
                        tombstone=True,
                    )
                )
            continue
        events.append(event)

    page = events[: max(1, page_size)]
    truncated = len(events) > len(page)
    next_cursor = ""
    if truncated and page:
        next_cursor = _encode_cursor(snapshot_id=snapshot_id, sequence=page[-1].sequence)
    return ChangesPage(
        rows=tuple(page),
        next_cursor=next_cursor,
        checkpoint=after_sequence,
        snapshot_id=snapshot_id,
        complete=not truncated,
        truncated=truncated,
        excluded_uids=tuple(dict.fromkeys(excluded)),
    )
