"""Canonical human-decision log (P07-B).

Field overrides, clear-override, and source-conflict records live in the vault
as versioned decision-critical state. Mutations go through the P02 ChangeJournal.
This is not a second write protocol and is not a warehouse table.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping

from archive_engine.contracts import ChangeRecord
from archive_engine.errors import IncompatibleStateError
from archive_vault.change_journal import (
    OPERATION_CREATE,
    OPERATION_UPDATE,
    ChangeJournal,
)
from archive_vault.paths import normalize_vault_rel

DECISIONS_REL_PATH = "_meta/canonical-decisions.json"
DECISIONS_UID = "hfa-decision-canonical"
DECISIONS_FORMAT_NAME = "ppa.canonical_decisions"
DECISIONS_FORMAT_VERSION = 1
RECOVERY_CLASS = "decision_critical"

KIND_FIELD_OVERRIDE = "field_override"
KIND_CLEAR_OVERRIDE = "clear_override"
KIND_SOURCE_CONFLICT = "source_conflict"
KIND_IDENTITY_MERGE = "identity_merge"
KIND_IDENTITY_UNDO = "identity_undo"
KIND_IDENTITY_SPLIT = "identity_split"

STATUS_ACTIVE = "active"
STATUS_CLEARED = "cleared"
STATUS_SUPERSEDED = "superseded"
STATUS_OPEN = "open"
STATUS_BLOCKED = "blocked"
STATUS_PARTIAL = "partial"

IDENTITY_KINDS = frozenset({KIND_IDENTITY_MERGE, KIND_IDENTITY_UNDO, KIND_IDENTITY_SPLIT})

DecisionKind = Literal["field_override", "clear_override", "source_conflict"]
DecisionStatus = Literal["active", "cleared", "superseded", "open"]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def value_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, default=str, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def values_equivalent(left: Any, right: Any) -> bool:
    if left == right:
        return True
    if isinstance(left, bool) or isinstance(right, bool):
        return left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return float(left) == float(right)
    return False


def _empty_log() -> dict[str, Any]:
    return {
        "format_name": DECISIONS_FORMAT_NAME,
        "format_version": DECISIONS_FORMAT_VERSION,
        "decisions": [],
    }


@dataclass(frozen=True, slots=True)
class FieldDecision:
    """One recorded human decision. Later inference reuses this shape."""

    decision_id: str
    kind: str
    status: str
    uid: str
    field: str
    replacement_value: Any
    original_value: Any
    original_value_hash: str
    latest_source_value: Any
    latest_source_value_hash: str
    source_revision: str
    input_revision: str
    author: str
    reason: str
    timestamp: str
    decision_mutation_id: str = ""
    card_mutation_id: str = ""
    supersedes: str = ""
    source_provenance: dict[str, Any] | None = None
    incoming_value: Any = None
    incoming_value_hash: str = ""
    incoming_source: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "kind": self.kind,
            "status": self.status,
            "uid": self.uid,
            "field": self.field,
            "replacement_value": self.replacement_value,
            "original_value": self.original_value,
            "original_value_hash": self.original_value_hash,
            "latest_source_value": self.latest_source_value,
            "latest_source_value_hash": self.latest_source_value_hash,
            "source_revision": self.source_revision,
            "input_revision": self.input_revision,
            "author": self.author,
            "reason": self.reason,
            "timestamp": self.timestamp,
            "decision_mutation_id": self.decision_mutation_id,
            "card_mutation_id": self.card_mutation_id,
            "supersedes": self.supersedes,
            "source_provenance": self.source_provenance or {},
            "incoming_value": self.incoming_value,
            "incoming_value_hash": self.incoming_value_hash,
            "incoming_source": self.incoming_source,
            "recovery_class": RECOVERY_CLASS,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> FieldDecision:
        return cls(
            decision_id=str(payload.get("decision_id") or ""),
            kind=str(payload.get("kind") or ""),
            status=str(payload.get("status") or ""),
            uid=str(payload.get("uid") or ""),
            field=str(payload.get("field") or ""),
            replacement_value=payload.get("replacement_value"),
            original_value=payload.get("original_value"),
            original_value_hash=str(payload.get("original_value_hash") or ""),
            latest_source_value=payload.get("latest_source_value", payload.get("original_value")),
            latest_source_value_hash=str(
                payload.get("latest_source_value_hash") or payload.get("original_value_hash") or ""
            ),
            source_revision=str(payload.get("source_revision") or ""),
            input_revision=str(payload.get("input_revision") or ""),
            author=str(payload.get("author") or ""),
            reason=str(payload.get("reason") or ""),
            timestamp=str(payload.get("timestamp") or ""),
            decision_mutation_id=str(payload.get("decision_mutation_id") or ""),
            card_mutation_id=str(payload.get("card_mutation_id") or ""),
            supersedes=str(payload.get("supersedes") or ""),
            source_provenance=dict(payload.get("source_provenance") or {}) or None,
            incoming_value=payload.get("incoming_value"),
            incoming_value_hash=str(payload.get("incoming_value_hash") or ""),
            incoming_source=str(payload.get("incoming_source") or ""),
        )


def load_decision_log(vault: str | Path) -> dict[str, Any]:
    root = Path(vault)
    path = root / DECISIONS_REL_PATH
    if not path.is_file():
        return _empty_log()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IncompatibleStateError(f"canonical decision log is unreadable: {exc}") from exc
    if not isinstance(payload, dict):
        raise IncompatibleStateError("canonical decision log root must be an object")
    if payload.get("format_name") != DECISIONS_FORMAT_NAME:
        raise IncompatibleStateError("unsupported canonical decision log format")
    if payload.get("format_version") != DECISIONS_FORMAT_VERSION:
        raise IncompatibleStateError(f"unsupported canonical decision log version: {payload.get('format_version')}")
    decisions = payload.get("decisions")
    if not isinstance(decisions, list):
        raise IncompatibleStateError("canonical decision log decisions must be a list")
    return payload


def list_decisions(vault: str | Path) -> list[FieldDecision]:
    payload = load_decision_log(vault)
    return [FieldDecision.from_payload(item) for item in payload["decisions"] if isinstance(item, Mapping)]


def active_overrides_for(vault: str | Path, uid: str) -> dict[str, FieldDecision]:
    """Latest active field_override per field for *uid*."""

    active: dict[str, FieldDecision] = {}
    for decision in list_decisions(vault):
        if decision.uid != uid or decision.kind != KIND_FIELD_OVERRIDE:
            continue
        if decision.status == STATUS_ACTIVE:
            active[decision.field] = decision
        elif decision.status in {STATUS_CLEARED, STATUS_SUPERSEDED}:
            active.pop(decision.field, None)
    return active


def protected_field_names(overrides: Mapping[str, FieldDecision]) -> frozenset[str]:
    return frozenset(overrides)


def overlay_overrides(
    data: dict[str, Any],
    overrides: Mapping[str, FieldDecision],
) -> tuple[dict[str, Any], list[str]]:
    """Force active override values onto *data*. Returns (data, restored fields)."""

    overlaid = dict(data)
    restored: list[str] = []
    for field_name, decision in overrides.items():
        if not values_equivalent(overlaid.get(field_name), decision.replacement_value):
            overlaid[field_name] = decision.replacement_value
            restored.append(field_name)
    return overlaid, restored


def persist_decision_log(
    vault: str | Path,
    payload: Mapping[str, Any],
    *,
    source: str = "decision",
    account: str = "",
    run_id: str = "",
) -> ChangeRecord:
    """Journal the decision log. Intent is durable before any card mutation."""

    root = Path(vault)
    rel = str(normalize_vault_rel(DECISIONS_REL_PATH))
    body = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    operation = OPERATION_UPDATE if (root / rel).is_file() else OPERATION_CREATE
    with ChangeJournal(root) as journal:
        return journal.apply_mutation(
            uid=DECISIONS_UID,
            rel_path=rel,
            operation=operation,
            content=body.encode("utf-8"),
            source=source,
            account=account,
            run_id=run_id,
        )


def append_decision(
    vault: str | Path,
    decision: FieldDecision,
    *,
    source: str = "decision",
) -> tuple[FieldDecision, ChangeRecord]:
    payload = load_decision_log(vault)
    decisions = list(payload["decisions"])
    decisions.append(decision.to_payload())
    payload = {
        "format_name": DECISIONS_FORMAT_NAME,
        "format_version": DECISIONS_FORMAT_VERSION,
        "decisions": decisions,
    }
    record = persist_decision_log(vault, payload, source=source)
    stored = decision.to_payload()
    stored["decision_mutation_id"] = record.mutation_id
    decisions[-1] = stored
    payload["decisions"] = decisions
    persist_decision_log(vault, payload, source=source)
    return FieldDecision.from_payload(stored), record


def update_decision(
    vault: str | Path,
    decision_id: str,
    updates: Mapping[str, Any],
    *,
    source: str = "decision",
) -> tuple[FieldDecision, ChangeRecord]:
    payload = load_decision_log(vault)
    updated: FieldDecision | None = None
    decisions: list[dict[str, Any]] = []
    for item in payload["decisions"]:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("decision_id") or "") != decision_id:
            decisions.append(dict(item))
            continue
        merged = dict(item)
        merged.update(dict(updates))
        updated = FieldDecision.from_payload(merged)
        decisions.append(updated.to_payload())
    if updated is None:
        raise IncompatibleStateError(f"decision not found: {decision_id}")
    payload = {
        "format_name": DECISIONS_FORMAT_NAME,
        "format_version": DECISIONS_FORMAT_VERSION,
        "decisions": decisions,
    }
    record = persist_decision_log(vault, payload, source=source)
    return updated, record


def new_decision_id() -> str:
    return uuid.uuid4().hex


def build_field_override(
    *,
    uid: str,
    field: str,
    replacement_value: Any,
    original_value: Any,
    source_revision: str,
    author: str,
    reason: str,
    source_provenance: Mapping[str, Any] | None = None,
    supersedes: str = "",
) -> FieldDecision:
    if not str(uid or "").strip():
        raise IncompatibleStateError("uid is required")
    if not str(field or "").strip():
        raise IncompatibleStateError("field is required")
    if not str(author or "").strip():
        raise IncompatibleStateError("author is required")
    if not str(reason or "").strip():
        raise IncompatibleStateError("reason is required")
    return FieldDecision(
        decision_id=new_decision_id(),
        kind=KIND_FIELD_OVERRIDE,
        status=STATUS_ACTIVE,
        uid=uid,
        field=field,
        replacement_value=replacement_value,
        original_value=original_value,
        original_value_hash=value_hash(original_value),
        latest_source_value=original_value,
        latest_source_value_hash=value_hash(original_value),
        source_revision=source_revision,
        input_revision=source_revision,
        author=author,
        reason=reason,
        timestamp=_utc_now(),
        supersedes=supersedes,
        source_provenance=dict(source_provenance) if source_provenance else None,
    )


def record_source_conflict(
    vault: str | Path,
    override: FieldDecision,
    incoming_value: Any,
    *,
    incoming_source: str = "",
    incoming_revision: str = "",
) -> FieldDecision:
    """Record that a new source fact disagrees with an active correction.

    The override remains effective. This is reviewable, not last-write-wins.
    """

    if values_equivalent(incoming_value, override.replacement_value):
        return override
    if values_equivalent(incoming_value, override.latest_source_value):
        return override
    conflict = FieldDecision(
        decision_id=new_decision_id(),
        kind=KIND_SOURCE_CONFLICT,
        status=STATUS_OPEN,
        uid=override.uid,
        field=override.field,
        replacement_value=override.replacement_value,
        original_value=override.original_value,
        original_value_hash=override.original_value_hash,
        latest_source_value=incoming_value,
        latest_source_value_hash=value_hash(incoming_value),
        source_revision=incoming_revision or override.source_revision,
        input_revision=override.input_revision,
        author=override.author,
        reason=f"source replay disagrees with correction {override.decision_id}",
        timestamp=_utc_now(),
        supersedes=override.decision_id,
        source_provenance=override.source_provenance,
        incoming_value=incoming_value,
        incoming_value_hash=value_hash(incoming_value),
        incoming_source=incoming_source,
    )
    stored, _ = append_decision(vault, conflict, source=incoming_source or "source-conflict")
    update_decision(
        vault,
        override.decision_id,
        {
            "latest_source_value": incoming_value,
            "latest_source_value_hash": value_hash(incoming_value),
        },
        source=incoming_source or "source-conflict",
    )
    return stored


def note_source_conflicts(
    vault: str | Path,
    uid: str,
    incoming: Mapping[str, Any],
    *,
    incoming_source: str = "",
    incoming_revision: str = "",
) -> list[FieldDecision]:
    """Record explicit conflicts when source facts disagree with overrides."""

    overrides = active_overrides_for(vault, uid)
    recorded: list[FieldDecision] = []
    for field_name, override in overrides.items():
        if field_name not in incoming:
            continue
        incoming_value = incoming[field_name]
        if values_equivalent(incoming_value, override.replacement_value):
            continue
        if values_equivalent(incoming_value, override.latest_source_value):
            continue
        recorded.append(
            record_source_conflict(
                vault,
                override,
                incoming_value,
                incoming_source=incoming_source,
                incoming_revision=incoming_revision,
            )
        )
    return recorded


def append_decision_payload(
    vault: str | Path,
    item: Mapping[str, Any],
    *,
    source: str = "decision",
) -> tuple[dict[str, Any], ChangeRecord]:
    """Append a raw decision object (field or identity) through the journal."""

    payload = load_decision_log(vault)
    decisions = list(payload["decisions"])
    stored = dict(item)
    if not stored.get("decision_id"):
        stored["decision_id"] = new_decision_id()
    decisions.append(stored)
    record = persist_decision_log(
        vault,
        {
            "format_name": DECISIONS_FORMAT_NAME,
            "format_version": DECISIONS_FORMAT_VERSION,
            "decisions": decisions,
        },
        source=source,
    )
    stored["decision_mutation_id"] = record.mutation_id
    decisions[-1] = stored
    persist_decision_log(
        vault,
        {
            "format_name": DECISIONS_FORMAT_NAME,
            "format_version": DECISIONS_FORMAT_VERSION,
            "decisions": decisions,
        },
        source=source,
    )
    return stored, record


def update_decision_payload(
    vault: str | Path,
    decision_id: str,
    updates: Mapping[str, Any],
    *,
    source: str = "decision",
) -> tuple[dict[str, Any], ChangeRecord]:
    """Patch a decision in place without dropping unknown keys."""

    payload = load_decision_log(vault)
    updated: dict[str, Any] | None = None
    decisions: list[dict[str, Any]] = []
    for item in payload["decisions"]:
        if not isinstance(item, Mapping):
            continue
        current = dict(item)
        if str(current.get("decision_id") or "") != decision_id:
            decisions.append(current)
            continue
        current.update(dict(updates))
        updated = current
        decisions.append(current)
    if updated is None:
        raise IncompatibleStateError(f"decision not found: {decision_id}")
    record = persist_decision_log(
        vault,
        {
            "format_name": DECISIONS_FORMAT_NAME,
            "format_version": DECISIONS_FORMAT_VERSION,
            "decisions": decisions,
        },
        source=source,
    )
    return updated, record


def list_identity_decisions(vault: str | Path) -> list[dict[str, Any]]:
    payload = load_decision_log(vault)
    return [
        dict(item)
        for item in payload["decisions"]
        if isinstance(item, Mapping) and str(item.get("kind") or "") in IDENTITY_KINDS
    ]


def latest_identity_merge(vault: str | Path, *, winner_uid: str = "", loser_uid: str = "") -> dict[str, Any] | None:
    found: dict[str, Any] | None = None
    for item in list_identity_decisions(vault):
        if str(item.get("kind") or "") != KIND_IDENTITY_MERGE:
            continue
        if winner_uid and str(item.get("winner_uid") or "") != winner_uid:
            continue
        if loser_uid and str(item.get("loser_uid") or "") != loser_uid:
            continue
        found = item
    return found


def active_identity_merge(vault: str | Path, *, winner_uid: str = "", loser_uid: str = "") -> dict[str, Any] | None:
    found = latest_identity_merge(vault, winner_uid=winner_uid, loser_uid=loser_uid)
    if found is None or str(found.get("status") or "") != STATUS_ACTIVE:
        return None
    return found


def open_conflicts_for(vault: str | Path, uid: str, field: str | None = None) -> list[FieldDecision]:
    found: list[FieldDecision] = []
    for decision in list_decisions(vault):
        if decision.kind != KIND_SOURCE_CONFLICT or decision.status != STATUS_OPEN:
            continue
        if decision.uid != uid:
            continue
        if field is not None and decision.field != field:
            continue
        found.append(decision)
    return found
