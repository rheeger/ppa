"""Manual field corrections as journaled human decisions (P07-B).

Correcting an amount or name writes a decision first, then a canonical card
mutation through the P02 ChangeJournal. Source replay cannot last-write-wins.
Clear-override restores source-derived behavior. This module is the engine
command request later CLI registration can call; P09 owns parser wiring.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

from archive_engine.errors import IncompatibleStateError
from archive_vault.change_journal import ChangeJournal, FaultHook, file_revision
from archive_vault.decisions import (
    KIND_CLEAR_OVERRIDE,
    KIND_FIELD_OVERRIDE,
    STATUS_ACTIVE,
    STATUS_CLEARED,
    FieldDecision,
    active_overrides_for,
    append_decision,
    build_field_override,
    list_decisions,
    new_decision_id,
    open_conflicts_for,
    update_decision,
    value_hash,
    values_equivalent,
)
from archive_vault.provenance import (
    PROVENANCE_METHOD_HUMAN,
    ProvenanceEntry,
    merge_provenance,
)
from archive_vault.schema import validate_card_strict
from archive_vault.vault import read_note, read_note_by_uid, write_card

CorrectionAction = Literal["apply_override", "clear_override", "reconcile"]


@dataclass(frozen=True, slots=True)
class CorrectionCommandRequest:
    """Engine command later CLI registration can dispatch. No parser here."""

    action: CorrectionAction
    uid: str = ""
    field: str = ""
    value: Any = None
    author: str = ""
    reason: str = ""
    rel_path: str = ""


@dataclass(frozen=True, slots=True)
class DecisionInvalidationHook:
    """Preimage + mutation identity P03/claims can subscribe to later."""

    decision_id: str
    uid: str
    field: str
    before_revision: str
    after_revision: str
    mutation_id: str


@dataclass(frozen=True, slots=True)
class CorrectionResult:
    decision_id: str
    uid: str
    field: str
    action: str
    decision_mutation_id: str
    card_mutation_id: str
    before_revision: str
    after_revision: str
    conflicts: tuple[FieldDecision, ...] = ()
    invalidation: DecisionInvalidationHook | None = None
    pending: bool = False


@dataclass
class _ResolvedCard:
    rel_path: str
    frontmatter: dict[str, Any]
    body: str
    provenance: dict[str, ProvenanceEntry]
    revision: str


def _utc_date() -> str:
    return date.today().isoformat()


def _human_provenance(decision: FieldDecision) -> ProvenanceEntry:
    return ProvenanceEntry(
        source=f"decision:{decision.decision_id}",
        date=_utc_date(),
        method=PROVENANCE_METHOD_HUMAN,
        input_hash=value_hash(
            {
                "author": decision.author,
                "reason": decision.reason,
                "field": decision.field,
                "value": decision.replacement_value,
                "input_revision": decision.input_revision,
            }
        ),
    )


def _source_provenance_entry(payload: dict[str, Any] | None, fallback: ProvenanceEntry | None) -> ProvenanceEntry:
    if payload:
        return ProvenanceEntry(
            source=str(payload.get("source") or "source"),
            date=str(payload.get("date") or _utc_date()),
            method=str(payload.get("method") or "deterministic"),
            model=str(payload.get("model") or ""),
            enrichment_version=int(payload.get("enrichment_version") or 0),
            input_hash=str(payload.get("input_hash") or ""),
        )
    if fallback is not None:
        return ProvenanceEntry(
            source=fallback.source,
            date=fallback.date,
            method=fallback.method if fallback.method != PROVENANCE_METHOD_HUMAN else "deterministic",
            model=fallback.model,
            enrichment_version=fallback.enrichment_version,
            input_hash=fallback.input_hash,
        )
    return ProvenanceEntry(source="source", date=_utc_date(), method="deterministic")


def _entry_payload(entry: ProvenanceEntry | None) -> dict[str, Any]:
    if entry is None:
        return {}
    return {
        "source": entry.source,
        "date": entry.date,
        "method": entry.method,
        "model": entry.model,
        "enrichment_version": entry.enrichment_version,
        "input_hash": entry.input_hash,
    }


def resolve_card(vault: str | Path, uid: str, rel_path: str = "") -> _ResolvedCard:
    root = Path(vault)
    if rel_path:
        frontmatter, body, provenance = read_note(root, rel_path)
        if str(frontmatter.get("uid") or "") != uid:
            raise IncompatibleStateError(f"uid {uid} does not match {rel_path}")
        revision = file_revision(root / rel_path)
        return _ResolvedCard(rel_path, frontmatter, body, provenance, revision)
    found = read_note_by_uid(root, uid)
    if found is None:
        raise IncompatibleStateError(f"card not found for uid {uid}")
    found_rel, frontmatter, body, provenance = found
    rel = found_rel.as_posix() if hasattr(found_rel, "as_posix") else str(found_rel)
    revision = file_revision(root / rel)
    return _ResolvedCard(rel, frontmatter, body, provenance, revision)


def _latest_card_mutation(vault: str | Path, uid: str) -> str:
    with ChangeJournal(vault) as journal:
        records = [row for row in journal.committed_records(limit=10_000) if row.uid == uid]
    return records[-1].mutation_id if records else ""


def _write_corrected_card(resolved: _ResolvedCard, vault: Path, data: dict[str, Any], provenance: dict[str, ProvenanceEntry]) -> None:
    data = dict(data)
    data["updated"] = _utc_date()
    card = validate_card_strict(data)
    write_card(vault, resolved.rel_path, card, body=resolved.body, provenance=provenance)


def apply_override(
    vault: str | Path,
    request: CorrectionCommandRequest,
    *,
    fault: FaultHook | None = None,
) -> CorrectionResult:
    """Persist decision intent, then apply the canonical field mutation."""

    if request.action not in {"apply_override", ""}:
        raise IncompatibleStateError(f"apply_override received action={request.action}")
    root = Path(vault)
    resolved = resolve_card(root, request.uid, request.rel_path)
    if request.field not in resolved.frontmatter:
        raise IncompatibleStateError(f"field {request.field!r} is not on card {request.uid}")
    existing = active_overrides_for(root, request.uid).get(request.field)
    original_value = resolved.frontmatter.get(request.field)
    if existing is not None and existing.status == STATUS_ACTIVE:
        original_value = existing.original_value
    decision = build_field_override(
        uid=request.uid,
        field=request.field,
        replacement_value=request.value,
        original_value=original_value,
        source_revision=resolved.revision,
        author=request.author,
        reason=request.reason,
        source_provenance=_entry_payload(resolved.provenance.get(request.field)),
        supersedes=existing.decision_id if existing is not None else "",
    )
    if existing is not None:
        update_decision(root, existing.decision_id, {"status": "superseded"}, source="decision")
    stored, decision_record = append_decision(root, decision, source="decision")
    hook = fault or FaultHook()
    hook.check("after_decision_intent")
    data = dict(resolved.frontmatter)
    data[request.field] = request.value
    incoming_prov = {request.field: _human_provenance(stored)}
    provenance = merge_provenance(resolved.provenance, incoming_prov)
    _write_corrected_card(resolved, root, data, provenance)
    card_mutation_id = _latest_card_mutation(root, request.uid)
    after = file_revision(root / resolved.rel_path)
    stored, _ = update_decision(
        root,
        stored.decision_id,
        {"card_mutation_id": card_mutation_id},
        source="decision",
    )
    return CorrectionResult(
        decision_id=stored.decision_id,
        uid=request.uid,
        field=request.field,
        action="apply_override",
        decision_mutation_id=decision_record.mutation_id,
        card_mutation_id=card_mutation_id,
        before_revision=resolved.revision,
        after_revision=after,
        invalidation=DecisionInvalidationHook(
            decision_id=stored.decision_id,
            uid=request.uid,
            field=request.field,
            before_revision=resolved.revision,
            after_revision=after,
            mutation_id=card_mutation_id,
        ),
    )


def clear_override(
    vault: str | Path,
    request: CorrectionCommandRequest,
    *,
    fault: FaultHook | None = None,
) -> CorrectionResult:
    """Remove an override and restore the latest source-derived value."""

    root = Path(vault)
    resolved = resolve_card(root, request.uid, request.rel_path)
    override = active_overrides_for(root, request.uid).get(request.field)
    if override is None:
        raise IncompatibleStateError(f"no active override for {request.uid}.{request.field}")
    author = request.author or override.author
    reason = request.reason or "clear override"
    if not author or not reason:
        raise IncompatibleStateError("author and reason are required to clear an override")
    clear = FieldDecision(
        decision_id=new_decision_id(),
        kind=KIND_CLEAR_OVERRIDE,
        status=STATUS_CLEARED,
        uid=request.uid,
        field=request.field,
        replacement_value=None,
        original_value=override.replacement_value,
        original_value_hash=value_hash(override.replacement_value),
        latest_source_value=override.latest_source_value,
        latest_source_value_hash=override.latest_source_value_hash,
        source_revision=resolved.revision,
        input_revision=override.input_revision,
        author=author,
        reason=reason,
        timestamp=override.timestamp,
        supersedes=override.decision_id,
        source_provenance=override.source_provenance,
    )
    # Rebuild timestamp via append path (decision.timestamp already set; refresh).
    stored, decision_record = append_decision(root, clear, source="decision")
    update_decision(root, override.decision_id, {"status": STATUS_CLEARED}, source="decision")
    hook = fault or FaultHook()
    hook.check("after_decision_intent")
    data = dict(resolved.frontmatter)
    data[request.field] = override.latest_source_value
    restored = _source_provenance_entry(override.source_provenance, resolved.provenance.get(request.field))
    provenance = merge_provenance(resolved.provenance, {request.field: restored})
    _write_corrected_card(resolved, root, data, provenance)
    card_mutation_id = _latest_card_mutation(root, request.uid)
    after = file_revision(root / resolved.rel_path)
    update_decision(root, stored.decision_id, {"card_mutation_id": card_mutation_id}, source="decision")
    return CorrectionResult(
        decision_id=stored.decision_id,
        uid=request.uid,
        field=request.field,
        action="clear_override",
        decision_mutation_id=decision_record.mutation_id,
        card_mutation_id=card_mutation_id,
        before_revision=resolved.revision,
        after_revision=after,
        conflicts=tuple(open_conflicts_for(root, request.uid, request.field)),
        invalidation=DecisionInvalidationHook(
            decision_id=stored.decision_id,
            uid=request.uid,
            field=request.field,
            before_revision=resolved.revision,
            after_revision=after,
            mutation_id=card_mutation_id,
        ),
    )


def reconcile_pending_corrections(vault: str | Path) -> list[CorrectionResult]:
    """Apply active override intents whose card mutation never committed."""

    root = Path(vault)
    results: list[CorrectionResult] = []
    for decision in list_decisions(root):
        if decision.kind != KIND_FIELD_OVERRIDE or decision.status != STATUS_ACTIVE:
            continue
        if decision.card_mutation_id:
            continue
        try:
            resolved = resolve_card(root, decision.uid)
        except IncompatibleStateError:
            continue
        current = resolved.frontmatter.get(decision.field)
        if values_equivalent(current, decision.replacement_value):
            mutation_id = _latest_card_mutation(root, decision.uid)
            update_decision(root, decision.decision_id, {"card_mutation_id": mutation_id}, source="decision")
            continue
        data = dict(resolved.frontmatter)
        data[decision.field] = decision.replacement_value
        provenance = merge_provenance(resolved.provenance, {decision.field: _human_provenance(decision)})
        _write_corrected_card(resolved, root, data, provenance)
        card_mutation_id = _latest_card_mutation(root, decision.uid)
        after = file_revision(root / resolved.rel_path)
        update_decision(root, decision.decision_id, {"card_mutation_id": card_mutation_id}, source="decision")
        results.append(
            CorrectionResult(
                decision_id=decision.decision_id,
                uid=decision.uid,
                field=decision.field,
                action="reconcile",
                decision_mutation_id=decision.decision_mutation_id,
                card_mutation_id=card_mutation_id,
                before_revision=resolved.revision,
                after_revision=after,
                invalidation=DecisionInvalidationHook(
                    decision_id=decision.decision_id,
                    uid=decision.uid,
                    field=decision.field,
                    before_revision=resolved.revision,
                    after_revision=after,
                    mutation_id=card_mutation_id,
                ),
            )
        )
    return results


def execute_correction_command(
    vault: str | Path,
    request: CorrectionCommandRequest,
    *,
    fault: FaultHook | None = None,
) -> CorrectionResult | list[CorrectionResult]:
    """Engine command entry. Suitable for later CLI registration."""

    if request.action == "apply_override":
        return apply_override(vault, request, fault=fault)
    if request.action == "clear_override":
        return clear_override(vault, request, fault=fault)
    if request.action == "reconcile":
        return reconcile_pending_corrections(vault)
    raise IncompatibleStateError(f"unsupported correction action: {request.action}")
