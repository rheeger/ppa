"""Manual field corrections as journaled human decisions (P07-B).

Correcting an amount or name writes a decision first, then a canonical card
mutation through the P02 ChangeJournal. Source replay cannot last-write-wins.
Clear-override restores source-derived behavior. This module is the engine
command request later CLI registration can call; P09 owns parser wiring.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

from archive_engine.contracts import OutputReceipt, OutputRevision
from archive_engine.errors import IncompatibleStateError
from archive_vault.change_journal import ChangeJournal, FaultHook, file_revision
from archive_vault.decisions import (
    KIND_CLEAR_OVERRIDE,
    KIND_FIELD_OVERRIDE,
    KIND_IDENTITY_MERGE,
    KIND_IDENTITY_SPLIT,
    KIND_IDENTITY_UNDO,
    STATUS_ACTIVE,
    STATUS_BLOCKED,
    STATUS_CLEARED,
    STATUS_PARTIAL,
    FieldDecision,
    active_identity_merge,
    active_overrides_for,
    append_decision,
    append_decision_payload,
    build_field_override,
    latest_identity_merge,
    list_decisions,
    list_identity_decisions,
    new_decision_id,
    open_conflicts_for,
    update_decision,
    update_decision_payload,
    value_hash,
    values_equivalent,
)
from archive_vault.identity import (
    REDIRECT_UID_PREFIX,
    REDIRECT_WIKILINK_PREFIX,
    UID_ALIAS_PREFIX,
    apply_alias_moves,
    load_identity_map,
    person_alias_pairs,
    revert_alias_moves,
    save_identity_map,
)
from archive_vault.provenance import (
    PROVENANCE_METHOD_HUMAN,
    ProvenanceEntry,
    merge_provenance,
)
from archive_vault.schema import validate_card_strict
from archive_vault.vault import iter_note_paths, read_note, read_note_by_uid, write_card

CorrectionAction = Literal[
    "apply_override",
    "clear_override",
    "reconcile",
    "merge_identities",
    "undo_identity",
    "split_identity",
]

IDENTITY_PROCESSOR = "identity-decision"
IDENTITY_PROCESSOR_VERSION = "p07c.1"


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
    winner_uid: str = ""
    loser_uid: str = ""
    decision_id: str = ""


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
    receipts: tuple[OutputReceipt, ...] = ()
    identity_conflicts: tuple[dict[str, Any], ...] = ()
    status: str = ""


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


def _write_corrected_card(
    resolved: _ResolvedCard, vault: Path, data: dict[str, Any], provenance: dict[str, ProvenanceEntry]
) -> None:
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
    if request.action == "merge_identities":
        return merge_identities(vault, request, fault=fault)
    if request.action in {"undo_identity", "split_identity"}:
        return undo_identity(vault, request, fault=fault)
    raise IncompatibleStateError(f"unsupported correction action: {request.action}")


def _wikilink_for(rel_path: str, frontmatter: dict[str, Any]) -> str:
    slug = Path(rel_path).stem
    summary = str(frontmatter.get("summary") or "").strip()
    return f"[[{slug}]]" if slug else f"[[{summary}]]"


def _snapshot_card(vault: Path, rel_path: str) -> dict[str, Any]:
    frontmatter, body, provenance = read_note(vault, rel_path)
    return {
        "rel_path": rel_path,
        "frontmatter": dict(frontmatter),
        "body": body,
        "provenance": {name: asdict(entry) for name, entry in provenance.items()},
        "revision": file_revision(vault / rel_path),
    }


def _provenance_from_snapshot(payload: Mapping[str, Any] | None) -> dict[str, ProvenanceEntry]:
    restored: dict[str, ProvenanceEntry] = {}
    for name, raw in (payload or {}).items():
        if not isinstance(raw, dict):
            continue
        restored[str(name)] = ProvenanceEntry(
            source=str(raw.get("source") or ""),
            date=str(raw.get("date") or _utc_date()),
            method=str(raw.get("method") or "deterministic"),
            model=str(raw.get("model") or ""),
            enrichment_version=int(raw.get("enrichment_version") or 0),
            input_hash=str(raw.get("input_hash") or ""),
            prior=list(raw["prior"]) if isinstance(raw.get("prior"), list) else None,
        )
    return restored


def _write_snapshot(vault: Path, snapshot: dict[str, Any]) -> None:
    frontmatter = dict(snapshot["frontmatter"])
    provenance = _provenance_from_snapshot(snapshot.get("provenance") or {})
    card = validate_card_strict(frontmatter)
    write_card(vault, str(snapshot["rel_path"]), card, body=str(snapshot.get("body") or ""), provenance=provenance)


def _list_added(existing: list[Any], incoming: list[Any]) -> list[Any]:
    seen = set(existing)
    added: list[Any] = []
    for item in incoming:
        if item in seen:
            continue
        seen.add(item)
        added.append(item)
    return added


def _union(existing: list[Any], incoming: list[Any]) -> list[Any]:
    out: list[Any] = []
    seen: set[Any] = set()
    for item in [*existing, *incoming]:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _find_people_references(vault: Path, wikilink: str, *, skip_rel: str) -> list[dict[str, Any]]:
    slug = wikilink.removeprefix("[[").removesuffix("]]")
    variants = {wikilink, f"[[{slug}]]", slug}
    found: list[dict[str, Any]] = []
    for rel in iter_note_paths(vault):
        rel_s = rel.as_posix() if hasattr(rel, "as_posix") else str(rel)
        if rel_s == skip_rel:
            continue
        frontmatter, _body, _prov = read_note(vault, rel_s)
        people = list(frontmatter.get("people") or [])
        if not any(str(item) in variants or str(item).removeprefix("[[").removesuffix("]]") == slug for item in people):
            continue
        found.append(
            {
                "uid": str(frontmatter.get("uid") or ""),
                "rel_path": rel_s,
                "field": "people",
                "before": people,
                "revision": file_revision(vault / rel_s),
            }
        )
    return found


def _rewrite_people(people: list[Any], loser_link: str, winner_link: str) -> list[Any]:
    loser_slug = loser_link.removeprefix("[[").removesuffix("]]")
    rewritten: list[Any] = []
    seen: set[str] = set()
    for item in people:
        text = str(item)
        slug = text.removeprefix("[[").removesuffix("]]")
        replacement = (
            winner_link if text in {loser_link, f"[[{loser_slug}]]", loser_slug} or slug == loser_slug else text
        )
        if replacement in seen:
            continue
        seen.add(replacement)
        rewritten.append(replacement)
    return rewritten


def identity_receipts(
    vault: str | Path, payload: dict[str, Any], *, status: str = "completed"
) -> tuple[OutputReceipt, ...]:
    """P03-adoptable receipts for UIDs touched by an identity decision."""

    root = Path(vault)
    outputs: list[OutputRevision] = []
    for uid, rel in (
        (str(payload.get("winner_uid") or ""), str(payload.get("winner_rel_path") or "")),
        (str(payload.get("loser_uid") or ""), str(payload.get("loser_rel_path") or "")),
    ):
        if uid and rel and (root / rel).is_file():
            outputs.append(OutputRevision(uid=uid, revision=file_revision(root / rel)))
    for ref in payload.get("references") or []:
        uid = str(ref.get("uid") or "")
        rel = str(ref.get("rel_path") or "")
        if uid and rel and (root / rel).is_file():
            outputs.append(OutputRevision(uid=uid, revision=file_revision(root / rel)))
    if not outputs:
        return ()
    receipt_status = (
        status
        if status in {"completed", "pending", "failed", "skipped", "blocked", "dependency_unmet"}
        else "completed"
    )
    return (
        OutputReceipt(
            processor=IDENTITY_PROCESSOR,
            processor_version=IDENTITY_PROCESSOR_VERSION,
            input_uid=str(payload.get("winner_uid") or payload.get("loser_uid") or "identity"),
            input_revision=str(payload.get("winner_revision") or payload.get("loser_revision") or outputs[0].revision),
            status=receipt_status,  # type: ignore[arg-type]
            outputs=tuple(outputs),
        ),
    )


def merge_identities(
    vault: str | Path,
    request: CorrectionCommandRequest,
    *,
    fault: FaultHook | None = None,
) -> CorrectionResult:
    """Merge loser into winner with an explicit preimage and redirect."""

    root = Path(vault)
    winner_uid = request.winner_uid or request.uid
    loser_uid = request.loser_uid
    if not winner_uid or not loser_uid:
        raise IncompatibleStateError("winner_uid and loser_uid are required")
    if winner_uid == loser_uid:
        raise IncompatibleStateError("cannot merge a person into itself")
    if not request.author or not request.reason:
        raise IncompatibleStateError("author and reason are required")

    existing = active_identity_merge(root, winner_uid=winner_uid, loser_uid=loser_uid)
    if existing is not None:
        receipts = identity_receipts(root, existing)
        return CorrectionResult(
            decision_id=str(existing.get("decision_id") or ""),
            uid=winner_uid,
            field="identity",
            action="merge_identities",
            decision_mutation_id=str(existing.get("decision_mutation_id") or ""),
            card_mutation_id=str(
                (existing.get("card_mutation_ids") or [""])[0] if existing.get("card_mutation_ids") else ""
            ),
            before_revision=str(existing.get("winner_revision") or ""),
            after_revision=file_revision(root / str(existing.get("winner_rel_path") or "")),
            receipts=receipts,
            status=STATUS_ACTIVE,
        )

    winner = resolve_card(root, winner_uid)
    loser = resolve_card(root, loser_uid)
    winner_link = _wikilink_for(winner.rel_path, winner.frontmatter)
    loser_link = _wikilink_for(loser.rel_path, loser.frontmatter)
    winner_snap = _snapshot_card(root, winner.rel_path)
    loser_snap = _snapshot_card(root, loser.rel_path)

    merged_values: dict[str, Any] = {}
    winner_data = dict(winner.frontmatter)
    for field_name in ("emails", "phones", "aliases", "source", "tags", "companies", "titles"):
        added = _list_added(list(winner_data.get(field_name) or []), list(loser.frontmatter.get(field_name) or []))
        if added:
            merged_values[field_name] = added
            winner_data[field_name] = _union(
                list(winner_data.get(field_name) or []), list(loser.frontmatter.get(field_name) or [])
            )
    for field_name in ("company", "title", "linkedin", "github"):
        if not winner_data.get(field_name) and loser.frontmatter.get(field_name):
            merged_values[field_name] = loser.frontmatter.get(field_name)
            winner_data[field_name] = loser.frontmatter.get(field_name)

    references = _find_people_references(root, loser_link, skip_rel=loser.rel_path)
    map_entries = load_identity_map(root)
    moves: list[dict[str, str]] = []
    for prefix, value in person_alias_pairs(loser_link, loser.frontmatter):
        key = f"{prefix}:{value}" if prefix != "uid" else f"{UID_ALIAS_PREFIX}{value}"
        moves.append({"key": key, "from": map_entries.get(key, loser_link), "to": winner_link})
    moves.append({"key": f"{REDIRECT_UID_PREFIX}{loser_uid}", "from": "", "to": winner_uid})
    moves.append({"key": f"{REDIRECT_WIKILINK_PREFIX}{loser_link}", "from": "", "to": winner_link})
    moves.append(
        {
            "key": f"{UID_ALIAS_PREFIX}{loser_uid}",
            "from": map_entries.get(f"{UID_ALIAS_PREFIX}{loser_uid}", ""),
            "to": winner_link,
        }
    )

    payload = {
        "decision_id": new_decision_id(),
        "kind": KIND_IDENTITY_MERGE,
        "status": STATUS_ACTIVE,
        "uid": winner_uid,
        "field": "identity",
        "author": request.author,
        "reason": request.reason,
        "timestamp": _utc_date(),
        "winner_uid": winner_uid,
        "loser_uid": loser_uid,
        "winner_wikilink": winner_link,
        "loser_wikilink": loser_link,
        "winner_rel_path": winner.rel_path,
        "loser_rel_path": loser.rel_path,
        "winner_revision": winner.revision,
        "loser_revision": loser.revision,
        "input_revision": loser.revision,
        "winner_preimage": winner_snap,
        "loser_preimage": loser_snap,
        "merged_values": merged_values,
        "aliases_moved": moves,
        "references": references,
        "recovery_class": "decision_critical",
    }
    stored, decision_record = append_decision_payload(root, payload, source="identity")
    hook = fault or FaultHook()
    hook.check("after_decision_intent")

    winner_prov = dict(winner.provenance)
    for field_name in merged_values:
        if field_name in loser.provenance:
            winner_prov[field_name] = loser.provenance[field_name]
        elif field_name not in winner_prov:
            winner_prov[field_name] = ProvenanceEntry(
                source=f"decision:{stored['decision_id']}",
                date=_utc_date(),
                method=PROVENANCE_METHOD_HUMAN,
            )
    from archive_vault.provenance import PROVENANCE_EXEMPT_FIELDS as _PROV_EXEMPT

    for field_name, value in winner_data.items():
        if field_name in _PROV_EXEMPT or value in ("", [], None, 0):
            continue
        if field_name not in winner_prov:
            winner_prov[field_name] = ProvenanceEntry(
                source=f"decision:{stored['decision_id']}",
                date=_utc_date(),
                method=PROVENANCE_METHOD_HUMAN,
            )
    winner_data["updated"] = _utc_date()
    write_card(root, winner.rel_path, validate_card_strict(winner_data), body=winner.body, provenance=winner_prov)

    stub = dict(loser.frontmatter)
    stub["redirect_to"] = winner_uid
    stub["updated"] = _utc_date()
    stub_prov = dict(loser.provenance)
    stub_prov["redirect_to"] = ProvenanceEntry(
        source=f"decision:{stored['decision_id']}",
        date=_utc_date(),
        method=PROVENANCE_METHOD_HUMAN,
        input_hash=value_hash({"winner_uid": winner_uid, "loser_uid": loser_uid}),
    )
    for field_name, value in stub.items():
        if field_name in _PROV_EXEMPT or value in ("", [], None, 0):
            continue
        if field_name not in stub_prov:
            stub_prov[field_name] = ProvenanceEntry(
                source=f"decision:{stored['decision_id']}",
                date=_utc_date(),
                method=PROVENANCE_METHOD_HUMAN,
            )
    write_card(
        root, loser.rel_path, validate_card_strict(stub), body=f"Redirected to {winner_link}\n", provenance=stub_prov
    )

    rewritten_refs: list[dict[str, Any]] = []
    for ref in references:
        frontmatter, body, provenance = read_note(root, str(ref["rel_path"]))
        after_people = _rewrite_people(list(frontmatter.get("people") or []), loser_link, winner_link)
        frontmatter = dict(frontmatter)
        frontmatter["people"] = after_people
        frontmatter["updated"] = _utc_date()
        write_card(root, str(ref["rel_path"]), validate_card_strict(frontmatter), body=body, provenance=provenance)
        rewritten_refs.append({**ref, "after": after_people})

    save_identity_map(root, apply_alias_moves(map_entries, moves))
    mutation_ids = [
        _latest_card_mutation(root, winner_uid),
        _latest_card_mutation(root, loser_uid),
        *[_latest_card_mutation(root, str(ref.get("uid") or "")) for ref in rewritten_refs if ref.get("uid")],
    ]
    stored, _ = update_decision_payload(
        root,
        stored["decision_id"],
        {"references": rewritten_refs, "card_mutation_ids": [mid for mid in mutation_ids if mid]},
        source="identity",
    )
    receipts = identity_receipts(root, stored)
    after = file_revision(root / winner.rel_path)
    return CorrectionResult(
        decision_id=str(stored["decision_id"]),
        uid=winner_uid,
        field="identity",
        action="merge_identities",
        decision_mutation_id=decision_record.mutation_id,
        card_mutation_id=mutation_ids[0] if mutation_ids else "",
        before_revision=winner.revision,
        after_revision=after,
        receipts=receipts,
        status=STATUS_ACTIVE,
        invalidation=DecisionInvalidationHook(
            decision_id=str(stored["decision_id"]),
            uid=winner_uid,
            field="identity",
            before_revision=winner.revision,
            after_revision=after,
            mutation_id=mutation_ids[0] if mutation_ids else "",
        ),
    )


def undo_identity(
    vault: str | Path,
    request: CorrectionCommandRequest,
    *,
    fault: FaultHook | None = None,
) -> CorrectionResult:
    """Inverse of a recorded merge. Later unrelated edits are kept."""

    root = Path(vault)
    merge = None
    if request.decision_id:
        for item in list_identity_decisions(root):
            if str(item.get("decision_id") or "") == request.decision_id:
                merge = item
                break
    if merge is None:
        merge = latest_identity_merge(
            root,
            winner_uid=request.winner_uid,
            loser_uid=request.loser_uid,
        )
    if merge is None:
        raise IncompatibleStateError("no identity merge preimage found; refusing to guess")
    if str(merge.get("kind") or "") != KIND_IDENTITY_MERGE:
        raise IncompatibleStateError("undo/split requires an identity_merge preimage")

    already = None
    for item in list_identity_decisions(root):
        if str(item.get("kind") or "") in {KIND_IDENTITY_UNDO, KIND_IDENTITY_SPLIT} and str(
            item.get("supersedes") or ""
        ) == str(merge.get("decision_id") or ""):
            already = item
    if already is not None and str(merge.get("status") or "") != STATUS_ACTIVE:
        return CorrectionResult(
            decision_id=str(already.get("decision_id") or ""),
            uid=str(merge.get("winner_uid") or ""),
            field="identity",
            action=request.action,
            decision_mutation_id=str(already.get("decision_mutation_id") or ""),
            card_mutation_id="",
            before_revision=str(merge.get("winner_revision") or ""),
            after_revision=file_revision(root / str(merge.get("winner_rel_path") or "")),
            receipts=identity_receipts(root, merge),
            status=str(already.get("status") or STATUS_CLEARED),
        )

    author = request.author or str(merge.get("author") or "")
    reason = request.reason or "undo identity merge"
    if not author or not reason:
        raise IncompatibleStateError("author and reason are required")

    conflicts: list[dict[str, Any]] = []
    loser_rel = str(merge.get("loser_rel_path") or "")
    winner_rel = str(merge.get("winner_rel_path") or "")
    loser_preimage = dict(merge.get("loser_preimage") or {})
    winner_preimage = dict(merge.get("winner_preimage") or {})
    if not loser_preimage or not winner_preimage:
        raise IncompatibleStateError("merge preimage is incomplete")

    if (root / loser_rel).is_file():
        current_loser, _, _ = read_note(root, loser_rel)
        if current_loser.get("redirect_to") not in {"", merge.get("winner_uid")} and current_loser.get(
            "uid"
        ) == merge.get("loser_uid"):
            if str(current_loser.get("redirect_to") or "") != str(merge.get("winner_uid") or ""):
                conflicts.append(
                    {
                        "uid": merge.get("loser_uid"),
                        "field": "redirect_to",
                        "reason": "loser redirect changed after merge",
                        "actual": current_loser.get("redirect_to"),
                    }
                )
    else:
        conflicts.append({"uid": merge.get("loser_uid"), "field": "rel_path", "reason": "loser card missing"})

    winner_now = dict(read_note(root, winner_rel)[0]) if (root / winner_rel).is_file() else {}
    merged_values = dict(merge.get("merged_values") or {})
    safe_winner = dict(winner_now)
    for field_name, added in merged_values.items():
        current_val = winner_now.get(field_name)
        if isinstance(added, list):
            if not isinstance(current_val, list):
                conflicts.append(
                    {"uid": merge.get("winner_uid"), "field": field_name, "reason": "winner field shape changed"}
                )
                continue
            remaining = [item for item in current_val if item not in added]
            extra = [
                item
                for item in current_val
                if item not in (winner_preimage.get("frontmatter") or {}).get(field_name, []) and item not in added
            ]
            if extra:
                conflicts.append(
                    {
                        "uid": merge.get("winner_uid"),
                        "field": field_name,
                        "reason": "later unrelated values preserved; merged values still removable",
                    }
                )
            safe_winner[field_name] = remaining
        else:
            if current_val != added:
                conflicts.append(
                    {
                        "uid": merge.get("winner_uid"),
                        "field": field_name,
                        "reason": "later edit replaced a merged scalar",
                        "actual": current_val,
                    }
                )
                continue
            safe_winner[field_name] = (winner_preimage.get("frontmatter") or {}).get(field_name, "")

    blocked = any(item.get("reason") == "loser card missing" for item in conflicts)
    if blocked:
        undo_payload = {
            "decision_id": new_decision_id(),
            "kind": KIND_IDENTITY_UNDO if request.action != "split_identity" else KIND_IDENTITY_SPLIT,
            "status": STATUS_BLOCKED,
            "uid": merge.get("winner_uid"),
            "field": "identity",
            "author": author,
            "reason": reason,
            "timestamp": _utc_date(),
            "supersedes": merge.get("decision_id"),
            "winner_uid": merge.get("winner_uid"),
            "loser_uid": merge.get("loser_uid"),
            "winner_rel_path": winner_rel,
            "loser_rel_path": loser_rel,
            "conflicts": conflicts,
            "recovery_class": "decision_critical",
        }
        stored, record = append_decision_payload(root, undo_payload, source="identity")
        return CorrectionResult(
            decision_id=str(stored["decision_id"]),
            uid=str(merge.get("winner_uid") or ""),
            field="identity",
            action=request.action,
            decision_mutation_id=record.mutation_id,
            card_mutation_id="",
            before_revision=str(merge.get("winner_revision") or ""),
            after_revision=file_revision(root / winner_rel) if (root / winner_rel).is_file() else "",
            receipts=identity_receipts(root, merge, status="blocked"),
            identity_conflicts=tuple(conflicts),
            status=STATUS_BLOCKED,
        )

    undo_payload = {
        "decision_id": new_decision_id(),
        "kind": KIND_IDENTITY_UNDO if request.action != "split_identity" else KIND_IDENTITY_SPLIT,
        "status": STATUS_PARTIAL if conflicts else STATUS_CLEARED,
        "uid": merge.get("winner_uid"),
        "field": "identity",
        "author": author,
        "reason": reason,
        "timestamp": _utc_date(),
        "supersedes": merge.get("decision_id"),
        "winner_uid": merge.get("winner_uid"),
        "loser_uid": merge.get("loser_uid"),
        "winner_wikilink": merge.get("winner_wikilink"),
        "loser_wikilink": merge.get("loser_wikilink"),
        "winner_rel_path": winner_rel,
        "loser_rel_path": loser_rel,
        "winner_revision": merge.get("winner_revision"),
        "loser_revision": merge.get("loser_revision"),
        "winner_preimage": winner_preimage,
        "loser_preimage": loser_preimage,
        "aliases_moved": merge.get("aliases_moved") or [],
        "references": merge.get("references") or [],
        "conflicts": conflicts,
        "recovery_class": "decision_critical",
    }
    stored, decision_record = append_decision_payload(root, undo_payload, source="identity")
    hook = fault or FaultHook()
    hook.check("after_decision_intent")

    _write_snapshot(root, loser_preimage)
    if (root / winner_rel).is_file():
        _fm, body, provenance = read_note(root, winner_rel)
        safe_winner["updated"] = _utc_date()
        write_card(root, winner_rel, validate_card_strict(safe_winner), body=body, provenance=provenance)

    for ref in merge.get("references") or []:
        rel = str(ref.get("rel_path") or "")
        if not rel or not (root / rel).is_file():
            conflicts.append({"uid": ref.get("uid"), "field": "people", "reason": "reference card missing"})
            continue
        frontmatter, body, provenance = read_note(root, rel)
        current_people = list(frontmatter.get("people") or [])
        expected_after = list(ref.get("after") or [])
        if expected_after and current_people != expected_after:
            conflicts.append(
                {
                    "uid": ref.get("uid"),
                    "field": "people",
                    "reason": "later edit on rewritten reference; left in place",
                    "actual": current_people,
                }
            )
            continue
        frontmatter = dict(frontmatter)
        frontmatter["people"] = list(ref.get("before") or [])
        frontmatter["updated"] = _utc_date()
        write_card(root, rel, validate_card_strict(frontmatter), body=body, provenance=provenance)

    save_identity_map(root, revert_alias_moves(load_identity_map(root), list(merge.get("aliases_moved") or [])))
    final_status = STATUS_PARTIAL if conflicts else STATUS_CLEARED
    update_decision_payload(root, str(merge["decision_id"]), {"status": STATUS_CLEARED}, source="identity")
    update_decision_payload(
        root, stored["decision_id"], {"status": final_status, "conflicts": conflicts}, source="identity"
    )
    receipts = identity_receipts(root, {**merge, **stored})
    return CorrectionResult(
        decision_id=str(stored["decision_id"]),
        uid=str(merge.get("winner_uid") or ""),
        field="identity",
        action=request.action,
        decision_mutation_id=decision_record.mutation_id,
        card_mutation_id=_latest_card_mutation(root, str(merge.get("loser_uid") or "")),
        before_revision=str(merge.get("winner_revision") or ""),
        after_revision=file_revision(root / winner_rel) if (root / winner_rel).is_file() else "",
        receipts=receipts,
        identity_conflicts=tuple(conflicts),
        status=final_status,
        invalidation=DecisionInvalidationHook(
            decision_id=str(stored["decision_id"]),
            uid=str(merge.get("loser_uid") or ""),
            field="identity",
            before_revision=str(merge.get("loser_revision") or ""),
            after_revision=file_revision(root / loser_rel) if (root / loser_rel).is_file() else "",
            mutation_id=_latest_card_mutation(root, str(merge.get("loser_uid") or "")),
        ),
    )
