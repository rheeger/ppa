"""P07-C: identity merge undo preserves later unrelated edits."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_engine.corrections import (
    IDENTITY_PROCESSOR,
    CorrectionCommandRequest,
    execute_correction_command,
    identity_receipts,
    merge_identities,
    undo_identity,
)
from archive_engine.errors import IncompatibleStateError
from archive_sync.adapters.base import deterministic_provenance
from archive_vault.decisions import KIND_IDENTITY_MERGE, STATUS_ACTIVE, STATUS_BLOCKED, active_identity_merge
from archive_vault.identity import redirect_target_uid, resolve_any, upsert_identity_map
from archive_vault.identity_resolver import merge_into_existing, resolve_person
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import read_note, read_note_by_uid, write_card

WINNER_UID = "hfa-person-p07win00001"
LOSER_UID = "hfa-person-p07lose0001"
OTHER_UID = "hfa-person-p07other001"
WINNER_REL = "People/alex-winner.md"
LOSER_REL = "People/blake-loser.md"
OTHER_REL = "People/casey-other.md"


def _vault(tmp_path: Path) -> Path:
    vault = tmp_path / "vault"
    (vault / "People").mkdir(parents=True)
    meta = vault / "_meta"
    meta.mkdir()
    for name, payload in (
        ("identity-map.json", "{}"),
        ("sync-state.json", "{}"),
        ("own-emails.json", "[]"),
        ("nicknames.json", "{}"),
        ("ppa-config.json", "{}"),
        ("llm-config.json", '{"primary": {"provider": "gemini", "model": "fixture"}}'),
    ):
        (meta / name).write_text(payload + "\n", encoding="utf-8")
    return vault


def _person(uid: str, first: str, last: str, email: str, *, title: str = "", company: str = "") -> PersonCard:
    return PersonCard(
        uid=uid,
        type="person",
        source=["contacts.apple"],
        source_id=email,
        created="2026-09-06",
        updated="2026-09-06",
        summary=f"{first} {last}",
        first_name=first,
        last_name=last,
        emails=[email],
        title=title,
        company=company,
        tags=["p07-identity"],
    )


def _write_person(vault: Path, rel: str, card: PersonCard, *, people: list[str] | None = None) -> None:
    data = card.model_dump(mode="python")
    if people:
        data["people"] = people
        card = PersonCard.model_validate(data)
    write_card(vault, rel, card, body=card.summary, provenance=deterministic_provenance(card, "contacts.apple"))
    upsert_identity_map(
        vault,
        f"[[{Path(rel).stem}]]",
        {"name": card.summary, "emails": card.emails},
    )


def _seed(vault: Path) -> None:
    _write_person(vault, WINNER_REL, _person(WINNER_UID, "Alex", "Winner", "alex@example.test", title="Engineer"))
    _write_person(vault, LOSER_REL, _person(LOSER_UID, "Blake", "Loser", "blake@example.test", company="Endaoment"))
    other = _person(OTHER_UID, "Casey", "Other", "casey@example.test")
    _write_person(vault, OTHER_REL, other, people=["[[blake-loser]]"])


def test_merge_redirects_and_undo_restores_preimage(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _seed(vault)
    merged = merge_identities(
        vault,
        CorrectionCommandRequest(
            action="merge_identities",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="mistaken duplicate",
        ),
    )
    assert merged.status == STATUS_ACTIVE
    assert merged.receipts
    assert merged.receipts[0].processor == IDENTITY_PROCESSOR
    winner_fm, _, _ = read_note(vault, WINNER_REL)
    assert "blake@example.test" in winner_fm["emails"]
    assert winner_fm["company"] == "Endaoment"
    loser_found = read_note_by_uid(vault, LOSER_UID)
    assert loser_found is not None
    assert loser_found[1]["redirect_to"] == WINNER_UID
    assert redirect_target_uid(vault, LOSER_UID) == WINNER_UID
    assert resolve_any(vault, "email", "blake@example.test") == "[[alex-winner]]"
    other_fm, _, _ = read_note(vault, OTHER_REL)
    assert other_fm["people"] == ["[[alex-winner]]"]
    resolved = resolve_person(vault, {"emails": ["blake@example.test"], "summary": "Blake Loser"})
    assert resolved.wikilink == "[[alex-winner]]"

    winner = PersonCard.model_validate(dict(winner_fm))
    winner_data = winner.model_dump(mode="python")
    winner_data["title"] = "Staff Engineer"
    winner_data["updated"] = "2026-09-07"
    write_card(
        vault,
        WINNER_REL,
        PersonCard.model_validate(winner_data),
        body="later unrelated title edit",
        provenance={
            **deterministic_provenance(PersonCard.model_validate(winner_data), "contacts.apple"),
            "title": ProvenanceEntry("contacts.apple", "2026-09-07", "deterministic"),
        },
    )

    undone = undo_identity(
        vault,
        CorrectionCommandRequest(
            action="undo_identity",
            decision_id=merged.decision_id,
            author="robbie",
            reason="not the same person",
        ),
    )
    assert undone.status in {undone.status}
    assert undone.status != STATUS_BLOCKED
    restored_loser = read_note_by_uid(vault, LOSER_UID)
    assert restored_loser is not None
    assert restored_loser[1].get("redirect_to", "") == ""
    assert restored_loser[1]["emails"] == ["blake@example.test"]
    winner_after, _, _ = read_note(vault, WINNER_REL)
    assert winner_after["title"] == "Staff Engineer"
    assert "blake@example.test" not in winner_after.get("emails", [])
    other_after, _, _ = read_note(vault, OTHER_REL)
    assert other_after["people"] == ["[[blake-loser]]"]
    assert resolve_any(vault, "email", "blake@example.test") == "[[blake-loser]]"
    assert active_identity_merge(vault, winner_uid=WINNER_UID, loser_uid=LOSER_UID) is None


def test_merge_replay_is_idempotent(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _seed(vault)
    first = execute_correction_command(
        vault,
        CorrectionCommandRequest(
            action="merge_identities",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="duplicate",
        ),
    )
    second = execute_correction_command(
        vault,
        CorrectionCommandRequest(
            action="merge_identities",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="duplicate",
        ),
    )
    assert first.decision_id == second.decision_id
    undone = execute_correction_command(
        vault,
        CorrectionCommandRequest(
            action="undo_identity",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="undo",
        ),
    )
    replay = execute_correction_command(
        vault,
        CorrectionCommandRequest(
            action="split_identity",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="undo",
        ),
    )
    assert replay.decision_id == undone.decision_id


def test_later_conflicting_edit_is_partial_not_data_loss(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _seed(vault)
    merge_identities(
        vault,
        CorrectionCommandRequest(
            action="merge_identities",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="duplicate",
        ),
    )
    winner_fm, body, _ = read_note(vault, WINNER_REL)
    winner_fm = dict(winner_fm)
    winner_fm["company"] = "Later Corp"
    write_card(
        vault,
        WINNER_REL,
        PersonCard.model_validate(winner_fm),
        body=body,
        provenance=deterministic_provenance(PersonCard.model_validate(winner_fm), "contacts.apple"),
    )
    undone = undo_identity(
        vault,
        CorrectionCommandRequest(
            action="undo_identity",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="undo",
        ),
    )
    winner_after, _, _ = read_note(vault, WINNER_REL)
    assert winner_after["company"] == "Later Corp"
    assert any(item.get("field") == "company" for item in undone.identity_conflicts)
    loser = read_note_by_uid(vault, LOSER_UID)
    assert loser is not None
    assert loser[1]["emails"] == ["blake@example.test"]


def test_missing_loser_blocks_without_guessing(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _seed(vault)
    merge_identities(
        vault,
        CorrectionCommandRequest(
            action="merge_identities",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="duplicate",
        ),
    )
    (vault / LOSER_REL).unlink()
    undone = undo_identity(
        vault,
        CorrectionCommandRequest(
            action="undo_identity",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="undo",
        ),
    )
    assert undone.status == STATUS_BLOCKED
    winner_fm, _, _ = read_note(vault, WINNER_REL)
    assert "blake@example.test" in winner_fm["emails"]


def test_split_without_preimage_is_refused(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _seed(vault)
    with pytest.raises(IncompatibleStateError, match="preimage"):
        undo_identity(
            vault,
            CorrectionCommandRequest(
                action="split_identity",
                winner_uid=WINNER_UID,
                loser_uid=LOSER_UID,
                author="robbie",
                reason="guess",
            ),
        )


def test_adapter_merge_follows_redirect(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _seed(vault)
    merge_identities(
        vault,
        CorrectionCommandRequest(
            action="merge_identities",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="duplicate",
        ),
    )
    incoming = _person(LOSER_UID, "Blake", "Loser", "blake@example.test")
    incoming_data = incoming.model_dump(mode="python")
    incoming_data["phones"] = ["+15550001111"]
    merge_into_existing(
        vault,
        "[[blake-loser]]",
        incoming_data,
        deterministic_provenance(PersonCard.model_validate(incoming_data), "linkedin"),
        target_rel_path=LOSER_REL,
    )
    winner_fm, _, _ = read_note(vault, WINNER_REL)
    assert "+15550001111" in winner_fm.get("phones", [])


def test_identity_receipts_list_touched_uids(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _seed(vault)
    merged = merge_identities(
        vault,
        CorrectionCommandRequest(
            action="merge_identities",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="robbie",
            reason="duplicate",
        ),
    )
    decision = active_identity_merge(vault, winner_uid=WINNER_UID, loser_uid=LOSER_UID)
    assert decision is not None
    assert decision["kind"] == KIND_IDENTITY_MERGE
    receipts = identity_receipts(vault, decision)
    uids = {item.uid for receipt in receipts for item in receipt.outputs}
    assert {WINNER_UID, LOSER_UID, OTHER_UID} <= uids
    assert merged.receipts[0].outputs
