"""P07-B: manual amount/name corrections survive source replay."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_engine.corrections import (
    CorrectionCommandRequest,
    apply_override,
    clear_override,
    execute_correction_command,
    reconcile_pending_corrections,
)
from archive_engine.recovery_manifest import generate_manifest
from archive_sync.adapters.base import BaseAdapter, deterministic_provenance
from archive_vault.change_journal import ChangeJournal, FaultHook, JournalFault
from archive_vault.decisions import (
    DECISIONS_REL_PATH,
    KIND_FIELD_OVERRIDE,
    KIND_SOURCE_CONFLICT,
    active_overrides_for,
    list_decisions,
    open_conflicts_for,
)
from archive_vault.provenance import PROVENANCE_METHOD_HUMAN
from archive_vault.schema import FinanceCard, PersonCard
from archive_vault.vault import read_note, write_card

DINNER_UID = "hfa-finance-p07dinner01"
DINNER_REL = "Finance/2026-09/hfa-finance-p07dinner01.md"
PERSON_UID = "hfa-person-p07name0001"
PERSON_REL = "People/p07-name.md"


class ReplayAdapter(BaseAdapter):
    source_id = "amex"

    def fetch(self, vault_path, cursor, config=None, **kwargs):
        return []

    def to_card(self, item):
        raise AssertionError("replay adapter is write-path only")


def _finance(amount: float, *, source_id: str = "amex:txn-dinner") -> FinanceCard:
    return FinanceCard(
        uid=DINNER_UID,
        type="finance",
        source=["amex"],
        source_id=source_id,
        created="2026-09-01",
        updated="2026-09-01",
        summary="Dinner",
        amount=amount,
        currency="USD",
        counterparty="Restaurant",
        note="synthetic dinner",
    )


def _person(first: str, last: str) -> PersonCard:
    return PersonCard(
        uid=PERSON_UID,
        type="person",
        source=["contacts.apple"],
        source_id="p07name@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=f"{first} {last}",
        first_name=first,
        last_name=last,
        emails=["p07name@example.test"],
    )


def _vault(tmp_path: Path) -> Path:
    vault = tmp_path / "vault"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance" / "2026-09").mkdir(parents=True)
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


def _write_dinner(vault: Path, amount: float) -> Path:
    card = _finance(amount)
    return write_card(
        vault,
        DINNER_REL,
        card,
        body="dinner receipt",
        provenance=deterministic_provenance(card, "amex"),
    )


def _write_person(vault: Path, first: str, last: str) -> Path:
    card = _person(first, last)
    return write_card(
        vault,
        PERSON_REL,
        card,
        body="person",
        provenance=deterministic_provenance(card, "contacts.apple"),
    )


def _replay_dinner(vault: Path, amount: float, *, source_id: str = "amex:txn-dinner") -> None:
    card = _finance(amount, source_id=source_id)
    ReplayAdapter()._replace_generic_card(
        vault,
        Path(DINNER_REL),
        card,
        "dinner receipt",
        deterministic_provenance(card, "amex"),
    )


def _replay_person(vault: Path, first: str, last: str) -> None:
    card = _person(first, last)
    adapter = ReplayAdapter()
    adapter.source_id = "contacts.apple"
    adapter._replace_generic_card(
        vault,
        Path(PERSON_REL),
        card,
        "person",
        deterministic_provenance(card, "contacts.apple"),
    )


def test_amount_correction_survives_older_and_newer_source_replay(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _write_dinner(vault, 42.0)
    result = execute_correction_command(
        vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=DINNER_UID,
            field="amount",
            value=38.0,
            author="robbie",
            reason="receipt was 38 not 42",
            rel_path=DINNER_REL,
        ),
    )
    assert result.decision_id
    assert result.card_mutation_id
    assert result.decision_mutation_id
    assert result.invalidation is not None

    frontmatter, _body, provenance = read_note(vault, DINNER_REL)
    assert frontmatter["amount"] == 38.0
    assert frontmatter["source"] == ["amex"]
    assert provenance["amount"].method == PROVENANCE_METHOD_HUMAN
    assert provenance["amount"].source.startswith("decision:")
    decision = active_overrides_for(vault, DINNER_UID)["amount"]
    assert decision.author == "robbie"
    assert decision.reason == "receipt was 38 not 42"
    assert decision.input_revision
    assert decision.kind == KIND_FIELD_OVERRIDE

    _replay_dinner(vault, 42.0)
    frontmatter, _, provenance = read_note(vault, DINNER_REL)
    assert frontmatter["amount"] == 38.0
    assert provenance["amount"].method == PROVENANCE_METHOD_HUMAN
    assert open_conflicts_for(vault, DINNER_UID, "amount") == []

    _replay_dinner(vault, 50.0, source_id="amex:txn-dinner-v2")
    frontmatter, _, provenance = read_note(vault, DINNER_REL)
    assert frontmatter["amount"] == 38.0
    assert provenance["amount"].method == PROVENANCE_METHOD_HUMAN
    conflicts = open_conflicts_for(vault, DINNER_UID, "amount")
    assert conflicts
    assert conflicts[-1].kind == KIND_SOURCE_CONFLICT
    assert conflicts[-1].incoming_value == 50.0
    assert frontmatter["source"] == ["amex"]


def test_name_correction_survives_source_replay(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _write_person(vault, "Jane", "Smith")
    apply_override(
        vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=PERSON_UID,
            field="first_name",
            value="Robert",
            author="robbie",
            reason="legal first name",
            rel_path=PERSON_REL,
        ),
    )
    _replay_person(vault, "Jane", "Smith")
    frontmatter, _, provenance = read_note(vault, PERSON_REL)
    assert frontmatter["first_name"] == "Robert"
    assert provenance["first_name"].method == PROVENANCE_METHOD_HUMAN
    _replay_person(vault, "Janet", "Smith")
    frontmatter, _, _ = read_note(vault, PERSON_REL)
    assert frontmatter["first_name"] == "Robert"
    assert open_conflicts_for(vault, PERSON_UID, "first_name")


def test_clear_override_restores_source_derived_behavior(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _write_dinner(vault, 42.0)
    apply_override(
        vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=DINNER_UID,
            field="amount",
            value=38.0,
            author="robbie",
            reason="receipt",
            rel_path=DINNER_REL,
        ),
    )
    _replay_dinner(vault, 50.0, source_id="amex:txn-dinner-v2")
    clear_override(
        vault,
        CorrectionCommandRequest(
            action="clear_override",
            uid=DINNER_UID,
            field="amount",
            author="robbie",
            reason="provider restated the charge",
            rel_path=DINNER_REL,
        ),
    )
    frontmatter, _, provenance = read_note(vault, DINNER_REL)
    assert frontmatter["amount"] == 50.0
    assert provenance["amount"].method == "deterministic"
    assert "amount" not in active_overrides_for(vault, DINNER_UID)
    _replay_dinner(vault, 51.0, source_id="amex:txn-dinner-v3")
    frontmatter, _, _ = read_note(vault, DINNER_REL)
    assert frontmatter["amount"] == 51.0


def test_set_empty_is_not_clear_override(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _write_person(vault, "Jane", "Smith")
    apply_override(
        vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=PERSON_UID,
            field="first_name",
            value="",
            author="robbie",
            reason="drop provider given name",
            rel_path=PERSON_REL,
        ),
    )
    frontmatter, _, _ = read_note(vault, PERSON_REL)
    assert frontmatter.get("first_name", "") == ""
    assert "first_name" in active_overrides_for(vault, PERSON_UID)
    _replay_person(vault, "Jane", "Smith")
    frontmatter, _, _ = read_note(vault, PERSON_REL)
    assert frontmatter.get("first_name", "") == ""
    clear_override(
        vault,
        CorrectionCommandRequest(
            action="clear_override",
            uid=PERSON_UID,
            field="first_name",
            author="robbie",
            reason="restore provider name",
            rel_path=PERSON_REL,
        ),
    )
    frontmatter, _, _ = read_note(vault, PERSON_REL)
    assert frontmatter["first_name"] == "Jane"


def test_source_write_without_decision_is_last_write_wins(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _write_dinner(vault, 42.0)
    card = _finance(38.0)
    write_card(vault, DINNER_REL, card, body="dinner receipt", provenance=deterministic_provenance(card, "amex"))
    frontmatter, _, provenance = read_note(vault, DINNER_REL)
    assert frontmatter["amount"] == 38.0
    assert provenance["amount"].method == "deterministic"
    assert active_overrides_for(vault, DINNER_UID) == {}
    _replay_dinner(vault, 42.0)
    frontmatter, _, _ = read_note(vault, DINNER_REL)
    assert frontmatter["amount"] == 42.0


def test_crash_after_decision_does_not_apply_value_until_reconcile(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _write_dinner(vault, 42.0)
    request = CorrectionCommandRequest(
        action="apply_override",
        uid=DINNER_UID,
        field="amount",
        value=38.0,
        author="robbie",
        reason="receipt",
        rel_path=DINNER_REL,
    )
    with pytest.raises(JournalFault, match="after_decision_intent"):
        apply_override(vault, request, fault=FaultHook(fail_at="after_decision_intent"))
    frontmatter, _, _ = read_note(vault, DINNER_REL)
    assert frontmatter["amount"] == 42.0
    pending = active_overrides_for(vault, DINNER_UID)["amount"]
    assert pending.card_mutation_id == ""
    assert pending.replacement_value == 38.0
    reconciled = reconcile_pending_corrections(vault)
    assert reconciled
    frontmatter, _, provenance = read_note(vault, DINNER_REL)
    assert frontmatter["amount"] == 38.0
    assert provenance["amount"].method == PROVENANCE_METHOD_HUMAN


def test_decision_log_and_journal_are_linked(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _write_dinner(vault, 42.0)
    result = apply_override(
        vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=DINNER_UID,
            field="amount",
            value=38.0,
            author="robbie",
            reason="receipt",
            rel_path=DINNER_REL,
        ),
    )
    assert (vault / DECISIONS_REL_PATH).is_file()
    kinds = {item.kind for item in list_decisions(vault)}
    assert KIND_FIELD_OVERRIDE in kinds
    with ChangeJournal(vault) as journal:
        records = journal.committed_records(limit=1000)
        uids = {record.uid for record in records}
        assert DINNER_UID in uids
        assert any(record.mutation_id == result.card_mutation_id for record in records)
        checkpoint = journal.checkpoint()
    assert checkpoint["status"] == "available"
    manifest = generate_manifest(vault)
    assert manifest["checkpoint"]["status"] == "available"
    paths = {item["rel_path"] for item in manifest["artifacts"]}
    assert DECISIONS_REL_PATH in paths
    assert "_meta/change-journal.sqlite3" in paths
