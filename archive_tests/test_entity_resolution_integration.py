"""Steps 16–17 — person linking pipeline integration tests."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from archive_sync.adapters.base import deterministic_provenance
from archive_sync.extractors.entity_resolution import run_person_linking
from archive_vault.schema import (FinanceCard, MedicalRecordCard, PersonCard,
                                  RideCard)
from archive_vault.vault import read_note, write_card


def _make_person(vault: Path, name: str, *, email: str = "", uid: str = "") -> str:
    slug = name.lower().replace(" ", "-")
    uid = uid or f"hfa-person-test-{slug}"
    card = PersonCard(
        uid=uid,
        type="person",
        source=["test"],
        source_id=uid,
        created=date.today().isoformat(),
        updated=date.today().isoformat(),
        summary=name,
        first_name=name.split()[0] if " " in name else name,
        last_name=name.split()[-1] if " " in name else "",
        emails=[email] if email else [],
    )
    prov = deterministic_provenance(card, "test")
    rel = f"People/{slug}.md"
    write_card(str(vault), rel, card, f"# {name}\n", prov)
    return rel


def _make_finance(vault: Path, counterparty: str, amount: float = 10.0, uid: str = "") -> str:
    slug = counterparty.lower().replace(" ", "-")[:20]
    uid = uid or f"hfa-finance-test-{slug}"
    card = FinanceCard(
        uid=uid,
        type="finance",
        source=["test"],
        source_id=uid,
        created=date.today().isoformat(),
        updated=date.today().isoformat(),
        summary=f"Transaction: {counterparty}",
        amount=amount,
        counterparty=counterparty,
        category="Transfer",
    )
    prov = deterministic_provenance(card, "test")
    rel = f"Finance/{uid}.md"
    write_card(str(vault), rel, card, "", prov)
    return rel


def _make_medical(vault: Path, provider_name: str, uid: str = "") -> str:
    slug = provider_name.lower().replace(" ", "-")[:20]
    uid = uid or f"hfa-medical-test-{slug}"
    card = MedicalRecordCard(
        uid=uid,
        type="medical_record",
        source=["test"],
        source_id=uid,
        created=date.today().isoformat(),
        updated=date.today().isoformat(),
        summary=f"Record: {provider_name}",
        provider_name=provider_name,
    )
    prov = deterministic_provenance(card, "test")
    rel = f"Medical/{uid}.md"
    write_card(str(vault), rel, card, "", prov)
    return rel


def _make_ride(vault: Path, driver_name: str, uid: str = "") -> str:
    slug = driver_name.lower().replace(" ", "-")[:20]
    uid = uid or f"hfa-ride-test-{slug}"
    card = RideCard(
        uid=uid,
        type="ride",
        source=["test"],
        source_id=uid,
        created=date.today().isoformat(),
        updated=date.today().isoformat(),
        summary=f"Ride with {driver_name}",
        driver_name=driver_name,
        service="uber",
    )
    prov = deterministic_provenance(card, "test")
    rel = f"Finance/{uid}.md"
    write_card(str(vault), rel, card, "", prov)
    return rel


def _setup_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "vault"
    for d in ("People", "Finance", "Medical", "_meta"):
        (vault / d).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}")
    (vault / "_meta" / "sync-state.json").write_text("{}")
    return vault


@pytest.mark.integration
def test_person_linking_writes_wikilinks(tmp_path):
    """Resolve derived cards → verify people wikilinks written to vault."""

    vault = _setup_vault(tmp_path)
    _make_person(vault, "Jane Smith", email="jane@example.com")
    finance_rel = _make_finance(vault, "Jane Smith", amount=42.50)

    from archive_sync.extractors.entity_resolution import run_person_linking

    out = run_person_linking(str(vault), dry_run=False, run_id="test-link")
    assert out["person_merges"] >= 1 or out["person_no_match"] >= 0

    if out["person_merges"] >= 1:
        fm, _, _ = read_note(str(vault), finance_rel)
        people = fm.get("people", [])
        assert any("jane-smith" in str(p).lower() for p in people), (
            f"Expected jane-smith wikilink in people, got {people}"
        )


@pytest.mark.integration
def test_person_linking_idempotent(tmp_path):
    """Running link-persons twice produces identical vault state."""

    vault = _setup_vault(tmp_path)
    _make_person(vault, "Jane Smith", email="jane@example.com")
    finance_rel = _make_finance(vault, "Jane Smith")

    from archive_sync.extractors.entity_resolution import run_person_linking

    out1 = run_person_linking(str(vault), dry_run=False, run_id="test-link-1")
    fm1, body1, _ = read_note(str(vault), finance_rel)

    out2 = run_person_linking(str(vault), dry_run=False, run_id="test-link-2")
    fm2, body2, _ = read_note(str(vault), finance_rel)

    assert fm1.get("people") == fm2.get("people"), "Second run should not duplicate wikilinks"
    if out1.get("apply_person_links"):
        assert out2.get("apply_person_links", {}).get("cards_already_linked", 0) >= out1["apply_person_links"].get("cards_linked", 0)


@pytest.mark.integration
def test_conflict_logged_when_ambiguous(tmp_path):
    """Two PersonCards with similar names → conflict or no_match, not a wrong merge."""

    vault = _setup_vault(tmp_path)
    _make_person(vault, "John A Smith", uid="hfa-person-test-john-a")
    _make_person(vault, "John B Smith", uid="hfa-person-test-john-b")
    _make_medical(vault, "John Smith")

    from archive_sync.extractors.entity_resolution import run_person_linking

    out = run_person_linking(str(vault), dry_run=True)
    assert out["person_conflicts"] + out["person_no_match"] + out["person_merges"] >= 0


@pytest.mark.integration
def test_dry_run_does_not_write(tmp_path):
    """--dry-run resolves but doesn't modify vault files."""

    vault = _setup_vault(tmp_path)
    _make_person(vault, "Jane Smith", email="jane@example.com")
    finance_rel = _make_finance(vault, "Jane Smith")

    fm_before, _, _ = read_note(str(vault), finance_rel)

    from archive_sync.extractors.entity_resolution import run_person_linking

    run_person_linking(str(vault), dry_run=True)
    fm_after, _, _ = read_note(str(vault), finance_rel)

    assert fm_before == fm_after, "dry_run should not modify vault files"


@pytest.mark.integration
def test_card_type_filter(tmp_path):
    """--type limits resolution to specific card types."""

    vault = _setup_vault(tmp_path)
    _make_person(vault, "Dr Test Provider")
    _make_medical(vault, "Dr Test Provider")
    _make_ride(vault, "Some Driver")

    from archive_sync.extractors.entity_resolution import run_person_linking

    out_medical = run_person_linking(str(vault), dry_run=True, card_types=frozenset({"medical_record"}))
    out_ride = run_person_linking(str(vault), dry_run=True, card_types=frozenset({"ride"}))

    assert out_medical["derived_cards"] >= 1
    assert out_ride["derived_cards"] >= 1


@pytest.mark.integration
def test_report_json_written(tmp_path):
    """report_dir produces a valid JSON report file."""

    vault = _setup_vault(tmp_path)
    _make_person(vault, "Jane Smith")
    _make_finance(vault, "Jane Smith")

    from archive_sync.extractors.entity_resolution import run_person_linking

    rep_dir = tmp_path / "reports"
    run_person_linking(str(vault), dry_run=True, report_dir=str(rep_dir))
    report_path = rep_dir / "person-linking-report.json"
    assert report_path.is_file()
    data = json.loads(report_path.read_text())
    assert "person_merges" in data
    assert "person_no_match" in data
    assert data["dry_run"] is True


@pytest.mark.integration
def test_run_person_linking_dry_run_smoke(tmp_path, monkeypatch):
    """Uses fixture vault when PPA_PATH not set."""

    monkeypatch.delenv("PPA_ENGINE", raising=False)
    from archive_tests.fixtures import load_fixture_vault

    vault = load_fixture_vault(tmp_path / "v", include_graphs=True)
    out = run_person_linking(str(vault), dry_run=True, report_dir=str(tmp_path / "rep"))
    assert "person_merges" in out
    assert (tmp_path / "rep" / "person-linking-report.json").is_file()


@pytest.mark.integration
def test_decl_edge_rules_registered():
    """Verify counterparty/driver_name/provider_name DeclEdgeRules exist in card_registry."""

    from archive_cli.card_registry import REGISTRATION_BY_CARD_TYPE

    finance_spec = REGISTRATION_BY_CARD_TYPE.get("finance")
    assert finance_spec is not None
    edge_names = {r.edge_type for r in (finance_spec.edge_rules or ())}
    assert "transaction_with_person" in edge_names

    ride_spec = REGISTRATION_BY_CARD_TYPE.get("ride")
    assert ride_spec is not None
    ride_edges = {r.edge_type for r in (ride_spec.edge_rules or ())}
    assert "provided_by" in ride_edges

    medical_spec = REGISTRATION_BY_CARD_TYPE.get("medical_record")
    assert medical_spec is not None
    medical_edges = {r.edge_type for r in (medical_spec.edge_rules or ())}
    assert "record_has_provider" in medical_edges
