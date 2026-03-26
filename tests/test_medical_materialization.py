"""Medical archive materialization tests."""

from __future__ import annotations

import json
from pathlib import Path

from archive_mcp.index_store import _build_person_lookup, _collect_canonical_rows, _materialize_row_batch
from archive_mcp.projections.registry import projection_for_card_type
from hfa.provenance import ProvenanceEntry
from hfa.schema import MedicalRecordCard, PersonCard, VaccinationCard
from hfa.vault import write_card


def _prov(*fields: str) -> dict[str, ProvenanceEntry]:
    return {field: ProvenanceEntry("seed-test", "2026-03-10", "deterministic") for field in fields}


def test_medical_cards_materialize_into_archive_metadata_tables(tmp_path: Path):
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Medical" / "2026-03").mkdir(parents=True)
    (vault / "Vaccinations" / "2024").mkdir(parents=True)
    (vault / "_meta").mkdir()
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "dedup-candidates.json").write_text("[]", encoding="utf-8")

    person = PersonCard(
        uid="hfa-person-robbie1234",
        type="person",
        source=["contacts.apple"],
        source_id="rheeger@gmail.com",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Robert Heeger",
        emails=["rheeger@gmail.com"],
    )
    medical = MedicalRecordCard(
        uid="hfa-medical-record-111111",
        type="medical_record",
        source=["onemedical", "onemedical.fhir"],
        source_id="onemedical:Condition:cond-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Anemia",
        people=["[[robert-heeger]]"],
        source_system="onemedical",
        source_format="fhir_json",
        record_type="condition",
        status="active",
        occurred_at="2024-10-06",
        code_system="http://snomed.info/sct",
        code="271737000",
        code_display="Anemia",
        raw_source_ref="fhir:Condition:cond-1",
        details_json={"fhir_id": "cond-1"},
    )
    vaccination = VaccinationCard(
        uid="hfa-vaccination-222222",
        type="vaccination",
        source=["onemedical", "onemedical.fhir", "vaccine.pdf"],
        source_id="onemedical:Immunization:imm-1",
        created="2024-10-05",
        updated="2026-03-10",
        summary="influenza (18+, Flublok, PF)",
        people=["[[robert-heeger]]"],
        source_system="onemedical",
        source_format="fhir_json",
        occurred_at="2024-10-05",
        vaccine_name="influenza (18+, Flublok, PF)",
        cvx_code="158",
        brand_name="Flublok trivalent, preservative-free",
        lot_number="TFAA2446",
        raw_source_ref="fhir:Immunization:imm-1",
        details_json={"fhir_id": "imm-1", "pdf_overlay": {"lot_number": "TFAA2446"}},
    )

    write_card(vault, "People/robert-heeger.md", person, provenance=_prov("summary", "emails"))
    write_card(
        vault,
        "Medical/2026-03/hfa-medical-record-111111.md",
        medical,
        body="Record type: condition",
        provenance=_prov(
            "summary",
            "people",
            "source_system",
            "source_format",
            "record_type",
            "status",
            "occurred_at",
            "code_system",
            "code",
            "code_display",
            "raw_source_ref",
            "details_json",
        ),
    )
    write_card(
        vault,
        "Vaccinations/2024/hfa-vaccination-222222.md",
        vaccination,
        body="Vaccine: influenza (18+, Flublok, PF)",
        provenance=_prov(
            "summary",
            "people",
            "source_system",
            "source_format",
            "occurred_at",
            "vaccine_name",
            "cvx_code",
            "brand_name",
            "lot_number",
            "raw_source_ref",
            "details_json",
        ),
    )

    rows, slug_map, duplicate_uid_count, duplicate_uid_rows, _fp, _fs = _collect_canonical_rows(
        vault, workers=1, executor_kind="serial", progress_every=0
    )
    assert duplicate_uid_count == 0
    assert duplicate_uid_rows == []
    path_to_uid = {row.rel_path: str(row.card.uid) for row in rows}
    person_lookup = _build_person_lookup(rows)
    batch = _materialize_row_batch(
        rows,
        vault_root=str(vault),
        slug_map=slug_map,
        path_to_uid=path_to_uid,
        person_lookup=person_lookup,
    )

    assert len(batch.medical_records) == 1
    assert len(batch.vaccinations) == 1

    medical_row = batch.medical_records[0]
    vaccination_row = batch.vaccinations[0]
    medical_projection = projection_for_card_type("medical_record")
    vaccination_projection = projection_for_card_type("vaccination")
    assert medical_projection is not None
    assert vaccination_projection is not None
    medical_columns = {column.name: index for index, column in enumerate(medical_projection.columns)}
    vaccination_columns = {column.name: index for index, column in enumerate(vaccination_projection.columns)}

    assert medical_row[medical_columns["card_uid"]] == "hfa-medical-record-111111"
    assert medical_row[medical_columns["person"]] == "[[robert-heeger]]"
    assert medical_row[medical_columns["record_type"]] == "condition"
    assert json.loads(medical_row[medical_columns["details_json"]])["fhir_id"] == "cond-1"
    assert vaccination_row[vaccination_columns["card_uid"]] == "hfa-vaccination-222222"
    assert vaccination_row[vaccination_columns["person"]] == "[[robert-heeger]]"
    assert vaccination_row[vaccination_columns["vaccine_name"]] == "influenza (18+, Flublok, PF)"
    assert json.loads(vaccination_row[vaccination_columns["details_json"]])["fhir_id"] == "imm-1"
