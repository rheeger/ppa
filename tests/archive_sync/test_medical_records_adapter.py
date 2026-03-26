"""Archive-sync medical record adapter tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.epic_ehi import collect_epic_ehi_items
from archive_sync.adapters.medical_records import MedicalRecordsAdapter, _parse_vaccine_pdf_text
from hfa.schema import PersonCard
from hfa.vault import read_note, write_card


def _seed_person(tmp_vault: Path) -> None:
    person = PersonCard(
        uid="hfa-person-robbie1234",
        type="person",
        source=["contacts.apple"],
        source_id="rheeger@gmail.com",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Robert Heeger",
        first_name="Robert",
        last_name="Heeger",
        emails=["rheeger@gmail.com"],
    )
    write_card(
        tmp_vault,
        "People/robert-heeger.md",
        person,
        provenance=deterministic_provenance(person, "contacts.apple"),
    )


def _write_fhir_bundle(path: Path) -> None:
    payload = {
        "entry": [
            {
                "resourceType": "Patient",
                "id": "patient-1",
                "name": [{"given": ["Robert"], "family": "Heeger"}],
                "telecom": [
                    {"system": "email", "value": "rheeger@gmail.com"},
                    {"system": "phone", "value": "650-799-2364"},
                ],
            },
            {
                "resourceType": "Condition",
                "id": "cond-1",
                "clinicalStatus": {
                    "coding": [{"code": "active", "display": "Active"}],
                },
                "category": [{"text": "Problems"}],
                "code": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "271737000", "display": "Anemia"}],
                },
                "recordedDate": "2024-10-06",
            },
            {
                "resourceType": "Immunization",
                "id": "imm-1",
                "status": "completed",
                "vaccineCode": {
                    "coding": [{"system": "http://hl7.org/fhir/sid/cvx", "code": "158"}],
                    "text": "influenza (18+, Flublok, PF)",
                },
                "occurrenceDateTime": "2024-10-05",
            },
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_ccd(path: Path) -> None:
    path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3">
  <component>
    <structuredBody>
      <component>
        <section>
          <title>Problems</title>
          <text>Anemia noted in follow-up labs.</text>
        </section>
      </component>
      <component>
        <section>
          <title>Immunizations</title>
          <text>Flublok vaccine documented.</text>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>
""",
        encoding="utf-8",
    )


def _write_epic_ccd(path: Path, *, title: str, section_title: str, section_text: str) -> None:
    path.write_text(
        f"""<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3">
  <id root="{path.stem}"/>
  <code code="34133-9" codeSystem="2.16.840.1.113883.6.1" displayName="{title}"/>
  <title>{title}</title>
  <effectiveTime value="20260310"/>
  <recordTarget>
    <patientRole>
      <patient>
        <name>
          <given>Amelia</given>
          <family>Heeger-Friedman</family>
        </name>
      </patient>
    </patientRole>
  </recordTarget>
  <component>
    <structuredBody>
      <component>
        <section>
          <title>{section_title}</title>
          <text>{section_text}</text>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>
""",
        encoding="utf-8",
    )


def test_parse_vaccine_pdf_text_extracts_structured_entries():
    entries = _parse_vaccine_pdf_text(
        """
Immunization Record
As of 03/10/2026
Patient Name: Robert Heeger
influenza (18+, Flublok, PF)
Brand: Flublok trivalent, preservative-free Lot No: TFAA2446 Expires at: 06/29/2025
10/05/2024 One Medical
"""
    )
    assert entries == [
        {
            "occurred_at": "2024-10-05",
            "vaccine_name": "influenza (18+, Flublok, PF)",
            "brand_name": "Flublok trivalent, preservative-free",
            "lot_number": "TFAA2446",
            "expiration_date": "2025-06-29",
            "location": "One Medical",
        }
    ]


def test_parse_vaccine_pdf_text_skips_phone_fax_and_lot_noise():
    entries = _parse_vaccine_pdf_text(
        """
Immunization Record
Patient Name: Robert Heeger
SARS-CoV-2 mRNA 2024-2025 (Moderna, 12+ yr)
Brand: Spikevax
3043366
10/05/2024 One Medical
794 Union Street Brooklyn, NY 11215 Phone: 888-663-6331 Fax: 888-663-
6331
Lot No:
"""
    )
    assert entries == [
        {
            "occurred_at": "2024-10-05",
            "vaccine_name": "SARS-CoV-2 mRNA 2024-2025 (Moderna, 12+ yr)",
            "brand_name": "Spikevax",
            "lot_number": "",
            "expiration_date": "",
            "location": "One Medical",
        }
    ]


def test_ingest_creates_medical_and_vaccination_cards_and_overlays_pdf(
    tmp_vault: Path, tmp_path: Path, monkeypatch
) -> None:
    _seed_person(tmp_vault)
    fhir_path = tmp_path / "fhir.json"
    ccd_path = tmp_path / "ccd.xml"
    vaccine_pdf = tmp_path / "vaccines.pdf"
    _write_fhir_bundle(fhir_path)
    _write_ccd(ccd_path)
    vaccine_pdf.write_bytes(b"%PDF-1.4\n% test pdf")

    monkeypatch.setattr(
        "archive_sync.adapters.medical_records._read_vaccine_pdf_entries",
        lambda path: [
            {
                "occurred_at": "2024-10-05",
                "vaccine_name": "influenza (18+, Flublok, PF)",
                "brand_name": "Flublok trivalent, preservative-free",
                "lot_number": "TFAA2446",
                "expiration_date": "2025-06-29",
                "location": "One Medical",
            },
            {
                "occurred_at": "2020-10",
                "vaccine_name": "influenza (6 mos+, preservative-free)",
                "brand_name": "Fluzone, quadrivalent, preservative free",
                "lot_number": "UT7006LA",
                "expiration_date": "2021-06-30",
                "location": "One Medical",
            },
        ],
    )

    result = MedicalRecordsAdapter().ingest(
        str(tmp_vault),
        fhir_json_path=str(fhir_path),
        ccd_xml_path=str(ccd_path),
        vaccine_pdf_path=str(vaccine_pdf),
        person_wikilink="[[robert-heeger]]",
    )

    assert result.created == 3

    medical_path = next((tmp_vault / "Medical").rglob("*.md"))
    medical_frontmatter, medical_body, _ = read_note(tmp_vault, str(medical_path.relative_to(tmp_vault)))
    assert medical_frontmatter["type"] == "medical_record"
    assert medical_frontmatter["people"] == ["[[robert-heeger]]"]
    assert medical_frontmatter["record_type"] == "condition"
    assert medical_frontmatter["code_display"] == "Anemia"
    assert medical_frontmatter["details_json"]["ccd_context"]["section_titles"] == ["Problems"]
    assert "Record type: condition" in medical_body

    vaccination_paths = sorted((tmp_vault / "Vaccinations").rglob("*.md"))
    assert len(vaccination_paths) == 2
    matched_frontmatter, matched_body, _ = read_note(tmp_vault, str(vaccination_paths[1].relative_to(tmp_vault)))
    assert matched_frontmatter["type"] == "vaccination"
    assert matched_frontmatter["brand_name"] == "Flublok trivalent, preservative-free"
    assert matched_frontmatter["lot_number"] == "TFAA2446"
    assert matched_frontmatter["location"] == "One Medical"
    assert matched_frontmatter["people"] == ["[[robert-heeger]]"]
    assert "Brand: Flublok trivalent, preservative-free" in matched_body


def test_ingest_supports_ccd_only_directory_for_epic_exports(tmp_vault: Path, tmp_path: Path) -> None:
    person = PersonCard(
        uid="hfa-person-amelia1234",
        type="person",
        source=["contacts.apple"],
        source_id="Amelia",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Amelia",
        first_name="Amelia",
    )
    write_card(
        tmp_vault,
        "People/amelia.md",
        person,
        provenance=deterministic_provenance(person, "contacts.apple"),
    )

    ccd_dir = tmp_path / "ccd"
    ccd_dir.mkdir()
    _write_epic_ccd(
        ccd_dir / "visit-1.XML",
        title="Epic CCD Visit Summary",
        section_title="Encounters",
        section_text="NICU follow-up encounter with pediatric team.",
    )
    _write_epic_ccd(
        ccd_dir / "visit-2.XML",
        title="Epic CCD Lab Summary",
        section_title="Results",
        section_text="Metabolic screening reviewed.",
    )

    result = MedicalRecordsAdapter().ingest(
        str(tmp_vault),
        ccd_dir_path=str(ccd_dir),
        person_wikilink="[[amelia]]",
    )

    assert result.created == 2
    medical_paths = sorted((tmp_vault / "Medical").rglob("*.md"))
    assert len(medical_paths) == 2
    frontmatter, body, _ = read_note(tmp_vault, str(medical_paths[0].relative_to(tmp_vault)))
    assert frontmatter["type"] == "medical_record"
    assert frontmatter["source_system"] == "epic"
    assert frontmatter["source_format"] == "ccd_xml"
    assert frontmatter["record_type"] == "ccd_document"
    assert frontmatter["people"] == ["[[amelia]]"]
    assert "CCD context:" in body


def test_ingest_supports_epic_ehi_tables_for_structured_medications(tmp_vault: Path, tmp_path: Path) -> None:
    person = PersonCard(
        uid="hfa-person-amelia1234",
        type="person",
        source=["contacts.apple"],
        source_id="Amelia",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Amelia",
        first_name="Amelia",
    )
    write_card(
        tmp_vault,
        "People/amelia.md",
        person,
        provenance=deterministic_provenance(person, "contacts.apple"),
    )

    ehi_dir = tmp_path / "ehi"
    ehi_dir.mkdir()
    (ehi_dir / "PATIENT.tsv").write_text(
        "PAT_ID\tPAT_NAME\nZ5773764\tHEEGER-FRIEDMAN,AMELIA DEVON\n",
        encoding="utf-8",
    )
    (ehi_dir / "CLARITY_MEDICATION.tsv").write_text(
        "MEDICATION_ID\tNAME\tGENERIC_NAME\n38677\tHEPATITIS B VACCINE\tHepatitis B Vaccine\n410192\tCAFFEINE CITRATE\tCaffeine Citrate\n",
        encoding="utf-8",
    )
    (ehi_dir / "ORDER_MED.tsv").write_text(
        "\n".join(
            [
                "ORDER_MED_ID\tPAT_ID\tPAT_ENC_CSN_ID\tMEDICATION_ID\tDESCRIPTION\tDISPLAY_NAME\tORDERING_DATE\tSTART_DATE\tEND_DATE\tORDER_INST\tDOSAGE\tMED_ROUTE_C_NAME\tRSN_FOR_DISCON_C_NAME\tORDER_CLASS_C_NAME\tORD_CREATR_USER_ID_NAME",
                "748045805\tZ5773764\t29276020659\t38677\tHEPATITIS B VIRUS VACC.REC(PF)\thepatitis B vaccine (PF) (ENGERIX-B) 10 mcg\t12/27/2025 12:00:00 AM\t1/24/2026 12:00:00 AM\t1/24/2026 12:00:00 AM\t12/27/2025 8:32:00 PM\t\tIntramuscular\t\tNormal\tDEORA, KANIKA",
                "748045817\tZ5773764\t29276020659\t410192\tCAFFEINE CITRATE IV SOLN\tcaffeine citrate pediatric\t12/27/2025 12:00:00 AM\t12/27/2025 12:00:00 AM\t12/27/2025 12:00:00 AM\t12/27/2025 8:32:00 PM\t\tIVPB\t\tNormal\tDEORA, KANIKA",
            ]
        ),
        encoding="utf-8",
    )

    result = MedicalRecordsAdapter().ingest(
        str(tmp_vault),
        ehi_tables_dir_path=str(ehi_dir),
        person_wikilink="[[amelia]]",
    )

    assert result.created == 2
    vaccination_paths = sorted((tmp_vault / "Vaccinations").rglob("*.md"))
    medical_paths = sorted((tmp_vault / "Medical").rglob("*.md"))
    assert len(vaccination_paths) == 1
    assert len(medical_paths) == 1

    vaccination_frontmatter, vaccination_body, _ = read_note(
        tmp_vault, str(vaccination_paths[0].relative_to(tmp_vault))
    )
    assert vaccination_frontmatter["type"] == "vaccination"
    assert vaccination_frontmatter["source_system"] == "epic"
    assert vaccination_frontmatter["source_format"] == "ehi_tsv"
    assert vaccination_frontmatter["people"] == ["[[amelia]]"]
    assert "Vaccine:" in vaccination_body

    medical_frontmatter, medical_body, _ = read_note(tmp_vault, str(medical_paths[0].relative_to(tmp_vault)))
    assert medical_frontmatter["type"] == "medical_record"
    assert medical_frontmatter["record_type"] == "medication_request"
    assert medical_frontmatter["source_system"] == "epic"
    assert medical_frontmatter["people"] == ["[[amelia]]"]
    assert "Record type: medication_request" in medical_body


def test_epic_ehi_rejects_multiple_pat_ids_without_epic_pat_id(tmp_path: Path) -> None:
    ehi_dir = tmp_path / "ehi"
    ehi_dir.mkdir()
    (ehi_dir / "PATIENT.tsv").write_text(
        "PAT_ID\tPAT_NAME\nA1\tONE\nB2\tTWO\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="multiple PAT_ID"):
        collect_epic_ehi_items(
            ehi_tables_dir=ehi_dir,
            person_link="[[x]]",
            source_id="medical-records",
            verbose=False,
            progress_every=None,
        )


def test_epic_ehi_selects_epic_pat_id_when_multiple_patients(tmp_path: Path) -> None:
    ehi_dir = tmp_path / "ehi"
    ehi_dir.mkdir()
    (ehi_dir / "PATIENT.tsv").write_text(
        "PAT_ID\tPAT_NAME\nA1\tONE\nB2\tTWO\n",
        encoding="utf-8",
    )
    (ehi_dir / "CLARITY_MEDICATION.tsv").write_text(
        "MEDICATION_ID\tNAME\tGENERIC_NAME\n1\tMed\tMed\n", encoding="utf-8"
    )
    (ehi_dir / "ORDER_MED.tsv").write_text(
        "\n".join(
            [
                "ORDER_MED_ID\tPAT_ID\tPAT_ENC_CSN_ID\tMEDICATION_ID\tDESCRIPTION\tDISPLAY_NAME\tORDERING_DATE\tSTART_DATE\tEND_DATE\tORDER_INST\tDOSAGE\tMED_ROUTE_C_NAME\tRSN_FOR_DISCON_C_NAME\tORDER_CLASS_C_NAME\tORD_CREATR_USER_ID_NAME",
                "10\tB2\t99\t1\tASPIRIN\taspirin\t1/1/2026\t1/1/2026\t1/1/2026\t\t\tOral\t\tNormal\tDr",
            ]
        ),
        encoding="utf-8",
    )
    items, meta = collect_epic_ehi_items(
        ehi_tables_dir=ehi_dir,
        person_link="[[x]]",
        source_id="medical-records",
        verbose=False,
        progress_every=None,
        epic_pat_id="B2",
    )
    assert meta["patient_id"] == "B2"
    assert len(items) == 1
    assert items[0]["source_id"] == "epic:order_med:10"


def test_epic_ehi_skips_order_vaccine_when_imm_admin_matches(tmp_path: Path) -> None:
    ehi_dir = tmp_path / "ehi"
    ehi_dir.mkdir()
    (ehi_dir / "PATIENT.tsv").write_text("PAT_ID\tPAT_NAME\nP1\tPatient\n", encoding="utf-8")
    (ehi_dir / "DOCS_RCVD.tsv").write_text(
        "DOCUMENT_ID\tTYPE_C_NAME\tPAT_ID\nDOC1\tNote\tP1\n",
        encoding="utf-8",
    )
    (ehi_dir / "IMM_ADMIN.tsv").write_text(
        "\t".join(
            [
                "DOCUMENT_ID",
                "CONTACT_DATE_REAL",
                "LINE",
                "CONTACT_DATE",
                "IMM_TYPE_ID_NAME",
                "IMM_TYPE_ID",
                "IMM_TYPE_FREE_TEXT",
                "IMM_DATE",
                "IMM_DOSE",
                "IMM_ROUTE_C_NAME",
                "IMM_ROUTE_FREE_TXT",
                "IMM_SITE_C_NAME",
                "IMM_SITE_FREE_TXT",
                "IMM_MANUFACTURER_C_NAME",
                "IMM_MANUF_FREE_TEXT",
                "IMM_LOT_NUMBER",
                "IMM_GIVEN_BY_ID_NAME",
                "IMM_GIVEN_BY_ID",
                "IMM_GIVEN_BY_FT",
                "IMM_VIS_PUB_DATE",
            ]
        )
        + "\n"
        + "\t".join(
            [
                "DOC1",
                "1",
                "1",
                "1/24/2026 12:00:00 AM",
                "HEPATITIS B, PEDIATRIC/ADOLESCENT VACCINE",
                "104",
                "",
                "1/24/2026 12:00:00 AM",
                "",
                "Intramuscular",
                "",
                "",
                "",
                "",
                "",
                "Lot1",
                "",
                "",
                "",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (ehi_dir / "CLARITY_MEDICATION.tsv").write_text(
        "MEDICATION_ID\tNAME\tGENERIC_NAME\n38677\tHEPATITIS B VACCINE\tHepatitis B Vaccine\n",
        encoding="utf-8",
    )
    (ehi_dir / "ORDER_MED.tsv").write_text(
        "\n".join(
            [
                "ORDER_MED_ID\tPAT_ID\tPAT_ENC_CSN_ID\tMEDICATION_ID\tDESCRIPTION\tDISPLAY_NAME\tORDERING_DATE\tSTART_DATE\tEND_DATE\tORDER_INST\tDOSAGE\tMED_ROUTE_C_NAME\tRSN_FOR_DISCON_C_NAME\tORDER_CLASS_C_NAME\tORD_CREATR_USER_ID_NAME",
                "748\tP1\t99\t38677\tHEPATITIS B VIRUS VACC\tHepatitis B vaccine display\t1/24/2026 12:00:00 AM\t1/24/2026 12:00:00 AM\t1/24/2026 12:00:00 AM\t1/24/2026 12:00:00 PM\t\tIM\t\tNormal\tDr",
            ]
        ),
        encoding="utf-8",
    )
    items, meta = collect_epic_ehi_items(
        ehi_tables_dir=ehi_dir,
        person_link="[[x]]",
        source_id="medical-records",
        verbose=False,
        progress_every=None,
        include_order_results=False,
        include_adt=False,
    )
    vacc = [i for i in items if i.get("kind") == "vaccination"]
    assert len(vacc) == 1
    assert vacc[0]["source_id"].startswith("epic:imm_admin:")
    assert meta["counts"]["vaccine_orders"] == 0


def test_epic_ehi_emits_encounter_problem_observation(tmp_path: Path) -> None:
    ehi_dir = tmp_path / "ehi"
    ehi_dir.mkdir()
    (ehi_dir / "PATIENT.tsv").write_text("PAT_ID\tPAT_NAME\nP1\tPatient\n", encoding="utf-8")
    (ehi_dir / "CLARITY_MEDICATION.tsv").write_text("MEDICATION_ID\tNAME\tGENERIC_NAME\n", encoding="utf-8")
    (ehi_dir / "ORDER_MED.tsv").write_text(
        "ORDER_MED_ID\tPAT_ID\tPAT_ENC_CSN_ID\tMEDICATION_ID\tDESCRIPTION\tDISPLAY_NAME\tORDERING_DATE\tSTART_DATE\tEND_DATE\tORDER_INST\tDOSAGE\tMED_ROUTE_C_NAME\tRSN_FOR_DISCON_C_NAME\tORDER_CLASS_C_NAME\tORD_CREATR_USER_ID_NAME\n",
        encoding="utf-8",
    )
    (ehi_dir / "PAT_ENC.tsv").write_text(
        "\t".join(
            [
                "PAT_ID",
                "PAT_ENC_DATE_REAL",
                "PAT_ENC_CSN_ID",
                "CONTACT_DATE",
                "PCP_PROV_ID",
                "FIN_CLASS_C_NAME",
                "VISIT_PROV_ID",
                "VISIT_PROV_TITLE_NAME",
                "DEPARTMENT_ID",
                "UPDATE_DATE",
            ]
        )
        + "\n"
        + "\t".join(["P1", "1", "CSN1", "1/2/2026 12:00:00 AM", "", "", "", "Dr", "D1", ""])
        + "\n",
        encoding="utf-8",
    )
    (ehi_dir / "CLARITY_DEP.tsv").write_text(
        "DEPARTMENT_ID\tDEPARTMENT_NAME\tEXTERNAL_NAME\nD1\tNICU\tNICU\n",
        encoding="utf-8",
    )
    (ehi_dir / "PAT_PROBLEM_LIST.tsv").write_text("PAT_ID\tLINE\tPROBLEM_LIST_ID\nP1\t1\tPL1\n", encoding="utf-8")
    (ehi_dir / "PROBLEM_LIST.tsv").write_text(
        "PROBLEM_LIST_ID\tDIAG_START_DATE\tDIAG_END_DATE\nPL1\t1/1/2026\t\n", encoding="utf-8"
    )
    (ehi_dir / "PAT_ENC_DX.tsv").write_text(
        "PAT_ENC_DATE_REAL\tLINE\tCONTACT_DATE\tPAT_ENC_CSN_ID\tDX_ID\tANNOTATION\tDX_QUALIFIER_C_NAME\tPRIMARY_DX_YN\tCOMMENTS\tDX_CHRONIC_YN\tDX_STAGE_ID\tDX_UNIQUE\tDX_ED_YN\tDX_LINK_PROB_ID\n"
        "1\t1\t1/2/2026 12:00:00 AM\tCSN1\t9\t\t\tY\t\tN\t\t1\tN\tPL1\n",
        encoding="utf-8",
    )
    (ehi_dir / "CLARITY_EDG.tsv").write_text(
        "DX_ID\tDX_NAME\tPAT_FRIENDLY_TEXT\n9\tPreterm newborn\tFriendly preterm\n",
        encoding="utf-8",
    )
    (ehi_dir / "ORDER_PROC.tsv").write_text(
        "ORDER_PROC_ID\tPAT_ID\tPAT_ENC_CSN_ID\tORDERING_DATE\tORDER_TYPE_C_NAME\tPROC_ID\tDESCRIPTION\tORDER_CLASS_C_NAME\tORD_CREATR_USER_ID_NAME\tORD_CREATR_USER_ID\tLAB_STATUS_C_NAME\tORDER_STATUS_C_NAME\n"
        "OP1\tP1\tCSN1\t1/2/2026\tLab\t1\tGlucose\tNormal\tDr\t1\tFinal\tCompleted\n",
        encoding="utf-8",
    )
    (ehi_dir / "ORDER_RESULTS.tsv").write_text(
        "ORDER_PROC_ID\tLINE\tORD_DATE_REAL\tORD_END_DATE_REAL\tRESULT_DATE\tCOMPONENT_ID_NAME\tCOMPONENT_ID\tPAT_ENC_CSN_ID\tORD_VALUE\tORD_NUM_VALUE\tRESULT_FLAG_C_NAME\tREFERENCE_LOW\tREFERENCE_HIGH\tREFERENCE_UNIT\tRESULT_STATUS_C_NAME\n"
        "OP1\t1\t\t\t1/2/2026 12:00:00 AM\tGlucose\tC1\tCSN1\t90\t90\t\t70\t100\tmg/dL\tFinal\n",
        encoding="utf-8",
    )
    items, meta = collect_epic_ehi_items(
        ehi_tables_dir=ehi_dir,
        person_link="[[x]]",
        source_id="medical-records",
        verbose=False,
        progress_every=None,
        include_adt=False,
    )
    kinds = {i.get("record_type") for i in items if i.get("kind") == "medical_record"}
    assert "encounter" in kinds
    assert "condition" in kinds
    assert "observation" in kinds
    assert meta["counts"]["encounters"] == 1
    assert meta["counts"]["problems"] == 1
    assert meta["counts"]["observations"] == 1
