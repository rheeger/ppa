"""Higher-level PPA integration tests."""

from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest

from archive_doctor.handler import cmd_dedup_sweep, cmd_validate
from archive_sync.adapters.contacts import ContactsAdapter
from archive_sync.adapters.copilot_finance import CopilotFinanceAdapter
from archive_sync.adapters.linkedin import LinkedInAdapter
from archive_sync.adapters.notion_people import NotionPeopleAdapter, NotionStaffAdapter
from hfa.vault import read_note


@pytest.fixture
def tmp_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
    (vault / "Attachments").mkdir()
    (vault / "_templates").mkdir()
    (vault / ".obsidian").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    (meta / "dedup-candidates.json").write_text("[]", encoding="utf-8")
    (meta / "enrichment-log.json").write_text("[]", encoding="utf-8")
    (meta / "llm-cache.json").write_text("{}", encoding="utf-8")
    (meta / "nicknames.json").write_text(json.dumps({"robert": ["rob", "robbie"]}), encoding="utf-8")
    (meta / "ppa-config.json").write_text(json.dumps({"finance_min_amount": 20.0}), encoding="utf-8")
    return vault


def _write_fixtures(tmp_path: Path) -> dict[str, Path]:
    vcf_path = tmp_path / "contacts.vcf"
    vcf_path.write_text(
        "\n".join(
            [
                "BEGIN:VCARD",
                "VERSION:3.0",
                "FN:Jane Smith",
                "EMAIL;TYPE=HOME:jane@example.com",
                "ORG:Endaoment",
                "END:VCARD",
                "BEGIN:VCARD",
                "VERSION:3.0",
                "FN:John Example",
                "EMAIL;TYPE=HOME:john@example.com",
                "ORG:ExampleCo",
                "END:VCARD",
            ]
        ),
        encoding="utf-8",
    )

    linkedin_path = tmp_path / "linkedin.csv"
    linkedin_path.write_text(
        "First Name,Last Name,Email Address,Company,Position,Connected On\n"
        "Jane,Smith,jane@example.com,Endaoment,VP,2024-01-01\n"
        "Mary,Jones,mary@example.com,Acme,Founder,2024-02-01\n",
        encoding="utf-8",
    )

    notion_path = tmp_path / "notion.csv"
    notion_path.write_text(
        'Name,Email,Company,Role,Tags\nJane Smith,jane@example.com,Endaoment,VP,"friend"\n'
        "Alex Person,alex@example.com,Elsewhere,Advisor,operator\n",
        encoding="utf-8",
    )
    notion_staff_path = tmp_path / "staff.csv"
    notion_staff_path.write_text(
        "Name,Company email,Personal Email,Primary Phone number,LinkedIn,Reports to,Role,Teams,Pronouns,Type\n"
        "Alexis Miller,alexis@endaoment.org,alexis@example.com,(847) 400-4343,https://www.linkedin.com/in/alexis-m-miller/,Zach Bronstein,Donor Engagement,.Org (https://www.notion.so/org),her she,Full time\n",
        encoding="utf-8",
    )

    copilot_path = tmp_path / "copilot.csv"
    copilot_path.write_text(
        "Date,Merchant,Amount,Category,Account\n"
        "2026-03-01,Flight,-120.00,Travel,Checking\n"
        "2026-03-02,Coffee,-5.00,Food,Checking\n",
        encoding="utf-8",
    )
    return {
        "vcf": vcf_path,
        "linkedin": linkedin_path,
        "notion": notion_path,
        "notion_staff": notion_staff_path,
        "copilot": copilot_path,
    }


def test_full_import_pipeline(tmp_vault, tmp_path):
    fixtures = _write_fixtures(tmp_path)

    contacts = ContactsAdapter()
    contacts._fetch_google = lambda: []  # type: ignore[method-assign]
    contacts._fetch_vcf_files = lambda: contacts._parse_vcf(str(fixtures["vcf"]))  # type: ignore[method-assign]
    r1 = contacts.ingest(str(tmp_vault), sources=["vcf"])
    r2 = LinkedInAdapter().ingest(str(tmp_vault), csv_path=str(fixtures["linkedin"]))
    r3 = NotionPeopleAdapter().ingest(str(tmp_vault), csv_path=str(fixtures["notion"]))
    r_staff = NotionStaffAdapter().ingest(str(tmp_vault), csv_path=str(fixtures["notion_staff"]))
    r4 = CopilotFinanceAdapter().ingest(str(tmp_vault), csv_path=str(fixtures["copilot"]))

    cmd_dedup_sweep(Namespace(vault=str(tmp_vault)))
    cmd_validate(Namespace(vault=str(tmp_vault)))

    assert r1.created == 2
    assert r2.created == 1 and r2.merged == 1
    assert r3.created == 1 and r3.merged == 1
    assert r_staff.created == 1
    assert r4.created == 1

    people_files = sorted(path.name for path in (tmp_vault / "People").glob("*.md"))
    assert people_files == ["alex-person.md", "alexis-miller.md", "jane-smith.md", "john-example.md", "mary-jones.md"]

    jane, jane_body, jane_prov = read_note(tmp_vault, "People/jane-smith.md")
    assert set(jane["source"]) == {"contacts.apple", "linkedin", "notion"}
    assert jane["emails"] == ["jane@example.com"]
    assert "Connected on: 2024-01-01" in jane_body
    assert "company" in jane_prov
    assert jane_prov["summary"].source == "contacts.apple"

    finance_files = list((tmp_vault / "Finance" / "2026-03").glob("*.md"))
    assert len(finance_files) == 1

    report = json.loads((tmp_vault / "_meta" / "validation-report.json").read_text(encoding="utf-8"))
    assert report["errors"] == []
    state = json.loads((tmp_vault / "_meta" / "sync-state.json").read_text(encoding="utf-8"))
    assert "contacts.apple" in state
    assert "linkedin" in state
    assert "notion-staff" in state
