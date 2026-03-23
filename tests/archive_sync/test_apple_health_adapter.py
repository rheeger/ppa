"""Archive-sync Apple Health adapter tests."""

from __future__ import annotations

from pathlib import Path

from archive_sync.adapters.apple_health import AppleHealthAdapter
from archive_sync.adapters.base import deterministic_provenance
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
        emails=["rheeger@gmail.com"],
    )
    write_card(
        tmp_vault,
        "People/robert-heeger.md",
        person,
        provenance=deterministic_provenance(person, "contacts.apple"),
    )


def _write_export(path: Path) -> None:
    path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<HealthData exportDate="2026-03-10 12:00:00 -0700">
  <Record type="HKQuantityTypeIdentifierStepCount" sourceName="iPhone" unit="count" creationDate="2026-03-10 09:00:00 -0700" startDate="2026-03-10 09:00:00 -0700" endDate="2026-03-10 09:30:00 -0700" value="1000"/>
  <Record type="HKQuantityTypeIdentifierStepCount" sourceName="iPhone" unit="count" creationDate="2026-03-10 10:00:00 -0700" startDate="2026-03-10 10:00:00 -0700" endDate="2026-03-10 10:15:00 -0700" value="2000"/>
  <Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Watch" unit="count/min" creationDate="2026-03-10 10:00:00 -0700" startDate="2026-03-10 10:00:00 -0700" endDate="2026-03-10 10:00:00 -0700" value="70"/>
  <Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Watch" unit="count/min" creationDate="2026-03-10 11:00:00 -0700" startDate="2026-03-10 11:00:00 -0700" endDate="2026-03-10 11:00:00 -0700" value="80"/>
  <Record type="HKCategoryTypeIdentifierSleepAnalysis" sourceName="Watch" creationDate="2026-03-10 07:00:00 -0700" startDate="2026-03-09 23:00:00 -0700" endDate="2026-03-10 07:00:00 -0700" value="HKCategoryValueSleepAnalysisAsleep"/>
  <Workout workoutActivityType="HKWorkoutActivityTypeRunning" duration="30" durationUnit="min" totalDistance="5" totalDistanceUnit="km" totalEnergyBurned="250" totalEnergyBurnedUnit="kcal" creationDate="2026-03-10 12:00:00 -0700" startDate="2026-03-10 12:00:00 -0700" endDate="2026-03-10 12:30:00 -0700"/>
</HealthData>
""",
        encoding="utf-8",
    )


def test_ingest_rolls_up_daily_apple_health_records(tmp_vault: Path, tmp_path: Path) -> None:
    _seed_person(tmp_vault)
    export_path = tmp_path / "export.xml"
    _write_export(export_path)

    result = AppleHealthAdapter().ingest(
        str(tmp_vault),
        export_xml_path=str(export_path),
        person_wikilink="[[robert-heeger]]",
    )

    assert result.created == 4

    notes = sorted((tmp_vault / "Medical").rglob("*.md"))
    assert len(notes) == 4
    frontmatters = []
    for path in notes:
        frontmatter, body, _ = read_note(tmp_vault, str(path.relative_to(tmp_vault)))
        frontmatters.append(frontmatter)
        assert frontmatter["type"] == "medical_record"
        assert frontmatter["source_system"] == "apple_health"
        assert frontmatter["people"] == ["[[robert-heeger]]"]
        assert "Source ref: apple_health:" in body

    by_type = {frontmatter["record_type"]: frontmatter for frontmatter in frontmatters}
    assert by_type["activity_rollup"]["value_numeric"] == 3000.0
    assert by_type["activity_rollup"]["record_subtype"] == "HKQuantityTypeIdentifierStepCount"
    assert by_type["heart_rate_rollup"]["value_numeric"] == 75.0
    assert by_type["sleep_rollup"]["value_numeric"] == 8.0
    assert by_type["workout_rollup"]["value_numeric"] == 30.0
