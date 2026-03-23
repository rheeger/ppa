"""Apple Health XML rollup adapter."""

from __future__ import annotations

import hashlib
import os
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from .base import BaseAdapter, deterministic_provenance
from ..cli_logging import CliProgressReporter, log_cli_step
from hfa.schema import MedicalRecordCard
from hfa.uid import generate_uid
from hfa.vault import find_note_by_slug

APPLE_HEALTH_SOURCE = "apple.health"
APPLE_HEALTH_CURSOR = "apple-health"
DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S %z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
)
ACTIVITY_TYPES = {
    "HKQuantityTypeIdentifierStepCount",
    "HKQuantityTypeIdentifierDistanceWalkingRunning",
    "HKQuantityTypeIdentifierFlightsClimbed",
    "HKQuantityTypeIdentifierActiveEnergyBurned",
    "HKQuantityTypeIdentifierBasalEnergyBurned",
    "HKQuantityTypeIdentifierAppleExerciseTime",
}
BODY_TYPES = {
    "HKQuantityTypeIdentifierBodyMass",
    "HKQuantityTypeIdentifierBodyMassIndex",
    "HKQuantityTypeIdentifierHeight",
    "HKQuantityTypeIdentifierLeanBodyMass",
    "HKQuantityTypeIdentifierBodyFatPercentage",
}
HEART_TYPES = {
    "HKQuantityTypeIdentifierHeartRate",
    "HKQuantityTypeIdentifierRestingHeartRate",
    "HKQuantityTypeIdentifierWalkingHeartRateAverage",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN",
    "HKQuantityTypeIdentifierVO2Max",
}
BLOOD_PRESSURE_TYPES = {
    "HKQuantityTypeIdentifierBloodPressureSystolic",
    "HKQuantityTypeIdentifierBloodPressureDiastolic",
}
RESPIRATORY_TYPES = {
    "HKQuantityTypeIdentifierRespiratoryRate",
    "HKQuantityTypeIdentifierOxygenSaturation",
    "HKQuantityTypeIdentifierPeakExpiratoryFlowRate",
    "HKQuantityTypeIdentifierForcedVitalCapacity",
}
SLEEP_TYPE = "HKCategoryTypeIdentifierSleepAnalysis"


def _clean(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _slug_to_wikilink(value: str) -> str:
    slug = value.strip()
    if slug.startswith("[[") and slug.endswith("]]"):
        return slug
    return f"[[{slug}]]"


def _normalize_person_wikilink(vault_path: str, person_wikilink: str) -> str:
    wikilink = _slug_to_wikilink(person_wikilink.strip().strip("[]"))
    slug = wikilink[2:-2]
    if find_note_by_slug(vault_path, slug) is None:
        raise ValueError(f"Person note not found for {wikilink}")
    return wikilink


def _file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_datetime(value: str) -> datetime | None:
    raw = _clean(value)
    if not raw:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _date_bucket(value: str) -> str:
    parsed = _parse_datetime(value)
    if parsed is not None:
        return parsed.date().isoformat()
    return _clean(value)[:10] or date.today().isoformat()


def _float_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _bucket_type(record_type: str) -> tuple[str, str]:
    if record_type in ACTIVITY_TYPES:
        return "activity_rollup", record_type
    if record_type in BODY_TYPES:
        return "body_measurement_rollup", record_type
    if record_type in HEART_TYPES:
        return "heart_rate_rollup", record_type
    if record_type in BLOOD_PRESSURE_TYPES:
        return "blood_pressure_rollup", record_type
    if record_type in RESPIRATORY_TYPES:
        return "respiratory_rollup", record_type
    if record_type == SLEEP_TYPE:
        return "sleep_rollup", record_type
    return "", ""


def _rollup_summary(bucket_type: str, subtype: str, day: str, payload: dict[str, Any]) -> tuple[str, float, str, str]:
    count = int(payload.get("count", 0))
    unit = _clean(payload.get("unit"))
    if bucket_type == "sleep_rollup":
        hours = round(float(payload.get("total_hours", 0.0) or 0.0), 3)
        return (f"{subtype} {hours}h across {count} entries", hours, "hours", "sleep duration")
    if bucket_type == "workout_rollup":
        minutes = round(float(payload.get("total_minutes", 0.0) or 0.0), 3)
        return (f"{subtype} {minutes}m across {count} workouts", minutes, "minutes", "workout duration")
    total_value = round(float(payload.get("sum", 0.0) or 0.0), 4)
    avg_value = round(float(payload.get("avg", 0.0) or 0.0), 4)
    if bucket_type == "activity_rollup":
        return (f"{subtype} total {total_value} {unit} across {count} samples", total_value, unit, "activity total")
    return (f"{subtype} avg {avg_value} {unit} across {count} samples", avg_value, unit, "daily average")


def _render_body(item: dict[str, Any]) -> str:
    details = dict(item.get("details_json") or {})
    metrics = dict(details.get("metrics") or {})
    lines = [
        f"Record type: {_clean(item.get('record_type'))}",
        f"Subtype: {_clean(item.get('record_subtype'))}",
        f"Date: {_clean(item.get('occurred_at'))}",
        f"Value: {_clean(item.get('value_text'))}",
        f"Unit: {_clean(item.get('unit'))}",
        f"Samples: {_clean(metrics.get('count'))}",
        f"Min: {_clean(metrics.get('min'))}",
        f"Max: {_clean(metrics.get('max'))}",
        f"Avg: {_clean(metrics.get('avg'))}",
        f"Sum: {_clean(metrics.get('sum'))}",
        f"Source ref: {_clean(item.get('raw_source_ref'))}",
    ]
    return "\n".join(line for line in lines if line.split(": ", 1)[-1])


class AppleHealthAdapter(BaseAdapter):
    source_id = APPLE_HEALTH_CURSOR
    preload_existing_uid_index = False
    enable_person_resolution = False

    def should_enable_person_resolution(self, **kwargs) -> bool:
        return False

    def ingest_verbose(self, **kwargs) -> bool:
        raw_value = kwargs.get("verbose")
        if raw_value is None and "HFA_IMPORT_VERBOSE" not in os.environ:
            return True
        return super().ingest_verbose(**kwargs)

    def get_cursor_key(self, **kwargs) -> str:
        return APPLE_HEALTH_CURSOR

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        *,
        export_xml_path: str | None = None,
        person_wikilink: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        if not export_xml_path:
            raise ValueError("export_xml_path is required")
        verbose = self.ingest_verbose(**kwargs)
        progress_every = self.ingest_progress_every(**kwargs)
        log_cli_step(self.source_id, 1, 3, "prepare Apple Health import", f"export={export_xml_path}")
        person_link = _normalize_person_wikilink(vault_path, person_wikilink or "")
        log_cli_step(self.source_id, 1, 3, "prepare Apple Health import complete", f"person={person_link}")
        log_cli_step(self.source_id, 2, 3, "parse and aggregate Apple Health export")
        root = ElementTree.parse(export_xml_path).getroot()
        export_hash = _file_sha256(export_xml_path)

        quantity_buckets: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "sum": 0.0, "min": None, "max": None, "unit": ""}
        )
        sleep_buckets: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(lambda: {"count": 0, "total_hours": 0.0})
        workout_buckets: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "total_minutes": 0.0, "distance_sum": 0.0, "energy_sum": 0.0}
        )
        records = root.findall("./Record")
        workouts = root.findall("./Workout")
        total_events = len(records) + len(workouts)
        reporter = CliProgressReporter(
            source_id=self.source_id,
            step_number=2,
            total_steps=3,
            stage="scan health entries",
            total_items=total_events,
            progress_every=progress_every,
            enabled=verbose,
        )
        processed = 0

        for record in records:
            record_type = _clean(record.attrib.get("type"))
            bucket_type, subtype = _bucket_type(record_type)
            if not bucket_type:
                processed += 1
                reporter.update(processed)
                continue
            day = _date_bucket(record.attrib.get("startDate", "") or record.attrib.get("creationDate", ""))
            if bucket_type == "sleep_rollup":
                start = _parse_datetime(record.attrib.get("startDate", ""))
                end = _parse_datetime(record.attrib.get("endDate", ""))
                hours = ((end - start).total_seconds() / 3600.0) if start and end else 0.0
                bucket = sleep_buckets[(bucket_type, subtype, day)]
                bucket["count"] += 1
                bucket["total_hours"] += max(hours, 0.0)
                processed += 1
                reporter.update(processed)
                continue
            numeric = _float_value(record.attrib.get("value"))
            bucket = quantity_buckets[(bucket_type, subtype, day)]
            bucket["count"] += 1
            bucket["sum"] += numeric
            bucket["unit"] = _clean(record.attrib.get("unit"))
            bucket["min"] = numeric if bucket["min"] is None else min(float(bucket["min"]), numeric)
            bucket["max"] = numeric if bucket["max"] is None else max(float(bucket["max"]), numeric)
            processed += 1
            reporter.update(processed)

        for workout in workouts:
            activity_type = _clean(workout.attrib.get("workoutActivityType")) or "workout"
            day = _date_bucket(workout.attrib.get("startDate", "") or workout.attrib.get("creationDate", ""))
            bucket = workout_buckets[("workout_rollup", activity_type, day)]
            bucket["count"] += 1
            bucket["total_minutes"] += _float_value(workout.attrib.get("duration"))
            bucket["distance_sum"] += _float_value(workout.attrib.get("totalDistance"))
            bucket["energy_sum"] += _float_value(workout.attrib.get("totalEnergyBurned"))
            processed += 1
            reporter.update(processed)
        reporter.complete(processed, extra=f"record_buckets={len(quantity_buckets)+len(sleep_buckets)+len(workout_buckets)}")

        items: list[dict[str, Any]] = []
        log_cli_step(self.source_id, 3, 3, "emit structured Apple Health cards")
        for (bucket_type, subtype, day), metrics in sorted(quantity_buckets.items()):
            count = int(metrics["count"])
            avg = round(float(metrics["sum"]) / max(count, 1), 4)
            summary, numeric_value, unit, code_display = _rollup_summary(
                bucket_type,
                subtype,
                day,
                {**metrics, "avg": avg},
            )
            items.append(
                {
                    "kind": "medical_record",
                    "source_id": f"apple-health:{subtype}:day:{day}",
                    "summary": summary,
                    "created": day,
                    "people": [person_link],
                    "source_system": "apple_health",
                    "source_format": "apple_health_xml",
                    "record_type": bucket_type,
                    "record_subtype": subtype,
                    "status": "aggregated",
                    "occurred_at": day,
                    "recorded_at": day,
                    "provider_name": "",
                    "facility_name": "",
                    "encounter_source_id": "",
                    "code_system": "apple_health",
                    "code": subtype,
                    "code_display": code_display,
                    "value_text": summary,
                    "value_numeric": numeric_value,
                    "unit": unit,
                    "raw_source_ref": f"apple_health:{subtype}:{day}",
                    "details_json": {
                        "source_hashes": {"apple_health_xml": export_hash},
                        "metrics": {**metrics, "avg": avg},
                    },
                }
            )

        for (bucket_type, subtype, day), metrics in sorted(sleep_buckets.items()):
            summary, numeric_value, unit, code_display = _rollup_summary(bucket_type, subtype, day, metrics)
            items.append(
                {
                    "kind": "medical_record",
                    "source_id": f"apple-health:{subtype}:day:{day}",
                    "summary": summary,
                    "created": day,
                    "people": [person_link],
                    "source_system": "apple_health",
                    "source_format": "apple_health_xml",
                    "record_type": bucket_type,
                    "record_subtype": subtype,
                    "status": "aggregated",
                    "occurred_at": day,
                    "recorded_at": day,
                    "provider_name": "",
                    "facility_name": "",
                    "encounter_source_id": "",
                    "code_system": "apple_health",
                    "code": subtype,
                    "code_display": code_display,
                    "value_text": summary,
                    "value_numeric": numeric_value,
                    "unit": unit,
                    "raw_source_ref": f"apple_health:{subtype}:{day}",
                    "details_json": {
                        "source_hashes": {"apple_health_xml": export_hash},
                        "metrics": metrics,
                    },
                }
            )

        for (bucket_type, subtype, day), metrics in sorted(workout_buckets.items()):
            summary, numeric_value, unit, code_display = _rollup_summary(bucket_type, subtype, day, metrics)
            items.append(
                {
                    "kind": "medical_record",
                    "source_id": f"apple-health:{subtype}:day:{day}",
                    "summary": summary,
                    "created": day,
                    "people": [person_link],
                    "source_system": "apple_health",
                    "source_format": "apple_health_xml",
                    "record_type": bucket_type,
                    "record_subtype": subtype,
                    "status": "aggregated",
                    "occurred_at": day,
                    "recorded_at": day,
                    "provider_name": "",
                    "facility_name": "",
                    "encounter_source_id": "",
                    "code_system": "apple_health",
                    "code": subtype,
                    "code_display": code_display,
                    "value_text": summary,
                    "value_numeric": numeric_value,
                    "unit": unit,
                    "raw_source_ref": f"apple_health:{subtype}:{day}",
                    "details_json": {
                        "source_hashes": {"apple_health_xml": export_hash},
                        "metrics": metrics,
                    },
                }
            )

        cursor.update(
            {
                "export_hash": export_hash,
                "emitted_records": len(items),
                "person_wikilink": person_link,
            }
        )
        log_cli_step(self.source_id, 3, 3, "emit structured Apple Health cards complete", f"items={len(items)}")
        return items

    def to_card(self, item: dict[str, Any]):
        today = date.today().isoformat()
        card = MedicalRecordCard(
            uid=generate_uid("medical-record", self.source_id, _clean(item.get("source_id"))),
            type="medical_record",
            source=[APPLE_HEALTH_SOURCE],
            source_id=_clean(item.get("source_id")),
            created=_clean(item.get("created")) or today,
            updated=today,
            summary=_clean(item.get("summary")),
            tags=["medical", "apple-health", _clean(item.get("record_type")).replace("_", "-")],
            people=list(item.get("people", [])),
            source_system=_clean(item.get("source_system")),
            source_format=_clean(item.get("source_format")),
            record_type=_clean(item.get("record_type")),
            record_subtype=_clean(item.get("record_subtype")),
            status=_clean(item.get("status")),
            occurred_at=_clean(item.get("occurred_at")),
            recorded_at=_clean(item.get("recorded_at")),
            provider_name=_clean(item.get("provider_name")),
            facility_name=_clean(item.get("facility_name")),
            encounter_source_id=_clean(item.get("encounter_source_id")),
            code_system=_clean(item.get("code_system")),
            code=_clean(item.get("code")),
            code_display=_clean(item.get("code_display")),
            value_text=_clean(item.get("value_text")),
            value_numeric=float(item.get("value_numeric", 0) or 0),
            unit=_clean(item.get("unit")),
            raw_source_ref=_clean(item.get("raw_source_ref")),
            details_json=dict(item.get("details_json") or {}),
        )
        provenance = deterministic_provenance(card, APPLE_HEALTH_SOURCE)
        return card, provenance, _render_body(item)

    def merge_card(self, vault_path, rel_path, card, body, provenance) -> None:
        self._replace_generic_card(vault_path, rel_path, card, body, provenance)
