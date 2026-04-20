"""Phase 6.5 Step 16.4 — UTC-Z canonicalization helpers."""

from __future__ import annotations

from archive_sync.adapters.datetime_canon import (AUDITED_TIMESTAMP_FIELDS,
                                                  classify_timestamp,
                                                  to_utc_z_iso)


def test_to_utc_z_iso_preserves_z_suffix() -> None:
    s = "2026-03-08T15:00:00Z"
    assert to_utc_z_iso(s) == s


def test_to_utc_z_iso_converts_offset_to_z() -> None:
    assert to_utc_z_iso("2023-08-07T11:00:00-07:00") == "2023-08-07T18:00:00Z"


def test_to_utc_z_iso_leaves_date_only_unchanged() -> None:
    """Schema requires ``YYYY-MM-DD`` for date-only fields (created/updated/pay_date)."""
    assert to_utc_z_iso("2023-08-07") == "2023-08-07"


def test_classify_timestamp_offset() -> None:
    cat, _ = classify_timestamp("2023-08-07T11:00:00-07:00")
    assert cat == "offset"


def test_classify_timestamp_utc_z() -> None:
    cat, _ = classify_timestamp("2023-08-07T18:00:00Z")
    assert cat == "utc_z"


def test_classify_timestamp_date_only() -> None:
    cat, _ = classify_timestamp("2023-08-07")
    assert cat == "date_only"


def test_audited_fields_nonempty() -> None:
    assert "calendar_event" in AUDITED_TIMESTAMP_FIELDS
    assert "start_at" in AUDITED_TIMESTAMP_FIELDS["calendar_event"]


def test_second_pass_on_canonical_emits_no_changes() -> None:
    """Idempotent rewrite: UTC-Z values stay unchanged."""
    fm = {
        "type": "calendar_event",
        "start_at": "2023-08-07T18:00:00Z",
        "end_at": "2023-08-07T19:00:00Z",
    }
    changed = 0
    fm2 = dict(fm)
    for field in AUDITED_TIMESTAMP_FIELDS["calendar_event"]:
        if field not in fm2:
            continue
        raw = fm2[field] if isinstance(fm2[field], str) else str(fm2[field])
        cat, _ = classify_timestamp(raw)
        if cat in ("offset", "naive"):
            new_v = to_utc_z_iso(raw)
            if new_v and new_v != raw:
                fm2[field] = new_v
                changed += 1
    assert changed == 0
    assert fm2 == fm
