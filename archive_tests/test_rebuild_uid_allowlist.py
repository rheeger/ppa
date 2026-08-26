"""Dirty-UID rebuild must stay incremental and never escalate to full-scan."""

from __future__ import annotations

from archive_cli.loader import resolve_uid_allowlist_rebuild


def test_allowlist_intersection_is_incremental() -> None:
    mode, materialize, missing = resolve_uid_allowlist_rebuild(
        {"hfa-a", "hfa-b", "hfa-missing"},
        {"hfa-a", "hfa-b", "hfa-c"},
    )
    assert mode == "incremental"
    assert materialize == {"hfa-a", "hfa-b"}
    assert missing == {"hfa-missing"}


def test_allowlist_all_missing_is_noop() -> None:
    mode, materialize, missing = resolve_uid_allowlist_rebuild(
        {"hfa-gone"},
        {"hfa-a"},
    )
    assert mode == "noop"
    assert materialize == set()
    assert missing == {"hfa-gone"}


def test_allowlist_does_not_escalate_when_every_present_uid_is_dirty() -> None:
    """Classification used to return full when materialize_uids >= all rows."""
    present = {f"hfa-{i}" for i in range(20)}
    mode, materialize, missing = resolve_uid_allowlist_rebuild(present, present)
    assert mode == "incremental"
    assert materialize == present
    assert missing == set()


def test_allowlist_strips_blank_uids() -> None:
    mode, materialize, missing = resolve_uid_allowlist_rebuild(
        {"hfa-a", "", "  "},
        {"hfa-a"},
    )
    assert mode == "incremental"
    assert materialize == {"hfa-a"}
    assert missing == set()
