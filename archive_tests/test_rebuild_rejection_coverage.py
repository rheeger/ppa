from __future__ import annotations

from archive_cli.scanner import try_canonical_row


def test_scan_rejections_are_journaled(tmp_path) -> None:
    from archive_cli.scanner import persist_scan_rejections
    from archive_engine.journaled_state import SCAN_REJECTIONS_REL, load_json_state

    vault = tmp_path / "vault"
    vault.mkdir()
    persist_scan_rejections(
        vault,
        [{"contained_path": "People/bad.md", "error_code": "schema_invalid", "field_names": ["source"]}],
    )
    payload = load_json_state(vault, SCAN_REJECTIONS_REL)
    assert payload["count"] == 1
    assert payload["complete"] is False
    assert (vault / "_meta" / "change-journal.sqlite3").is_file()


def test_invalid_card_returns_rejection_not_silent_none() -> None:
    row, rejection = try_canonical_row("People/bad.md", {"uid": "x", "type": "person"})
    assert row is None
    assert rejection is not None
    assert rejection["error_code"] == "schema_invalid"
    assert rejection["contained_path"] == "People/bad.md"
    assert "input" not in rejection
