from __future__ import annotations

from pathlib import Path

from archive_cli.commands.identity_repair import _compatible_name, merge_people


def _write_person(vault: Path, uid: str, first: str, last: str, **kwargs) -> None:
    dest = vault / "People" / f"{uid}.md"
    dest.parent.mkdir(parents=True, exist_ok=True)
    emails = kwargs.get("emails") or []
    phones = kwargs.get("phones") or []
    dest.write_text(
        "\n".join(
            [
                "---",
                f"uid: {uid}",
                "type: person",
                "source: [test]",
                f"source_id: {uid}",
                "created: '2026-01-01'",
                "updated: '2026-01-01'",
                f"summary: {first} {last}".strip() or uid,
                f"first_name: {first}",
                f"last_name: {last}",
                "emails:",
                *[f"  - {item}" for item in emails],
                "phones:",
                *[f"  - {item}" for item in phones],
                "---",
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_alice_bob_smith_household_phone_never_auto_merges(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    _write_person(vault, "hfa-person-alice", "Alice", "Smith", phones=["+15551230000"])
    _write_person(vault, "hfa-person-bob", "Bob", "Smith", phones=["+15551230000"])
    assert (
        _compatible_name(
            {"first_name": "Alice", "last_name": "Smith"},
            {"first_name": "Bob", "last_name": "Smith"},
            {},
        )
        is False
    )
    result = merge_people(vault, apply=False)
    assert result["redirected"] == []
    assert result["queued"]


def test_apply_stub_email_merge_and_queue_household(tmp_path: Path) -> None:
    from archive_engine.journaled_state import IDENTITY_PROPOSALS_REL, load_json_state
    from archive_sync.adapters.base import deterministic_provenance
    from archive_vault.schema import PersonCard
    from archive_vault.vault import write_card

    vault = tmp_path / "vault"
    (vault / "People").mkdir(parents=True)
    (vault / "_meta").mkdir()
    (vault / "_meta" / "identity-map.json").write_text("{}\n", encoding="utf-8")
    (vault / "_meta" / "nicknames.json").write_text("{}\n", encoding="utf-8")

    def write(uid: str, first: str, last: str, **kwargs) -> None:
        card = PersonCard(
            uid=uid,
            type="person",
            source=["contacts.apple"],
            source_id=uid,
            created="2026-01-01",
            updated="2026-01-01",
            summary=f"{first} {last}".strip() or uid,
            first_name=first,
            last_name=last,
            emails=list(kwargs.get("emails") or []),
            phones=list(kwargs.get("phones") or []),
        )
        write_card(
            vault,
            f"People/{uid}.md",
            card,
            body=card.summary,
            provenance=deterministic_provenance(card, "contacts.apple"),
        )

    write("hfa-person-alice000001", "Alice", "Smith", phones=["+15551230000"])
    write("hfa-person-bob00000001", "Bob", "Smith", phones=["+15551230000"])
    write("hfa-person-pat00000001", "Pat", "Lee", emails=["x@example.com"])
    write("hfa-person-stub0000001", "", "", emails=["x@example.com"])
    result = merge_people(vault, apply=True)
    assert result["applied"] is True
    touched = {item["winner"] for item in result["redirected"]} | {item["loser"] for item in result["redirected"]}
    assert "hfa-person-alice000001" not in touched
    assert {"hfa-person-pat00000001", "hfa-person-stub0000001"} <= touched
    queued = load_json_state(vault, IDENTITY_PROPOSALS_REL).get("proposals") or []
    assert queued


def test_dry_run_merge_writes_nothing(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    _write_person(vault, "hfa-person-stub", "", "", emails=["x@example.com"])
    _write_person(vault, "hfa-person-named", "Pat", "Lee", emails=["x@example.com"])
    before = {p.read_text(encoding="utf-8") for p in vault.rglob("*.md")}
    result = merge_people(vault, apply=False)
    after = {p.read_text(encoding="utf-8") for p in vault.rglob("*.md")}
    assert before == after
    assert result["applied"] is False
