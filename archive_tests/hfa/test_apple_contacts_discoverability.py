"""Conversation retarget and discoverability report for Apple Contacts catch-up."""

from __future__ import annotations

from pathlib import Path

from archive_cli.commands.identity_repair import (
    apply_accepted_reviews,
    discoverability_report,
    discoverability_snapshot,
    resolve_people_fields,
)
from archive_sync.adapters.beeper import BeeperAdapter, ParticipantRecord
from archive_vault.identity import IdentityCache, upsert_identity_map
from archive_vault.identity_resolver import PersonIndex
from archive_vault.schema import PersonCard
from archive_vault.vault import read_note, write_card


def _write_thread(vault: Path, *, rel: str, uid: str, people: list[str], handle: str) -> None:
    dest = vault / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    people_yaml = "\n".join(f'  - "{item}"' for item in people) if people else "  []"
    dest.write_text(
        "\n".join(
            [
                "---",
                f"uid: {uid}",
                "type: imessage_thread",
                "source:",
                "  - imessage",
                f"source_id: {uid}",
                'created: "2026-01-01"',
                'updated: "2026-01-01"',
                'summary: "Jane chat"',
                "imessage_chat_id: chat-jane",
                "participant_handles:",
                f'  - "{handle}"',
                "people:",
                people_yaml,
                "messages: []",
                "orgs: []",
                "tags: []",
                "---",
                "",
                "thread",
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_resolve_people_replaces_stub_instead_of_stacking(tmp_vault, sample_person_card, sample_person_provenance):
    payload = sample_person_card.model_dump(mode="python")
    payload["phones"] = ["+15551234567"]
    card = PersonCard.model_validate(payload)
    write_card(tmp_vault, "People/jane-smith.md", card, provenance=sample_person_provenance)
    upsert_identity_map(tmp_vault, "[[jane-smith]]", {"phones": ["+15551234567"]})
    identity = IdentityCache(tmp_vault)
    identity.entries["redirect-wikilink:[[jane-stub-deadbeef]]"] = "[[jane-smith]]"
    identity.flush()
    _write_thread(
        tmp_vault,
        rel="Messages/Threads/jane-chat.md",
        uid="hfa-imessage-thread-janeretarget",
        people=["[[jane-stub-deadbeef]]"],
        handle="+15551234567",
    )
    result = resolve_people_fields(tmp_vault, apply=True)
    assert result["updated_cards"] == 1
    assert result["replaced_wikilink_by_type"].get("imessage_thread") == 1
    fm, _body, _prov = read_note(tmp_vault, "Messages/Threads/jane-chat.md")
    assert fm["people"] == ["[[jane-smith]]"]


def test_beeper_does_not_mint_stub_when_apple_owns_phone(
    tmp_vault, sample_person_card, sample_person_provenance
):
    payload = sample_person_card.model_dump(mode="python")
    payload["phones"] = ["+15551234567"]
    card = PersonCard.model_validate(payload)
    write_card(tmp_vault, "People/jane-smith.md", card, provenance=sample_person_provenance)
    upsert_identity_map(tmp_vault, "[[jane-smith]]", {"phones": ["+15551234567"]})
    adapter = BeeperAdapter()
    participant = ParticipantRecord(
        participant_id="@jane:beeper.local",
        full_name="Jane Smith",
        is_self=False,
        identifiers=[("phone", "(555) 123-4567")],
    )
    item = adapter._participant_person_item(
        account_id="imessage",
        protocol="imessage",
        participant=participant,
    )
    assert item is not None
    plan, matched = adapter._prepare_person_write(
        item,
        cache=IdentityCache(tmp_vault),
        vault_path=tmp_vault,
        people_index=PersonIndex(tmp_vault, preload=True),
    )
    assert matched is True
    assert plan is None
    extra = [path.name for path in (tmp_vault / "People").glob("*.md") if path.name != "jane-smith.md"]
    assert extra == []


def test_beeper_name_conflict_is_not_counted_as_existing_match(
    tmp_vault, sample_person_card, sample_person_provenance
):
    payload = sample_person_card.model_dump(mode="python")
    payload["phones"] = ["+15551234567"]
    card = PersonCard.model_validate(payload)
    write_card(tmp_vault, "People/jane-smith.md", card, provenance=sample_person_provenance)
    upsert_identity_map(tmp_vault, "[[jane-smith]]", {"phones": ["+15551234567"]})
    adapter = BeeperAdapter()
    participant = ParticipantRecord(
        participant_id="@john:beeper.local",
        full_name="John Doe",
        is_self=False,
        identifiers=[("phone", "+15551234567")],
    )
    item = adapter._participant_person_item(
        account_id="imessage",
        protocol="imessage",
        participant=participant,
    )
    assert item is not None
    plan, matched = adapter._prepare_person_write(
        item,
        cache=IdentityCache(tmp_vault),
        vault_path=tmp_vault,
        people_index=PersonIndex(tmp_vault, preload=True),
    )
    assert matched is False
    assert plan is None


def test_discoverability_report_shape(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    upsert_identity_map(tmp_vault, "[[jane-smith]]", {"emails": ["jane@example.com"], "phones": ["+15550123"]})
    _write_thread(
        tmp_vault,
        rel="Messages/Threads/empty-people.md",
        uid="hfa-imessage-thread-empty01",
        people=[],
        handle="+15551239999",
    )
    before = discoverability_snapshot(tmp_vault)
    assert before["person_count"] == 1
    assert before["people_with_contacts_apple"] == 1
    assert before["missing_people"] == 1
    assert before["empty_people_by_type"]["imessage_thread"] == 1
    assert before["identity_phone_keys"] >= 1
    assert before["review_queue"]["open"] == 0
    after = dict(before)
    after["missing_people"] = 0
    after["empty_people_by_type"] = {**before["empty_people_by_type"], "imessage_thread": 0}
    after["missing_people_by_type"] = {**before["missing_people_by_type"], "imessage_thread": 0}
    after["identity_phone_keys"] = before["identity_phone_keys"] + 2
    report = discoverability_report(
        before=before,
        after=after,
        match_outcomes={"merge": 1, "create": 0, "review": 1, "skip": 0},
        match_reasons={"exact_phone": 1, "fuzzy_name": 1},
        resolve_people={"gained_wikilink_by_type": {"imessage_thread": 1}, "replaced_wikilink_by_type": {}},
    )
    assert report["delta"]["missing_people"] == -1
    assert report["delta"]["identity_phone_keys"] == 2
    assert report["gained_wikilink_by_type"]["imessage_thread"] == 1
    assert report["match_outcomes"]["review"] == 1
    assert "open" in report["review_queue"]


def test_apply_accepted_reviews_dry_run_does_not_write(tmp_vault):
    from archive_engine.journaled_state import (
        IDENTITY_PROPOSALS_REL,
        IDENTITY_PROPOSALS_UID,
        persist_json_state,
    )

    persist_json_state(
        tmp_vault,
        uid=IDENTITY_PROPOSALS_UID,
        rel=IDENTITY_PROPOSALS_REL,
        payload={
            "status": "queued",
            "proposals": [
                {
                    "uids": ["hfa-person-in", "hfa-person-ex"],
                    "status": "accepted",
                    "existing_wikilink": "[[jane-smith]]",
                    "incoming": {"uid": "hfa-person-in", "summary": "Jane", "emails": [], "phones": []},
                }
            ],
        },
        source="test",
    )
    result = apply_accepted_reviews(tmp_vault, apply=False)
    assert result["accepted"] == 1
    assert result["applied"] is False
    assert result["applied_rows"]
