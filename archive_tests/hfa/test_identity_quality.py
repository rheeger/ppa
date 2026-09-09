from archive_cli.commands.identity_repair import alias_hygiene
from archive_sync.adapters.base import deterministic_provenance
from archive_vault.identity_quality import (
    alias_is_trustworthy,
    is_shared_mailbox_email,
    looks_like_event_title,
    looks_like_person_name,
)
from archive_vault.identity_resolver import merge_into_existing
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import read_note, write_card


def test_looks_like_person_name_rejects_event_titles() -> None:
    assert looks_like_person_name("Lisa Messinger") is True
    assert looks_like_person_name("Jane A. Smith") is True
    assert looks_like_person_name("Candy and Annie") is False
    assert looks_like_person_name("Cigars & Conversation") is False
    assert looks_like_person_name("40 years of Candy") is False
    assert looks_like_event_title("Candy and Annie") is True
    assert looks_like_event_title("Panken, Aaron") is False
    assert looks_like_event_title("alex.bae@circle.com") is False


def test_shared_mailbox_email_matches_subdomains() -> None:
    assert is_shared_mailbox_email("paperlesspost@paperlesspost.com") is True
    assert is_shared_mailbox_email("paperlesspost@accounts.paperlesspost.com") is True
    assert is_shared_mailbox_email("susan.g.wolfe@gmail.com") is False


def test_alias_from_shared_mailbox_is_not_trustworthy() -> None:
    assert alias_is_trustworthy("Lisa Messinger") is True
    assert (
        alias_is_trustworthy(
            "Lisa Messinger",
            incoming_emails=["paperlesspost@paperlesspost.com"],
        )
        is False
    )


def test_merge_skips_shared_mailbox_from_name_alias(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    merge_into_existing(
        tmp_vault,
        "[[jane-smith]]",
        {
            "uid": sample_person_card.uid,
            "type": "person",
            "source": ["gmail-correspondents"],
            "source_id": "paperlesspost@paperlesspost.com",
            "created": sample_person_card.created,
            "updated": sample_person_card.updated,
            "summary": "Lisa Messinger",
            "emails": ["paperlesspost@paperlesspost.com"],
            "aliases": ["Candy and Annie"],
        },
        {
            "summary": ProvenanceEntry("gmail-correspondents", "2026-08-28", "deterministic"),
            "emails": ProvenanceEntry("gmail-correspondents", "2026-08-28", "deterministic"),
            "aliases": ProvenanceEntry("gmail-correspondents", "2026-08-28", "deterministic"),
        },
    )
    frontmatter, _, _ = read_note(tmp_vault, "People/jane-smith.md")
    assert "Lisa Messinger" not in (frontmatter.get("aliases") or [])
    assert "Candy and Annie" not in (frontmatter.get("aliases") or [])
    assert "paperlesspost@paperlesspost.com" not in (frontmatter.get("emails") or [])


def test_alias_hygiene_strips_stolen_invitation_aliases(tmp_vault) -> None:
    host = PersonCard(
        uid="hfa-person-susan000001",
        type="person",
        source=["gmail-correspondents"],
        source_id="paperlesspost@paperlesspost.com",
        created="2026-01-01",
        updated="2026-01-01",
        summary="Susan Wolfe",
        first_name="Susan",
        last_name="Wolfe",
        emails=["paperlesspost@paperlesspost.com", "susan.g.wolfe@gmail.com"],
        aliases=["Lisa Messinger", "Candy and Annie"],
    )
    real = PersonCard(
        uid="hfa-person-lisa0000001",
        type="person",
        source=["contacts.apple"],
        source_id="lisamess8@gmail.com",
        created="2026-01-01",
        updated="2026-01-01",
        summary="Lisa Messinger",
        first_name="Lisa",
        last_name="Messinger",
        emails=["lisamess8@gmail.com"],
    )
    write_card(tmp_vault, "People/susan-wolfe.md", host, provenance=deterministic_provenance(host, "gmail-correspondents"))
    write_card(tmp_vault, "People/lisa-messinger.md", real, provenance=deterministic_provenance(real, "contacts.apple"))
    dry = alias_hygiene(tmp_vault, apply=False)
    assert dry["stolen_alias_count"] >= 1
    assert dry["shared_mailbox_email_count"] >= 1
    assert dry["applied"] is False
    applied = alias_hygiene(tmp_vault, apply=True)
    assert applied["changed_people"] == 1
    frontmatter, _, _ = read_note(tmp_vault, "People/susan-wolfe.md")
    assert frontmatter["emails"] == ["susan.g.wolfe@gmail.com"]
    assert "Lisa Messinger" not in (frontmatter.get("aliases") or [])
    assert "Candy and Annie" not in (frontmatter.get("aliases") or [])
