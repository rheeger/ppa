from archive_vault.identity import upsert_identity_map
from archive_vault.identity_resolver import (
    PersonIndex,
    ResolveResult,
    _auto_approve_if_confident,
    merge_into_existing,
    names_match,
    resolve_person,
)
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard, validate_card_permissive
from archive_vault.vault import read_note, write_card


def test_person_index_skips_invalid_companies_card(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    (tmp_vault / "People" / "verified-caller.md").write_text(
        "---\n"
        "uid: hfa-person-bad00000001\n"
        "type: person\n"
        "source: [contacts.apple]\n"
        "source_id: Verified Caller\n"
        "created: '2026-03-07'\n"
        "updated: '2026-09-12'\n"
        "summary: Verified Caller\n"
        "first_name: Verified\n"
        "last_name: Caller\n"
        "companies:\n"
        "  - Protected by Cloaked:\n"
        "---\n",
        encoding="utf-8",
    )
    notes: list[str] = []
    index = PersonIndex(tmp_vault, preload=True, log=notes.append)
    assert "[[jane-smith]]" in index.records
    assert "[[verified-caller]]" not in index.records
    assert any("skip invalid card rel=People/verified-caller.md" in line for line in notes)


def test_names_match_supports_nicknames(tmp_vault):
    match, score = names_match("Robert Heeger", "Robbie Heeger", {"robert": ["robbie"]})
    assert match is True
    assert score == 95.0


def test_resolve_person_exact_email_merge(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    upsert_identity_map(tmp_vault, "[[jane-smith]]", {"emails": ["jane@example.com"]})
    result = resolve_person(tmp_vault, {"summary": "Jane Smith", "emails": ["jane@example.com"]})
    assert result.action == "merge"
    assert result.wikilink == "[[jane-smith]]"
    assert result.confidence == 100


def test_resolve_person_exact_linkedin_merge(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    upsert_identity_map(tmp_vault, "[[jane-smith]]", {"linkedin": "janesmith"})
    result = resolve_person(
        tmp_vault,
        {
            "summary": "Jane Smith",
            "linkedin": "janesmith",
            "linkedin_url": "https://www.linkedin.com/in/janesmith",
        },
    )
    assert result.action == "merge"
    assert result.wikilink == "[[jane-smith]]"
    assert result.confidence == 100


def test_resolve_person_exact_discord_merge(tmp_vault, sample_person_card, sample_person_provenance):
    payload = sample_person_card.model_dump(mode="python")
    payload["discord"] = "pedroyan"
    card = PersonCard.model_validate(payload)
    provenance = {
        **sample_person_provenance,
        "discord": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
    }
    write_card(tmp_vault, "People/jane-smith.md", card, provenance=provenance)
    upsert_identity_map(tmp_vault, "[[jane-smith]]", {"discord": "pedroyan"})
    result = resolve_person(
        tmp_vault,
        {
            "summary": "Jane Smith",
            "discord": "pedroyan",
        },
    )
    assert result.action == "merge"
    assert result.wikilink == "[[jane-smith]]"
    assert result.confidence == 100


def test_merge_into_existing_unions_arrays(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    merge_into_existing(
        tmp_vault,
        "[[jane-smith]]",
        {
            "uid": sample_person_card.uid,
            "type": "person",
            "source": ["linkedin"],
            "source_id": sample_person_card.source_id,
            "created": sample_person_card.created,
            "updated": sample_person_card.updated,
            "summary": "Jane A. Smith",
            "aliases": ["Janie Smith"],
            "emails": ["jane@example.com", "j.smith@corp.com"],
            "phones": ["+15550123"],
            "company": "Endaoment Labs",
            "companies": ["Endaoment Labs"],
            "title": "VP Partnerships and Ecosystem",
            "titles": ["VP Partnerships and Ecosystem"],
            "linkedin": "janesmith",
            "linkedin_url": "https://www.linkedin.com/in/janesmith",
            "linkedin_connected_on": "2024-01-01",
            "tags": ["endaoment", "linkedin"],
            "people": [],
            "orgs": [],
        },
        {
            "summary": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "aliases": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "emails": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "company": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "companies": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "title": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "titles": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "linkedin": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "linkedin_url": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "linkedin_connected_on": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "tags": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
        },
        "Connected on: 2024-01-01",
    )
    frontmatter, body, provenance = read_note(tmp_vault, "People/jane-smith.md")
    card = validate_card_permissive(frontmatter)
    assert card.source == ["contacts.apple", "linkedin"]
    assert card.emails == ["jane@example.com", "j.smith@corp.com"]
    assert card.aliases == ["Jane A. Smith", "Janie Smith"]
    assert card.company == "Endaoment Labs"
    assert card.companies == ["Endaoment", "Endaoment Labs"]
    assert card.title == "VP Partnerships and Ecosystem"
    assert card.titles == ["VP Partnerships", "VP Partnerships and Ecosystem"]
    assert card.tags == ["endaoment", "linkedin"]
    assert body == "Connected on: 2024-01-01"
    assert provenance["linkedin"].source == "contacts.apple"
    assert provenance["company"].source == "linkedin"
    assert provenance["title"].source == "linkedin"
    assert provenance["emails"].source == "linkedin"
    assert provenance["summary"].source == "contacts.apple"
    assert card.uid == sample_person_card.uid


def test_merge_into_existing_keeps_host_uid(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    incoming_uid = "hfa-person-phantom0001"
    merge_into_existing(
        tmp_vault,
        "[[jane-smith]]",
        {
            "uid": incoming_uid,
            "type": "person",
            "source": ["linkedin"],
            "source_id": "linkedin-jane",
            "created": sample_person_card.created,
            "updated": sample_person_card.updated,
            "summary": sample_person_card.summary,
            "emails": ["jane@example.com"],
        },
        {
            "emails": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
        },
    )
    frontmatter, _, _ = read_note(tmp_vault, "People/jane-smith.md")
    assert frontmatter["uid"] == sample_person_card.uid
    assert incoming_uid != sample_person_card.uid


def test_resolve_person_fuzzy_name_with_company_support_auto_approves(tmp_vault):
    existing = PersonCard(
        uid="hfa-person-existing0001",
        type="person",
        source=["contacts.apple"],
        source_id="robbie@endaoment.org",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Robbie Heeger",
        first_name="Robbie",
        last_name="Heeger",
        company="Endaoment",
        title="CEO",
        emails=["robbie@endaoment.org"],
    )
    provenance = {
        "summary": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "first_name": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "last_name": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "company": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "title": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "emails": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
    }
    write_card(tmp_vault, "People/robbie-heeger.md", existing, provenance=provenance)
    result = resolve_person(
        tmp_vault,
        {
            "summary": "Robert Heeger",
            "first_name": "Robert",
            "last_name": "Heeger",
            "emails": ["robert@endaoment.org"],
            "company": "Endaoment",
            "title": "CEO",
            "linkedin": "rheeger",
        },
    )
    assert result.action == "merge"
    assert result.wikilink == "[[robbie-heeger]]"
    assert result.confidence >= 80
    assert "auto_approved" in result.reasons
    assert "fuzzy_name" in result.reasons or "close_name" in result.reasons


def test_resolve_person_same_name_without_support_stays_review(tmp_vault):
    existing = PersonCard(
        uid="hfa-person-existing0002",
        type="person",
        source=["contacts.apple"],
        source_id="alex@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Alex Johnson",
        first_name="Alex",
        last_name="Johnson",
        company="Endaoment",
        emails=["alex@example.com"],
    )
    provenance = {
        "summary": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "first_name": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "last_name": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "company": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "emails": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
    }
    write_card(tmp_vault, "People/alex-johnson.md", existing, provenance=provenance)
    result = resolve_person(
        tmp_vault,
        {
            "summary": "Alex Johnson",
            "first_name": "Alex",
            "last_name": "Johnson",
        },
    )
    assert result.action == "conflict"
    assert result.wikilink == "[[alex-johnson]]"
    assert "auto_approved" not in result.reasons


def test_merge_into_existing_derives_alias_provenance_from_summary(
    tmp_vault, sample_person_card, sample_person_provenance
):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    merge_into_existing(
        tmp_vault,
        "[[jane-smith]]",
        {
            "uid": sample_person_card.uid,
            "type": "person",
            "source": ["linkedin"],
            "source_id": sample_person_card.source_id,
            "created": sample_person_card.created,
            "updated": sample_person_card.updated,
            "summary": "Jane Alexandra Smith",
            "emails": ["jane@example.com"],
            "company": "Endaoment",
            "title": "VP Partnerships",
            "linkedin": "janesmith",
        },
        {
            "summary": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "emails": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "company": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "title": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
            "linkedin": ProvenanceEntry("linkedin", "2026-03-06", "deterministic"),
        },
    )
    frontmatter, _, provenance = read_note(tmp_vault, "People/jane-smith.md")
    assert "Jane Alexandra Smith" in frontmatter["aliases"]
    assert provenance["aliases"].source == "linkedin"


def test_merge_into_existing_backfills_phone_provenance_when_incoming_omits_it(
    tmp_vault, sample_person_card, sample_person_provenance
):
    payload = sample_person_card.model_dump(mode="python")
    payload["phones"] = []
    card = PersonCard.model_validate(payload)
    provenance = {key: value for key, value in sample_person_provenance.items() if key != "phones"}
    write_card(tmp_vault, "People/jane-smith.md", card, provenance=provenance)
    merge_into_existing(
        tmp_vault,
        "[[jane-smith]]",
        {
            "uid": sample_person_card.uid,
            "type": "person",
            "source": ["contacts.apple"],
            "source_id": sample_person_card.source_id,
            "created": sample_person_card.created,
            "updated": sample_person_card.updated,
            "summary": sample_person_card.summary,
            "phones": ["+15559876543"],
        },
        {},
    )
    frontmatter, _, written = read_note(tmp_vault, "People/jane-smith.md")
    assert frontmatter["phones"] == ["+15559876543"]
    assert written["phones"].source == "contacts.apple"
    assert written["phones"].method == "deterministic"


def test_merge_into_existing_skips_write_when_identity_unchanged(
    tmp_vault, sample_person_card, sample_person_provenance
):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    path = tmp_vault / "People/jane-smith.md"
    before = path.stat().st_mtime_ns
    result = merge_into_existing(
        tmp_vault,
        "[[jane-smith]]",
        {
            **sample_person_card.model_dump(mode="python"),
            "updated": "2026-09-10",
        },
        sample_person_provenance,
    )
    assert result is None
    assert path.stat().st_mtime_ns == before


def test_resolve_person_canon_phone_formats_merge(tmp_vault, sample_person_card, sample_person_provenance):
    payload = sample_person_card.model_dump(mode="python")
    payload["phones"] = ["+15551234567"]
    card = PersonCard.model_validate(payload)
    write_card(tmp_vault, "People/jane-smith.md", card, provenance=sample_person_provenance)
    upsert_identity_map(tmp_vault, "[[jane-smith]]", {"phones": ["+15551234567"]})
    result = resolve_person(
        tmp_vault,
        {"summary": "Jane Smith", "first_name": "Jane", "last_name": "Smith", "phones": ["(555) 123-4567"]},
    )
    assert result.action == "merge"
    assert result.wikilink == "[[jane-smith]]"
    assert "exact_phone" in result.reasons


def test_resolve_person_same_phone_conflicting_names_stays_review(
    tmp_vault, sample_person_card, sample_person_provenance
):
    payload = sample_person_card.model_dump(mode="python")
    payload["phones"] = ["+15551234567"]
    card = PersonCard.model_validate(payload)
    write_card(tmp_vault, "People/jane-smith.md", card, provenance=sample_person_provenance)
    upsert_identity_map(tmp_vault, "[[jane-smith]]", {"phones": ["+15551234567"]})
    result = resolve_person(
        tmp_vault,
        {"summary": "John Doe", "first_name": "John", "last_name": "Doe", "phones": ["+15551234567"]},
    )
    assert result.action == "conflict"
    assert result.wikilink == "[[jane-smith]]"
    assert "name_conflict" in result.reasons
    assert "auto_approved" not in result.reasons


def test_auto_approve_keeps_review_below_merge_threshold():
    held = _auto_approve_if_confident(ResolveResult("conflict", "[[jane-smith]]", 89, ["close_name"]))
    assert held.action == "conflict"
    approved = _auto_approve_if_confident(ResolveResult("conflict", "[[jane-smith]]", 90, ["close_name"]))
    assert approved.action == "merge"
    assert "auto_approved" in approved.reasons


def test_person_index_candidates_need_a_last_name(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    index = PersonIndex(tmp_vault, preload=True)
    assert index.candidates({"phones": ["+15551234567"]}) == []
    assert index.candidates({"first_name": "Jane", "last_name": "Smith"})


def test_resolve_person_name_only_skips_create(tmp_vault):
    result = resolve_person(tmp_vault, {"summary": "Jane Smith", "first_name": "Jane", "last_name": "Smith"})
    assert result.action == "skip"
    assert "no_identifier" in result.reasons


def test_resolve_person_named_with_phone_creates(tmp_vault):
    result = resolve_person(
        tmp_vault,
        {"summary": "Jane Smith", "first_name": "Jane", "last_name": "Smith", "phones": ["+15551234567"]},
    )
    assert result.action == "create"
    assert "no_match" in result.reasons
