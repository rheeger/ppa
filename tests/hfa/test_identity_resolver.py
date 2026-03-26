from hfa.identity import upsert_identity_map
from hfa.identity_resolver import merge_into_existing, names_match, resolve_person
from hfa.provenance import ProvenanceEntry
from hfa.schema import PersonCard, validate_card_permissive
from hfa.vault import read_note, write_card


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


def test_resolve_person_fuzzy_name_with_company_support_merges(tmp_vault):
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
    assert result.confidence >= 75


def test_resolve_person_same_name_without_support_creates(tmp_vault):
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
    assert result.action == "create"
    assert result.wikilink is None


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
