from hfa.vault import (extract_wikilinks, iter_note_paths, iter_notes,
                       iter_parsed_notes, parse_note_content, read_note,
                       read_note_by_uid, read_note_frontmatter_file,
                       write_card)


def test_write_card_and_read_back(tmp_vault, sample_person_card, sample_person_provenance):
    path = write_card(tmp_vault, "People/jane-smith.md", sample_person_card, body="hello", provenance=sample_person_provenance)
    assert path.exists()

    frontmatter, body, provenance = read_note(tmp_vault, "People/jane-smith.md")
    assert frontmatter["summary"] == "Jane Smith"
    assert body == "hello"
    assert provenance["summary"].source == "contacts.apple"


def test_write_card_derives_alias_provenance_from_summary(tmp_vault, sample_person_card, sample_person_provenance):
    sample_person_card.aliases = ["Jane Alexandra Smith"]
    sample_person_provenance = {key: value for key, value in sample_person_provenance.items() if key != "aliases"}
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    _, _, provenance = read_note(tmp_vault, "People/jane-smith.md")
    assert provenance["aliases"].source == "contacts.apple"


def test_write_card_derives_linkedin_url_provenance_from_linkedin(tmp_vault, sample_person_card, sample_person_provenance):
    sample_person_provenance = {key: value for key, value in sample_person_provenance.items() if key != "linkedin_url"}
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    frontmatter, _, provenance = read_note(tmp_vault, "People/jane-smith.md")
    assert frontmatter["linkedin"] == "janesmith"
    assert frontmatter["linkedin_url"] == "https://www.linkedin.com/in/janesmith"
    assert provenance["linkedin"].source == "contacts.apple"
    assert provenance["linkedin_url"].source == "contacts.apple"


def test_read_note_by_uid(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    match = read_note_by_uid(tmp_vault, sample_person_card.uid)
    assert match is not None
    assert str(match[0]) == "People/jane-smith.md"


def test_iter_notes_skips_excluded_dirs(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    (tmp_vault / "_meta" / "ignored.md").write_text("skip", encoding="utf-8")
    rel_paths = [str(path) for path, _ in iter_notes(tmp_vault)]
    assert rel_paths == ["People/jane-smith.md"]


def test_iter_note_paths_and_iter_parsed_notes_use_single_visible_note(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, body="hello", provenance=sample_person_provenance)
    (tmp_vault / "_meta" / "ignored.md").write_text("skip", encoding="utf-8")

    rel_paths = [str(path) for path in iter_note_paths(tmp_vault)]
    parsed_notes = list(iter_parsed_notes(tmp_vault))

    assert rel_paths == ["People/jane-smith.md"]
    assert [str(note.rel_path) for note in parsed_notes] == ["People/jane-smith.md"]
    assert parsed_notes[0].body == "hello"
    assert parsed_notes[0].frontmatter["summary"] == "Jane Smith"


def test_parse_note_content_extracts_provenance(sample_person_provenance):
    content = """---
uid: hfa-person-test
type: person
source: [contacts.apple]
summary: Jane Smith
---

hello
<!-- provenance
summary: {"source":"contacts.apple","date":"2026-03-10","method":"deterministic"}
-->
"""
    frontmatter, body, provenance = parse_note_content(content)

    assert frontmatter["summary"] == "Jane Smith"
    assert body == "hello"
    assert provenance["summary"].source == "contacts.apple"


def test_read_note_frontmatter_file_only_reads_frontmatter(tmp_vault, sample_person_card, sample_person_provenance):
    path = write_card(tmp_vault, "People/jane-smith.md", sample_person_card, body="hello", provenance=sample_person_provenance)

    note = read_note_frontmatter_file(path, vault_root=tmp_vault)

    assert str(note.rel_path) == "People/jane-smith.md"
    assert note.frontmatter["summary"] == "Jane Smith"


def test_extract_wikilinks_supports_aliases():
    links = extract_wikilinks("hello [[jane-smith]] and [[endaoment|Endaoment]]")
    assert links == ["jane-smith", "endaoment"]
