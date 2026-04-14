"""Archive doctor tests."""

from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest

from archive_doctor.handler import cmd_dedup_sweep, cmd_purge_source, cmd_stats, cmd_validate
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import EmailMessageCard, EmailThreadCard, FinanceCard, PersonCard
from archive_vault.sync_state import save_sync_state
from archive_vault.vault import read_note, write_card


@pytest.fixture
def tmp_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
    (vault / "Photos").mkdir()
    (vault / "Attachments").mkdir()
    (vault / "_templates").mkdir()
    (vault / ".obsidian").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    (meta / "dedup-candidates.json").write_text("[]", encoding="utf-8")
    (meta / "enrichment-log.json").write_text("[]", encoding="utf-8")
    (meta / "llm-cache.json").write_text("{}", encoding="utf-8")
    (meta / "nicknames.json").write_text(json.dumps({"robert": ["rob", "robbie"]}), encoding="utf-8")
    return vault


def person_provenance(source: str) -> dict[str, ProvenanceEntry]:
    return {
        "summary": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "emails": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "company": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "tags": ProvenanceEntry(source, "2026-03-06", "deterministic"),
    }


def finance_provenance(source: str) -> dict[str, ProvenanceEntry]:
    return {
        "summary": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "tags": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "amount": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "currency": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "counterparty": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "category": ProvenanceEntry(source, "2026-03-06", "deterministic"),
    }


def email_message_provenance(source: str) -> dict[str, ProvenanceEntry]:
    return {
        "summary": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "gmail_message_id": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "gmail_thread_id": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "thread": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "account_email": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "from_email": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "to_emails": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "participant_emails": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "subject": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "snippet": ProvenanceEntry(source, "2026-03-08", "deterministic"),
    }


def email_thread_provenance(source: str) -> dict[str, ProvenanceEntry]:
    return {
        "summary": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "gmail_thread_id": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "gmail_history_id": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "account_email": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "subject": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "messages": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "message_count": ProvenanceEntry(source, "2026-03-08", "deterministic"),
        "thread_body_sha": ProvenanceEntry(source, "2026-03-08", "deterministic"),
    }


def test_dedup_sweep_merges_exact_duplicates(tmp_vault):
    jane_contacts = PersonCard(
        uid="hfa-person-aaaabbbbcccc",
        type="person",
        source=["contacts.apple"],
        source_id="jane@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
        emails=["jane@example.com"],
        company="Endaoment",
    )
    jane_linkedin = PersonCard(
        uid="hfa-person-ddddeeeeffff",
        type="person",
        source=["linkedin"],
        source_id="jane@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
        emails=["jane@example.com", "j.smith@corp.com"],
        company="Endaoment",
        tags=["linkedin"],
    )
    write_card(tmp_vault, "People/jane-smith.md", jane_contacts, provenance=person_provenance("contacts.apple"))
    write_card(tmp_vault, "People/jane-smith-alt.md", jane_linkedin, provenance=person_provenance("linkedin"))

    cmd_dedup_sweep(Namespace(vault=str(tmp_vault)))

    people_files = sorted(path.name for path in (tmp_vault / "People").glob("*.md"))
    assert len(people_files) == 1
    frontmatter, _, _ = read_note(tmp_vault, f"People/{people_files[0]}")
    assert set(frontmatter["source"]) == {"contacts.apple", "linkedin"}
    assert frontmatter["emails"] == ["jane@example.com", "j.smith@corp.com"]


def test_validate_writes_report_and_flags_bad_cards(tmp_vault):
    person = PersonCard(
        uid="hfa-person-aaaabbbbcccc",
        type="person",
        source=["contacts.apple"],
        source_id="jane@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
        emails=["jane@example.com"],
        company="Endaoment",
    )
    write_card(tmp_vault, "People/jane-smith.md", person, provenance=person_provenance("contacts.apple"))
    (tmp_vault / "People" / "bad-person.md").write_text(
        "---\nuid: hfa-person-badbadbadbad\ntype: person\nsource: [contacts.apple]\nsource_id: bad@example.com\ncreated: 2026-03-06\nupdated: 2026-03-06\nsummary: Bad Person\nemails: [bad@example.com]\n---\n",
        encoding="utf-8",
    )

    cmd_validate(Namespace(vault=str(tmp_vault)))

    report = json.loads((tmp_vault / "_meta" / "validation-report.json").read_text(encoding="utf-8"))
    assert report["total_cards"] == 2
    assert report["valid"] == 1
    assert len(report["errors"]) == 1


def test_validate_flags_orphaned_frontmatter_wikilinks(tmp_vault):
    message = EmailMessageCard(
        uid="hfa-email-message-aaaabbbbcccc",
        type="email_message",
        source=["gmail.message"],
        source_id="message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Broken link message",
        gmail_message_id="message-1",
        gmail_thread_id="thread-1",
        thread="[[missing-thread]]",
        account_email="me@example.com",
        from_email="alice@example.com",
        to_emails=["me@example.com"],
        participant_emails=["alice@example.com", "me@example.com"],
        subject="Broken link message",
        snippet="broken",
    )
    write_card(
        tmp_vault,
        "Email/2026-03/hfa-email-message-aaaabbbbcccc.md",
        message,
        provenance=email_message_provenance("gmail.message"),
    )

    cmd_validate(Namespace(vault=str(tmp_vault)))

    report = json.loads((tmp_vault / "_meta" / "validation-report.json").read_text(encoding="utf-8"))
    assert report["valid"] == 0
    assert any("orphaned wikilink" in entry["error"] for entry in report["errors"])


def test_stats_prints_counts(tmp_vault, capsys):
    person = PersonCard(
        uid="hfa-person-aaaabbbbcccc",
        type="person",
        source=["contacts.apple"],
        source_id="jane@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
        emails=["jane@example.com"],
        company="Endaoment",
    )
    finance = FinanceCard(
        uid="hfa-finance-111122223333",
        type="finance",
        source=["copilot"],
        source_id="2026-03-01:Flight:-120.0",
        created="2026-03-01",
        updated="2026-03-06",
        summary="Flight -120.00",
        tags=["copilot", "transaction", "travel"],
        amount=-120.0,
        currency="USD",
        counterparty="Flight",
        category="Travel",
    )
    write_card(tmp_vault, "People/jane-smith.md", person, provenance=person_provenance("contacts.apple"))
    write_card(
        tmp_vault, "Finance/2026-03/hfa-finance-111122223333.md", finance, provenance=finance_provenance("copilot")
    )

    cmd_stats(Namespace(vault=str(tmp_vault)))
    output = capsys.readouterr().out
    assert "Total notes: 2" in output
    assert "person: 1" in output
    assert "finance: 1" in output


def test_stats_counts_orphaned_thread_and_message_links(tmp_vault, capsys):
    message = EmailMessageCard(
        uid="hfa-email-message-111122223333",
        type="email_message",
        source=["gmail.message"],
        source_id="message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Broken link message",
        gmail_message_id="message-1",
        gmail_thread_id="thread-1",
        thread="[[missing-thread]]",
        account_email="me@example.com",
        from_email="alice@example.com",
        to_emails=["me@example.com"],
        participant_emails=["alice@example.com", "me@example.com"],
        subject="Broken link message",
        snippet="broken",
        attachments=["[[missing-attachment]]"],
    )
    write_card(
        tmp_vault,
        "Email/2026-03/hfa-email-message-111122223333.md",
        message,
        provenance={
            **email_message_provenance("gmail.message"),
            "attachments": ProvenanceEntry("gmail.message", "2026-03-08", "deterministic"),
        },
    )

    cmd_stats(Namespace(vault=str(tmp_vault)))
    output = capsys.readouterr().out
    assert "Orphaned wikilinks: 2" in output


def test_stats_prints_hash_coverage_and_quick_update_counters(tmp_vault, capsys):
    thread = EmailThreadCard(
        uid="hfa-email-thread-aaaabbbbcccc",
        type="email_thread",
        source=["gmail.thread"],
        source_id="thread-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Project thread",
        gmail_thread_id="thread-1",
        gmail_history_id="history-1",
        account_email="me@example.com",
        subject="Project thread",
        messages=["[[hfa-email-message-aaaabbbbcccc]]"],
        thread_body_sha="sha-thread-1",
    )
    message = EmailMessageCard(
        uid="hfa-email-message-aaaabbbbcccc",
        type="email_message",
        source=["gmail.message"],
        source_id="message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Project message",
        gmail_message_id="message-1",
        gmail_thread_id="thread-1",
        thread="[[hfa-email-thread-aaaabbbbcccc]]",
        account_email="me@example.com",
        from_email="alice@example.com",
        to_emails=["me@example.com"],
        participant_emails=["alice@example.com", "me@example.com"],
        subject="Project message",
        snippet="hello",
        message_body_sha="sha-message-1",
    )
    write_card(
        tmp_vault,
        "EmailThreads/2026-03/hfa-email-thread-aaaabbbbcccc.md",
        thread,
        provenance=email_thread_provenance("gmail.thread"),
    )
    write_card(
        tmp_vault,
        "Email/2026-03/hfa-email-message-aaaabbbbcccc.md",
        message,
        provenance={
            **email_message_provenance("gmail.message"),
            "message_body_sha": ProvenanceEntry("gmail.message", "2026-03-08", "deterministic"),
        },
    )
    save_sync_state(
        tmp_vault,
        {
            "gmail-messages:me@example.com": {
                "skipped": 3,
                "skip_details": {
                    "skipped_unchanged_threads": 2,
                    "skipped_unchanged_messages": 1,
                },
            }
        },
    )

    cmd_stats(Namespace(vault=str(tmp_vault)))
    output = capsys.readouterr().out
    assert "Hash coverage:" in output
    assert "email_thread.thread_body_sha: 1/1" in output
    assert "email_message.message_body_sha: 1/1" in output
    assert "Quick update totals:" in output
    assert "skipped_unchanged_threads: 2" in output
    assert "Quick update by source:" in output
    assert "gmail-messages:me@example.com: skipped_unchanged_messages=1, skipped_unchanged_threads=2" in output


def test_purge_source_removes_cards_and_sync_state(tmp_vault):
    person = PersonCard(
        uid="hfa-person-aaaabbbbcccc",
        type="person",
        source=["linkedin"],
        source_id="jane@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
        emails=["jane@example.com"],
        company="Endaoment",
    )
    write_card(tmp_vault, "People/jane-smith.md", person, provenance=person_provenance("linkedin"))
    save_sync_state(tmp_vault, {"linkedin": {"page": 2}, "contacts.apple": {"seen": 1}})

    cmd_purge_source(Namespace(vault=str(tmp_vault), source="linkedin"))

    assert not (tmp_vault / "People" / "jane-smith.md").exists()
    state = json.loads((tmp_vault / "_meta" / "sync-state.json").read_text(encoding="utf-8"))
    assert "linkedin" not in state
    assert "contacts.apple" in state
