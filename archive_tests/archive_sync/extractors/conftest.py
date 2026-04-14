"""Shared fixtures for extractor tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_sync.adapters.base import deterministic_provenance
from archive_vault.schema import EmailMessageCard
from archive_vault.vault import write_card


@pytest.fixture
def extractor_vault(tmp_path):
    """Minimal vault layout for extraction / entity-resolution tests."""
    vault = tmp_path / "vault"
    for d in [
        "People",
        "Email",
        "EmailThreads",
        "Transactions/MealOrders",
        "Transactions/Groceries",
        "Transactions/Rides",
        "Transactions/Flights",
        "Transactions/Accommodations",
        "Transactions/CarRentals",
        "Transactions/Purchases",
        "Transactions/Shipments",
        "Transactions/Subscriptions",
        "Transactions/EventTickets",
        "Transactions/Payroll",
        "Medical",
        "Entities/Places",
        "Entities/Organizations",
        "_meta",
        "_templates",
        ".obsidian",
    ]:
        (vault / d).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "dedup-candidates.json").write_text("[]", encoding="utf-8")
    (vault / "_meta" / "enrichment-log.json").write_text("[]", encoding="utf-8")
    (vault / "_meta" / "llm-cache.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "own-emails.json").write_text('["me@example.com"]', encoding="utf-8")
    (vault / "_meta" / "nicknames.json").write_text('{"robert": ["rob", "robbie"]}', encoding="utf-8")
    (vault / "_meta" / "ppa-config.json").write_text('{"finance_min_amount": 20.0}', encoding="utf-8")
    return str(vault)


@pytest.fixture
def sample_email_card():
    """Factory: (frontmatter dict, body str) for a gmail email_message card."""

    def _make(
        uid: str,
        from_email: str,
        subject: str,
        body: str,
        sent_at: str = "2024-03-15T14:30:00-08:00",
    ) -> tuple[dict, str]:
        suffix = uid.split("-")[-1]
        return (
            {
                "uid": uid,
                "type": "email_message",
                "source": ["gmail"],
                "source_id": f"gmail.msg.{suffix}",
                "created": "2024-03-15",
                "updated": "2024-03-15",
                "summary": subject,
                "gmail_message_id": f"msgid-{suffix}",
                "gmail_thread_id": f"thread-{suffix}",
                "account_email": "me@example.com",
                "from_email": from_email,
                "to_emails": ["me@example.com"],
                "subject": subject,
                "sent_at": sent_at,
                "people": [],
                "orgs": [],
                "tags": [],
            },
            body,
        )

    return _make


@pytest.fixture
def email_fixture_dir() -> Path:
    """Path to archive_tests/fixtures/emails/."""
    return Path(__file__).resolve().parent.parent.parent / "fixtures" / "emails"


def load_email_fixture(fixture_dir: Path, extractor_name: str, fixture_name: str) -> tuple[dict, str, dict]:
    """Load an email .md fixture and its .expected.json companion."""
    from archive_vault.vault import read_note_file

    email_path = fixture_dir / extractor_name / f"{fixture_name}.md"
    expected_path = fixture_dir / extractor_name / f"{fixture_name}.expected.json"
    parsed = read_note_file(email_path)
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    return parsed.frontmatter, parsed.body, expected


def write_email_to_vault(vault_path: str, rel_path: str, frontmatter: dict, body: str) -> None:
    """Write an email_message card to the vault for testing."""
    card = EmailMessageCard(**frontmatter)
    prov = deterministic_provenance(card, "gmail")
    write_card(vault_path, rel_path, card, body, prov)
