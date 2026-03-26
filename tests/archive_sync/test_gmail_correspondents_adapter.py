"""Archive-sync gmail correspondent adapter tests."""

from archive_sync.adapters.gmail_correspondents import (
    GmailCorrespondentsAdapter,
    _extract_addresses_from_headers,
    _should_keep_correspondent,
)
from hfa.schema import PersonCard


def test_extract_addresses_from_headers_parses_multiple_fields():
    headers = [
        {"name": "From", "value": "Alice Example <alice@example.com>"},
        {"name": "To", "value": "Bob <bob@example.com>, carol@example.org"},
        {"name": "Cc", "value": '"Dan D" <dan@example.net>'},
        {"name": "Subject", "value": "ignored"},
    ]
    pairs = _extract_addresses_from_headers(headers)
    assert ("Alice Example", "alice@example.com") in pairs
    assert ("Bob", "bob@example.com") in pairs
    assert ("", "carol@example.org") in pairs
    assert ("Dan D", "dan@example.net") in pairs


def test_to_card_returns_correspondent_person():
    card, provenance, _ = GmailCorrespondentsAdapter().to_card(
        {"name": "John Smith", "email": "john@example.com", "count": 8}
    )
    assert isinstance(card, PersonCard)
    assert card.first_name == "John"
    assert card.last_name == "Smith"
    assert card.tags == ["email-correspondent", "gmail-correspondent"]
    assert card.emails_seen_count == 8
    assert provenance["emails_seen_count"].method == "deterministic"


def test_should_keep_correspondent_filters_automated_senders():
    assert _should_keep_correspondent("Marty Messinger", "mmessinger@nb.com") is True
    assert _should_keep_correspondent("Taylor Kimmett", "notifications@github.com") is False
    assert _should_keep_correspondent("American Express", "americanexpress@welcome.americanexpress.com") is False
    assert _should_keep_correspondent("Baruch Piller", "reply-abc123@reply.linkedin.com") is False


def test_fetch_max_messages_is_per_run_not_cumulative(tmp_vault):
    adapter = GmailCorrespondentsAdapter()
    responses = iter(
        [
            {
                "messages": [{"id": "m1"}],
                "nextPageToken": "page-2",
            },
            {
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Marty Messinger <mmessinger@nb.com>"},
                    ]
                }
            },
            {
                "messages": [{"id": "m2"}],
                "nextPageToken": None,
            },
            {
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Navin Ram <navin_ram@yahoo.com>"},
                    ]
                }
            },
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]
    first = adapter.fetch(
        str(tmp_vault), {"page_token": None, "scanned_messages": 0}, account_email="me@example.com", max_messages=1
    )
    second = adapter.fetch(
        str(tmp_vault),
        {"page_token": "page-2", "scanned_messages": 1},
        account_email="me@example.com",
        max_messages=1,
    )
    assert first[0]["email"] == "mmessinger@nb.com"
    assert first[0]["scanned_messages"] == 1
    assert second[0]["email"] == "navin_ram@yahoo.com"
    assert second[0]["scanned_messages"] == 2


def test_fetch_from_local_messages_filters_by_account_email(tmp_vault):
    email_root = tmp_vault / "Email" / "2026-03"
    email_root.mkdir(parents=True, exist_ok=True)
    (email_root / "one.md").write_text(
        """---
type: email_message
account_email: one@example.com
from_name: Alice Example
from_email: alice@example.com
to_emails:
  - one@example.com
reply_to_emails: []
---
""",
        encoding="utf-8",
    )
    (email_root / "two.md").write_text(
        """---
type: email_message
account_email: two@example.com
from_name: Bob Example
from_email: bob@example.com
to_emails:
  - two@example.com
reply_to_emails: []
---
""",
        encoding="utf-8",
    )

    adapter = GmailCorrespondentsAdapter()
    items = adapter.fetch(str(tmp_vault), {}, account_email="one@example.com")

    assert [item["email"] for item in items] == ["alice@example.com"]
    assert items[0]["count"] == 1
