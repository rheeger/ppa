"""Archive-sync identity behavior integration tests."""

import json

from archive_sync.adapters.contacts import ContactsAdapter
from archive_sync.adapters.gmail_correspondents import GmailCorrespondentsAdapter
from archive_sync.adapters.linkedin import LinkedInAdapter
from archive_sync.adapters.notion_people import NotionStaffAdapter
from hfa.vault import read_note


def test_contacts_ingest_indexes_all_aliases(tmp_vault):
    adapter = ContactsAdapter()
    adapter.fetch = lambda vault_path, cursor, config=None, **kwargs: [  # type: ignore[method-assign]
        {
            "source": "contacts.apple",
            "name": "Jenny Souza",
            "emails": ["jenny@example.com", "jenny@work.com"],
            "phones": ["123", "456"],
            "company": "Endaoment",
            "title": "Ops",
        }
    ]
    result = adapter.ingest(str(tmp_vault))
    payload = json.loads((tmp_vault / "_meta" / "identity-map.json").read_text(encoding="utf-8"))
    assert result.created == 1
    assert payload["email:jenny@example.com"] == "[[jenny-souza]]"
    assert payload["email:jenny@work.com"] == "[[jenny-souza]]"
    assert payload["phone:456"] == "[[jenny-souza]]"


def test_mixed_source_merge_keeps_identity_map_to_supported_aliases(tmp_vault):
    contacts = ContactsAdapter()
    contacts.fetch = lambda vault_path, cursor, config=None, **kwargs: [  # type: ignore[method-assign]
        {
            "source": "contacts.apple",
            "name": "Jane Smith",
            "emails": ["jane@example.com"],
            "phones": ["123"],
            "linkedin": "https://www.linkedin.com/in/janesmith/",
            "company": "Endaoment",
            "title": "Ops",
        }
    ]
    linkedin = LinkedInAdapter()
    linkedin.fetch = lambda vault_path, cursor, config=None, **kwargs: [  # type: ignore[method-assign]
        {
            "source": "linkedin",
            "name": "Jane Smith",
            "emails": ["jane@example.com", "j.smith@corp.com"],
            "company": "Endaoment",
            "title": "VP",
            "connected_on": "2024-01-01",
        }
    ]

    contacts.ingest(str(tmp_vault), sources=["apple", "vcf"])
    linkedin.ingest(str(tmp_vault))

    payload = json.loads((tmp_vault / "_meta" / "identity-map.json").read_text(encoding="utf-8"))
    assert payload["name:jane smith"] == "[[jane-smith]]"
    assert payload["email:j.smith@corp.com"] == "[[jane-smith]]"
    assert payload["linkedin:janesmith"] == "[[jane-smith]]"
    assert "summary:Jane Smith" not in payload
    assert "company:Endaoment" not in payload
    assert "title:VP" not in payload


def test_linkedin_without_email_merges_on_exact_handle(tmp_vault):
    contacts = ContactsAdapter()
    contacts.fetch = lambda vault_path, cursor, config=None, **kwargs: [  # type: ignore[method-assign]
        {
            "source": "contacts.apple",
            "name": "Jane Smith",
            "first_name": "Jane",
            "last_name": "Smith",
            "phones": ["+1 (650) 799-2364"],
            "linkedin": "https://www.linkedin.com/in/janesmith/",
            "company": "Endaoment",
        }
    ]
    linkedin = LinkedInAdapter()
    linkedin.fetch = lambda vault_path, cursor, config=None, **kwargs: [  # type: ignore[method-assign]
        {
            "source": "linkedin",
            "name": "Jane Smith",
            "first_name": "Jane",
            "last_name": "Smith",
            "linkedin": "janesmith",
            "linkedin_url": "https://www.linkedin.com/in/janesmith",
            "company": "Endaoment Labs",
            "title": "VP Partnerships",
            "connected_on": "2024-01-01",
            "emails": [],
        }
    ]

    contacts_result = contacts.ingest(str(tmp_vault), sources=["apple"])
    linkedin_result = linkedin.ingest(str(tmp_vault))

    assert contacts_result.created == 1
    assert linkedin_result.merged == 1
    frontmatter, body, _ = read_note(tmp_vault, "People/jane-smith.md")
    assert set(frontmatter["source"]) == {"contacts.apple", "linkedin"}
    assert frontmatter["linkedin"] == "janesmith"
    assert frontmatter["company"] == "Endaoment Labs"
    assert "Connected on: 2024-01-01" in body


def test_notion_staff_merge_enriches_existing_contact(tmp_vault):
    contacts = ContactsAdapter()
    contacts.fetch = lambda vault_path, cursor, config=None, **kwargs: [  # type: ignore[method-assign]
        {
            "source": "contacts.apple",
            "name": "Alexis Miller",
            "first_name": "Alexis",
            "last_name": "Miller",
            "emails": ["alexis@endaoment.org"],
            "phones": ["(847) 400-4343"],
        }
    ]
    notion_staff = NotionStaffAdapter()
    notion_staff.fetch = lambda vault_path, cursor, config=None, **kwargs: [  # type: ignore[method-assign]
        {
            "source": "notion.staff",
            "name": "Alexis Miller",
            "first_name": "Alexis",
            "last_name": "Miller",
            "emails": ["alexis@endaoment.org", "alexis@example.com"],
            "company": "Endaoment",
            "companies": ["Endaoment"],
            "title": "Donor Engagement & Strategic Partnerships",
            "titles": ["Donor Engagement & Strategic Partnerships"],
            "pronouns": "her she",
            "reports_to": "Zach Bronstein",
            "linkedin": "https://www.linkedin.com/in/alexis-m-miller/",
            "tags": ["notion", "staff", "full-time"],
            "description": "Leads partnerships",
        }
    ]

    contacts.ingest(str(tmp_vault), sources=["apple"])
    result = notion_staff.ingest(str(tmp_vault), csv_path="staff.csv")

    assert result.merged == 1
    frontmatter, _, _ = read_note(tmp_vault, "People/alexis-miller.md")
    assert set(frontmatter["source"]) == {"contacts.apple", "notion.staff"}
    assert frontmatter["linkedin"] == "alexis-m-miller"
    assert frontmatter["reports_to"] == "Zach Bronstein"
    assert frontmatter["pronouns"] == "her she"


def test_gmail_correspondent_merge_enriches_existing_person(tmp_vault):
    contacts = ContactsAdapter()
    contacts.fetch = lambda vault_path, cursor, config=None, **kwargs: [  # type: ignore[method-assign]
        {
            "source": "contacts.apple",
            "name": "John Smith",
            "first_name": "John",
            "last_name": "Smith",
            "emails": ["john@example.com"],
        }
    ]
    gmail = GmailCorrespondentsAdapter()
    gmail.fetch = lambda vault_path, cursor, config=None, **kwargs: [  # type: ignore[method-assign]
        {"name": "John Smith", "email": "john@example.com", "count": 8}
    ]

    contacts.ingest(str(tmp_vault), sources=["apple"])
    result = gmail.ingest(str(tmp_vault), account_email="me@example.com")

    assert result.merged == 1
    frontmatter, _, provenance = read_note(tmp_vault, "People/john-smith.md")
    assert set(frontmatter["source"]) == {"contacts.apple", "gmail-correspondents"}
    assert frontmatter["emails_seen_count"] == 8
    assert "gmail-correspondent" in frontmatter["tags"]
    assert provenance["emails_seen_count"].source == "gmail-correspondents"
