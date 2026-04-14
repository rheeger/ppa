"""Archive-sync Notion staff adapter tests."""

from archive_sync.adapters.notion_people import NotionStaffAdapter
from archive_vault.schema import PersonCard


def test_fetch_parses_staff_export_shape(tmp_path):
    csv_path = tmp_path / "staff.csv"
    csv_path.write_text(
        "Name,Company email,Personal Email,Primary Phone number,LinkedIn,PR Bio,ProtonMail,Reports to,Role,Teams,Pronouns,Twittter,Instagram,Type,No Longer Employed\n"
        "Alexis Miller,alexis@endaoment.org,alexis@example.com,(847) 400-4343,https://www.linkedin.com/in/alexis-m-miller/,Leads partnerships,alexis@proton.me,Zach Bronstein,Donor Engagement,.Org (https://www.notion.so/org),her she,@alexismm61,https://www.instagram.com/alexismm/,Full time,No\n",
        encoding="utf-8",
    )
    items = NotionStaffAdapter().fetch("/dev/null", {}, csv_path=str(csv_path))
    assert items[0]["source"] == "notion.staff"
    assert items[0]["company"] == "Endaoment"
    assert items[0]["emails"] == ["alexis@endaoment.org", "alexis@example.com", "alexis@proton.me"]
    assert items[0]["pronouns"] == "her she"
    assert "staff" in items[0]["tags"]
    assert "full-time" in items[0]["tags"]


def test_to_card_returns_valid_staff_person():
    card, provenance, _ = NotionStaffAdapter().to_card(
        {
            "source": "notion.staff",
            "name": "Alexis Miller",
            "first_name": "Alexis",
            "last_name": "Miller",
            "emails": ["alexis@endaoment.org", "alexis@example.com"],
            "phones": ["(847) 400-4343"],
            "company": "Endaoment",
            "companies": ["Endaoment"],
            "title": "Donor Engagement",
            "titles": ["Donor Engagement"],
            "linkedin": "https://www.linkedin.com/in/alexis-m-miller/",
            "twitter": "@alexismm61",
            "instagram": "https://www.instagram.com/alexismm/",
            "pronouns": "her, she",
            "reports_to": "Zach Bronstein",
            "tags": ["notion", "staff", "full-time"],
            "description": "Leads partnerships",
        }
    )
    assert isinstance(card, PersonCard)
    assert card.source == ["notion.staff"]
    assert card.linkedin == "alexis-m-miller"
    assert card.twitter == "alexismm61"
    assert card.instagram == "alexismm"
    assert card.reports_to == "Zach Bronstein"
    assert provenance["reports_to"].source == "notion.staff"
