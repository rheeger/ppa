"""Archive-sync Notion people adapter tests."""

from archive_sync.adapters.notion_people import NotionPeopleAdapter
from hfa.schema import PersonCard


def test_fetch_parses_tags_and_column_fallbacks(tmp_path):
    csv_path = tmp_path / "notion.csv"
    csv_path.write_text(
        'First Name,Last Name,Email Address,Organization,Role,Tags\nJane,Smith,jane@example.com,Endaoment,VP,"friend, donor"\n'
    )
    items = NotionPeopleAdapter().fetch("/dev/null", {}, csv_path=str(csv_path))
    assert items[0]["name"] == "Jane Smith"
    assert items[0]["company"] == "Endaoment"
    assert items[0]["tags"][0] == "notion"


def test_to_card_returns_valid_person():
    card, provenance, _ = NotionPeopleAdapter().to_card(
        {
            "name": "Jane Smith",
            "emails": ["jane@example.com"],
            "company": "Endaoment",
            "title": "VP",
            "tags": ["notion", "friend"],
        }
    )
    assert isinstance(card, PersonCard)
    assert card.tags == ["notion", "friend"]
    assert card.companies == ["Endaoment"]
    assert card.titles == ["VP"]
    assert provenance["title"].source == "notion"


def test_fetch_parses_real_people_export_shape(tmp_path):
    csv_path = tmp_path / "people.csv"
    csv_path.write_text(
        "Full Name,Birthday,Calendly,Company,Contact Type,Description,Discord,Email,Org,Phone,Status,Tags,Telegram,Title,Twitter,Website,LinkedIn\n"
        "Amy Fass,,,,Community Member,Example desc,,amy@shoesthatfit.org,Shoes that Fit (https://www.notion.so/foo),(123) 456-7890,Active,\"friend-of-ndao, nonprofit\",amyfit,CEO,https://twitter.com/amyfit,https://shoesthatfit.org,https://www.linkedin.com/in/amy-fass/\n",
        encoding="utf-8",
    )
    items = NotionPeopleAdapter().fetch("/dev/null", {}, csv_path=str(csv_path))
    assert items[0]["name"] == "Amy Fass"
    assert items[0]["company"] == "Shoes that Fit"
    assert items[0]["phones"] == ["(123) 456-7890"]
    assert items[0]["telegram"] == "amyfit"
    assert items[0]["relationship_type"] == "community-member"
    assert "friend-of-ndao" in items[0]["tags"]
