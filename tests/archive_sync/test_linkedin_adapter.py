"""Archive-sync LinkedIn adapter tests."""

from archive_sync.adapters.linkedin import LinkedInAdapter
from hfa.schema import PersonCard


def test_fetch_handles_alternate_columns(tmp_path):
    csv_path = tmp_path / "linkedin.csv"
    csv_path.write_text(
        "FirstName,LastName,URL,Email,Company,Title,ConnectedOn\n"
        "Jane,Smith,https://www.linkedin.com/in/janesmith,jane@example.com,Endaoment,VP,08 Dec 2023\n"
    )
    items = LinkedInAdapter().fetch("/dev/null", {}, csv_path=str(csv_path))
    assert items[0]["name"] == "Jane Smith"
    assert items[0]["emails"] == ["jane@example.com"]
    assert items[0]["linkedin"] == "janesmith"
    assert items[0]["connected_on"] == "2023-12-08"


def test_fetch_skips_linkedin_export_notes_preamble(tmp_path):
    csv_path = tmp_path / "linkedin.csv"
    csv_path.write_text(
        "Notes:\n"
        '"When exporting your connection data..."\n\n'
        "First Name,Last Name,URL,Email Address,Company,Position,Connected On\n"
        "Jane,Smith,https://www.linkedin.com/in/janesmith,,Endaoment,VP,08 Dec 2023\n",
        encoding="utf-8",
    )
    items = LinkedInAdapter().fetch("/dev/null", {}, csv_path=str(csv_path))
    assert items[0]["name"] == "Jane Smith"
    assert items[0]["linkedin"] == "janesmith"


def test_fetch_from_export_directory_includes_profile_enrichment(tmp_path):
    (tmp_path / "Connections.csv").write_text(
        "First Name,Last Name,URL,Email Address,Company,Position,Connected On\n"
        "Jane,Smith,https://www.linkedin.com/in/janesmith,jane@example.com,Endaoment,VP,08 Dec 2023\n",
        encoding="utf-8",
    )
    (tmp_path / "Profile.csv").write_text(
        "First Name,Last Name,Maiden Name,Address,Birth Date,Headline,Summary,Industry,Zip Code,Geo Location,Twitter Handles,Websites,Instant Messengers\n"
        'Robbie,Heeger,,,"Nov 5, 1990",President & CEO at Endaoment,,Philanthropic Fundraising Services,11205,"Brooklyn, New York, United States",[RobbieHeeger],[https://endaoment.org],[GTALK:rheeger]\n',
        encoding="utf-8",
    )
    (tmp_path / "Email Addresses.csv").write_text(
        "Email Address,Confirmed,Primary,Updated On\nrheeger@gmail.com,Yes,Yes,Not Available\n",
        encoding="utf-8",
    )
    (tmp_path / "PhoneNumbers.csv").write_text(
        "Extension,Number,Type\n,6507992364,Mobile\n",
        encoding="utf-8",
    )
    (tmp_path / "Whatsapp Phone Numbers.csv").write_text(
        "Number,Extension,Is_WhatsApp_Number\n16507992364,,false\n",
        encoding="utf-8",
    )
    (tmp_path / "Positions.csv").write_text(
        "Company Name,Title,Description,Location,Started On,Finished On\n"
        "Endaoment,President & CEO,,San Francisco,Mar 2019,\n"
        "Apple,Manager - Production Operations,,Cupertino,Mar 2016,Nov 2018\n",
        encoding="utf-8",
    )
    (tmp_path / "Education.csv").write_text(
        "School Name,Start Date,End Date,Notes,Degree Name,Activities\n"
        "University of Southern California,2008,2012,,B.A.,\n",
        encoding="utf-8",
    )
    (tmp_path / "Invitations.csv").write_text(
        "From,To,Sent At,Message,Direction,inviterProfileUrl,inviteeProfileUrl\n"
        'Robbie Heeger,Tira Grey,"2/10/26, 9:23 AM",,OUTGOING,https://www.linkedin.com/in/rheeger,https://www.linkedin.com/in/tira-grey-5179ab3\n',
        encoding="utf-8",
    )
    verification_dir = tmp_path / "Verifications"
    verification_dir.mkdir()
    (verification_dir / "Verifications.csv").write_text(
        "First name,Middle name,Last name,Verification type,Organization name,Email address,Country,State,City,Year of birth,Issuing authority,Document type,Verification service provider,Verified date,Expiry date\n"
        "ROBERT,EVAN,HEEGER,ID verification,N/A,N/A,N/A,N/A,N/A,0,United States,PASSPORT,Clear,2023-10-31,N/A\n",
        encoding="utf-8",
    )

    items = LinkedInAdapter().fetch("/dev/null", {}, csv_path=str(tmp_path))

    assert len(items) == 2
    profile_item = next(item for item in items if item["name"] == "Robbie Heeger")
    assert profile_item["linkedin"] == "rheeger"
    assert profile_item["emails"] == ["rheeger@gmail.com"]
    assert profile_item["phones"] == ["+16507992364"]
    assert profile_item["birthday"] == "1990-11-05"
    assert profile_item["company"] == "Endaoment"
    assert profile_item["title"] == "President & CEO"
    assert profile_item["twitter"] == "RobbieHeeger"
    assert profile_item["websites"] == ["https://endaoment.org"]
    assert "LinkedIn positions" in profile_item["profile_body"]
    assert "LinkedIn verifications" in profile_item["profile_body"]


def test_to_card_returns_valid_person():
    card, provenance, body = LinkedInAdapter().to_card(
        {
            "name": "Jane Smith",
            "first_name": "Jane",
            "last_name": "Smith",
            "emails": ["jane@example.com"],
            "linkedin": "janesmith",
            "linkedin_url": "https://www.linkedin.com/in/janesmith",
            "company": "Endaoment",
            "title": "VP",
            "connected_on": "2024-01-01",
        }
    )
    assert isinstance(card, PersonCard)
    assert card.tags == ["linkedin"]
    assert card.linkedin == "janesmith"
    assert card.linkedin_url == "https://www.linkedin.com/in/janesmith"
    assert card.linkedin_connected_on == "2024-01-01"
    assert card.source_id == "janesmith"
    assert provenance["tags"].source == "linkedin"
    assert "Connected on" in body


def test_to_card_maps_profile_fields():
    card, provenance, body = LinkedInAdapter().to_card(
        {
            "name": "Robbie Heeger",
            "first_name": "Robbie",
            "last_name": "Heeger",
            "emails": ["rheeger@gmail.com"],
            "phones": ["+16507992364"],
            "birthday": "1990-11-05",
            "linkedin": "rheeger",
            "linkedin_url": "https://www.linkedin.com/in/rheeger",
            "company": "Endaoment",
            "companies": ["Endaoment", "Apple"],
            "title": "President & CEO",
            "titles": ["President & CEO", "Manager - Production Operations"],
            "twitter": "RobbieHeeger",
            "websites": ["https://endaoment.org"],
            "description": "President & CEO at Endaoment",
            "profile_body": "LinkedIn positions:\n- Endaoment - President & CEO",
        }
    )

    assert isinstance(card, PersonCard)
    assert card.linkedin == "rheeger"
    assert card.phones == ["+16507992364"]
    assert card.birthday == "1990-11-05"
    assert card.companies == ["Endaoment", "Apple"]
    assert card.titles == ["President & CEO", "Manager - Production Operations"]
    assert card.twitter == "robbieheeger"
    assert card.websites == ["https://endaoment.org"]
    assert card.description == "President & CEO at Endaoment"
    assert provenance["phones"].source == "linkedin"
    assert body == "LinkedIn positions:\n- Endaoment - President & CEO"


def test_linkedin_adapter_skips_existing_uid_preload():
    assert LinkedInAdapter.preload_existing_uid_index is False
