from archive_sync.adapters.contacts import ContactsAdapter
from archive_vault.canon import email, handle, phone, place, slug, wikilink
from archive_vault.schema import PersonCard


def test_phone_forms_are_equivalent() -> None:
    expected = "+19147153533"
    assert phone.canonical("+19147153533") == expected
    assert phone.canonical("19147153533") == expected
    assert phone.canonical("9147153533") == expected
    assert phone.canonical("(914) 715-3533") == expected
    assert set(phone.alias_forms("(914) 715-3533")) >= {expected, "19147153533", "9147153533"}


def test_person_card_phones_are_e164() -> None:
    card = PersonCard(
        uid="hfa-person-canonphone01",
        source=["contacts.apple"],
        source_id="canon-phone",
        created="2024-01-01",
        updated="2024-01-01",
        summary="Sam",
        phones=["(914) 715-3533", "+1-914-715-3533"],
    )
    assert card.phones == ["+19147153533"]


def test_email_does_not_fold_gmail_dots() -> None:
    assert email.canonical("  rheeger+tag@Gmail.COM ") == "rheeger+tag@gmail.com"
    assert email.canonical("r.heeger@gmail.com") == "r.heeger@gmail.com"


def test_linkedin_url_and_handle_match() -> None:
    assert handle.canonical("https://www.linkedin.com/in/JaneSmith/", provider="linkedin") == "janesmith"
    assert handle.canonical("@JaneSmith", provider="linkedin") == "janesmith"
    handle_value, url = handle.linkedin_fields("", "https://linkedin.com/in/JaneSmith")
    assert handle_value == "janesmith"
    assert "linkedin.com/in/janesmith" in url.lower() or url.endswith("/JaneSmith")


def test_slug_drops_punctuation() -> None:
    assert slug.canonical("Sam Panken!") == "sam-panken"


def test_wikilink_round_trip() -> None:
    assert wikilink.person_ref("sam-panken") == "[[sam-panken]]"
    assert wikilink.parse("[[hfa-person-54fc3b19aeda]]") == "hfa-person-54fc3b19aeda"
    assert wikilink.uid_ref("hfa-person-54fc3b19aeda") == "[[hfa-person-54fc3b19aeda]]"


def test_contacts_to_card_canonicalizes_dirty_phone_and_email() -> None:
    card, _provenance, _body = ContactsAdapter().to_card(
        {
            "source": "contacts.apple",
            "name": "Jenny Souza",
            "first_name": "Jenny",
            "last_name": "Souza",
            "emails": ["  Jenny@Example.COM "],
            "phones": ["+1-617-555-1212"],
        }
    )
    assert card.emails == ["jenny@example.com"]
    assert card.phones == ["+16175551212"]


def test_merchant_bank_profile_strips_square() -> None:
    assert place.canonical("SQ *BLUE BOTTLE", profile="merchant_bank") == "blue bottle"
    assert place.merchants_match("SQ *BLUE BOTTLE", "Blue Bottle Coffee")
