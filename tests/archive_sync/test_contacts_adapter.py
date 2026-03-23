"""Archive-sync contacts adapter tests."""

from __future__ import annotations

import os
import sys
import types

from archive_sync.adapters.contacts import ContactsAdapter
from hfa.schema import PersonCard


def test_parse_vcf_extracts_multivalue_fields(tmp_path):
    vcf = tmp_path / "sample.vcf"
    vcf.write_text(
        "\n".join(
            [
                "BEGIN:VCARD",
                "VERSION:3.0",
                "N:Souza;Jenny;;;",
                "FN:Jenny Souza",
                "EMAIL;TYPE=HOME:jenny@example.com",
                "EMAIL;TYPE=WORK:jenny@work.com",
                "TEL;TYPE=CELL:+1-617-555-1212",
                "ORG:Endaoment",
                "TITLE:Operations",
                "BDAY:1990-01-01",
                "X-SOCIALPROFILE;TYPE=linkedin:https://www.linkedin.com/in/jennysouza/",
                "X-SOCIALPROFILE;TYPE=github:https://github.com/jennysouza",
                "END:VCARD",
            ]
        ),
        encoding="utf-8",
    )
    rows = ContactsAdapter()._parse_vcf(str(vcf))
    assert rows[0]["emails"] == ["jenny@example.com", "jenny@work.com"]
    assert rows[0]["phones"] == ["+1-617-555-1212"]
    assert rows[0]["company"] == "Endaoment"
    assert rows[0]["first_name"] == "Jenny"
    assert rows[0]["last_name"] == "Souza"


def test_parse_vcf_skips_company_cards(tmp_path):
    vcf = tmp_path / "company.vcf"
    vcf.write_text(
        "\n".join(
            [
                "BEGIN:VCARD",
                "VERSION:3.0",
                "FN:Riad Mokhtar",
                "ORG:Riad Mokhtar;",
                "TEL;TYPE=HOME:+212 664-990269",
                "X-ABShowAs:COMPANY",
                "END:VCARD",
            ]
        ),
        encoding="utf-8",
    )
    rows = ContactsAdapter()._parse_vcf(str(vcf))
    assert rows == []


def test_to_card_returns_valid_person():
    card, provenance, body = ContactsAdapter().to_card(
        {
            "source": "contacts.apple",
            "name": "Jenny Souza",
            "emails": ["jenny@example.com", "jenny@work.com"],
            "phones": ["123", "456"],
            "company": "Endaoment",
            "title": "Ops",
            "birthday": "1990-01-01",
            "linkedin": "janesmith",
        }
    )
    assert isinstance(card, PersonCard)
    assert card.emails == ["jenny@example.com", "jenny@work.com"]
    assert card.phones == ["123", "456"]
    assert card.companies == ["Endaoment"]
    assert card.titles == ["Ops"]
    assert provenance["emails"].method == "deterministic"
    assert body == ""


def test_cursor_key_normalizes_apple_and_vcf_to_contacts_apple():
    adapter = ContactsAdapter()
    assert adapter.get_cursor_key(sources=["apple"]) == "contacts.apple"
    assert adapter.get_cursor_key(sources=["vcf"]) == "contacts.apple"
    assert adapter.get_cursor_key(sources=["apple", "vcf"]) == "contacts.apple"
    assert adapter.get_cursor_key(sources=["google"]) == "contacts.google"


def test_google_fields_extracts_richer_profile_data():
    item = ContactsAdapter()._google_fields(
        {
            "names": [{"displayName": "Jane Smith", "givenName": "Jane", "familyName": "Smith"}],
            "emailAddresses": [{"value": "Jane@example.com"}],
            "phoneNumbers": [{"value": "+1-555-0100"}],
            "organizations": [{"name": "Endaoment", "title": "VP Partnerships"}],
            "nicknames": [{"value": "Janie"}],
            "biographies": [{"value": "Operator in crypto philanthropy"}],
            "urls": [
                {"value": "https://www.linkedin.com/in/janesmith/"},
                {"value": "https://twitter.com/janesmith"},
                {"value": "https://github.com/janesmith"},
            ],
            "resourceName": "people/123",
        }
    )
    assert item["aliases"] == ["Janie"]
    assert item["company"] == "Endaoment"
    assert item["linkedin"] == "https://www.linkedin.com/in/janesmith/"
    assert item["twitter"] == "https://twitter.com/janesmith"
    assert item["github"] == "https://github.com/janesmith"
    assert item["description"] == "Operator in crypto philanthropy"


def test_fetch_uses_configured_vcf_paths_from_env(tmp_path, monkeypatch):
    preferred = tmp_path / "preferred.vcf"
    preferred.write_text(
        "\n".join(
            [
                "BEGIN:VCARD",
                "VERSION:3.0",
                "FN:Preferred Person",
                "EMAIL;TYPE=HOME:preferred@example.com",
                "END:VCARD",
            ]
        ),
        encoding="utf-8",
    )
    ignored = tmp_path / "ignored.vcf"
    ignored.write_text(
        "\n".join(
            [
                "BEGIN:VCARD",
                "VERSION:3.0",
                "FN:Ignored Person",
                "EMAIL;TYPE=HOME:ignored@example.com",
                "END:VCARD",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HFA_CONTACTS_VCF_PATHS", os.pathsep.join([str(preferred), str(tmp_path / "missing.vcf")]))
    rows = ContactsAdapter().fetch(str(tmp_path), {}, sources=["apple"])
    assert [row["name"] for row in rows] == ["Preferred Person"]


def test_fetch_google_falls_back_to_direct_for_selected_account(monkeypatch):
    adapter = ContactsAdapter()
    monkeypatch.setenv("GOOGLE_ACCOUNT", "rheeger@gmail.com")

    def fake_proxy(account, *, fields, page_token):
        raise RuntimeError("auto-issue failed: connection refused")

    calls: list[str] = []

    def fake_direct(account, *, fields, page_token):
        calls.append(account)
        return {
            "connections": [
                {
                    "names": [{"displayName": "Jane Smith", "givenName": "Jane", "familyName": "Smith"}],
                    "emailAddresses": [{"value": "jane@example.com"}],
                    "resourceName": "people/123",
                }
            ]
        }

    monkeypatch.setattr(adapter, "_fetch_google_page_via_proxy", fake_proxy)
    monkeypatch.setattr(adapter, "_fetch_google_page_via_direct", fake_direct)
    fake_bootstrap = types.SimpleNamespace(bootstrap=lambda: None)
    fake_accounts = types.SimpleNamespace(
        ACCOUNTS={
            "arnold": {"email": "arnold@shloopydoopy.com"},
            "rheeger": {"email": "rheeger@gmail.com"},
        }
    )
    fake_google_cli_auth = types.SimpleNamespace(
        account_name_from_email=lambda email: "rheeger" if email == "rheeger@gmail.com" else None
    )
    monkeypatch.setitem(sys.modules, "arnoldlib.bootstrap", fake_bootstrap)
    monkeypatch.setitem(sys.modules, "arnoldlib.accounts", fake_accounts)
    monkeypatch.setitem(sys.modules, "arnoldlib.google_cli_auth", fake_google_cli_auth)

    rows = adapter._fetch_google()
    assert calls == ["rheeger"]
    assert rows[0]["name"] == "Jane Smith"
