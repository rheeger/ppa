"""Archive-sync contacts adapter tests."""

from __future__ import annotations

import os
import sys
import types

from archive_sync.adapters.contacts import AppleContactsPermissionError, ContactsAdapter, dump_apple_contacts_vcf
from archive_sync.source_updaters.runner import classify_run_exception
from archive_vault.schema import PersonCard


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
            "phones": ["+15551234567", "+15551239999"],
            "company": "Endaoment",
            "title": "Ops",
            "birthday": "1990-01-01",
            "linkedin": "janesmith",
        }
    )
    assert isinstance(card, PersonCard)
    assert card.emails == ["jenny@example.com", "jenny@work.com"]
    assert card.phones == ["+15551234567", "+15551239999"]
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

    def fake_proxy(account, *, fields, page_token, sync_token=None, request_sync_token=False):
        raise RuntimeError("auto-issue failed: connection refused")

    calls: list[str] = []

    def fake_direct(account, *, fields, page_token, sync_token=None, request_sync_token=False):
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
    monkeypatch.setitem(
        sys.modules,
        "archive_auth",
        types.SimpleNamespace(
            ACCOUNTS=fake_accounts.ACCOUNTS,
            account_name_from_email=fake_google_cli_auth.account_name_from_email,
        ),
    )

    rows = adapter._fetch_google()
    assert calls == ["rheeger"]
    assert rows[0]["name"] == "Jane Smith"


def test_fetch_google_skips_matching_etag_and_stores_sync_token(monkeypatch):
    adapter = ContactsAdapter()

    def fake_page(account, *, fields, page_token, sync_token, request_sync_token, has_arnoldlib):
        assert sync_token == "sync-1"
        assert request_sync_token is True
        return {
            "connections": [
                {"resourceName": "people/same", "etag": "etag-same", "names": [{"displayName": "Same"}]},
                {"resourceName": "people/new", "etag": "etag-new", "names": [{"displayName": "New"}]},
            ],
            "nextSyncToken": "sync-2",
        }

    monkeypatch.setattr(adapter, "_fetch_google_page", fake_page)
    monkeypatch.setattr(adapter, "_selected_google_accounts", lambda _accounts: ["rheeger"])
    monkeypatch.setitem(
        sys.modules,
        "archive_auth",
        types.SimpleNamespace(ACCOUNTS={"rheeger": {"email": "rheeger@gmail.com"}}),
    )
    rows = adapter._fetch_google(cursor={"sync_token": "sync-1", "person_etags": {"people/same": "etag-same"}})
    assert [row["name"] for row in rows] == ["New"]
    assert adapter._last_google_sync_token == "sync-2"
    patch = adapter.finalize_cursor({})
    assert patch["sync_token"] == "sync-2"
    assert patch["person_etags"]["people/new"] == "etag-new"


_SAMPLE_VCARD = "\n".join(
    [
        "BEGIN:VCARD",
        "VERSION:3.0",
        "UID:ABUID:ABC-123",
        "N:Souza;Jenny;;;",
        "FN:Jenny Souza",
        "NICKNAME:Jen",
        "NOTE:Met at Endaoment",
        "EMAIL;TYPE=HOME:jenny@example.com",
        "TEL;TYPE=CELL:+1-617-555-1212",
        "URL:https://endaoment.org",
        "END:VCARD",
        "",
    ]
)


def test_parse_vcf_reads_uid_note_nickname_url(tmp_path):
    vcf = tmp_path / "uid.vcf"
    vcf.write_text(_SAMPLE_VCARD, encoding="utf-8")
    rows = ContactsAdapter()._parse_vcf(str(vcf))
    assert rows[0]["apple_uid"] == "ABUID:ABC-123"
    assert rows[0]["description"] == "Met at Endaoment"
    assert rows[0]["aliases"] == ["Jen"]
    assert rows[0]["websites"] == ["https://endaoment.org"]
    card, _prov, _body = ContactsAdapter().to_card(rows[0])
    assert card.source_id == "ABUID:ABC-123"


def test_fetch_apple_uses_mocked_dump_and_skips_unchanged_hash(monkeypatch, tmp_path):
    monkeypatch.delenv("HFA_CONTACTS_VCF_PATHS", raising=False)
    adapter = ContactsAdapter()

    def fake_dump(dest):
        dest.write_text(_SAMPLE_VCARD, encoding="utf-8")

    adapter._dump_apple_contacts_vcf = fake_dump
    first = adapter.fetch(str(tmp_path), {}, sources=["apple"])
    assert [row["name"] for row in first] == ["Jenny Souza"]
    assert first[0]["apple_uid"] == "ABUID:ABC-123"
    assert first[0]["_content_hash"]
    before_ingest = adapter.finalize_cursor({})
    assert "ABUID:ABC-123" not in (before_ingest.get("contact_hashes") or {})
    adapter.cursor_checkpoint(first[0])
    cursor = adapter.finalize_cursor({})
    assert "ABUID:ABC-123" in cursor["contact_hashes"]
    second = adapter.fetch(str(tmp_path), cursor, sources=["apple"])
    assert second == []


def test_fetch_apple_permission_error_does_not_fall_through(monkeypatch, tmp_path):
    monkeypatch.delenv("HFA_CONTACTS_VCF_PATHS", raising=False)
    adapter = ContactsAdapter()

    def fake_dump(_dest):
        raise AppleContactsPermissionError("Contacts permission denied: not authorized")

    adapter._dump_apple_contacts_vcf = fake_dump
    try:
        adapter.fetch(str(tmp_path), {}, sources=["apple"])
        raise AssertionError("expected AppleContactsPermissionError")
    except AppleContactsPermissionError:
        pass
    assert classify_run_exception(AppleContactsPermissionError("not authorized")) == "blocked"


def test_dump_apple_contacts_vcf_raises_on_permission(monkeypatch, tmp_path):
    dest = tmp_path / "out.vcf"

    class Result:
        returncode = 1
        stderr = "osascript is not allowed to send Apple events to Contacts"
        stdout = ""

    monkeypatch.setattr("archive_sync.adapters.contacts.subprocess.run", lambda *a, **k: Result())
    try:
        dump_apple_contacts_vcf(dest)
        raise AssertionError("expected AppleContactsPermissionError")
    except AppleContactsPermissionError:
        pass


def test_dump_apple_contacts_vcf_writes_batches(monkeypatch, tmp_path):
    writes: list[tuple[int, int]] = []
    monkeypatch.setattr("archive_sync.adapters.contacts._apple_contacts_count", lambda: 120)

    def fake_range(start, end, dest):
        writes.append((start, end))
        dest.write_bytes(f"BEGIN:VCARD\nFN:{start}-{end}\nEND:VCARD\n".encode("utf-8"))

    monkeypatch.setattr("archive_sync.adapters.contacts._write_apple_vcard_range", fake_range)
    dest = tmp_path / "out.vcf"
    dump_apple_contacts_vcf(dest)
    assert writes == [(1, 50), (51, 100), (101, 120)]
    assert dest.read_text(encoding="utf-8").count("BEGIN:VCARD") == 3


def test_dump_apple_contacts_vcf_splits_on_1741(monkeypatch, tmp_path):
    monkeypatch.setattr("archive_sync.adapters.contacts.APPLE_CONTACTS_DUMP_BATCH", 4)
    monkeypatch.setattr("archive_sync.adapters.contacts._apple_contacts_count", lambda: 4)

    def fake_range(start, end, dest):
        if start != end:
            raise RuntimeError("Contacts.app dump failed: An error of type -1741 has occurred. (-1741)")
        dest.write_bytes(f"CARD{start}\n".encode("utf-8"))

    monkeypatch.setattr("archive_sync.adapters.contacts._write_apple_vcard_range", fake_range)
    dest = tmp_path / "out.vcf"
    dump_apple_contacts_vcf(dest)
    assert dest.read_text(encoding="utf-8") == "CARD1\nCARD2\nCARD3\nCARD4\n"
