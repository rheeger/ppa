"""Identity-repair census, canonicalize, hybrid merge, and S4 fixture tracers."""

from __future__ import annotations

import json
from pathlib import Path

from archive_cli.commands.identity_repair import canonicalize_people, merge_people, run_census
from archive_cli.index_query import _jsonb_text_array_sql, people_filter_terms
from archive_cli.materializer import _build_search_text, _identifier_search_tokens
from archive_engine.query import Predicate, row_matches_predicate
from archive_vault.canon import phone as canon_phone
from archive_vault.canon import place
from archive_vault.canon.wikilink import parse as parse_wikilink
from archive_vault.vault import read_note


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "identity-repair"


def _copy_fixture(vault: Path, name: str, dest_rel: str) -> None:
    src = FIXTURE_DIR / name
    dest = vault / dest_rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")


def test_people_filter_terms_accept_name_slug_phone() -> None:
    name = people_filter_terms("Sam Panken")
    assert name["slug"] == "sam-panken"
    slug = people_filter_terms("[[sam-panken]]")
    assert slug["parsed"] == "sam-panken"
    phone = people_filter_terms("9147153533")
    assert "+19147153533" in phone["phone_forms"]
    assert "9147153533" in phone["phone_forms"]


def test_search_text_includes_phone_alias_tokens() -> None:
    fm = {"uid": "hfa-person-x", "type": "person", "phones": ["(914) 715-3533"], "summary": "Sam"}
    tokens = _identifier_search_tokens(fm)
    assert "+19147153533" in tokens
    assert "9147153533" in tokens
    text = _build_search_text(fm, "")
    assert "9147153533" in text


def test_canonicalize_rewrites_person_phones(tmp_vault: Path) -> None:
    _copy_fixture(tmp_vault, "person-phone-ambig-a.md", "People/jordan-hale.md")
    dry = canonicalize_people(tmp_vault, apply=False)
    assert dry["changed_people"] == 1
    applied = canonicalize_people(tmp_vault, apply=True)
    assert applied["changed_people"] == 1
    fm, _body, _prov = read_note(tmp_vault, "People/jordan-hale.md")
    assert fm["phones"] == ["+15550109988"]
    identity = json.loads((tmp_vault / "_meta" / "identity-map.json").read_text(encoding="utf-8"))
    assert identity["phone:+15550109988"] == "[[jordan-hale]]"


def test_hybrid_merge_redirects_email_dup_and_queues_phone_ambig(tmp_vault: Path) -> None:
    _copy_fixture(tmp_vault, "person-email-dup-a.md", "People/alex-rivera.md")
    _copy_fixture(tmp_vault, "person-email-dup-b.md", "People/alex-rivera-dup.md")
    _copy_fixture(tmp_vault, "person-phone-ambig-a.md", "People/jordan-hale.md")
    _copy_fixture(tmp_vault, "person-phone-ambig-b.md", "People/casey-quinn.md")
    canonicalize_people(tmp_vault, apply=True)
    result = merge_people(tmp_vault, apply=True)
    redirected = {item["loser"]: item for item in result["redirected"]}
    assert "hfa-person-idrepemailb2" in redirected
    assert redirected["hfa-person-idrepemailb2"]["winner"] == "hfa-person-idrepemaila1"
    loser_fm, _body, _prov = read_note(tmp_vault, "People/alex-rivera-dup.md")
    assert loser_fm["redirect_to"] == "hfa-person-idrepemaila1"
    assert (tmp_vault / "People" / "alex-rivera-dup.md").is_file()
    queued_uids = {tuple(sorted(item["uids"])) for item in result["queued"]}
    assert ("hfa-person-idrepphona01", "hfa-person-idrepphonb02") in queued_uids
    queue = json.loads((tmp_vault / "_meta" / "dedup-candidates.json").read_text(encoding="utf-8"))
    assert any("hfa-person-idrepphona01" in item.get("uids", []) for item in queue)


def test_s4_planted_ical_and_merchant_are_code_only() -> None:
    calendar = (FIXTURE_DIR / "calendar-ical.md").read_text(encoding="utf-8")
    transcript = (FIXTURE_DIR / "transcript-ical.md").read_text(encoding="utf-8")
    assert "ical_uid: idrep-shared-ical-uid-001@example.com" in calendar
    assert "ical_uid: idrep-shared-ical-uid-001@example.com" in transcript
    bank = place.canonical("SQ *BLUE BOTTLE", profile="merchant_bank")
    receipt = place.canonical("Blue Bottle Coffee", profile="restaurant_receipt")
    assert bank
    assert receipt
    assert place.merchants_match("SQ *BLUE BOTTLE", "Blue Bottle Coffee") or bank.replace(" ", "") in receipt.replace(
        " ", ""
    )


def test_census_always_lists_proven_rewrite_families(tmp_vault: Path) -> None:
    _copy_fixture(tmp_vault, "person-phone-ambig-a.md", "People/jordan-hale.md")
    report = run_census(tmp_vault)
    assert "phones" in report["rewrite_families"]
    assert "person_refs" in report["rewrite_families"]
    assert "thread_rollups" in report["rewrite_families"]


def test_phone_alias_forms_cover_sam_witness() -> None:
    forms = set(canon_phone.alias_forms("(914) 715-3533"))
    assert forms == {"+19147153533", "19147153533", "9147153533"}
    assert parse_wikilink("[[sam-panken]]") == "sam-panken"


def test_jsonb_text_array_sql_coerces_stringified_arrays() -> None:
    sql = _jsonb_text_array_sql("p.phones_json")
    assert "jsonb_typeof" in sql
    assert "jsonb_build_array" in sql


def test_people_predicate_ignored_after_serving_resolution() -> None:
    row = {
        "uid": "hfa-imessage-thread-45b3a963c99a",
        "people": ["hfa-person-54fc3b19aeda"],
        "type": "imessage_thread",
    }
    predicate = Predicate(op="eq", field="people", value="Sam Panken")
    assert row_matches_predicate(row, predicate) is False
    assert row_matches_predicate(row, predicate, ignore_fields=frozenset({"people"})) is True
