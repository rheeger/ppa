"""Tests for entity_resolution."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from archive_sync.adapters.base import deterministic_provenance
from archive_sync.extractors.entity_resolution import (OrgResolver,
                                                       PersonLinker,
                                                       PlaceResolver)
from hfa.schema import PersonCard
from hfa.uid import generate_uid
from hfa.vault import write_card


def test_place_compound_key_merges_same_name_city(extractor_vault):
    cards = [
        {
            "type": "meal_order",
            "restaurant": "Brooklyn Hero Shop",
            "delivery_address": "1 Main St, Brooklyn, NY",
        },
        {
            "type": "meal_order",
            "restaurant": "Brooklyn Hero Shop",
            "delivery_address": "2 Oak Ave, Brooklyn, NY",
        },
    ]
    r = PlaceResolver(extractor_vault).resolve(cards, dry_run=False)
    assert r.places_created == 1


def test_place_different_city_does_not_merge(extractor_vault):
    cards = [
        {
            "type": "meal_order",
            "restaurant": "Main Street Pizza",
            "delivery_address": "Brooklyn, NY",
        },
        {
            "type": "meal_order",
            "restaurant": "Main Street Pizza",
            "delivery_address": "San Francisco, CA",
        },
    ]
    r = PlaceResolver(extractor_vault).resolve(cards, dry_run=False)
    assert r.places_created == 2


def test_place_no_city_creates_with_warning(extractor_vault, caplog):
    caplog.set_level(logging.WARNING)
    cards = [{"type": "ride", "pickup_location": "Somewhere"}]
    r = PlaceResolver(extractor_vault).resolve(cards, dry_run=False)
    assert r.places_created == 1
    assert any("missing city" in rec.message.lower() for rec in caplog.records)


def test_org_domain_dedup(extractor_vault):
    cards = [{"type": "meal_order", "service": "DoorDash"}, {"type": "meal_order", "service": "DoorDash"}]
    r = OrgResolver(extractor_vault).resolve(
        cards,
        sender_domains=["messages.doordash.com", "doordash.com"],
        dry_run=False,
    )
    assert r.orgs_created == 1


def test_org_relationship_inferred_from_card_type(extractor_vault):
    cards = [{"type": "meal_order", "service": "DoorDash"}]
    OrgResolver(extractor_vault).resolve(cards, dry_run=False)
    from hfa.vault import iter_parsed_notes

    for note in iter_parsed_notes(extractor_vault):
        if note.frontmatter.get("type") == "organization":
            assert note.frontmatter.get("relationship") == "customer"
            assert note.frontmatter.get("org_type") == "merchant"
            return
    pytest.fail("organization not written")


def test_person_linkage_uses_identity_cache(extractor_vault):
    Path(extractor_vault, "_meta/identity-map.json").write_text(
        json.dumps({"name:jane doe": "[[jane-doe]]"}),
        encoding="utf-8",
    )
    uid = generate_uid("person", "test", "jd")
    person = PersonCard(
        uid=uid,
        type="person",
        source=["test"],
        source_id=uid,
        created="2024-01-01",
        updated="2024-01-01",
        summary="Jane Doe",
        first_name="Jane",
        last_name="Doe",
    )
    write_card(extractor_vault, "People/jane-doe.md", person, "", deterministic_provenance(person, "test"))
    linker = PersonLinker(extractor_vault)
    res = linker.link([{"type": "medical_record", "provider_name": "Jane Doe"}])
    assert res.persons_linked >= 1


def test_uid_deterministic():
    from hfa.uid import generate_uid as gu

    a = gu("place", "entity-resolution", "brooklyn hero shop:brooklyn")
    b = gu("place", "entity-resolution", "brooklyn hero shop:brooklyn")
    assert a == b
