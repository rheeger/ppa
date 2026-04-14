"""Cross-type entity resolution, reports, and validate_entities."""

from __future__ import annotations

import json

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.extractors.entity_resolution import (
    run_entity_resolution,
    validate_entities,
    write_entity_resolution_reports,
)
from archive_vault.schema import MealOrderCard, OrganizationCard, PlaceCard
from archive_vault.uid import generate_uid
from archive_vault.vault import write_card


def test_ride_dropoff_and_meal_order_distinct_place_keys(extractor_vault):
    """Same venue text but meal has city from address vs ride with empty city => two PlaceCards."""
    cards = [
        {
            "uid": "mo1",
            "type": "meal_order",
            "restaurant": "Same Cafe",
            "delivery_address": "1 St, Brooklyn, NY",
        },
        {
            "uid": "r1",
            "type": "ride",
            "dropoff_location": "Same Cafe",
        },
    ]
    from archive_sync.extractors.entity_resolution import PlaceResolver

    r = PlaceResolver(extractor_vault).resolve(cards, dry_run=False)
    assert r.places_created == 2


def test_cross_type_same_name_different_city(extractor_vault):
    cards = [
        {
            "uid": "a",
            "type": "meal_order",
            "restaurant": "Joe's",
            "delivery_address": "Brooklyn, NY",
        },
        {
            "uid": "b",
            "type": "meal_order",
            "restaurant": "Joe's",
            "delivery_address": "San Francisco, CA",
        },
    ]
    from archive_sync.extractors.entity_resolution import PlaceResolver

    r = PlaceResolver(extractor_vault).resolve(cards, dry_run=False)
    assert r.places_created == 2


def test_org_merging_across_extractors(extractor_vault):
    cards = [
        {"uid": "x1", "type": "meal_order", "service": "DoorDash"},
        {"uid": "x2", "type": "purchase", "vendor": "DoorDash"},
    ]
    from archive_sync.extractors.entity_resolution import OrgResolver

    r = OrgResolver(extractor_vault).resolve(cards, sender_domains=None, dry_run=False)
    assert r.orgs_created == 1


def test_report_json_written(extractor_vault, tmp_path):
    cards = [
        {"uid": "m1", "type": "meal_order", "service": "DoorDash", "restaurant": "R", "delivery_address": "Brooklyn, NY"},
    ]
    from archive_sync.extractors.entity_resolution import OrgResolver, PlaceResolver

    PlaceResolver(extractor_vault).resolve(cards, dry_run=False)
    OrgResolver(extractor_vault).resolve(cards, sender_domains=None, dry_run=False)
    rep = tmp_path / "rep"
    write_entity_resolution_reports(extractor_vault, cards, str(rep))
    js = rep / "entity-resolution-report.json"
    assert js.is_file()
    data = json.loads(js.read_text(encoding="utf-8"))
    assert "place_clusters" in data
    assert "org_clusters" in data


def test_report_spot_check_written(extractor_vault, tmp_path):
    cards = [
        {"uid": "m1", "type": "meal_order", "service": "DoorDash", "restaurant": "R", "delivery_address": "Brooklyn, NY"},
    ]
    from archive_sync.extractors.entity_resolution import OrgResolver, PlaceResolver

    PlaceResolver(extractor_vault).resolve(cards, dry_run=False)
    OrgResolver(extractor_vault).resolve(cards, sender_domains=None, dry_run=False)
    rep = tmp_path / "rep"
    write_entity_resolution_reports(extractor_vault, cards, str(rep))
    md = rep / "entity-resolution-spot-check.md"
    assert md.is_file()
    assert "Sample PlaceCards" in md.read_text(encoding="utf-8")


def test_validate_no_duplicate_place_keys(extractor_vault):
    today = "2024-03-15"
    uid1 = generate_uid("place", "t", "a")
    uid2 = generate_uid("place", "t", "b")
    p1 = PlaceCard(
        uid=uid1,
        type="place",
        source=["test"],
        source_id=uid1,
        created=today,
        updated=today,
        summary="Dup",
        name="Dup Name",
        city="Brooklyn",
        first_seen=today,
        last_seen=today,
    )
    p2 = PlaceCard(
        uid=uid2,
        type="place",
        source=["test"],
        source_id=uid2,
        created=today,
        updated=today,
        summary="Dup",
        name="Dup Name",
        city="Brooklyn",
        first_seen=today,
        last_seen=today,
    )
    write_card(extractor_vault, f"Entities/Places/{today[:7]}/{uid1}.md", p1, "# a\n", deterministic_provenance(p1, "test"))
    write_card(extractor_vault, f"Entities/Places/{today[:7]}/{uid2}.md", p2, "# b\n", deterministic_provenance(p2, "test"))
    errs = validate_entities(extractor_vault)
    assert any("duplicate place key" in e for e in errs)


def test_validate_no_duplicate_org_domains(extractor_vault):
    today = "2024-03-15"
    uid1 = generate_uid("organization", "t", "a")
    uid2 = generate_uid("organization", "t", "b")
    o1 = OrganizationCard(
        uid=uid1,
        type="organization",
        source=["test"],
        source_id=uid1,
        created=today,
        updated=today,
        summary="A",
        name="A",
        org_type="merchant",
        domain="dup.example.com",
        relationship="customer",
        first_seen=today,
        last_seen=today,
    )
    o2 = OrganizationCard(
        uid=uid2,
        type="organization",
        source=["test"],
        source_id=uid2,
        created=today,
        updated=today,
        summary="B",
        name="B",
        org_type="merchant",
        domain="dup.example.com",
        relationship="customer",
        first_seen=today,
        last_seen=today,
    )
    write_card(
        extractor_vault,
        f"Entities/Organizations/{today[:7]}/{uid1}.md",
        o1,
        "# a\n",
        deterministic_provenance(o1, "test"),
    )
    write_card(
        extractor_vault,
        f"Entities/Organizations/{today[:7]}/{uid2}.md",
        o2,
        "# b\n",
        deterministic_provenance(o2, "test"),
    )
    errs = validate_entities(extractor_vault)
    assert any("duplicate organization domain" in e for e in errs)


def test_validate_empty_name_flagged(extractor_vault):
    today = "2024-03-15"
    uid = generate_uid("place", "t", "empty")
    p = PlaceCard(
        uid=uid,
        type="place",
        source=["test"],
        source_id=uid,
        created=today,
        updated=today,
        summary="",
        name="",
        city="Brooklyn",
        first_seen=today,
        last_seen=today,
    )
    write_card(extractor_vault, f"Entities/Places/{today[:7]}/{uid}.md", p, "#\n", deterministic_provenance(p, "test"))
    errs = validate_entities(extractor_vault)
    assert any("empty name" in e for e in errs)


def test_run_entity_resolution_report_dir(extractor_vault, tmp_path):
    uid = generate_uid("meal_order", "test", "em1")
    m = MealOrderCard(
        uid=uid,
        type="meal_order",
        source=["test"],
        source_id=uid,
        created="2024-03-15",
        updated="2024-03-15",
        summary="m",
        service="DoorDash",
        restaurant="Z Cafe",
        delivery_address="1 Main, Brooklyn, NY",
        items=[],
    )
    write_card(
        extractor_vault,
        f"Transactions/MealOrders/2024-03/{uid}.md",
        m,
        "# m\n",
        deterministic_provenance(m, "test"),
    )

    rep = tmp_path / "out"
    out = run_entity_resolution(extractor_vault, entity_filter="all", dry_run=False, report_dir=str(rep))
    assert (rep / "entity-resolution-report.json").is_file()
    assert isinstance(out.get("validation_errors"), list)


def test_grocery_and_car_rental_place_tuples():
    from archive_sync.extractors.entity_resolution import _place_tuples_from_card

    g = _place_tuples_from_card(
        {"type": "grocery_order", "store": "Whole Foods", "delivery_address": "100 St, Oakland, CA"}
    )
    assert g and "whole foods" in g[0][0].lower()
    cr = _place_tuples_from_card(
        {"type": "car_rental", "pickup_location": "SFO", "dropoff_location": "OAK"},
    )
    assert len(cr) == 2
