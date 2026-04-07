"""Basic entity resolution for places, organizations, and person references."""

from __future__ import annotations

import json
import logging
import random
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from archive_sync.adapters.base import deterministic_provenance
from hfa.identity import IdentityCache
from hfa.identity_resolver import PersonIndex, resolve_person
from hfa.schema import OrganizationCard, PlaceCard
from hfa.uid import generate_uid
from hfa.vault import iter_parsed_notes, write_card

log = logging.getLogger("ppa.extractor.entity_resolution")

DERIVED_ENTITY_CARD_TYPES = frozenset(
    {
        "meal_order",
        "ride",
        "grocery_order",
        "flight",
        "accommodation",
        "car_rental",
        "purchase",
        "shipment",
        "event_ticket",
        "medical_record",
        "payroll",
    }
)


def iter_derived_card_dicts(vault_path: str) -> list[dict[str, Any]]:
    """Load frontmatter dicts for card types that feed entity resolution."""
    out: list[dict[str, Any]] = []
    for note in iter_parsed_notes(vault_path):
        t = note.frontmatter.get("type")
        if t in DERIVED_ENTITY_CARD_TYPES:
            out.append(dict(note.frontmatter))
    return out


def estimate_entity_resolution_candidates(vault_path: str) -> dict[str, int]:
    """Counts for ``resolve-entities --dry-run`` (no writes)."""
    cards = iter_derived_card_dicts(vault_path)
    place_refs = sum(len(_place_tuples_from_card(c)) for c in cards)
    domains: set[str] = set()
    for c in cards:
        dom = _service_to_domain(
            str(c.get("service") or c.get("vendor") or c.get("airline") or c.get("carrier") or c.get("company") or "")
        )
        if dom:
            domains.add(dom)
    person_strings = 0
    for c in cards:
        ct = str(c.get("type") or "")
        if ct == "medical_record" and str(c.get("provider_name") or "").strip():
            person_strings += 1
        if ct == "ride" and str(c.get("driver_name") or "").strip():
            person_strings += 1
        for key in ("passengers", "guests"):
            for item in c.get(key) or []:
                if isinstance(item, str) and item.strip():
                    person_strings += 1
    return {
        "derived_cards": len(cards),
        "place_references": place_refs,
        "distinct_org_domains_inferred": len(domains),
        "person_strings": person_strings,
    }


def _domain_from_derived_card(card: dict[str, Any]) -> str:
    """Infer registrable domain from a derived card (same heuristics as OrgResolver)."""
    svc = (
        str(card.get("service") or "")
        or str(card.get("vendor") or "")
        or str(card.get("airline") or "")
        or str(card.get("carrier") or "")
        or str(card.get("company") or "")
    )
    return _registrable_domain(_service_to_domain(svc))


def _place_evidence_index(cards: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Map 'name_lower\\tcity_lower' -> evidence card uids."""
    idx: dict[str, list[str]] = {}
    for c in cards:
        uid = str(c.get("uid") or "").strip()
        if not uid:
            continue
        for raw_name, city in _place_tuples_from_card(c):
            nk = _normalize_place_name(raw_name)
            ck = city.strip().lower()
            if not nk:
                continue
            key = f"{nk}\t{ck}"
            idx.setdefault(key, []).append(uid)
    return idx


def _org_evidence_index(cards: list[dict[str, Any]]) -> dict[str, list[str]]:
    idx: dict[str, list[str]] = {}
    for c in cards:
        uid = str(c.get("uid") or "").strip()
        if not uid:
            continue
        dom = _domain_from_derived_card(c)
        if dom:
            idx.setdefault(dom, []).append(uid)
    return idx


def write_entity_resolution_reports(
    vault_path: str,
    derived_cards: list[dict[str, Any]],
    report_dir: str,
    *,
    rng_seed: int = 42,
) -> None:
    """Write entity-resolution-report.json and entity-resolution-spot-check.md."""
    out = Path(report_dir)
    out.mkdir(parents=True, exist_ok=True)
    place_idx = _place_evidence_index(derived_cards)
    org_idx = _org_evidence_index(derived_cards)
    payload = {
        "place_clusters": [
            {"normalized_key": k, "evidence_uids": sorted(set(v))} for k, v in sorted(place_idx.items())
        ],
        "org_clusters": [{"domain": d, "evidence_uids": sorted(set(v))} for d, v in sorted(org_idx.items())],
    }
    (out / "entity-resolution-report.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )

    root = Path(vault_path)
    places: list[Path] = []
    orgs: list[Path] = []
    for note in iter_parsed_notes(vault_path):
        t = note.frontmatter.get("type")
        if t == "place":
            places.append(root / note.rel_path)
        elif t == "organization":
            orgs.append(root / note.rel_path)

    rng = random.Random(rng_seed)
    sp = rng.sample(places, min(20, len(places)))
    so = rng.sample(orgs, min(20, len(orgs)))
    lines = ["# Entity resolution spot check", "", "## Sample PlaceCards (body excerpts omitted)", ""]
    for p in sp:
        try:
            rel = p.relative_to(root)
        except ValueError:
            rel = Path(p.name)
        lines.append(f"- `{rel}`")
    lines.extend(["", "## Sample OrgCards", ""])
    for p in so:
        try:
            rel = p.relative_to(root)
        except ValueError:
            rel = Path(p.name)
        lines.append(f"- `{rel}`")
    lines.append("")
    (out / "entity-resolution-spot-check.md").write_text("\n".join(lines), encoding="utf-8")


def validate_entities(vault_path: str) -> list[str]:
    """Post-resolution quality checks; empty list means pass."""
    errors: list[str] = []
    place_keys: dict[tuple[str, str], list[str]] = {}
    org_domains: dict[str, list[str]] = {}

    for note in iter_parsed_notes(vault_path):
        fm = note.frontmatter
        t = fm.get("type")
        uid = str(fm.get("uid") or note.rel_path)
        if t == "place":
            name = str(fm.get("name") or "").strip()
            if not name:
                errors.append(f"place {uid}: empty name")
            nk = _normalize_place_name(name)
            ck = str(fm.get("city") or "").strip().lower()
            key = (nk, ck)
            place_keys.setdefault(key, []).append(uid)
        elif t == "organization":
            dom = _registrable_domain(str(fm.get("domain") or ""))
            nm = str(fm.get("name") or "").strip()
            ot = str(fm.get("org_type") or "").strip()
            if not nm:
                errors.append(f"organization {uid}: empty name")
            if not ot:
                errors.append(f"organization {uid}: empty org_type")
            if dom:
                org_domains.setdefault(dom, []).append(uid)

    for key, uids in place_keys.items():
        if len(uids) > 1:
            nk, ck = key
            errors.append(f"duplicate place key (name={nk!r}, city={ck!r}): {uids}")

    for dom, uids in org_domains.items():
        if len(uids) > 1:
            errors.append(f"duplicate organization domain {dom!r}: {uids}")

    return errors


def run_entity_resolution(
    vault_path: str,
    *,
    entity_filter: str = "all",
    dry_run: bool = False,
    report_dir: str = "",
) -> dict[str, Any]:
    """Run place/org/person resolution passes."""
    cards = iter_derived_card_dicts(vault_path)
    if dry_run:
        est = estimate_entity_resolution_candidates(vault_path)
        return {"dry_run": True, **est}

    merged = EntityResolutionResult()
    if entity_filter in ("place", "all"):
        pr = PlaceResolver(vault_path).resolve(cards, dry_run=False)
        merged.places_created += pr.places_created
        merged.places_merged += pr.places_merged
        merged.errors.extend(pr.errors)
    if entity_filter in ("org", "all"):
        or_ = OrgResolver(vault_path).resolve(cards, sender_domains=None, dry_run=False)
        merged.orgs_created += or_.orgs_created
        merged.orgs_merged += or_.orgs_merged
        merged.errors.extend(or_.errors)
    if entity_filter in ("person", "all"):
        pl = PersonLinker(vault_path).link(cards, dry_run=False)
        merged.persons_linked += pl.persons_linked
        merged.errors.extend(pl.errors)

    validation_errors: list[str] = []
    rd = str(report_dir or "").strip()
    if rd:
        write_entity_resolution_reports(vault_path, cards, rd)
    validation_errors = validate_entities(vault_path)

    return {
        "dry_run": False,
        "places_created": merged.places_created,
        "places_merged": merged.places_merged,
        "orgs_created": merged.orgs_created,
        "orgs_merged": merged.orgs_merged,
        "persons_linked": merged.persons_linked,
        "errors": merged.errors,
        "validation_errors": validation_errors,
    }


def _normalize_place_name(name: str) -> str:
    n = (name or "").lower().strip()
    if n.startswith("the "):
        n = n[4:]
    n = re.sub(r"\bst\b\.?", "street", n)
    n = re.sub(r"\bave\b\.?", "avenue", n)
    n = re.sub(r"\s+", " ", n)
    return n.strip()


def _city_from_address(addr: str) -> str:
    if not addr:
        return ""
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[-2]
    return ""


def _registrable_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    if not d:
        return ""
    parts = d.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return d


def _org_display_name(domain: str) -> str:
    base = _registrable_domain(domain).split(".")[0]
    if not base:
        return domain
    return base.replace("-", " ").title()


def _relationship_for_card_type(card_type: str) -> str:
    if card_type == "payroll":
        return "employer"
    return "customer"


def _place_tuples_from_card(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Yield (normalized_name_key, city) place seeds from a derived card dict."""
    ct = str(data.get("type") or "")
    out: list[tuple[str, str]] = []
    if ct == "meal_order":
        name = str(data.get("restaurant") or "")
        city = _city_from_address(str(data.get("delivery_address") or ""))
        if name:
            out.append((name, city))
    elif ct == "ride":
        for loc_key in ("pickup_location", "dropoff_location"):
            loc = str(data.get(loc_key) or "")
            if loc:
                out.append((loc, ""))
    elif ct == "accommodation":
        prop = str(data.get("property_name") or "")
        addr = str(data.get("address") or "")
        city = _city_from_address(addr)
        if prop:
            out.append((prop, city))
    elif ct == "event_ticket":
        venue = str(data.get("venue") or "")
        vaddr = str(data.get("venue_address") or "")
        city = _city_from_address(vaddr)
        if venue:
            out.append((venue, city))
    elif ct == "grocery_order":
        name = str(data.get("store") or "")
        city = _city_from_address(str(data.get("delivery_address") or ""))
        if name:
            out.append((name, city))
    elif ct == "car_rental":
        for loc_key in ("pickup_location", "dropoff_location"):
            loc = str(data.get(loc_key) or "")
            if loc:
                out.append((loc, ""))
    return out


@dataclass
class EntityResolutionResult:
    places_created: int = 0
    places_merged: int = 0
    orgs_created: int = 0
    orgs_merged: int = 0
    persons_linked: int = 0
    errors: list[str] = field(default_factory=list)


class PlaceResolver:
    """Cluster place references from derived cards into PlaceCards."""

    def __init__(self, vault_path: str) -> None:
        self.vault_path = vault_path

    def _existing_places(self) -> dict[tuple[str, str], tuple[str, dict[str, Any]]]:
        """Map (normalized_name, city_lower) -> (rel_path_str, frontmatter)."""
        found: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
        for note in iter_parsed_notes(self.vault_path):
            fm = note.frontmatter
            if fm.get("type") != "place":
                continue
            name = _normalize_place_name(str(fm.get("name") or ""))
            city = str(fm.get("city") or "").strip().lower()
            key = (name, city)
            found[key] = (str(note.rel_path), fm)
        return found

    def resolve(self, derived_cards: list[dict[str, Any]], *, dry_run: bool = False) -> EntityResolutionResult:
        result = EntityResolutionResult()
        clusters: dict[tuple[str, str], str] = {}
        for card in derived_cards:
            for raw_name, city in _place_tuples_from_card(card):
                nk = _normalize_place_name(raw_name)
                ck = city.strip().lower()
                key = (nk, ck)
                if not nk:
                    continue
                clusters.setdefault(key, raw_name)

        existing = self._existing_places()
        today = date.today().isoformat()

        for (nk, ck), display_name in clusters.items():
            if not ck:
                log.warning("place resolution: missing city for %r — creating with empty city", display_name)
            if (nk, ck) in existing:
                result.places_merged += 1
                continue
            uid = generate_uid("place", "entity-resolution", f"{nk}:{ck}")
            place = PlaceCard(
                uid=uid,
                type="place",
                source=["entity_resolution"],
                source_id=uid,
                created=today,
                updated=today,
                summary=display_name or nk,
                name=display_name or nk,
                city=ck.title() if ck else "",
                first_seen=today,
                last_seen=today,
            )
            prov = deterministic_provenance(place, "entity_resolution")
            rel = f"Entities/Places/{today[:7]}/{uid}.md"
            if not dry_run:
                try:
                    write_card(self.vault_path, rel, place, f"# {display_name}\n", prov)
                except Exception as exc:
                    result.errors.append(str(exc))
                    continue
            result.places_created += 1
            existing[(nk, ck)] = (rel, place.model_dump(mode="python"))

        return result


class OrgResolver:
    """Deduplicate organizations by sender domain."""

    def __init__(self, vault_path: str) -> None:
        self.vault_path = vault_path

    def _existing_orgs(self) -> dict[str, tuple[str, dict[str, Any]]]:
        """Map registrable domain -> (rel_path, frontmatter)."""
        found: dict[str, tuple[str, dict[str, Any]]] = {}
        for note in iter_parsed_notes(self.vault_path):
            fm = note.frontmatter
            if fm.get("type") != "organization":
                continue
            dom = _registrable_domain(str(fm.get("domain") or ""))
            if dom:
                found[dom] = (str(note.rel_path), fm)
        return found

    def resolve(
        self,
        derived_cards: list[dict[str, Any]],
        *,
        sender_domains: list[str] | None = None,
        dry_run: bool = False,
    ) -> EntityResolutionResult:
        """Create OrgCards keyed by registrable domain.

        When *sender_domains* is provided (parallel to *derived_cards*), use it for org clustering;
        otherwise infer from card ``service`` / ``vendor`` / ``airline`` heuristics.
        """
        result = EntityResolutionResult()
        domains: set[str] = set()
        card_types_by_domain: dict[str, str] = {}

        for i, card in enumerate(derived_cards):
            dom = ""
            if sender_domains is not None and i < len(sender_domains):
                dom = sender_domains[i]
            dom = _registrable_domain(dom)
            if not dom:
                svc = (
                    str(card.get("service") or "")
                    or str(card.get("vendor") or "")
                    or str(card.get("airline") or "")
                    or str(card.get("carrier") or "")
                    or str(card.get("company") or "")
                )
                dom = _service_to_domain(svc)
            if not dom:
                continue
            domains.add(dom)
            card_types_by_domain.setdefault(dom, str(card.get("type") or ""))

        existing = self._existing_orgs()
        today = date.today().isoformat()

        for dom in sorted(domains):
            if dom in existing:
                result.orgs_merged += 1
                continue
            uid = generate_uid("organization", "entity-resolution", dom)
            ct = card_types_by_domain.get(dom, "")
            org = OrganizationCard(
                uid=uid,
                type="organization",
                source=["entity_resolution"],
                source_id=uid,
                created=today,
                updated=today,
                summary=_org_display_name(dom),
                name=_org_display_name(dom),
                org_type="merchant",
                domain=dom,
                relationship=_relationship_for_card_type(ct),
                first_seen=today,
                last_seen=today,
            )
            prov = deterministic_provenance(org, "entity_resolution")
            rel = f"Entities/Organizations/{today[:7]}/{uid}.md"
            if not dry_run:
                try:
                    write_card(self.vault_path, rel, org, f"# {org.name}\n", prov)
                except Exception as exc:
                    result.errors.append(str(exc))
                    continue
            result.orgs_created += 1

        return result


def _service_to_domain(service: str) -> str:
    s = (service or "").strip().lower()
    mapping = {
        "doordash": "doordash.com",
        "uber": "uber.com",
        "ubereats": "uber.com",
        "amazon": "amazon.com",
        "instacart": "instacart.com",
        "ups": "ups.com",
        "fedex": "fedex.com",
        "usps": "usps.com",
        "lyft": "lyft.com",
        "united": "united.com",
        "airbnb": "airbnb.com",
        "national": "nationalcar.com",
        "hertz": "hertz.com",
    }
    return mapping.get(s, "")


class PersonLinker:
    """Link person-like strings on derived cards to existing PersonCards."""

    def __init__(self, vault_path: str) -> None:
        self.vault_path = vault_path

    def link(self, derived_cards: list[dict[str, Any]], *, dry_run: bool = False) -> EntityResolutionResult:
        result = EntityResolutionResult()
        cache = IdentityCache(self.vault_path)
        index = PersonIndex(self.vault_path, preload=True)

        for card in derived_cards:
            names: list[str] = []
            ct = str(card.get("type") or "")
            if ct == "medical_record":
                pn = str(card.get("provider_name") or "").strip()
                if pn:
                    names.append(pn)
            elif ct == "ride":
                dn = str(card.get("driver_name") or "").strip()
                if dn:
                    names.append(dn)
            for key in ("passengers", "guests"):
                for item in card.get(key) or []:
                    if isinstance(item, str) and item.strip():
                        names.append(item.strip())

            for raw in names:
                rr = resolve_person(
                    self.vault_path,
                    {"name": raw},
                    cache=cache,
                    people_index=index,
                )
                if rr.action == "merge" and rr.wikilink:
                    result.persons_linked += 1

        return result
