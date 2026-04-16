"""Basic entity resolution for places, organizations, and person references."""

from __future__ import annotations

import json
import logging
import random
import re
import warnings
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from archive_cli.ppa_engine import ppa_engine
from archive_sync.adapters.base import deterministic_provenance
from archive_vault.identity import IdentityCache
from archive_vault.identity_resolver import PersonIndex, ResolveResult
from archive_vault.identity_resolver import \
    resolve_person_batch as resolve_person_batch_python
from archive_vault.schema import OrganizationCard, PlaceCard
from archive_vault.uid import generate_uid
from archive_vault.vault import (iter_parsed_notes,
                                 iter_parsed_notes_for_card_types, write_card)

log = logging.getLogger("ppa.extractor.entity_resolution")


def _resolve_person_batch(
    vault_path: str,
    batch_ids: list[dict[str, Any]],
    *,
    cache: IdentityCache,
    people_index: PersonIndex,
) -> list[ResolveResult]:
    """Use Rust batch resolver when ``PPA_ENGINE=rust``; else Python (shared cache/index)."""

    if ppa_engine() == "rust":
        try:
            import archive_crate

            return list(archive_crate.resolve_person_batch(vault_path, batch_ids))
        except (ImportError, Exception) as e:
            warnings.warn(
                f"PPA: falling back to Python for resolve_person_batch — archive_crate error: {e}",
                stacklevel=2,
            )
    return resolve_person_batch_python(
        vault_path,
        batch_ids,
        cache=cache,
        people_index=people_index,
    )


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

PERSON_RESOLVABLE_CARD_TYPES = DERIVED_ENTITY_CARD_TYPES | frozenset(
    {
        "finance",
    }
)


def iter_derived_card_dicts(
    vault_path: str,
    *,
    card_types: frozenset[str] | None = None,
) -> list[dict[str, Any]]:
    """Load frontmatter dicts for card types that feed entity resolution.

    *card_types* defaults to ``PERSON_RESOLVABLE_CARD_TYPES`` (derived entity types + finance).

    When ``PPA_ENGINE=rust`` and a tier-2 cache exists, reads frontmatter directly from SQLite
    via ``archive_crate.frontmatter_dicts_from_cache`` — no per-note file I/O.
    """
    types_set = card_types or PERSON_RESOLVABLE_CARD_TYPES
    vault = Path(vault_path)
    if ppa_engine() == "rust":
        from archive_vault.vault import _tier2_cache_path

        cache_path = _tier2_cache_path(vault)
        if cache_path is not None:
            try:
                import archive_crate

                rows = archive_crate.frontmatter_dicts_from_cache(
                    str(cache_path), types=list(types_set),
                )
                out: list[dict[str, Any]] = []
                for row in rows:
                    fm = dict(row["frontmatter"])
                    fm["_rel_path"] = row["rel_path"]
                    out.append(fm)
                return out
            except Exception:
                pass

    out = []
    for note in iter_parsed_notes(vault_path):
        t = note.frontmatter.get("type")
        if t in types_set:
            fm = dict(note.frontmatter)
            fm["_rel_path"] = note.rel_path.as_posix()
            out.append(fm)
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
    for note in iter_parsed_notes_for_card_types(vault_path, frozenset({"place", "organization"})):
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

    for note in iter_parsed_notes_for_card_types(vault_path, frozenset({"place", "organization"})):
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


def _entity_mention_jsonl_paths(staging_root: Path) -> list[Path]:
    return sorted(staging_root.glob("*/entity_mentions.jsonl"))


def load_entity_mention_dicts(paths: list[Path]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in paths:
        if not p.is_file():
            log.warning("entity mentions file missing: %s", p)
            continue
        with p.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    log.warning("skip bad JSONL line in %s: %s", p, exc)
                    continue
                if isinstance(row, dict):
                    out.append(row)
    return out


def _city_from_entity_context(ctx: Any) -> str:
    if not isinstance(ctx, dict):
        return ""
    c = str(ctx.get("city") or "").strip()
    if c:
        return c
    return _city_from_address(str(ctx.get("address") or ""))


def _place_tuple_from_entity_mention(m: dict[str, Any]) -> tuple[str, str]:
    name = str(m.get("raw_text") or "").strip()
    ctx = m.get("context") or {}
    city = _city_from_entity_context(ctx)
    return (name, city)


def _org_domain_from_entity_mention(m: dict[str, Any]) -> str:
    ctx = m.get("context") or {}
    if isinstance(ctx, dict):
        d = str(ctx.get("domain") or "").strip()
        if d:
            return _registrable_domain(d)
    raw = str(m.get("raw_text") or "").strip()
    return _registrable_domain(_service_to_domain(raw))


def run_entity_resolution(
    vault_path: str,
    *,
    entity_filter: str = "all",
    dry_run: bool = False,
    report_dir: str = "",
    entity_mentions_staging_root: Path | None = None,
    person_mentions_out: Path | None = None,
) -> dict[str, Any]:
    """Run place/org/person resolution passes.

    When *entity_mentions_staging_root* is set (typically ``{run_dir}/staging``), load
    ``*/entity_mentions.jsonl`` from Phase 2.875 enrichment: merge place/org seeds into
    resolvers and optionally write all person rows to *person_mentions_out*.
    """
    jsonl_paths: list[Path] = []
    if entity_mentions_staging_root is not None:
        jsonl_paths = _entity_mention_jsonl_paths(entity_mentions_staging_root)
        if not jsonl_paths:
            log.warning("no entity_mentions.jsonl under %s", entity_mentions_staging_root)

    mentions: list[dict[str, Any]] = load_entity_mention_dicts(jsonl_paths) if jsonl_paths else []

    if dry_run:
        out: dict[str, Any] = {"dry_run": True}
        # Full vault scan for derived-card estimates is very slow on multi-million-file vaults;
        # skip when the caller only needs JSONL staging stats (Phase 3 --staging-root dry-run).
        if entity_mentions_staging_root is None:
            out.update(estimate_entity_resolution_candidates(vault_path))
        else:
            out["derived_card_estimate_skipped"] = True
        if mentions:
            counts = {"person": 0, "place": 0, "organization": 0}
            for m in mentions:
                et = str(m.get("entity_type") or "").lower()
                if et in counts:
                    counts[et] += 1
            out["entity_mentions_jsonl"] = {
                "files": len(jsonl_paths),
                "rows": len(mentions),
                **counts,
            }
        return out

    cards = iter_derived_card_dicts(vault_path)

    extra_place_seeds: list[tuple[str, str]] | None = None
    extra_domains: set[str] | None = None
    person_rows_written = 0

    if mentions:
        extra_place_seeds = []
        extra_domains = set()
        person_records: list[dict[str, Any]] = []
        for m in mentions:
            et = str(m.get("entity_type") or "").lower()
            if et == "person":
                person_records.append(m)
            elif et == "place" and entity_filter in ("place", "all"):
                nm, city = _place_tuple_from_entity_mention(m)
                if nm:
                    extra_place_seeds.append((nm, city))
            elif et == "organization" and entity_filter in ("org", "all"):
                dom = _org_domain_from_entity_mention(m)
                if dom:
                    extra_domains.add(dom)

        if person_records:
            po = person_mentions_out
            if po is None:
                po = Path("_artifacts/_staging-enrichment/person_mentions.jsonl")
            po.parent.mkdir(parents=True, exist_ok=True)
            with po.open("w", encoding="utf-8") as f:
                for pr in person_records:
                    f.write(json.dumps(pr, ensure_ascii=False) + "\n")
            person_rows_written = len(person_records)

        if not extra_place_seeds:
            extra_place_seeds = None
        if extra_domains is not None and not extra_domains:
            extra_domains = None

    merged = EntityResolutionResult()
    if entity_filter in ("place", "all"):
        pr = PlaceResolver(vault_path).resolve(
            cards,
            dry_run=False,
            extra_place_seeds=extra_place_seeds,
        )
        merged.places_created += pr.places_created
        merged.places_merged += pr.places_merged
        merged.errors.extend(pr.errors)
    if entity_filter in ("org", "all"):
        or_ = OrgResolver(vault_path).resolve(
            cards,
            sender_domains=None,
            dry_run=False,
            extra_domains=extra_domains,
        )
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

    result: dict[str, Any] = {
        "dry_run": False,
        "places_created": merged.places_created,
        "places_merged": merged.places_merged,
        "orgs_created": merged.orgs_created,
        "orgs_merged": merged.orgs_merged,
        "persons_linked": merged.persons_linked,
        "person_merges": merged.person_merges,
        "person_conflicts": merged.person_conflicts,
        "person_no_match": merged.person_no_match,
        "errors": merged.errors,
        "validation_errors": validation_errors,
    }
    if mentions:
        result["entity_mentions_jsonl_files"] = len(jsonl_paths)
        result["entity_mention_rows"] = len(mentions)
        result["person_mentions_staged"] = person_rows_written
    return result


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
    person_merges: int = 0
    person_conflicts: int = 0
    person_no_match: int = 0
    errors: list[str] = field(default_factory=list)


class PlaceResolver:
    """Cluster place references from derived cards into PlaceCards."""

    def __init__(self, vault_path: str) -> None:
        self.vault_path = vault_path

    def _existing_places(self) -> dict[tuple[str, str], tuple[str, dict[str, Any]]]:
        """Map (normalized_name, city_lower) -> (rel_path_str, frontmatter)."""
        found: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
        for note in iter_parsed_notes_for_card_types(self.vault_path, frozenset({"place"})):
            fm = note.frontmatter
            if fm.get("type") != "place":
                continue
            name = _normalize_place_name(str(fm.get("name") or ""))
            city = str(fm.get("city") or "").strip().lower()
            key = (name, city)
            found[key] = (str(note.rel_path), fm)
        return found

    def resolve(
        self,
        derived_cards: list[dict[str, Any]],
        *,
        dry_run: bool = False,
        extra_place_seeds: list[tuple[str, str]] | None = None,
    ) -> EntityResolutionResult:
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

        for raw_name, city in extra_place_seeds or []:
            nk = _normalize_place_name(raw_name)
            ck = city.strip().lower()
            if not nk:
                continue
            clusters.setdefault((nk, ck), raw_name)

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
        for note in iter_parsed_notes_for_card_types(self.vault_path, frozenset({"organization"})):
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
        extra_domains: set[str] | None = None,
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

        for d in extra_domains or set():
            rd = _registrable_domain(d)
            if rd:
                domains.add(rd)
                card_types_by_domain.setdefault(rd, "")

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

        batch_ids: list[dict[str, Any]] = []
        for card in derived_cards:
            for raw in _person_names_from_derived_card(card):
                batch_ids.append({"name": raw})

        if not batch_ids:
            return result

        rr_list = _resolve_person_batch(
            self.vault_path,
            batch_ids,
            cache=cache,
            people_index=index,
        )
        for rr in rr_list:
            if rr.action == "merge":
                result.person_merges += 1
                if rr.wikilink:
                    result.persons_linked += 1
            elif rr.action == "conflict":
                result.person_conflicts += 1
            else:
                result.person_no_match += 1

        return result


def _person_names_from_derived_card(card: dict[str, Any]) -> list[str]:
    names: list[str] = []
    ct = str(card.get("type") or "")
    if ct == "medical_record":
        pn = str(card.get("provider_name") or "").strip()
        if pn:
            names.append(pn)
    elif ct == "ride":
        pass
    elif ct == "finance":
        cp = str(card.get("counterparty") or "").strip()
        if cp:
            names.append(cp)
    for key in ("passengers", "guests"):
        for item in card.get(key) or []:
            if isinstance(item, str) and item.strip():
                names.append(item.strip())
    return names


def apply_person_links(
    vault_path: str,
    rows: list[tuple[str, ResolveResult]],
    *,
    dry_run: bool = False,
    run_id: str = "",
) -> dict[str, Any]:
    """Write ``people`` wikilinks onto vault cards for merge :class:`ResolveResult` rows."""

    from archive_vault.provenance import ProvenanceEntry
    from archive_vault.schema import (validate_card_permissive,
                                      validate_card_strict)
    from archive_vault.vault import read_note, write_card

    today = date.today().isoformat()
    cards_linked = 0
    wikilinks_added = 0
    cards_already_linked = 0
    skipped = 0

    for rel_path, rr in rows:
        if rr.action != "merge" or not rr.wikilink:
            skipped += 1
            continue
        fm, body, prov = read_note(vault_path, rel_path)
        card = validate_card_permissive(fm)
        data = card.model_dump(mode="python")
        people = list(data.get("people") or [])
        if rr.wikilink in people:
            cards_already_linked += 1
            continue
        if dry_run:
            wikilinks_added += 1
            cards_linked += 1
            continue
        people.append(rr.wikilink)
        data["people"] = people
        data["updated"] = today
        updated = validate_card_strict(data)
        new_prov = dict(prov)
        new_prov["people"] = ProvenanceEntry(
            source="entity_resolution",
            date=today,
            method="deterministic",
            model=run_id or "",
        )
        write_card(vault_path, rel_path, updated, body, new_prov)
        wikilinks_added += 1
        cards_linked += 1

    return {
        "cards_linked": cards_linked,
        "wikilinks_added": wikilinks_added,
        "cards_already_linked": cards_already_linked,
        "skipped_non_merge": skipped,
        "dry_run": dry_run,
    }


_DISAMBIGUATE_SYSTEM = """\
You are an identity resolution assistant for a personal archive. A transaction \
or service card has a person-like name (counterparty, driver, provider) that \
partially matches one or more PersonCards in the vault, but the match is \
ambiguous (confidence is between the conflict and merge thresholds).

Your job: decide which PersonCard, if any, is the correct match.

Reply with ONLY a JSON object:
{"choice": <1-based index of the matching person, or 0 if none match>, "reason": "<one sentence>"}
"""


def _build_disambiguate_prompt(
    card_fm: dict[str, Any],
    card_rel: str,
    candidate_wikilink: str,
    candidate_data: dict[str, Any] | None,
    reasons: list[str],
) -> str:
    ct = str(card_fm.get("type") or "")
    lines = [
        "## Card to resolve",
        f"- type: {ct}",
        f"- rel_path: {card_rel}",
    ]
    for k in ("counterparty", "driver_name", "provider_name", "summary", "amount", "currency",
              "service", "vendor", "restaurant", "airline"):
        v = card_fm.get(k)
        if v:
            lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Candidate PersonCard")
    lines.append(f"- wikilink: {candidate_wikilink}")
    if candidate_data:
        for k in ("summary", "first_name", "last_name", "emails", "phones", "companies",
                   "title", "aliases"):
            v = candidate_data.get(k)
            if v:
                lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append(f"## Match reasons: {', '.join(reasons)}")
    lines.append("")
    lines.append("Is this person the correct match for the card? Reply JSON only.")
    return "\n".join(lines)


def disambiguate_conflicts(
    vault_path: str,
    conflicts: list[tuple[str, ResolveResult]],
    *,
    provider: str = "",
    model: str = "",
    dry_run: bool = True,
) -> dict[str, Any]:
    """LLM disambiguation for conflict rows.

    Without a provider, conflicts are logged but not resolved (safe default).
    When a provider is set, each conflict is sent to the LLM for a yes/no verdict.
    """

    if not conflicts:
        return {
            "conflicts_resolved": 0,
            "conflicts_skipped": 0,
            "llm_calls": 0,
            "llm_tokens": 0,
            "dry_run": dry_run,
            "conflict_details": [],
        }

    details = []
    for rel_path, rr in conflicts:
        details.append({
            "rel_path": rel_path,
            "wikilink": rr.wikilink,
            "confidence": rr.confidence,
            "reasons": rr.reasons,
        })

    if not provider:
        log.info("disambiguate_conflicts: %d conflicts logged (no provider configured)", len(conflicts))
        return {
            "conflicts_resolved": 0,
            "conflicts_skipped": len(conflicts),
            "llm_calls": 0,
            "llm_tokens": 0,
            "dry_run": dry_run,
            "conflict_details": details,
        }

    from archive_vault.llm_provider import GeminiProvider, OllamaProvider
    from archive_vault.vault import read_note

    kind = (provider or "ollama").strip().lower()
    use_model = model or ("gemini-2.5-flash-lite" if kind == "gemini" else "gemma4:e4b")
    if kind == "gemini":
        llm = GeminiProvider(model=use_model)
    else:
        llm = OllamaProvider(model=use_model)

    resolved = 0
    skipped = 0
    llm_calls = 0
    total_tokens = 0
    resolved_merges: list[tuple[str, ResolveResult]] = []

    for rel_path, rr in conflicts:
        if not rr.wikilink:
            skipped += 1
            continue

        try:
            card_fm, _, _ = read_note(vault_path, rel_path)
        except Exception:
            card_fm = {}

        person_slug = rr.wikilink.strip("[]").split("/")[-1] if rr.wikilink else ""
        candidate_data: dict[str, Any] | None = None
        if person_slug:
            try:
                from archive_vault.schema import validate_card_permissive
                person_fm, _, _ = read_note(vault_path, f"People/{person_slug}.md")
                candidate_data = validate_card_permissive(person_fm).model_dump(mode="python")
            except Exception:
                pass

        prompt = _build_disambiguate_prompt(card_fm, rel_path, rr.wikilink or "", candidate_data, rr.reasons)
        messages = [
            {"role": "system", "content": _DISAMBIGUATE_SYSTEM},
            {"role": "user", "content": prompt},
        ]

        try:
            resp = llm.chat_json(messages, model=use_model, temperature=0.0, seed=42, max_tokens=256)
            llm_calls += 1
            total_tokens += resp.prompt_tokens + resp.completion_tokens

            parsed = resp.parsed_json
            if parsed and isinstance(parsed.get("choice"), int) and parsed["choice"] >= 1:
                resolved += 1
                resolved_merges.append((rel_path, ResolveResult(
                    action="merge",
                    wikilink=rr.wikilink,
                    confidence=rr.confidence,
                    reasons=rr.reasons + [f"llm_disambiguated:{parsed.get('reason', '')}"],
                )))
                log.info("disambiguate: %s → %s (LLM choice=%d)", rel_path, rr.wikilink, parsed["choice"])
            else:
                skipped += 1
                reason = parsed.get("reason", "") if parsed else resp.content[:100]
                log.info("disambiguate: %s → no match (LLM: %s)", rel_path, reason)
        except Exception as exc:
            log.warning("disambiguate LLM error for %s: %s", rel_path, exc)
            skipped += 1

    if not dry_run and resolved_merges:
        apply_person_links(vault_path, resolved_merges, dry_run=False, run_id="disambiguate-conflicts")

    return {
        "conflicts_resolved": resolved,
        "conflicts_skipped": skipped,
        "llm_calls": llm_calls,
        "llm_tokens": total_tokens,
        "dry_run": dry_run,
        "conflict_details": details,
    }


def run_person_linking(
    vault_path: str,
    *,
    dry_run: bool = False,
    report_dir: str = "",
    run_id: str = "",
    card_types: frozenset[str] | None = None,
    conflict_provider: str = "",
    conflict_model: str = "",
) -> dict[str, Any]:
    """Resolve person-like strings on derived cards; optionally apply merges; write JSON report.

    *card_types* limits resolution to specific card types (default: all in
    ``PERSON_RESOLVABLE_CARD_TYPES`` — derived entity types + finance).
    *conflict_provider* / *conflict_model* enable LLM disambiguation for conflicts.
    """

    cards = iter_derived_card_dicts(vault_path, card_types=card_types)

    cache = IdentityCache(vault_path)
    index = PersonIndex(vault_path, preload=True)
    batch_ids: list[dict[str, Any]] = []
    meta: list[str] = []
    for card in cards:
        rel = str(card.get("_rel_path") or "").strip()
        for raw in _person_names_from_derived_card(card):
            batch_ids.append({"name": raw})
            meta.append(rel)

    merges = conflicts = no_match = linked = 0
    rr_list: list[ResolveResult] = []
    if batch_ids:
        rr_list = _resolve_person_batch(
            vault_path,
            batch_ids,
            cache=cache,
            people_index=index,
        )
        for rr in rr_list:
            if rr.action == "merge":
                merges += 1
                if rr.wikilink:
                    linked += 1
            elif rr.action == "conflict":
                conflicts += 1
            else:
                no_match += 1

    apply_payload: dict[str, Any] | None = None
    if not dry_run and rr_list:
        rows = [
            (rel, rr)
            for rel, rr in zip(meta, rr_list)
            if rel and rr.action == "merge" and rr.wikilink
        ]
        apply_payload = apply_person_links(
            vault_path,
            rows,
            dry_run=False,
            run_id=run_id or "link-persons",
        )

    conflict_payload: dict[str, Any] | None = None
    conflict_rows = [
        (rel, rr)
        for rel, rr in zip(meta, rr_list)
        if rel and rr.action == "conflict"
    ] if rr_list else []
    if conflict_rows:
        conflict_payload = disambiguate_conflicts(
            vault_path,
            conflict_rows,
            provider=conflict_provider,
            model=conflict_model,
            dry_run=dry_run,
        )

    out: dict[str, Any] = {
        "vault_path": vault_path,
        "derived_cards": len(cards),
        "card_types_filter": sorted(card_types) if card_types else None,
        "person_merges": merges,
        "person_conflicts": conflicts,
        "person_no_match": no_match,
        "persons_linked": linked,
        "dry_run": dry_run,
    }
    if apply_payload is not None:
        out["apply_person_links"] = apply_payload
    if conflict_payload is not None:
        out["disambiguate_conflicts"] = conflict_payload

    rd = str(report_dir or "").strip()
    if rd:
        p = Path(rd)
        p.mkdir(parents=True, exist_ok=True)
        (p / "person-linking-report.json").write_text(
            json.dumps(out, indent=2, default=str) + "\n",
            encoding="utf-8",
        )

    return out
