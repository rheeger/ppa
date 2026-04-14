"""Seed importer for canonical local People notes."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from archive_vault.config import load_config
from archive_vault.identity import IdentityCache
from archive_vault.schema import PersonCard
from archive_vault.sync_state import load_sync_state, update_cursor
from archive_vault.uid import generate_uid
from archive_vault.vault import write_card
from archive_vault.yaml_parser import parse_frontmatter

from .base import BaseAdapter, IngestResult, deterministic_provenance

DEFAULT_SOURCE_DIR = Path.home() / "Archive" / "seed" / "hf-archives-seed-20260307-235127" / "People"
SOURCE_MAP = {
    "vcf": "contacts.apple",
    "apple": "contacts.apple",
    "contacts.apple": "contacts.apple",
    "google": "contacts.google",
    "contacts.google": "contacts.google",
    "manual": "manual",
}
KNOWN_LEGACY_FIELDS = {
    "uid",
    "type",
    "source",
    "source_id",
    "created",
    "updated",
    "summary",
    "tags",
    "people",
    "orgs",
    "emails",
    "email",
    "phones",
    "phone",
    "birthday",
    "company",
    "title",
    "linkedin",
    "twitter",
    "github",
    "description",
    "relationship",
    "relationship_type",
    "family",
    "schema_v",
}


def _as_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    return str(value).strip()


def _as_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [item.strip() for item in map(str, value) if str(item).strip()]
    item = _as_string(value)
    return [item] if item else []


def _normalized_sources(value: Any) -> list[str]:
    sources = _as_list(value)
    normalized: list[str] = []
    seen: set[str] = set()
    for source in sources or ["manual"]:
        mapped = SOURCE_MAP.get(source.lower(), source.lower())
        if mapped and mapped not in seen:
            seen.add(mapped)
            normalized.append(mapped)
    return normalized or ["manual"]


def _legacy_metadata_block(frontmatter: dict[str, Any]) -> str:
    extras = {key: frontmatter[key] for key in sorted(frontmatter) if key not in KNOWN_LEGACY_FIELDS}
    if not extras:
        return ""
    lines = ["## Legacy Metadata", ""]
    for key, value in extras.items():
        lines.append(f"- `{key}`: `{json.dumps(value, ensure_ascii=False, sort_keys=True)}`")
    return "\n".join(lines)


class SeedPeopleAdapter(BaseAdapter):
    source_id = "seed-people"

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        source_dir: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        directory = Path(source_dir).expanduser() if source_dir else DEFAULT_SOURCE_DIR
        rows: list[dict[str, Any]] = []
        for path in sorted(directory.glob("*.md")):
            frontmatter, body = parse_frontmatter(path.read_text(encoding="utf-8"))
            rows.append(
                {
                    "frontmatter": frontmatter,
                    "body": body.strip(),
                    "path": path,
                }
            )
        return rows

    def get_cursor_key(self, **kwargs) -> str:
        return self.source_id

    def ingest(self, vault_path: str, dry_run: bool = False, **kwargs) -> IngestResult:
        vault = Path(vault_path)
        people_dir = vault / "People"
        if any(people_dir.glob("*.md")):
            return super().ingest(vault_path, dry_run=dry_run, **kwargs)

        result = IngestResult()
        config = load_config(vault)
        cursor_key = self.get_cursor_key(**kwargs)
        cursor = load_sync_state(vault).get(cursor_key, {})
        if not isinstance(cursor, dict):
            cursor = {}
        identity_cache = IdentityCache(vault)
        items = self.fetch(str(vault), cursor, config=config, **kwargs)
        processed_successfully = 0

        for index, item in enumerate(items):
            try:
                card, provenance, body = self.to_card(item)
                rel_path = self._person_rel_path(vault, card)
                if not dry_run:
                    write_card(vault, rel_path, card, body=body, provenance=provenance)
                    identity_cache.upsert(f"[[{Path(rel_path).stem}]]", self._person_identifiers(card))
                result.created += 1
                processed_successfully += 1
            except Exception as exc:
                result.errors.append(f"item {index}: {exc}")

        if not dry_run:
            identity_cache.flush()

        update_cursor(
            vault,
            cursor_key,
            {
                **cursor,
                "last_sync": datetime.now().isoformat(),
                "seen": len(items),
                "processed": processed_successfully,
                "last_processed_index": max(processed_successfully - 1, -1),
                "created": result.created,
                "merged": result.merged,
                "conflicted": result.conflicted,
                "skipped": result.skipped,
                "errors": len(result.errors),
            },
        )
        return result

    def to_card(self, item: dict[str, Any]):
        frontmatter = dict(item["frontmatter"])
        body = str(item.get("body", "")).strip()
        sources = _normalized_sources(frontmatter.get("source"))
        primary_source = sources[0]

        tags = _as_list(frontmatter.get("tags"))
        if bool(frontmatter.get("family")) and "family" not in tags:
            tags.append("family")

        relationship_type = _as_string(frontmatter.get("relationship_type")) or _as_string(
            frontmatter.get("relationship")
        )
        emails = _as_list(frontmatter.get("emails")) or _as_list(frontmatter.get("email"))
        phones = _as_list(frontmatter.get("phones")) or _as_list(frontmatter.get("phone"))
        summary = _as_string(frontmatter.get("summary")) or (
            emails[0] if emails else Path(item["path"]).stem.replace("-", " ").title()
        )
        source_id = _as_string(frontmatter.get("source_id")) or (emails[0] if emails else summary)
        uid = _as_string(frontmatter.get("uid")) or generate_uid("person", primary_source, source_id)
        created = _as_string(frontmatter.get("created")) or date.today().isoformat()
        updated = _as_string(frontmatter.get("updated")) or created

        card = PersonCard(
            uid=uid,
            type="person",
            source=sources,
            source_id=source_id,
            created=created,
            updated=updated,
            summary=summary,
            emails=emails,
            phones=phones,
            birthday=_as_string(frontmatter.get("birthday")),
            company=_as_string(frontmatter.get("company")),
            title=_as_string(frontmatter.get("title")),
            linkedin=_as_string(frontmatter.get("linkedin")),
            twitter=_as_string(frontmatter.get("twitter")),
            github=_as_string(frontmatter.get("github")),
            description=_as_string(frontmatter.get("description")),
            relationship_type=relationship_type,
            tags=tags,
            people=_as_list(frontmatter.get("people")),
            orgs=_as_list(frontmatter.get("orgs")),
        )

        legacy_block = _legacy_metadata_block(frontmatter)
        if legacy_block:
            body = f"{body}\n\n{legacy_block}".strip() if body else legacy_block

        provenance = deterministic_provenance(card, primary_source)
        return card, provenance, body
