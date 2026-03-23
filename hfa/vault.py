"""Vault iteration and card I/O."""

from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from hfa.provenance import (ProvenanceEntry, read_provenance, strip_provenance,
                            validate_provenance, write_provenance)
from hfa.schema import BaseCard, card_to_frontmatter, validate_card_strict
from hfa.yaml_parser import parse_frontmatter, render_card

EXCLUDED_DIRS = {"_templates", "Attachments", ".obsidian", "_meta"}
WIKILINK_RE = re.compile(r"\[\[([^\]|]+)(?:\|[^\]]*)?\]\]")


@dataclass(frozen=True, slots=True)
class ParsedNoteRecord:
    """Parsed note payload produced from a single file read."""

    rel_path: Path
    content: str
    frontmatter: dict
    body: str
    provenance: dict[str, ProvenanceEntry]


@dataclass(frozen=True, slots=True)
class FrontmatterNoteRecord:
    """Frontmatter-only note payload for cheap vault-wide scans."""

    rel_path: Path
    frontmatter: dict


def iter_note_paths(vault: str | Path) -> Iterator[Path]:
    """Yield markdown note paths in a vault, excluding metadata directories."""

    vault = Path(vault)
    for root, dirs, files in os.walk(vault):
        dirs[:] = [name for name in dirs if name not in EXCLUDED_DIRS and not name.startswith(".")]
        for file_name in sorted(files):
            if file_name.startswith(".") or not file_name.endswith(".md"):
                continue
            yield (Path(root) / file_name).relative_to(vault)


def iter_notes(vault: str | Path) -> Iterator[tuple[Path, str]]:
    """Yield markdown notes and raw contents from a vault."""

    vault = Path(vault)
    for rel_path in iter_note_paths(vault):
        path = vault / rel_path
        try:
            content = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        yield rel_path, content


def parse_note_content(content: str) -> tuple[dict, str, dict[str, ProvenanceEntry]]:
    """Parse raw markdown content into frontmatter, body, and provenance."""

    frontmatter, body = parse_frontmatter(content)
    provenance = read_provenance(body)
    return frontmatter, strip_provenance(body), provenance


def _read_frontmatter_prefix(path: Path) -> str:
    with path.open("r", encoding="utf-8") as handle:
        first_line = handle.readline()
        if first_line.strip() != "---":
            return ""
        lines = ["---\n"]
        for line in handle:
            lines.append(line)
            if line.strip() == "---":
                break
    return "".join(lines)


def read_note_frontmatter_file(path: str | Path, *, vault_root: str | Path | None = None) -> FrontmatterNoteRecord:
    """Read just the YAML frontmatter from a note file."""

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(str(path))
    frontmatter_prefix = _read_frontmatter_prefix(path)
    frontmatter, _ = parse_frontmatter(frontmatter_prefix)
    if vault_root is None:
        rel_path = path
    else:
        rel_path = path.relative_to(Path(vault_root))
    return FrontmatterNoteRecord(rel_path=rel_path, frontmatter=frontmatter)


def read_note_file(path: str | Path, *, vault_root: str | Path | None = None) -> ParsedNoteRecord:
    """Read and parse a note from an absolute or vault-relative path."""

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(str(path))
    content = path.read_text(encoding="utf-8")
    frontmatter, body, provenance = parse_note_content(content)
    if vault_root is None:
        rel_path = path
    else:
        rel_path = path.relative_to(Path(vault_root))
    return ParsedNoteRecord(rel_path=rel_path, content=content, frontmatter=frontmatter, body=body, provenance=provenance)


def iter_parsed_notes(vault: str | Path) -> Iterator[ParsedNoteRecord]:
    """Yield parsed note records from a vault using a single file read per note."""

    vault = Path(vault)
    for rel_path in iter_note_paths(vault):
        path = vault / rel_path
        try:
            yield read_note_file(path, vault_root=vault)
        except FileNotFoundError:
            continue


def read_note(vault: str | Path, rel_path: str) -> tuple[dict, str, dict[str, ProvenanceEntry]]:
    """Read and parse a note by relative path."""

    vault = Path(vault)
    path = vault / rel_path
    if not path.exists():
        raise FileNotFoundError(rel_path)
    parsed = read_note_file(path, vault_root=vault)
    return parsed.frontmatter, parsed.body, parsed.provenance


def read_note_by_uid(vault: str | Path, uid: str) -> tuple[Path, dict, str, dict[str, ProvenanceEntry]] | None:
    """Return the first note matching a UID."""

    for note in iter_parsed_notes(vault):
        if note.frontmatter.get("uid") == uid:
            return note.rel_path, note.frontmatter, note.body, note.provenance
    return None


def _copy_provenance_entry(entry: ProvenanceEntry) -> ProvenanceEntry:
    return ProvenanceEntry(
        source=entry.source,
        date=entry.date,
        method=entry.method,
        model=entry.model,
        enrichment_version=entry.enrichment_version,
        input_hash=entry.input_hash,
    )


def write_card(
    vault: str | Path,
    rel_path: str,
    card: BaseCard,
    body: str = "",
    provenance: dict[str, ProvenanceEntry] | None = None,
) -> Path:
    """Validate, render, and atomically write a card."""

    vault = Path(vault)
    provenance = provenance or {}
    validated = validate_card_strict(card.model_dump(mode="python"))
    frontmatter = card_to_frontmatter(validated)
    if frontmatter.get("aliases") and "aliases" not in provenance and "summary" in provenance:
        provenance = {**provenance, "aliases": _copy_provenance_entry(provenance["summary"])}
    if frontmatter.get("linkedin") and "linkedin" not in provenance and "linkedin_url" in provenance:
        provenance = {**provenance, "linkedin": _copy_provenance_entry(provenance["linkedin_url"])}
    if frontmatter.get("linkedin_url") and "linkedin_url" not in provenance and "linkedin" in provenance:
        provenance = {**provenance, "linkedin_url": _copy_provenance_entry(provenance["linkedin"])}
    errors = validate_provenance(frontmatter, provenance)
    if errors:
        raise ValueError("; ".join(errors))

    target = vault / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    rendered_body = write_provenance(body, provenance)
    content = render_card(frontmatter, rendered_body)

    fd, tmp_path = tempfile.mkstemp(prefix=f".tmp_{target.stem}_", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(tmp_path, target)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return target


def extract_wikilinks(content: str) -> list[str]:
    """Extract wikilink targets from markdown content."""

    return WIKILINK_RE.findall(content)


def find_note_by_slug(vault: str | Path, slug: str) -> Path | None:
    """Find a note by slug, preferring the People directory."""

    vault = Path(vault)
    people_path = vault / "People" / f"{slug}.md"
    if people_path.exists():
        return people_path
    for rel_path in iter_note_paths(vault):
        if rel_path.stem == slug:
            return vault / rel_path
    return None
