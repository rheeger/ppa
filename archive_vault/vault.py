"""Vault iteration and card I/O."""

from __future__ import annotations

import os
import re
import tempfile
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from archive_cli.ppa_engine import ppa_engine
from archive_vault.provenance import (
    ProvenanceEntry,
    read_provenance,
    strip_provenance,
    validate_provenance,
    write_provenance,
)
from archive_vault.schema import BaseCard, card_to_frontmatter, validate_card_strict
from archive_vault.yaml_parser import parse_frontmatter, render_card

EXCLUDED_DIRS = {"_templates", "Attachments", ".obsidian", "_meta"}
WIKILINK_RE = re.compile(r"\[\[([^\]|]+)(?:\|[^\]]*)?\]\]")
_CACHE_FILENAME = "vault-scan-cache.sqlite3"


def _tier2_cache_path(vault: Path) -> Path | None:
    """Return the tier-2 cache path if it exists, else ``None``."""
    p = vault / "_meta" / _CACHE_FILENAME
    return p if p.exists() else None


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


def _iter_note_paths_python(vault: Path) -> Iterator[Path]:
    """Python os.walk — exclusion rules match Obsidian vault layout."""

    for root, dirs, files in os.walk(vault):
        dirs[:] = [name for name in dirs if name not in EXCLUDED_DIRS and not name.startswith(".")]
        for file_name in sorted(files):
            if file_name.startswith(".") or not file_name.endswith(".md"):
                continue
            yield (Path(root) / file_name).relative_to(vault)


def iter_note_paths(vault: str | Path) -> Iterator[Path]:
    """Yield markdown note paths in a vault, excluding metadata directories."""

    vault = Path(vault)
    if ppa_engine() == "rust":
        try:
            import archive_crate

            for s in archive_crate.walk_vault(str(vault)):
                yield Path(s)
            return
        except (ImportError, Exception) as e:
            warnings.warn(
                f"PPA: falling back to Python for iter_note_paths — archive_crate not available: {e}",
                stacklevel=2,
            )
    yield from _iter_note_paths_python(vault)


def _iter_parsed_notes_python_walk(vault: Path) -> Iterator[ParsedNoteRecord]:
    """Full vault parse using ``iter_note_paths`` (Python path when cache is unavailable)."""

    for rel_path in iter_note_paths(vault):
        path = vault / rel_path
        try:
            yield read_note_file(path, vault_root=vault)
        except FileNotFoundError:
            continue


def iter_parsed_notes_from_disk(vault: str | Path) -> Iterator[ParsedNoteRecord]:
    """Read every note from markdown files (no tier-2 SQLite shortcut). Includes provenance."""

    yield from _iter_parsed_notes_python_walk(Path(vault))


def iter_parsed_notes_for_card_types(
    vault: str | Path,
    card_types: frozenset[str] | set[str],
) -> Iterator[ParsedNoteRecord]:
    """Yield parsed notes whose ``type`` is in *card_types*.

    When ``PPA_ENGINE=rust`` and a tier-2 cache exists, reads frontmatter + body from SQLite via
    ``archive_crate.notes_from_cache`` (GIL released during read) — no per-note file I/O.
    """

    vault = Path(vault)
    types_set = frozenset(card_types)
    if not types_set:
        return

    if ppa_engine() == "rust":
        cache_path = _tier2_cache_path(vault)
        if cache_path is not None:
            try:
                import archive_crate

                rows = archive_crate.notes_from_cache(
                    str(cache_path), types=list(types_set),
                )
                for row in rows:
                    yield ParsedNoteRecord(
                        rel_path=Path(row["rel_path"]),
                        content="",
                        frontmatter=row["frontmatter"],
                        body=row.get("body", ""),
                        provenance={},
                    )
                return
            except (ImportError, Exception) as e:
                warnings.warn(
                    f"PPA: falling back to Python for iter_parsed_notes_for_card_types — archive_crate not available: {e}",
                    stacklevel=2,
                )

    for note in _iter_parsed_notes_python_walk(vault):
        if note.frontmatter.get("type") in types_set:
            yield note


def iter_notes(vault: str | Path) -> Iterator[tuple[Path, str]]:
    """Yield ``(rel_path, raw_content)`` from a vault.

    When ``PPA_ENGINE=rust`` and a tier-2 cache exists, reads from SQLite (body is the
    provenance-stripped body; ``content`` is reconstructed as frontmatter JSON + body for
    compatibility, though most callers only use the body or frontmatter separately).
    """

    vault = Path(vault)
    if ppa_engine() == "rust":
        cache_path = _tier2_cache_path(vault)
        if cache_path is not None:
            try:
                import archive_crate

                rows = archive_crate.notes_from_cache(str(cache_path))
                for row in rows:
                    body = row.get("body", "")
                    yield Path(row["rel_path"]), body
                return
            except (ImportError, Exception) as e:
                warnings.warn(
                    f"PPA: falling back to Python for iter_notes — archive_crate not available: {e}",
                    stacklevel=2,
                )

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
    return ParsedNoteRecord(
        rel_path=rel_path, content=content, frontmatter=frontmatter, body=body, provenance=provenance
    )


def iter_parsed_notes(vault: str | Path) -> Iterator[ParsedNoteRecord]:
    """Yield parsed note records from a vault.

    When ``PPA_ENGINE=rust`` and a tier-2 cache exists, reads all rows from SQLite via
    ``archive_crate.notes_from_cache`` — no per-note file I/O or YAML parsing.
    """

    vault = Path(vault)
    if ppa_engine() == "rust":
        cache_path = _tier2_cache_path(vault)
        if cache_path is not None:
            try:
                import archive_crate

                rows = archive_crate.notes_from_cache(str(cache_path))
                for row in rows:
                    yield ParsedNoteRecord(
                        rel_path=Path(row["rel_path"]),
                        content="",
                        frontmatter=row["frontmatter"],
                        body=row.get("body", ""),
                        provenance={},
                    )
                return
            except (ImportError, Exception) as e:
                warnings.warn(
                    f"PPA: falling back to Python for iter_parsed_notes — archive_crate not available: {e}",
                    stacklevel=2,
                )
    yield from _iter_parsed_notes_python_walk(vault)


def iter_email_message_notes(vault: str | Path) -> Iterator[ParsedNoteRecord]:
    """Yield parsed notes only under ``Email/``.

    When ``PPA_ENGINE=rust`` and a tier-2 cache exists, uses SQL ``WHERE card_type = 'email_message'``
    with no per-note file I/O.
    """

    vault = Path(vault)
    if ppa_engine() == "rust":
        cache_path = _tier2_cache_path(vault)
        if cache_path is not None:
            try:
                import archive_crate

                rows = archive_crate.notes_from_cache(
                    str(cache_path),
                    types=["email_message"],
                    prefix="Email/",
                )
                for row in rows:
                    yield ParsedNoteRecord(
                        rel_path=Path(row["rel_path"]),
                        content="",
                        frontmatter=row["frontmatter"],
                        body=row.get("body", ""),
                        provenance={},
                    )
                return
            except (ImportError, Exception) as e:
                warnings.warn(
                    f"PPA: falling back to Python for iter_email_message_notes — archive_crate not available: {e}",
                    stacklevel=2,
                )

    for rel_path in iter_note_paths(vault):
        if not rel_path.parts or rel_path.parts[0] != "Email":
            continue
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
    """Return the first note matching a UID.

    When ``PPA_ENGINE=rust`` and a tier-2 cache exists, uses a direct SQLite UID index lookup.
    """

    vault = Path(vault)
    if ppa_engine() == "rust":
        cache_path = _tier2_cache_path(vault)
        if cache_path is not None:
            try:
                import json
                import sqlite3
                import zlib

                conn = sqlite3.connect(str(cache_path))
                row = conn.execute(
                    "SELECT rel_path, frontmatter_json, body_compressed FROM notes WHERE uid = ? LIMIT 1",
                    (uid,),
                ).fetchone()
                conn.close()
                if row is not None:
                    rel_path = Path(row[0])
                    frontmatter = json.loads(row[1])
                    body = zlib.decompress(row[2]).decode("utf-8") if row[2] else ""
                    return rel_path, frontmatter, body, {}
                return None
            except (ImportError, Exception) as e:
                warnings.warn(
                    f"PPA: falling back to Python for read_note_by_uid — archive_crate not available: {e}",
                    stacklevel=2,
                )

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


def update_frontmatter_fields(vault_root: Path | str, rel_path: str, updates: dict[str, Any]) -> None:
    """Update specific frontmatter fields in a vault card without touching the body."""
    from io import StringIO

    from ruamel.yaml import YAML

    full_path = Path(vault_root) / rel_path
    if not full_path.is_file():
        raise FileNotFoundError(f"Card not found: {full_path}")
    text = full_path.read_text(encoding="utf-8")
    parts = text.split("---", 2)
    if len(parts) < 3:
        raise ValueError(f"No YAML frontmatter found in {rel_path}")
    yaml = YAML()
    yaml.preserve_quotes = True
    frontmatter = yaml.load(parts[1])
    if frontmatter is None:
        frontmatter = {}
    for key, value in updates.items():
        frontmatter[key] = value
    sio = StringIO()
    yaml.dump(frontmatter, sio)
    full_path.write_text(f"---\n{sio.getvalue()}---{parts[2]}", encoding="utf-8")


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
