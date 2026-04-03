"""Field-level provenance tracking."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any

from hfa.schema import DETERMINISTIC_ONLY

_PROVENANCE_RE = re.compile(r"\n?<!-- provenance\n(.*?)\n-->\s*", re.DOTALL)
PROVENANCE_EXEMPT_FIELDS = frozenset(
    {"uid", "type", "source", "source_id", "created", "updated", "people", "orgs", "source_email"}
)


@dataclass
class ProvenanceEntry:
    source: str
    date: str
    method: str
    model: str = ""
    enrichment_version: int = 0
    input_hash: str = ""


def strip_provenance(body: str) -> str:
    """Remove a provenance block from a markdown body."""

    return _PROVENANCE_RE.sub("", body).strip()


def read_provenance(body: str) -> dict[str, ProvenanceEntry]:
    """Parse a provenance block from a card body."""

    match = _PROVENANCE_RE.search(body)
    if not match:
        return {}

    entries: dict[str, ProvenanceEntry] = {}
    for line in match.group(1).splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        field_name, raw_json = line.split(":", 1)
        try:
            payload = json.loads(raw_json.strip())
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        entries[field_name.strip()] = ProvenanceEntry(
            source=str(payload.get("source", "")),
            date=str(payload.get("date", "")),
            method=str(payload.get("method", "")),
            model=str(payload.get("model", "")),
            enrichment_version=int(payload.get("enrichment_version", 0) or 0),
            input_hash=str(payload.get("input_hash", "")),
        )
    return entries


def write_provenance(body: str, prov: dict[str, ProvenanceEntry]) -> str:
    """Replace or append the provenance block in a body."""

    stripped = strip_provenance(body)
    if not prov:
        return stripped

    lines = ["<!-- provenance"]
    for field_name in sorted(prov):
        payload = asdict(prov[field_name])
        lines.append(f"{field_name}: {json.dumps(payload, sort_keys=True, separators=(',', ': '))}")
    lines.append("-->")
    block = "\n".join(lines)
    if stripped:
        return f"{stripped}\n\n{block}"
    return block


def merge_provenance(
    existing: dict[str, ProvenanceEntry],
    incoming: dict[str, ProvenanceEntry],
) -> dict[str, ProvenanceEntry]:
    """Merge provenance maps, letting incoming entries win field-by-field."""

    merged = dict(existing)
    merged.update(incoming)
    return merged


def validate_provenance(card_data: dict[str, Any], prov: dict[str, ProvenanceEntry]) -> list[str]:
    """Validate provenance coverage and deterministic-only field protections."""

    errors: list[str] = []
    for field_name, value in card_data.items():
        if field_name in PROVENANCE_EXEMPT_FIELDS:
            continue
        if value in ("", [], None, 0):
            continue
        entry = prov.get(field_name)
        if entry is None:
            errors.append(f"Field '{field_name}' is missing provenance")
            continue
        if field_name in DETERMINISTIC_ONLY and entry.method != "deterministic":
            errors.append(f"Field '{field_name}' is deterministic-only but provenance method is '{entry.method}'")
    return errors


def compute_input_hash(data: dict[str, Any]) -> str:
    """Return a deterministic SHA-256 hash for a JSON-serializable dict."""

    payload = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
