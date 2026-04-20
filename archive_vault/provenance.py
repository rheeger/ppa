"""Field-level provenance tracking."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any

from archive_vault.schema import DETERMINISTIC_ONLY

_PROVENANCE_RE = re.compile(r"\n?<!-- provenance\n(.*?)\n-->\s*", re.DOTALL)
PROVENANCE_EXEMPT_FIELDS = frozenset(
    {
        "uid",
        "type",
        "source",
        "source_id",
        "created",
        "updated",
        "people",
        "orgs",
        "source_email",
        "extraction_confidence",
    }
)


@dataclass
class ProvenanceEntry:
    source: str
    date: str
    method: str
    model: str = ""
    enrichment_version: int = 0
    input_hash: str = ""
    # Append-only forensic trail of prior writes to this field. Each entry is
    # a serialized ``ProvenanceEntry`` minus its own ``prior``. Newest-first.
    # Capped to MAX_PROVENANCE_HISTORY items so cards don't grow unbounded.
    prior: list[dict[str, Any]] | None = None


MAX_PROVENANCE_HISTORY: int = 5


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
        prior_raw = payload.get("prior")
        prior: list[dict[str, Any]] | None = None
        if isinstance(prior_raw, list) and prior_raw:
            prior = [dict(p) for p in prior_raw if isinstance(p, dict)]
        entries[field_name.strip()] = ProvenanceEntry(
            source=str(payload.get("source", "")),
            date=str(payload.get("date", "")),
            method=str(payload.get("method", "")),
            model=str(payload.get("model", "")),
            enrichment_version=int(payload.get("enrichment_version", 0) or 0),
            input_hash=str(payload.get("input_hash", "")),
            prior=prior,
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
        # Drop empty ``prior`` to keep card bodies small for the common path.
        if not payload.get("prior"):
            payload.pop("prior", None)
        lines.append(f"{field_name}: {json.dumps(payload, sort_keys=True, separators=(',', ': '))}")
    lines.append("-->")
    block = "\n".join(lines)
    if stripped:
        return f"{stripped}\n\n{block}"
    return block


def _entry_to_history_dict(entry: ProvenanceEntry) -> dict[str, Any]:
    """Serialize an entry for the ``prior`` chain, excluding its own ``prior``."""
    return {
        "source": entry.source,
        "date": entry.date,
        "method": entry.method,
        "model": entry.model,
        "enrichment_version": entry.enrichment_version,
        "input_hash": entry.input_hash,
    }


def merge_provenance(
    existing: dict[str, ProvenanceEntry],
    incoming: dict[str, ProvenanceEntry],
) -> dict[str, ProvenanceEntry]:
    """Merge provenance maps, letting incoming entries win field-by-field.

    When ``incoming`` overwrites an ``existing`` entry for the same field,
    the existing entry is appended to the incoming entry's ``prior`` chain
    (newest-first, capped at ``MAX_PROVENANCE_HISTORY``). This preserves a
    forensic trail of prior writes — useful for backfills, re-enrichments,
    and "who wrote this field last?" investigations. Only the latest entry
    is consulted by ``validate_provenance``; ``prior`` is for humans + audits.
    """
    merged: dict[str, ProvenanceEntry] = dict(existing)
    for field_name, new_entry in incoming.items():
        old_entry = merged.get(field_name)
        if old_entry is None:
            merged[field_name] = new_entry
            continue
        # If the new entry is byte-identical to the old one, no audit signal —
        # don't grow the history chain (keeps idempotent backfills clean).
        if (
            old_entry.source == new_entry.source
            and old_entry.date == new_entry.date
            and old_entry.method == new_entry.method
            and old_entry.model == new_entry.model
            and old_entry.enrichment_version == new_entry.enrichment_version
            and old_entry.input_hash == new_entry.input_hash
        ):
            merged[field_name] = new_entry
            continue
        prior_chain: list[dict[str, Any]] = []
        prior_chain.append(_entry_to_history_dict(old_entry))
        if old_entry.prior:
            prior_chain.extend(old_entry.prior)
        prior_chain = prior_chain[:MAX_PROVENANCE_HISTORY]
        merged[field_name] = ProvenanceEntry(
            source=new_entry.source,
            date=new_entry.date,
            method=new_entry.method,
            model=new_entry.model,
            enrichment_version=new_entry.enrichment_version,
            input_hash=new_entry.input_hash,
            prior=prior_chain,
        )
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
