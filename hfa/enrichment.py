"""Versioned enrichment pipeline interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from hfa.provenance import (ProvenanceEntry, compute_input_hash,
                            merge_provenance)
from hfa.schema import BaseCard, validate_card_permissive, validate_card_strict
from hfa.vault import iter_notes, read_note, write_card


def _current_input_hash(card: BaseCard, body: str) -> str:
    return compute_input_hash({"card": card.model_dump(mode="python"), "body": body})


@dataclass
class EnrichmentStep(ABC):
    name: str
    version: int
    target_fields: list[str]
    method: str

    def should_run(
        self,
        card: BaseCard,
        body: str,
        provenance: dict[str, ProvenanceEntry],
        vault_path: str = "",
    ) -> bool:
        """Return True when the step should refresh any target field."""

        current_hash = _current_input_hash(card, body)
        for field_name in self.target_fields:
            entry = provenance.get(field_name)
            if entry is None:
                return True
            if entry.enrichment_version < self.version:
                return True
            if entry.input_hash and entry.input_hash != current_hash:
                return True
        return False

    @abstractmethod
    def run(
        self,
        card: BaseCard,
        body: str,
        vault_path: str,
    ) -> dict[str, tuple[Any, ProvenanceEntry]]:
        """Execute enrichment and return field updates with provenance."""


def run_enrichment_pipeline(
    vault_path: str,
    steps: list[EnrichmentStep],
    card_filter: str = "",
    dry_run: bool = False,
) -> dict[str, dict[str, int]]:
    """Run enrichment steps across the vault."""

    summary = {step.name: {"processed": 0, "skipped": 0, "errors": 0} for step in steps}
    for rel_path, _ in iter_notes(vault_path):
        frontmatter, body, provenance = read_note(vault_path, str(rel_path))
        card = validate_card_permissive(frontmatter)
        if card_filter and card.type != card_filter and card_filter not in str(rel_path):
            for step in steps:
                summary[step.name]["skipped"] += 1
            continue

        card_data = card.model_dump(mode="python")
        current_body = body
        current_provenance = dict(provenance)

        for step in steps:
            if not step.should_run(card, current_body, current_provenance, str(vault_path)):
                summary[step.name]["skipped"] += 1
                continue
            try:
                updates = step.run(card, current_body, str(vault_path))
                summary[step.name]["processed"] += 1
            except Exception:
                summary[step.name]["errors"] += 1
                continue

            if not updates:
                continue
            for field_name, (value, prov_entry) in updates.items():
                card_data[field_name] = value
                current_provenance = merge_provenance(current_provenance, {field_name: prov_entry})
            card = validate_card_strict(card_data)

        if not dry_run and card.model_dump(mode="python") != validate_card_permissive(frontmatter).model_dump(mode="python"):
            write_card(vault_path, str(rel_path), card, body=current_body, provenance=current_provenance)

    return summary
