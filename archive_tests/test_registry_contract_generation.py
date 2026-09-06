"""P06-C: live registry export must match the checked-in native artifact."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from archive_cli.card_registry import (
    CARD_TYPE_REGISTRATIONS,
    dump_registry_json,
    materializer_registry_payload,
    validate_card_type_registrations,
)
from archive_cli.materializer import _materialize_row
from archive_cli.scanner import CanonicalRow
from archive_cli.store import DefaultArchiveStore
from archive_vault.card_contracts import CARD_TYPE_SPECS, validate_card_type_specs
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import CARD_TYPES, PersonCard, validate_card_permissive
from archive_vault.vault import read_note_file, write_card

PPA_ROOT = Path(__file__).resolve().parents[1]
CHECKED_IN_REGISTRY = PPA_ROOT / "archive_crate" / "materializer_registry.json"
REGISTRY_FILE_KEYS = {"registry_version", "projection_registry_version", "card_types"}
CARD_TYPE_KEYS = {
    "card_type",
    "edge_rules",
    "person_edge_type",
    "projection_table",
    "quality_critical_fields",
    "shared_typed_columns",
    "typed_columns",
}
COLUMN_KEYS = {"default", "indexed", "name", "nullable", "source_field", "sql_type", "value_mode"}
EDGE_KEYS = {
    "edge_type",
    "field_name",
    "multi",
    "source_fields",
    "target",
    "target_card_type",
    "target_lookup_field",
}


class StaleRegistryError(ValueError):
    """Checked-in native registry does not match a fresh export."""


def unknown_registry_fields(payload: dict) -> list[str]:
    hits: list[str] = []
    extra_root = set(payload) - REGISTRY_FILE_KEYS
    hits.extend(f"registry:{key}" for key in sorted(extra_root))
    for card in payload.get("card_types") or []:
        card_type = str(card.get("card_type") or "?")
        hits.extend(f"{card_type}:{key}" for key in sorted(set(card) - CARD_TYPE_KEYS))
        for column in list(card.get("shared_typed_columns") or []) + list(card.get("typed_columns") or []):
            hits.extend(f"{card_type}.column:{key}" for key in sorted(set(column) - COLUMN_KEYS))
        for rule in card.get("edge_rules") or []:
            hits.extend(f"{card_type}.edge:{key}" for key in sorted(set(rule) - EDGE_KEYS))
    return hits


def compare_registry_export(generated: dict, checked: dict) -> None:
    gen_text = dump_registry_json(generated)
    chk_text = dump_registry_json(checked)
    if gen_text == chk_text:
        return
    gen_types = {row["card_type"] for row in generated.get("card_types") or []}
    chk_types = {row["card_type"] for row in checked.get("card_types") or []}
    details = []
    if gen_types != chk_types:
        details.append(f"types added={sorted(gen_types - chk_types)} removed={sorted(chk_types - gen_types)}")
    if generated.get("projection_registry_version") != checked.get("projection_registry_version"):
        details.append("projection_registry_version")
    digest_gen = hashlib.sha256(gen_text.encode()).hexdigest()[:12]
    digest_chk = hashlib.sha256(chk_text.encode()).hexdigest()[:12]
    raise StaleRegistryError(
        "stale materializer registry: "
        + ("; ".join(details) or "canonical JSON differs")
        + f" generated={digest_gen} checked_in={digest_chk}"
    )


def test_card_contracts_and_registrations_validate() -> None:
    validate_card_type_specs()
    validate_card_type_registrations()
    assert set(CARD_TYPES) == set(CARD_TYPE_SPECS)
    assert {reg.card_type for reg in CARD_TYPE_REGISTRATIONS} == set(CARD_TYPES)


def test_export_matches_checked_in_artifact() -> None:
    generated = materializer_registry_payload()
    checked = json.loads(CHECKED_IN_REGISTRY.read_text(encoding="utf-8"))
    unknown = unknown_registry_fields(generated)
    assert unknown == []
    compare_registry_export(generated, checked)
    assert (
        hashlib.sha256(dump_registry_json(generated).encode()).hexdigest()
        == hashlib.sha256(dump_registry_json(checked).encode()).hexdigest()
    )


def test_stale_export_fails_with_named_diagnostic() -> None:
    generated = materializer_registry_payload()
    stale = json.loads(json.dumps(generated))
    stale["card_types"] = [row for row in stale["card_types"] if row["card_type"] != "person"]
    with pytest.raises(StaleRegistryError, match="stale materializer registry") as exc:
        compare_registry_export(generated, stale)
    assert "person" in str(exc.value)


def test_unknown_registry_field_fails() -> None:
    payload = materializer_registry_payload()
    payload["card_types"][0]["unexpected_field"] = True
    hits = unknown_registry_fields(payload)
    assert hits
    assert "unexpected_field" in hits[0]


class _Index:
    def __init__(self, mapping: dict[str, str]):
        self.mapping = mapping

    def read_path_for_uid(self, uid: str) -> str | None:
        return self.mapping.get(uid)

    def search(self, query: str, limit: int = 20, **_kwargs):
        return [
            {
                "card_uid": uid,
                "rel_path": rel,
                "summary": query,
                "type": "person",
                "source": ["test"],
                "required_sources": ["test"],
                "lineage_complete": True,
            }
            for uid, rel in list(self.mapping.items())[:limit]
        ]

    def query_cards(self, **kwargs):
        return self.search("", limit=int(kwargs.get("limit", 20) or 20))

    def graph(self, note_path: str, hops: int = 2):
        return {note_path: []}

    def status(self):
        return {"card_count": len(self.mapping)}

    def bootstrap(self):
        return {"ok": True}

    def rebuild(self):
        return {"cards": len(self.mapping)}


def test_synthetic_card_write_materialize_query(tmp_path: Path) -> None:
    uid = "hfa-person-regtrace"
    vault = tmp_path / "vault"
    (vault / "People").mkdir(parents=True)
    card = PersonCard(
        uid=uid,
        type="person",
        source=["test"],
        source_id=f"{uid}@example.com",
        created="2026-09-06",
        updated="2026-09-06",
        summary="registry-tracer",
    )
    write_card(
        vault,
        "People/card.md",
        card,
        body="REGISTRY-TRACER",
        provenance={"summary": ProvenanceEntry("test", "2026-09-06", "deterministic")},
    )
    note = read_note_file(vault / "People" / "card.md", vault_root=vault)
    parsed = validate_card_permissive(dict(note.frontmatter))
    row = CanonicalRow(rel_path="People/card.md", frontmatter=dict(note.frontmatter), card=parsed)
    batch = _materialize_row(
        row,
        vault_root=str(vault),
        slug_map={"card": "People/card.md"},
        path_to_uid={"People/card.md": uid},
        person_lookup={},
        batch_id="p06c",
    )
    spec = CARD_TYPE_SPECS["person"]
    typed_rows = batch.rows_for(spec.typed_projection)
    assert typed_rows, f"missing typed projection {spec.typed_projection}"
    assert uid in {str(item[0]) for item in typed_rows}
    store = DefaultArchiveStore(vault=vault, index=_Index({uid: "People/card.md"}))
    try:
        read = store.runtime.read(uid)
        query = store.runtime.query(type_filter="person", limit=5)
        assert read["found"] is True
        assert "REGISTRY-TRACER" in str(read.get("content"))
        assert query["rows"][0]["card_uid"] == uid
    finally:
        store.close()
