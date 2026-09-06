"""P06-C acceptance: registry authority and engine import boundaries."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from archive_cli.card_registry import (
    dump_registry_json,
    materializer_registry_payload,
    validate_card_type_registrations,
)
from archive_cli.materializer import _materialize_row
from archive_cli.scanner import CanonicalRow
from archive_cli.store import DefaultArchiveStore
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_engine_boundaries import assert_engine_import_boundaries
from archive_tests.test_registry_contract_generation import compare_registry_export
from archive_vault.card_contracts import CARD_TYPE_SPECS, validate_card_type_specs
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard, validate_card_permissive
from archive_vault.vault import read_note_file, write_card

PPA_ROOT = Path(__file__).resolve().parents[3]
CHECKED_IN_REGISTRY = PPA_ROOT / "archive_crate" / "materializer_registry.json"


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


def run_p06_contracts(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    validate_card_type_specs()
    validate_card_type_registrations()
    generated = materializer_registry_payload()
    checked = json.loads(CHECKED_IN_REGISTRY.read_text(encoding="utf-8"))
    compare_registry_export(generated, checked)
    assert_engine_import_boundaries(PPA_ROOT / "archive_engine")

    uid = "hfa-person-p06c0001"
    vault = runtime.root / "contracts-vault"
    (vault / "People").mkdir(parents=True)
    card = PersonCard(
        uid=uid,
        type="person",
        source=["test"],
        source_id=f"{uid}@example.com",
        created="2026-09-06",
        updated="2026-09-06",
        summary="p06c-contracts",
    )
    write_card(
        vault,
        "People/card.md",
        card,
        body="P06C-CONTRACTS",
        provenance={"summary": ProvenanceEntry("test", "2026-09-06", "deterministic")},
    )
    note = read_note_file(vault / "People" / "card.md", vault_root=vault)
    parsed = validate_card_permissive(dict(note.frontmatter))
    batch = _materialize_row(
        CanonicalRow(rel_path="People/card.md", frontmatter=dict(note.frontmatter), card=parsed),
        vault_root=str(vault),
        slug_map={"card": "People/card.md"},
        path_to_uid={"People/card.md": uid},
        person_lookup={},
        batch_id="p06c",
    )
    table = CARD_TYPE_SPECS["person"].typed_projection
    if uid not in {str(item[0]) for item in batch.rows_for(table)}:
        raise AssertionError(f"materialize missed {table} for {uid}")
    store = DefaultArchiveStore(vault=vault, index=_Index({uid: "People/card.md"}))
    try:
        read = store.runtime.read(uid)
        query = store.runtime.query(type_filter="person", limit=4)
        if "P06C-CONTRACTS" not in str(read.get("content")):
            raise AssertionError(f"runtime read missed body: {read}")
        if query["rows"][0]["card_uid"] != uid:
            raise AssertionError(f"runtime query missed uid: {query}")
    finally:
        store.close()

    payload = {
        "id": "p06.contracts.registry_and_boundaries",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "card_types": len(generated.get("card_types") or []),
        "registry_sha256": hashlib.sha256(dump_registry_json(generated).encode()).hexdigest(),
        "typed_projection": table,
        "read_found": bool(read.get("found")),
    }
    (runtime.root.parent / "p06-contracts.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return payload


register(
    Scenario(
        id="p06.contracts.registry_and_boundaries",
        suite="p06",
        product_guarantee="Live card registry export matches the checked-in native artifact and engine imports stay one-way",
        proof_tier="isolated_integration",
        fixture_seed=6,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p06-contracts.json",),
        run=run_p06_contracts,
    )
)
