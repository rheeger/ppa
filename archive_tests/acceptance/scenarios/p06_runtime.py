"""P06-B acceptance: one instance-scoped engine, isolated archives."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from archive_cli.store import DefaultArchiveStore
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card


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


def _write_person(vault: Path, uid: str, body: str) -> None:
    (vault / "People").mkdir(parents=True, exist_ok=True)
    card = PersonCard(
        uid=uid,
        type="person",
        source=["test"],
        source_id=f"{uid}@example.com",
        created="2026-09-06",
        updated="2026-09-06",
        summary=uid,
    )
    prov = {"summary": ProvenanceEntry("test", "2026-09-06", "deterministic")}
    write_card(vault, "People/card.md", card, body=body, provenance=prov)


def run_p06_runtime(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    left = runtime.root / "archive-a"
    right = runtime.root / "archive-b"
    _write_person(left, "hfa-person-shared", "ARCHIVE-A")
    _write_person(right, "hfa-person-shared", "ARCHIVE-B")
    store_a = DefaultArchiveStore(vault=left, index=_Index({"hfa-person-shared": "People/card.md"}))
    store_b = DefaultArchiveStore(vault=right, index=_Index({"hfa-person-shared": "People/card.md"}))
    try:
        read_a = store_a.runtime.read("hfa-person-shared")
        read_b = store_b.runtime.read("hfa-person-shared")
        search_a = store_a.runtime.search("shared")
        if "ARCHIVE-A" not in str(read_a.get("content")) or "ARCHIVE-B" not in str(read_b.get("content")):
            raise AssertionError(f"contexts leaked: {read_a} {read_b}")
        if store_a.runtime.identity.archive_id == store_b.runtime.identity.archive_id:
            raise AssertionError("archive identities collided")
        if store_a.rebuild()["cards"] != 1:
            raise AssertionError("rebuild did not use warehouse adapter")
        if hasattr(type(store_a.runtime.warehouse), "_connect"):
            raise AssertionError("warehouse adapter exposed _connect")
    finally:
        store_a.close()
        store_b.close()
    payload = {
        "id": "p06.runtime.isolated_contexts",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "left_archive_id": store_a.runtime.identity.archive_id,
        "right_archive_id": store_b.runtime.identity.archive_id,
        "search_rows": len(search_a.get("rows") or []),
        "warehouse_has_connect": hasattr(type(store_a.runtime.warehouse), "_connect"),
    }
    artifact = runtime.root.parent / "p06-runtime.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


register(
    Scenario(
        id="p06.runtime.isolated_contexts",
        suite="p06",
        product_guarantee="Two archive contexts with the same UID keep their own cards through one engine runtime",
        proof_tier="isolated_integration",
        fixture_seed=6,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p06-runtime.json",),
        run=run_p06_runtime,
    )
)
