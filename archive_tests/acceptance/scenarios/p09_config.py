"""P09-B acceptance: two instance configs, redacted explain, empty scopes."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from archive_cli.commands.configuration import explain_configuration
from archive_cli.engine_factory import build_runtime, resolve_archive_identity, schema_binding_for, trusted_local_access
from archive_engine.config import resolve_instance_config
from archive_engine.contracts import AccessContext
from archive_engine.scopes import RequestFilters, SavedScope, empty_scope_result, resolve_effective_scope
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
    write_card(
        vault,
        "People/card.md",
        card,
        body=body,
        provenance={"summary": ProvenanceEntry("test", "2026-09-06", "deterministic")},
    )


def _write_instance(root: Path, schema: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "ppa.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "identity": {},
                "entity": {"name": root.name, "entity_type": "person"},
                "storage": {"index_schema": schema, "index_dsn_ref": {"name": "PPA_INDEX_DSN", "provider": "env"}},
                "engine": {"native": "rust"},
                "embeddings": {"provider": "hash", "model": "archive-hash-dev", "dimension": 8, "model_revision": "1"},
                "access": {"principal": "local-operator", "profile": "trusted-local"},
                "scopes": [
                    {
                        "name": "family-2024",
                        "sources": ["gmail"],
                        "start_date": "2024-01-01",
                        "end_date": "2024-12-31",
                        "retrieval_profile": "historical",
                    }
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return root


def run_p09_config(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    left = _write_instance(runtime.root / "archive-a", "schema_a")
    right = _write_instance(runtime.root / "archive-b", "schema_b")
    _write_person(left, "hfa-person-shared", "ARCHIVE-A")
    _write_person(right, "hfa-person-shared", "ARCHIVE-B")

    cfg_a = resolve_instance_config(instance_dir=left, environ={}, allow_cwd_discovery=False)
    cfg_b = resolve_instance_config(instance_dir=right, environ={}, allow_cwd_discovery=False)
    identity_a = resolve_archive_identity(left, schema_binding=schema_binding_for(cfg_a.storage.index_schema))
    identity_b = resolve_archive_identity(right, schema_binding=schema_binding_for(cfg_b.storage.index_schema))
    if cfg_a.identity.archive_id != identity_a.archive_id or cfg_b.identity.archive_id != identity_b.archive_id:
        raise AssertionError("resolved identity drifted from resolve_archive_identity")
    if identity_a.archive_id == identity_b.archive_id:
        raise AssertionError("archive identities collided")
    if cfg_a.storage.query_embed_cache_path == cfg_b.storage.query_embed_cache_path:
        raise AssertionError("embed cache paths collided")

    explained = explain_configuration(
        instance_dir=left,
        environ={"PPA_INDEX_DSN": "postgresql://archive:hunter2@127.0.0.1:5432/archive"},
        allow_cwd_discovery=False,
    )
    blob = json.dumps(explained)
    if "hunter2" in blob or "sk-" in blob or "postgresql://" in blob.lower():
        raise AssertionError("explain leaked a secret")

    access = AccessContext(
        archive_id=identity_a.archive_id,
        principal="local-operator",
        profile="read-only",
        allowed_sources=("gmail",),
        deny=False,
    )
    effective = resolve_effective_scope(
        access=access,
        scope=SavedScope.from_payload({"name": "family-2024", "sources": ["slack"]}),
        request=RequestFilters(),
    )
    if not effective.empty:
        raise AssertionError("scope widened or ignored access intersection")
    empty = empty_scope_result(reason=effective.reason, effective=effective)
    if empty.get("rows") or empty.get("hits"):
        raise AssertionError("empty-scope returned unconstrained rows")

    runtime_a = build_runtime(
        vault=left,
        index=_Index({"hfa-person-shared": "People/card.md"}),
        access=trusted_local_access(identity_a.archive_id),
        identity=identity_a,
    )
    runtime_b = build_runtime(
        vault=right,
        index=_Index({"hfa-person-shared": "People/card.md"}),
        access=trusted_local_access(identity_b.archive_id),
        identity=identity_b,
    )
    try:
        read_a = runtime_a.read("hfa-person-shared")
        read_b = runtime_b.read("hfa-person-shared")
        if "ARCHIVE-A" not in str(read_a.get("content")) or "ARCHIVE-B" not in str(read_b.get("content")):
            raise AssertionError(f"contexts leaked: {read_a} {read_b}")
    finally:
        runtime_a.close()
        runtime_b.close()

    payload = {
        "id": "p09.config.instance_scopes",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "left_archive_id": identity_a.archive_id,
        "right_archive_id": identity_b.archive_id,
        "left_schema": cfg_a.storage.index_schema,
        "right_schema": cfg_b.storage.index_schema,
        "explain_redacted": True,
        "empty_scope": True,
    }
    artifact = runtime.root.parent / "p09-config.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


register(
    Scenario(
        id="p09.config.instance_scopes",
        suite="p09",
        product_guarantee="Two instance configs stay isolated; saved scopes cannot widen AccessContext; explain redacts secrets",
        proof_tier="isolated_integration",
        fixture_seed=9,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p09-config.json",),
        run=run_p09_config,
    )
)
