"""P06-B instance-scoped runtime: isolation, facade, no private connections."""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from archive_cli.commands._resolve import resolve_runtime
from archive_cli.engine_factory import build_runtime, resolve_archive_identity, schema_binding_for
from archive_cli.store import DefaultArchiveStore
from archive_engine.access import resolve_access_context
from archive_engine.adapters.warehouse import WarehouseAdapter
from archive_engine.runtime import ArchiveRuntime
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card

PPA_ROOT = Path(__file__).resolve().parents[1]


def _person(uid: str, summary: str) -> tuple[PersonCard, dict[str, ProvenanceEntry]]:
    card = PersonCard(
        uid=uid,
        type="person",
        source=["test"],
        source_id=f"{uid}@example.com",
        created="2026-09-06",
        updated="2026-09-06",
        summary=summary,
    )
    return card, {"summary": ProvenanceEntry("test", "2026-09-06", "deterministic")}


class _Index:
    def __init__(self, mapping: dict[str, str]):
        self.mapping = mapping
        self.closed = False

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
            for uid, rel in self.mapping.items()
        ][:limit]

    def query_cards(self, **_kwargs):
        return self.search("", limit=20)

    def graph(self, note_path: str, hops: int = 2):
        return {note_path: []}

    def status(self):
        return {"card_count": len(self.mapping)}

    def bootstrap(self):
        return {"ok": True}

    def rebuild(self):
        return {"cards": len(self.mapping)}


def _vault_with_card(root: Path, name: str, uid: str, body: str) -> Path:
    vault = root / name
    (vault / "People").mkdir(parents=True)
    card, prov = _person(uid, uid)
    write_card(vault, "People/card.md", card, body=body, provenance=prov)
    return vault


def test_two_runtimes_same_uid_different_roots(tmp_path: Path) -> None:
    vault_a = _vault_with_card(tmp_path, "a", "hfa-person-shared", "ROOT-A")
    vault_b = _vault_with_card(tmp_path, "b", "hfa-person-shared", "ROOT-B")
    store_a = DefaultArchiveStore(vault=vault_a, index=_Index({"hfa-person-shared": "People/card.md"}))
    store_b = DefaultArchiveStore(vault=vault_b, index=_Index({"hfa-person-shared": "People/card.md"}))
    assert store_a.runtime is not store_b.runtime
    assert store_a.runtime.identity.archive_id != store_b.runtime.identity.archive_id
    assert "ROOT-A" in store_a.read("hfa-person-shared")["content"]
    assert "ROOT-B" in store_b.read("hfa-person-shared")["content"]
    assert store_a.search("card")["rows"][0]["card_uid"] == "hfa-person-shared"
    store_a.close()
    store_b.close()
    assert store_a.runtime._closed is True


def test_runtime_close_survives_exception(tmp_path: Path) -> None:
    vault = _vault_with_card(tmp_path, "c", "hfa-person-close", "C")
    identity = resolve_archive_identity(vault, schema_binding=schema_binding_for("ppa"))
    access = resolve_access_context(identity.archive_id, identity=identity)
    runtime = build_runtime(
        vault=vault, index=_Index({"hfa-person-close": "People/card.md"}), access=access, identity=identity
    )
    with pytest.raises(RuntimeError):
        with runtime:
            assert isinstance(runtime, ArchiveRuntime)
            raise RuntimeError("boom")
    assert runtime._closed is True


def test_warehouse_adapter_hides_private_connection() -> None:
    class _ConnIndex:
        def status(self):
            return {"ok": True}

        def rebuild(self):
            return {"cards": 0}

        def bootstrap(self):
            return {"ok": True}

    adapter = WarehouseAdapter(_ConnIndex())
    assert not hasattr(WarehouseAdapter, "_connect")
    public = [name for name in dir(adapter) if not name.startswith("_") or name == "_connect"]
    assert "_connect" not in public
    assert adapter.status()["ok"] is True
    with adapter.snapshot():
        pass


def test_new_engine_modules_do_not_call_private_connect() -> None:
    forbidden = {"_connect", "store.index._connect"}
    roots = [
        PPA_ROOT / "archive_engine" / "runtime.py",
        PPA_ROOT / "archive_engine" / "service.py",
        PPA_ROOT / "archive_engine" / "adapters" / "retrieval.py",
        PPA_ROOT / "archive_engine" / "adapters" / "providers.py",
    ]
    hits: list[str] = []
    for path in roots:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "_connect":
                hits.append(f"{path.name}:_connect")
            if isinstance(node, ast.Name) and node.id in forbidden:
                hits.append(f"{path.name}:{node.id}")
    assert hits == []


def test_core_runtime_does_not_import_cli_commands() -> None:
    forbidden_prefixes = ("archive_cli.commands", "archive_cli.server", "archive_cli.__main__")
    found: list[str] = []
    for path in (PPA_ROOT / "archive_engine").rglob("*.py"):
        if path.name == "warehouse.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            for name in names:
                if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden_prefixes):
                    found.append(f"{path.name}:{name}")
    assert found == []


def test_cli_and_mcp_share_runtime_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    vault = _vault_with_card(tmp_path, "shared", "hfa-person-runtime", "RUNTIME-OK")
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://unused:unused@127.0.0.1:1/unused")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    store = DefaultArchiveStore(vault=vault, index=_Index({"hfa-person-runtime": "People/card.md"}))
    import archive_cli.server as archive_server

    monkeypatch.setattr(archive_server, "resolve_store", lambda vault=None: store)
    cli = store.search("RUNTIME")
    mcp = archive_server.archive_search("RUNTIME")
    assert cli["rows"][0]["card_uid"] == "hfa-person-runtime"
    assert "hfa-person-runtime" in mcp or "People/card.md" in mcp
    read_cli = store.read("People/card.md")
    read_mcp = archive_server.archive_read("People/card.md")
    assert read_cli["found"] is True
    assert "RUNTIME-OK" in read_cli["content"]
    assert "RUNTIME-OK" in read_mcp


def test_resolve_runtime_matches_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    vault = _vault_with_card(tmp_path, "r", "hfa-person-resolve", "R")
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://unused:unused@127.0.0.1:1/unused")
    monkeypatch.setattr(
        "archive_cli.commands._resolve.get_index", lambda vault=None: _Index({"hfa-person-resolve": "People/card.md"})
    )
    runtime = resolve_runtime(vault)
    assert runtime.identity.canonical_root == str(vault.resolve())
    assert runtime.read("People/card.md")["found"] is True


def test_cli_subprocess_search_and_read(tmp_path: Path) -> None:
    vault = _vault_with_card(tmp_path, "cli", "hfa-person-cli", "CLI-RUNTIME")
    env = {
        **os.environ,
        "PPA_PATH": str(vault),
        "PPA_INDEX_DSN": "postgresql://unused:unused@127.0.0.1:1/unused",
        "PPA_EMBEDDING_PROVIDER": "hash",
        "PPA_EMBEDDING_MODEL": "archive-hash-dev",
        "PPA_EMBEDDING_VERSION": "1",
        "PYTHONPATH": str(PPA_ROOT),
    }
    env.pop("PPA_TEST_PG_DSN", None)
    env.pop("PPA_CONFIG_PATH", None)
    proc = subprocess.run(
        [sys.executable, "-m", "archive_cli", "read", "People/card.md"],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["found"] is True
    assert "CLI-RUNTIME" in payload["content"]
