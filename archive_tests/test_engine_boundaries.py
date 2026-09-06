"""P06-C architecture guards: imports, package discovery, public compatibility."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

from archive_cli.commands._resolve import resolve_runtime
from archive_cli.store import DefaultArchiveStore
from archive_engine import ArchiveEngineService, ArchiveRuntime, dump_contract, load_contract
from archive_engine.contracts import AccessContext, ArchiveIdentity, ChangeBatch, OutputReceipt

PPA_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_TRANSPORT_PREFIXES = ("archive_cli.commands", "archive_cli.server", "archive_cli.__main__")
# Sibling modules that still reach CLI helpers. New pairs fail the allowlist.
ALLOWED_ENGINE_CLI_IMPORTS = frozenset(
    {
        ("publication.py", "archive_cli.index_config"),
        ("publication.py", "archive_cli.serving_index"),
    }
)
ALLOWED_VAULT_CLI_IMPORTS = frozenset(
    {
        ("identity_resolver.py", "archive_cli.vault_cache"),
    }
)
CORE_EXCLUDE_DIRS = {"adapters"}


class EngineBoundaryError(ValueError):
    """A core/domain module imported a forbidden CLI or transport module."""


def _import_names(tree: ast.AST) -> list[str]:
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    return names


def _is_cli_import(name: str) -> bool:
    return name == "archive_cli" or name.startswith("archive_cli.")


def _is_transport_import(name: str) -> bool:
    return any(name == prefix or name.startswith(prefix + ".") for prefix in FORBIDDEN_TRANSPORT_PREFIXES)


def forbidden_engine_imports(root: Path) -> list[str]:
    """Return ``path:module`` hits for forbidden engine→CLI imports."""

    hits: list[str] = []
    engine_root = Path(root)
    for path in sorted(engine_root.rglob("*.py")):
        rel = path.relative_to(engine_root)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        names = _import_names(tree)
        adapter = bool(rel.parts and rel.parts[0] in CORE_EXCLUDE_DIRS)
        for name in names:
            if _is_transport_import(name):
                hits.append(f"{rel.as_posix()}:{name}")
                continue
            if adapter or not _is_cli_import(name):
                continue
            allowed = (path.name, name) in ALLOWED_ENGINE_CLI_IMPORTS
            if not allowed:
                hits.append(f"{rel.as_posix()}:{name}")
    return hits


def assert_engine_import_boundaries(root: Path) -> None:
    hits = forbidden_engine_imports(root)
    if hits:
        raise EngineBoundaryError("forbidden engine import: " + "; ".join(hits))


def test_core_engine_does_not_import_cli() -> None:
    assert_engine_import_boundaries(PPA_ROOT / "archive_engine")


def test_vault_does_not_import_cli_transport() -> None:
    hits: list[str] = []
    for path in (PPA_ROOT / "archive_vault").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for name in _import_names(tree):
            if _is_transport_import(name) or name == "archive_cli.ppa_engine":
                hits.append(f"{path.name}:{name}")
            elif _is_cli_import(name) and (path.name, name) not in ALLOWED_VAULT_CLI_IMPORTS:
                hits.append(f"{path.name}:{name}")
    assert hits == []


def test_engine_does_not_import_store_recursively() -> None:
    hits: list[str] = []
    for path in (PPA_ROOT / "archive_engine").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text, filename=str(path))
        for name in _import_names(tree):
            if name in {"archive_cli.store", "archive_cli.store.DefaultArchiveStore"}:
                hits.append(f"{path.name}:{name}")
        if "DefaultArchiveStore" in text and path.name != "__init__.py":
            if "DefaultArchiveStore" in ast.dump(tree):
                hits.append(f"{path.name}:DefaultArchiveStore")
    assert hits == []


def test_forbidden_import_diagnostic(tmp_path: Path) -> None:
    fake = tmp_path / "archive_engine"
    fake.mkdir()
    (fake / "bad.py").write_text("from archive_cli.commands.read import run\n", encoding="utf-8")
    with pytest.raises(EngineBoundaryError, match="forbidden engine import") as exc:
        assert_engine_import_boundaries(fake)
    assert "bad.py" in str(exc.value)
    assert "archive_cli.commands" in str(exc.value)


def test_package_discovery_includes_engine() -> None:
    text = (PPA_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "archive_engine*" in text
    assert (PPA_ROOT / "archive_engine" / "runtime.py").is_file()
    assert (PPA_ROOT / "archive_engine" / "contracts.py").is_file()


def test_public_engine_imports_remain() -> None:
    assert ArchiveRuntime is not None
    assert ArchiveEngineService is not None
    identity = ArchiveIdentity(
        archive_id="a" * 64, canonical_root="/tmp/x", schema_binding="warehouse:ppa+index_schema_v1"
    )
    dumped = dump_contract(identity)
    loaded = load_contract(dumped)
    assert isinstance(loaded, ArchiveIdentity)
    assert loaded.archive_id == identity.archive_id
    assert ChangeBatch.__dataclass_fields__["consumer_name"]
    assert OutputReceipt.__dataclass_fields__["outputs"]
    assert AccessContext.__dataclass_fields__["archive_id"]


def test_cli_and_mcp_entry_points_remain() -> None:
    from archive_cli import ppa_engine as ppa_engine_mod
    from archive_cli.commands.query import query
    from archive_cli.commands.read import read
    from archive_cli.commands.search import search
    from archive_cli.server import archive_query, archive_read, archive_search

    assert callable(ppa_engine_mod.ppa_engine)
    assert callable(read)
    assert callable(search)
    assert callable(query)
    assert callable(archive_read)
    assert callable(archive_search)
    assert callable(archive_query)
    assert callable(resolve_runtime)
    assert "runtime" in DefaultArchiveStore.__init__.__code__.co_names or hasattr(DefaultArchiveStore, "runtime")


def test_cli_help_keeps_read_search_query() -> None:
    env = {**dict(**__import__("os").environ), "PYTHONDONTWRITEBYTECODE": "1"}
    env.pop("PPA_TEST_PG_DSN", None)
    for command in ("read", "search", "query"):
        proc = subprocess.run(
            [sys.executable, "-m", "archive_cli", command, "--help"],
            cwd=str(PPA_ROOT),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        assert command in proc.stdout.lower() or "usage" in proc.stdout.lower()
