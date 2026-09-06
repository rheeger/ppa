"""P08-D: contributor template and SDK check command. No live Google."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from archive_cli.engine_factory import ContainedCanonicalReader, trusted_local_access
from archive_engine.contracts import ArchiveIdentity
from archive_engine.errors import IncompatibleContractError
from archive_engine.service import ArchiveEngineService
from archive_sync.connectors.cli import (
    SDK_MIGRATED_ADAPTERS,
    check_migrated_manifests,
    check_package,
    load_manifest,
    main,
    reject_incompatible_manifest,
    remaining_legacy_adapters,
)
from archive_sync.connectors.contracts import parse_manifest
from archive_sync.connectors.runtime import ContainedVaultWriter
from archive_sync.connectors.sample import sample_manifest

PPA_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = PPA_ROOT / "archive_docs" / "examples" / "connector-template"
HANDLER = PPA_ROOT / "archive_sync" / "handler.py"
CONNECTORS = PPA_ROOT / "archive_sync" / "connectors"


class _UidLookup:
    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def resolve_rel_path(self, uid: str) -> str | None:
        return self._mapping.get(uid)


def _identity(vault: Path) -> ArchiveIdentity:
    return ArchiveIdentity(
        archive_id="aid-p08d",
        canonical_root=str(vault.resolve()),
        schema_binding="warehouse:ppa+index_schema_v9",
    )


@pytest.fixture
def vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "disposable-vault"
    for name in ("Email", "_meta"):
        (root / name).mkdir(parents=True)
    monkeypatch.setenv("PPA_PATH", str(root))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://unused:unused@127.0.0.1:1/unused")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    monkeypatch.delenv("PPA_CONFIG_PATH", raising=False)
    return root


def test_template_package_replays_and_keeps_accounts_distinct(vault: Path) -> None:
    result = check_package(
        TEMPLATE,
        vault=vault,
        identity=_identity(vault),
        access=trusted_local_access("aid-p08d"),
    )
    assert result["status"] == "compatible"
    assert result["created_count"] == 2
    assert result["replay_created_count"] == 0
    assert result["distinct_accounts_same_provider"] is True
    assert result["quality"]["ok"] is True
    assert result["quality"]["false_promotions"] == []
    files = list((vault / "Email").rglob("*.md"))
    assert len(files) == 2


def test_incompatible_manifest_fails_before_write(vault: Path) -> None:
    writer = ContainedVaultWriter(vault)
    payload = load_manifest(TEMPLATE)
    payload["sdk_version"] = "99"
    rejected = reject_incompatible_manifest(
        payload,
        identity=_identity(vault),
        access=trusted_local_access("aid-p08d"),
        writer=writer,
    )
    assert rejected["rejected"] is True
    assert writer.write_attempts == 0
    assert list((vault / "Email").rglob("*.md")) == []


def test_migrated_gmail_and_calendar_manifests_pass() -> None:
    verdicts = check_migrated_manifests()
    assert verdicts == {"calendar-events": "compatible", "gmail-messages": "compatible"}
    parse_manifest(sample_manifest().to_payload())


def test_remaining_legacy_list_is_honest() -> None:
    listing = remaining_legacy_adapters()
    assert listing["sdk_migrated"] == ["calendar-events", "gmail-messages"]
    assert "imessage" in listing["legacy_executable"]
    assert "otter-transcripts" in listing["legacy_executable"]
    assert "beeper" in listing["legacy_executable"]
    assert "photos" in listing["legacy_executable"]
    assert "linkedin" in listing["legacy_export"]
    assert "gmail-messages" not in listing["legacy_executable"]
    assert "calendar-events" not in listing["legacy_export"]


def test_cli_legacy_list_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["legacy-list"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["sdk_migrated"] == ["calendar-events", "gmail-messages"]


def test_cli_check_writes_verdict(vault: Path, tmp_path: Path) -> None:
    out = tmp_path / "verdict.json"
    code = main(
        [
            "check",
            "--package",
            str(TEMPLATE),
            "--vault",
            str(vault),
            "--archive-id",
            "aid-p08d",
            "--output",
            str(out),
        ]
    )
    assert code == 0
    verdict = json.loads(out.read_text(encoding="utf-8"))
    assert verdict["status"] == "compatible"
    assert verdict["legacy"]["legacy_executable"]


def test_contributor_does_not_edit_core_dispatch() -> None:
    text = HANDLER.read_text(encoding="utf-8")
    assert "example.contributor" not in text
    assert "connectors.cli" not in text
    tree = ast.parse((CONNECTORS / "cli.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and isinstance(node.left, ast.Name) and node.left.id == "connector_id":
            raise AssertionError("cli.py added a connector_id dispatch branch")


def test_engine_can_read_template_card(vault: Path) -> None:
    identity = _identity(vault)
    result = check_package(TEMPLATE, vault=vault, identity=identity, access=trusted_local_access("aid-p08d"))
    uid = result["uids"][0]
    rel = next(path.relative_to(vault).as_posix() for path in (vault / "Email").rglob("*.md") if uid in path.name)
    service = ArchiveEngineService(
        identity=identity,
        access=trusted_local_access("aid-p08d"),
        lookup=_UidLookup({uid: rel}),
        reader=ContainedCanonicalReader(vault),
    )
    read = service.read_exact(uid)
    assert read.found is True
    assert uid in read.content


def test_template_readme_names_the_check_command() -> None:
    text = (TEMPLATE / "README.md").read_text(encoding="utf-8")
    assert "python -m archive_sync.connectors.cli check" in text
    assert "register_connector" in text
    assert "handler.py" in text
