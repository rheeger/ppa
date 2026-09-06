"""P08-A SDK tracer: fixture fetch/normalize/batch, replay, and manifest fail-closed."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from archive_cli.engine_factory import ContainedCanonicalReader, trusted_local_access
from archive_engine.contracts import AccessContext, ArchiveIdentity
from archive_engine.errors import AccessDeniedError, IncompatibleContractError, IncompatibleStateError
from archive_engine.service import ArchiveEngineService
from archive_sync.connectors.contracts import CanonicalProposal, PersistResult, parse_manifest
from archive_sync.connectors.registry import known_connectors
from archive_sync.connectors.runtime import ContainedVaultWriter, execute_connector, execute_from_manifest
from archive_sync.connectors.sample import (
    SAMPLE_ACCOUNT_ALPHA,
    SAMPLE_ACCOUNT_BETA,
    SAMPLE_CONNECTOR_ID,
    SAMPLE_PROVIDER_OBJECT_ID,
    SAMPLE_SOURCE,
    SampleConnector,
    sample_manifest,
)

PPA_ROOT = Path(__file__).resolve().parents[2]
CONNECTORS = PPA_ROOT / "archive_sync" / "connectors"
STORE_TYPES = frozenset({"DefaultArchiveStore", "ArchiveStore", "PostgresStore", "SqliteStore"})


def _identity(vault: Path) -> ArchiveIdentity:
    return ArchiveIdentity(
        archive_id="aid-p08a",
        canonical_root=str(vault.resolve()),
        schema_binding="warehouse:ppa+index_schema_v9",
    )


def _access(archive_id: str = "aid-p08a") -> AccessContext:
    return trusted_local_access(archive_id)


class _UidLookup:
    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def resolve_rel_path(self, uid: str) -> str | None:
        return self._mapping.get(uid)


class RecordingWriter:
    def __init__(self) -> None:
        self.write_attempts = 0
        self.proposals: list[CanonicalProposal] = []

    def write_canonical(
        self,
        proposal: CanonicalProposal,
        *,
        identity: ArchiveIdentity,
        access: AccessContext,
    ) -> PersistResult:
        self.write_attempts += 1
        self.proposals.append(proposal)
        uid = str(proposal.card.get("uid") or "")
        return PersistResult(uid=uid, rel_path=proposal.rel_path, revision="rec", created=True, duplicate=False)


@pytest.fixture
def vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "disposable-vault"
    (root / "Email").mkdir(parents=True)
    monkeypatch.setenv("PPA_PATH", str(root))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://unused:unused@127.0.0.1:1/unused")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    monkeypatch.delenv("PPA_CONFIG_PATH", raising=False)
    return root


def test_sample_is_registered() -> None:
    assert SAMPLE_CONNECTOR_ID in known_connectors()


def test_sample_writes_typed_card_and_engine_can_read(vault: Path) -> None:
    identity = _identity(vault)
    writer = ContainedVaultWriter(vault)
    result = execute_connector(
        SAMPLE_CONNECTOR_ID,
        identity=identity,
        access=_access(),
        writer=writer,
        cursor={},
        run_id="p08a-write",
    )
    assert result.created_count == 2
    assert len({item.uid for item in result.persists}) == 2
    alpha = next(item for item, proposal in zip(result.persists, result.proposals) if proposal.identity.account_scope == SAMPLE_ACCOUNT_ALPHA)
    beta = next(item for item, proposal in zip(result.persists, result.proposals) if proposal.identity.account_scope == SAMPLE_ACCOUNT_BETA)
    assert alpha.uid != beta.uid
    assert alpha.created and beta.created
    assert all(change.committed for change in result.changes)
    assert all(change.account in {SAMPLE_ACCOUNT_ALPHA, SAMPLE_ACCOUNT_BETA} for change in result.changes)
    assert result.committed_cursor == {"page_token": "done", "page_index": 1}

    service = ArchiveEngineService(
        identity=identity,
        access=_access(),
        lookup=_UidLookup(writer.uid_to_rel),
        reader=ContainedCanonicalReader(vault),
    )
    read = service.read_exact(alpha.uid)
    assert read.found is True
    assert alpha.uid in read.content
    assert "email_message" in read.content
    assert SAMPLE_ACCOUNT_ALPHA in read.content
    assert SAMPLE_PROVIDER_OBJECT_ID in read.content


def test_replay_same_uid_no_duplicate(vault: Path) -> None:
    identity = _identity(vault)
    writer = ContainedVaultWriter(vault)
    first = execute_connector(SAMPLE_CONNECTOR_ID, identity=identity, access=_access(), writer=writer, cursor={})
    replay = execute_connector(SAMPLE_CONNECTOR_ID, identity=identity, access=_access(), writer=writer, cursor={})
    assert first.uids == replay.uids
    assert replay.created_count == 0
    assert replay.changes == ()
    assert all(item.duplicate for item in replay.persists)
    files = list((vault / "Email").rglob("*.md"))
    assert len(files) == 2


def test_two_accounts_same_provider_id_stay_distinct(vault: Path) -> None:
    identity = _identity(vault)
    connector = SampleConnector()
    batch = connector.fetch(cursor={}, identity=identity, access=_access())
    proposals = connector.normalize(batch, identity=identity, access=_access())
    assert {item.identity.provider_object_id for item in proposals} == {SAMPLE_PROVIDER_OBJECT_ID}
    uids = {item.identity.derive_uid("email-message") for item in proposals}
    source_ids = {item.identity.source_id for item in proposals}
    assert len(uids) == 2
    assert source_ids == {
        f"{SAMPLE_ACCOUNT_ALPHA}:{SAMPLE_PROVIDER_OBJECT_ID}",
        f"{SAMPLE_ACCOUNT_BETA}:{SAMPLE_PROVIDER_OBJECT_ID}",
    }


def test_malformed_manifest_fails_before_write(vault: Path) -> None:
    writer = RecordingWriter()
    payload = sample_manifest().to_payload()
    payload["sdk_version"] = "99"
    with pytest.raises(IncompatibleContractError, match="sdk_version"):
        execute_from_manifest(
            payload,
            identity=_identity(vault),
            access=_access(),
            writer=writer,
            cursor={},
        )
    assert writer.write_attempts == 0
    assert list((vault / "Email").rglob("*.md")) == []


@pytest.mark.parametrize(
    ("patch", "match"),
    [
        ({"connector_id": ""}, "connector_id"),
        ({"token": "secret-value"}, "secret"),
        ({"min_engine_version": 9, "max_engine_version": 9}, "engine contract"),
        ({"identity_recipe": "provider_object_id"}, "account_scope"),
        ({"emitted_card_types": []}, "emitted_card_types"),
        ({"freshness_capability": "push-only"}, "freshness_capability"),
    ],
)
def test_parse_manifest_rejects_malformed(patch: dict[str, object], match: str) -> None:
    payload = sample_manifest().to_payload()
    payload.update(patch)
    with pytest.raises(IncompatibleContractError, match=match):
        parse_manifest(payload)


def test_denied_access_fails_before_write(vault: Path) -> None:
    writer = RecordingWriter()
    access = AccessContext(
        archive_id="aid-p08a",
        principal="local-operator",
        profile="trusted-local",
        deny=True,
        deny_reason="denied-for-test",
    )
    with pytest.raises(AccessDeniedError):
        execute_connector(SAMPLE_CONNECTOR_ID, identity=_identity(vault), access=access, writer=writer)
    assert writer.write_attempts == 0


def test_mismatched_access_identity_rejected(vault: Path) -> None:
    writer = RecordingWriter()
    with pytest.raises(IncompatibleStateError):
        execute_connector(SAMPLE_CONNECTOR_ID, identity=_identity(vault), access=_access("other"), writer=writer)
    assert writer.write_attempts == 0


def test_runtime_has_no_store_type_checks_or_dispatch() -> None:
    found_store: list[str] = []
    found_dispatch: list[str] = []
    forbidden_imports = {"archive_cli.commands", "archive_cli.server", "archive_cli.__main__", "mcp"}
    found_imports: list[str] = []
    for path in sorted(CONNECTORS.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "isinstance":
                for arg in node.args[1:]:
                    names = []
                    if isinstance(arg, ast.Name):
                        names = [arg.id]
                    elif isinstance(arg, ast.Tuple):
                        names = [elt.id for elt in arg.elts if isinstance(elt, ast.Name)]
                    for name in names:
                        if name in STORE_TYPES:
                            found_store.append(f"{path.name}:{name}")
            if isinstance(node, ast.Compare):
                left = node.left
                if isinstance(left, ast.Name) and left.id == "connector_id":
                    found_dispatch.append(path.name)
                if isinstance(left, ast.Attribute) and left.attr == "connector_id":
                    # identity equality against the requested id is validation, not source dispatch
                    continue
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            else:
                continue
            for name in names:
                if name in forbidden_imports or name.startswith("archive_cli.commands") or name.startswith("archive_cli.server"):
                    found_imports.append(f"{path.name}:{name}")
    assert found_store == []
    assert found_dispatch == []
    assert found_imports == []


def test_sample_source_is_not_gmail() -> None:
    assert SAMPLE_SOURCE == "sample"
    text = (CONNECTORS / "sample.py").read_text(encoding="utf-8")
    assert "gmail.googleapis" not in text
    assert "accounts.google.com" not in text
