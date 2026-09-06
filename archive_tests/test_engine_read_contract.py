"""P06-A exact-read and frozen shared-record contract."""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from archive_cli.engine_factory import (
    ContainedCanonicalReader,
    IndexUidPathLookup,
    build_exact_read_service,
    resolve_archive_identity,
    schema_binding_for,
    trusted_local_access,
)
from archive_cli.server import archive_read
from archive_cli.store import DefaultArchiveStore
from archive_engine.contracts import (
    AccessContext,
    ArchiveIdentity,
    ChangeBatch,
    ChangeRecord,
    ChunkEvidenceRef,
    EmbeddingSpec,
    EvidenceEnvelope,
    OutputReceipt,
    OutputRevision,
    RunEvidence,
    ServingEdge,
    ServingManifest,
    SourceSpan,
    dump_contract,
    load_contract,
)
from archive_engine.errors import IncompatibleContractError, IncompatibleStateError
from archive_engine.service import ArchiveEngineService
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card

PPA_ROOT = Path(__file__).resolve().parents[1]
SENTINEL = "P06A_SENTINEL_HARMLESS_OUTSIDE_VAULT"


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
    prov = {"summary": ProvenanceEntry("test", "2026-09-06", "deterministic")}
    return card, prov


@pytest.fixture
def isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    vault = tmp_path / "disposable-vault"
    (vault / "People").mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel.md"
    sentinel.write_text(SENTINEL + "\n", encoding="utf-8")
    card, prov = _person("hfa-person-engine01", "Engine Fixture")
    write_card(vault, "People/engine-fixture.md", card, body="engine-inside-ok", provenance=prov)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://unused:unused@127.0.0.1:1/unused")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.setenv("PPA_EMBEDDING_MODEL", "archive-hash-dev")
    monkeypatch.setenv("PPA_EMBEDDING_VERSION", "1")
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    monkeypatch.delenv("PPA_CONFIG_PATH", raising=False)
    monkeypatch.delenv("PPA_ARCHIVE_ID", raising=False)
    return {"vault": vault, "outside": outside, "sentinel": sentinel, "root": tmp_path}


class _UidIndex:
    def __init__(self, mapping: dict[str, str | None]):
        self._mapping = mapping

    def read_path_for_uid(self, uid: str) -> str | None:
        return self._mapping.get(uid)


def _spec() -> EmbeddingSpec:
    return EmbeddingSpec(
        provider_namespace="hash",
        model="archive-hash-dev",
        model_revision="1",
        dimension=8,
        metric="cosine",
        normalization="none",
        chunk_schema="chunk_schema_v6",
    )


def test_frozen_records_round_trip() -> None:
    identity = ArchiveIdentity(archive_id="aid-1", canonical_root="/tmp/vault-a", schema_binding="warehouse:ppa+index_schema_v9")
    spec = _spec()
    change = ChangeRecord(
        archive_id="aid-1",
        sequence=3,
        mutation_id="mut-1",
        uid="hfa-person-engine01",
        operation="upsert",
        before_revision="",
        after_revision="rev-2",
        source="test",
        account="acct-1",
        run_id="run-1",
        committed=True,
    )
    batch = ChangeBatch(high_watermark=3, consumer_name="publication", records=(change,))
    receipt = OutputReceipt(
        processor="derive_purchase",
        processor_version="1",
        input_uid="hfa-email-1",
        input_revision="rev-1",
        status="completed",
        outputs=(OutputRevision(uid="hfa-purchase-1", revision="rev-9"),),
        chunk_keys=("chunk-1",),
        embedding_spec=spec,
    )
    manifest = ServingManifest(
        format_version="1",
        registry_version="1",
        snapshot_id="snap-1",
        source_watermark=3,
        embedding_spec=spec,
        artifact_checksums=(("vectors", "abc"),),
        logical_counts=(("cards", 2),),
        parent_generation="",
        base_generation="gen-0",
        validation_summary="ok",
        edge_defaults=ServingEdge(),
    )
    access = AccessContext(archive_id="aid-1", principal="local-operator", profile="trusted-local", deny=False)
    envelope = EvidenceEnvelope(
        generation="gen-1",
        watermark=3,
        method="lexical",
        corpus_state="snapshot:snap-1",
        provenance="index",
        evidence_kind="source_reported",
        pipeline_version="v2",
        query="fixture",
    )
    run = RunEvidence(
        commit_sha="a" * 40,
        base_sha="b" * 40,
        scenario_ids=("p06a-roundtrip",),
        environment=(("PPA_EMBEDDING_PROVIDER", "hash"),),
        versions=(("python", "3.12"),),
        commands=("pytest archive_tests/test_engine_read_contract.py",),
        started_at="2026-09-06T00:00:00Z",
        finished_at="2026-09-06T00:00:01Z",
        result="pass",
    )
    for record, cls in (
        (identity, ArchiveIdentity),
        (spec, EmbeddingSpec),
        (change, ChangeRecord),
        (batch, ChangeBatch),
        (receipt, OutputReceipt),
        (manifest, ServingManifest),
        (access, AccessContext),
        (envelope, EvidenceEnvelope),
        (run, RunEvidence),
    ):
        loaded = load_contract(dump_contract(record), expected=cls)
        assert loaded == record


def test_change_batch_requires_consumer_name() -> None:
    with pytest.raises(IncompatibleContractError, match="consumer_name"):
        ChangeBatch.from_payload({"high_watermark": 1, "consumer_name": "", "records": []})


def test_unknown_serialization_version_rejected() -> None:
    envelope = {
        "contract": "ArchiveIdentity",
        "version": 99,
        "payload": {"archive_id": "x", "canonical_root": "/tmp", "schema_binding": "x"},
    }
    with pytest.raises(IncompatibleContractError, match="unsupported contract serialization"):
        load_contract(envelope)


def test_chunk_evidence_unknown_version_and_missing_spans() -> None:
    with pytest.raises(IncompatibleContractError, match="unsupported ChunkEvidenceRef"):
        ChunkEvidenceRef.from_payload(
            {
                "version": 2,
                "archive_id": "aid",
                "card_uid": "uid",
                "chunk_id": "c1",
                "chunk_schema_version": "6",
                "algorithm_version": "1",
            }
        )
    ref = ChunkEvidenceRef.from_payload(
        {
            "version": 1,
            "archive_id": "aid",
            "card_uid": "uid",
            "chunk_id": "c1",
            "chunk_schema_version": "6",
            "algorithm_version": "1",
            "evidence_kind": "unknown",
        }
    )
    assert ref.span_unavailable is True
    assert ref.message_refs_available is False
    assert ref.lineage_complete is False
    assert ref.evidence_kind == "unknown"
    span = SourceSpan(
        source_uid="uid",
        source_message_id="m1",
        source_revision="rev",
        representation="canonical_body_utf8",
        start_byte=0,
        end_byte=4,
    )
    present = ChunkEvidenceRef(
        version=1,
        archive_id="aid",
        card_uid="uid",
        chunk_id="c1",
        chunk_schema_version="6",
        algorithm_version="1",
        evidence_kind="source_reported",
        lineage_complete=True,
        source_spans=(span,),
        span_unavailable=False,
        message_refs_available=True,
        message_refs=(),
    )
    loaded = load_contract(dump_contract(present), expected=ChunkEvidenceRef)
    assert loaded == present
    assert loaded.span_unavailable is False
    assert loaded.message_refs_available is True


def test_serving_edge_absent_fields_are_unknown() -> None:
    edge = ServingEdge.from_payload({})
    assert edge.method == "unknown"
    assert edge.confidence is None
    assert edge.evidence_uids == ()


def test_run_evidence_rejects_secrets() -> None:
    with pytest.raises(IncompatibleContractError, match="secret"):
        RunEvidence.from_payload(
            {
                "commit_sha": "a" * 40,
                "base_sha": "b" * 40,
                "scenario_ids": [],
                "environment": {"api_key": "nope"},
                "versions": {},
                "commands": [],
                "started_at": "t0",
                "finished_at": "t1",
                "result": "pass",
            }
        )


def test_engine_read_with_injected_adapters(isolated: dict[str, Path]) -> None:
    identity = ArchiveIdentity(
        archive_id="aid-inject",
        canonical_root=str(isolated["vault"].resolve()),
        schema_binding="test",
    )
    access = trusted_local_access("aid-inject")
    files = {"People/engine-fixture.md": "injected-body"}
    seen: list[str] = []

    class Lookup:
        def resolve_rel_path(self, uid: str) -> str | None:
            seen.append(uid)
            return "People/engine-fixture.md" if uid == "hfa-person-engine01" else None

    class Reader:
        def read_contained(self, user_path: str) -> tuple[str | None, str]:
            body = files.get(user_path)
            return (None, "") if body is None else (body, user_path)

    engine = ArchiveEngineService(identity=identity, access=access, lookup=Lookup(), reader=Reader())
    by_path = engine.read("People/engine-fixture.md")
    assert by_path["found"] is True
    assert by_path["content"] == "injected-body"
    by_uid = engine.read("hfa-person-engine01")
    assert by_uid["found"] is True
    assert seen == ["hfa-person-engine01"]
    missing = engine.read("missing-uid")
    assert missing == {"path_or_uid": "missing-uid", "content": "", "found": False}


def test_store_read_delegates_to_engine(isolated: dict[str, Path], monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    original = ArchiveEngineService.read

    def wrapped(self: ArchiveEngineService, path_or_uid: str, *, access=None):
        calls.append(path_or_uid)
        return original(self, path_or_uid, access=access)

    monkeypatch.setattr(ArchiveEngineService, "read", wrapped)
    store = DefaultArchiveStore(vault=isolated["vault"], index=_UidIndex({"hfa-person-engine01": "People/engine-fixture.md"}))
    path_hit = store.read("People/engine-fixture.md")
    uid_hit = store.read("hfa-person-engine01")
    assert path_hit["found"] is True
    assert "engine-inside-ok" in path_hit["content"]
    assert uid_hit["found"] is True
    assert calls == ["People/engine-fixture.md", "hfa-person-engine01"]


def test_denied_paths_stay_denied(isolated: dict[str, Path]) -> None:
    store = DefaultArchiveStore(vault=isolated["vault"], index=_UidIndex({"escape": "../outside/sentinel.md"}))
    escaped = store.read(str(isolated["sentinel"]))
    traversal = store.read("../outside/sentinel.md")
    uid_escape = store.read("escape")
    assert escaped["found"] is False
    assert traversal["found"] is False
    assert uid_escape["found"] is False
    assert SENTINEL not in escaped["content"]
    assert SENTINEL not in traversal["content"]
    assert SENTINEL not in uid_escape["content"]
    ok = store.read("People/engine-fixture.md")
    assert ok["found"] is True
    assert "engine-inside-ok" in ok["content"]


def test_explicit_access_deny_does_not_open_reader(isolated: dict[str, Path]) -> None:
    identity = resolve_archive_identity(isolated["vault"], schema_binding="test", archive_id="aid-deny")
    access = AccessContext(
        archive_id="aid-deny",
        principal="local-operator",
        profile="restricted",
        deny=True,
        deny_reason="test",
    )
    opened: list[str] = []

    class Reader(ContainedCanonicalReader):
        def read_contained(self, user_path: str) -> tuple[str | None, str]:
            opened.append(user_path)
            return super().read_contained(user_path)

    engine = build_exact_read_service(
        vault=isolated["vault"],
        index=_UidIndex({}),
        identity=identity,
        access=access,
        reader=Reader(isolated["vault"]),
    )
    result = engine.read("People/engine-fixture.md")
    assert result["found"] is False
    assert result["content"] == ""
    assert opened == []


def test_two_archive_contexts_do_not_share_state(tmp_path: Path) -> None:
    vault_a = tmp_path / "vault-a"
    vault_b = tmp_path / "vault-b"
    for vault, uid, body in (
        (vault_a, "hfa-person-engine-a", "CONTENT-A"),
        (vault_b, "hfa-person-engine-b", "CONTENT-B"),
    ):
        (vault / "People").mkdir(parents=True)
        card, prov = _person(uid, uid)
        write_card(vault, "People/card.md", card, body=body, provenance=prov)
    engine_a = build_exact_read_service(
        vault=vault_a,
        index=_UidIndex({"shared-uid": "People/card.md"}),
        schema_binding=schema_binding_for("schema_a"),
        identity=resolve_archive_identity(vault_a, schema_binding="schema_a", archive_id="archive-a"),
    )
    engine_b = build_exact_read_service(
        vault=vault_b,
        index=_UidIndex({"shared-uid": "People/card.md"}),
        schema_binding=schema_binding_for("schema_b"),
        identity=resolve_archive_identity(vault_b, schema_binding="schema_b", archive_id="archive-b"),
    )
    assert engine_a.identity.archive_id != engine_b.identity.archive_id
    assert engine_a.identity.canonical_root != engine_b.identity.canonical_root
    assert engine_a.read("People/card.md")["content"] != engine_b.read("People/card.md")["content"]
    assert "CONTENT-A" in engine_a.read("shared-uid")["content"]
    assert "CONTENT-B" in engine_b.read("shared-uid")["content"]
    assert engine_a.access.archive_id == "archive-a"
    assert engine_b.access.archive_id == "archive-b"


def test_mismatched_access_identity_rejected(isolated: dict[str, Path]) -> None:
    identity = ArchiveIdentity(archive_id="a", canonical_root=str(isolated["vault"]), schema_binding="x")
    access = trusted_local_access("b")
    with pytest.raises(IncompatibleStateError):
        ArchiveEngineService(
            identity=identity,
            access=access,
            lookup=IndexUidPathLookup(_UidIndex({})),
            reader=ContainedCanonicalReader(isolated["vault"]),
        )


def test_identity_is_not_display_name(isolated: dict[str, Path]) -> None:
    identity = resolve_archive_identity(isolated["vault"], schema_binding="warehouse:ppa+index_schema_v9")
    assert identity.archive_id != isolated["vault"].name
    assert identity.canonical_root == str(isolated["vault"].resolve())
    other = resolve_archive_identity(isolated["outside"], schema_binding="warehouse:ppa+index_schema_v9")
    assert identity.archive_id != other.archive_id


def test_core_engine_does_not_import_cli_or_mcp() -> None:
    engine_dir = PPA_ROOT / "archive_engine"
    forbidden = {
        "archive_cli.commands",
        "archive_cli.server",
        "archive_cli.__main__",
        "mcp",
        "mcp.server",
    }
    found: list[str] = []
    for path in sorted(engine_dir.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            else:
                continue
            for name in names:
                if name in forbidden or name.startswith("archive_cli.commands") or name.startswith("archive_cli.server"):
                    found.append(f"{path.name}:{name}")
    assert found == []


def test_cli_subprocess_exact_read(isolated: dict[str, Path]) -> None:
    env = {
        **os.environ,
        "PPA_PATH": str(isolated["vault"]),
        "PPA_INDEX_DSN": "postgresql://unused:unused@127.0.0.1:1/unused",
        "PPA_EMBEDDING_PROVIDER": "hash",
        "PPA_EMBEDDING_MODEL": "archive-hash-dev",
        "PPA_EMBEDDING_VERSION": "1",
        "PPA_ENGINE": "rust",
        "PYTHONPATH": str(PPA_ROOT),
    }
    env.pop("PPA_TEST_PG_DSN", None)
    env.pop("PPA_CONFIG_PATH", None)
    env.pop("PPA_SERVING_INDEX_PATH", None)
    legit = subprocess.run(
        [sys.executable, "-m", "archive_cli", "read", "People/engine-fixture.md"],
        cwd=str(isolated["root"]),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert legit.returncode == 0, legit.stderr
    payload = json.loads(legit.stdout)
    assert payload["found"] is True
    assert "engine-inside-ok" in payload["content"]
    denied = subprocess.run(
        [sys.executable, "-m", "archive_cli", "read", "../outside/sentinel.md"],
        cwd=str(isolated["root"]),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert SENTINEL not in denied.stdout
    assert SENTINEL not in denied.stderr
    denied_payload = json.loads(denied.stdout)
    assert denied_payload["found"] is False


def test_mcp_archive_read_uses_engine(isolated: dict[str, Path], monkeypatch: pytest.MonkeyPatch) -> None:
    import archive_cli.server as archive_server

    store = DefaultArchiveStore(
        vault=isolated["vault"],
        index=_UidIndex({"hfa-person-engine01": "People/engine-fixture.md"}),
    )
    monkeypatch.setattr(archive_server, "resolve_store", lambda vault=None: store)
    calls: list[str] = []
    original = ArchiveEngineService.read

    def wrapped(self: ArchiveEngineService, path_or_uid: str, *, access=None):
        calls.append(path_or_uid)
        return original(self, path_or_uid, access=access)

    monkeypatch.setattr(ArchiveEngineService, "read", wrapped)
    text = archive_read("hfa-person-engine01")
    assert "engine-inside-ok" in text
    assert calls == ["hfa-person-engine01"]
    assert archive_read("../outside/sentinel.md") == "Not found"
    assert SENTINEL not in archive_read(str(isolated["sentinel"]))


def test_affected_context_absence_is_pending(isolated: dict[str, Path]) -> None:
    engine = build_exact_read_service(vault=isolated["vault"], index=_UidIndex({}))
    pending = engine.resolve_affected("uid", "rev")
    assert pending.status == "reconciliation_pending"
