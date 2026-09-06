"""P09-B: typed instance config precedence, redaction, and isolation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_cli.commands.configuration import (
    explain_configuration,
    migrate_legacy_configuration,
    rollback_legacy_migration,
    validate_configuration,
)
from archive_cli.engine_factory import build_runtime, resolve_archive_identity, schema_binding_for, trusted_local_access
from archive_cli.index_config import get_index_schema, get_query_embed_cache_path, get_serving_index_path
from archive_engine.config import (
    bind_instance_config,
    current_secret_values,
    explain_instance_config,
    resolve_instance_config,
)
from archive_engine.errors import ConfigError
from archive_engine.scopes import SavedScope


@pytest.fixture(autouse=True)
def _unset_test_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _v1_payload(**overrides: object) -> dict:
    payload = {
        "schema_version": 1,
        "identity": {},
        "entity": {"name": "fixture", "entity_type": "person"},
        "storage": {"index_schema": "from_file", "index_dsn_ref": {"name": "PPA_INDEX_DSN", "provider": "env"}},
        "engine": {"native": "rust"},
        "embeddings": {"provider": "hash", "model": "archive-hash-dev", "dimension": 8, "model_revision": "1"},
        "access": {"principal": "local-operator", "profile": "trusted-local"},
        "retrieval": {"default_limit": 5},
        "scopes": [
            {
                "name": "family-2024",
                "description": "family year",
                "sources": ["gmail"],
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "retrieval_profile": "historical",
            }
        ],
    }
    payload.update(overrides)
    return payload


def test_cli_overrides_env_file_and_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    instance = tmp_path / "inst"
    instance.mkdir()
    _write_json(instance / "ppa.json", _v1_payload())
    environ = {
        "PPA_INDEX_SCHEMA": "from_env",
        "PPA_RETRIEVAL_DEFAULT_LIMIT": "7",
        "PPA_ENGINE": "python",
        "PPA_PATH": str(tmp_path / "stolen"),
    }
    config = resolve_instance_config(
        instance_dir=instance,
        environ=environ,
        cli_overrides={"index_schema": "from_cli"},
        allow_cwd_discovery=False,
    )
    origins = config.origin_map()
    assert config.storage.index_schema == "from_cli"
    assert origins["storage.index_schema"].origin == "cli"
    assert config.retrieval.default_limit == 7
    assert origins["retrieval.default_limit"].origin == "env"
    assert config.engine.native == "python"
    assert origins["engine.native"].origin == "env"
    assert config.entity.name == "fixture"
    assert origins["entity.name"].origin == "file"
    assert config.access.principal == "local-operator"
    assert origins["access.principal"].origin == "file"
    assert Path(config.storage.vault_path) == instance.resolve()
    assert origins["storage.vault_path"].origin == "cli"
    assert origins["storage.vault_path"].source == "instance_dir"


def test_env_wins_over_file(tmp_path: Path) -> None:
    instance = tmp_path / "inst"
    instance.mkdir()
    _write_json(instance / "ppa.json", _v1_payload())
    config = resolve_instance_config(
        instance_dir=instance,
        environ={"PPA_INDEX_SCHEMA": "from_env"},
        allow_cwd_discovery=False,
    )
    assert config.storage.index_schema == "from_env"
    assert config.origin_map()["storage.index_schema"].origin == "env"


def test_unknown_critical_keys_fail(tmp_path: Path) -> None:
    path = _write_json(tmp_path / "ppa.json", _v1_payload(mystery_dsn="postgresql://x:y@h/db"))
    with pytest.raises(ConfigError, match="unknown"):
        resolve_instance_config(config_path=path, environ={}, allow_cwd_discovery=False)


def test_plaintext_secret_in_v1_file_fails(tmp_path: Path) -> None:
    payload = _v1_payload()
    payload["storage"]["index_dsn"] = "postgresql://archive:hunter2@127.0.0.1:5432/archive"
    path = _write_json(tmp_path / "ppa.json", payload)
    with pytest.raises(ConfigError, match="plaintext"):
        resolve_instance_config(config_path=path, environ={}, allow_cwd_discovery=False)


def test_invalid_profile_fails(tmp_path: Path) -> None:
    payload = _v1_payload()
    payload["access"]["profile"] = "superuser"
    path = _write_json(tmp_path / "ppa.json", payload)
    with pytest.raises(ConfigError, match="profile"):
        resolve_instance_config(config_path=path, environ={}, allow_cwd_discovery=False)


def test_invalid_embedding_dimension_fails(tmp_path: Path) -> None:
    payload = _v1_payload()
    payload["embeddings"]["dimension"] = 0
    path = _write_json(tmp_path / "ppa.json", payload)
    with pytest.raises(ConfigError, match="dimension"):
        resolve_instance_config(config_path=path, environ={}, allow_cwd_discovery=False)


def test_explain_redacts_secrets(tmp_path: Path) -> None:
    instance = tmp_path / "inst"
    instance.mkdir()
    _write_json(instance / "ppa.json", _v1_payload())
    dsn = "postgresql://archive:hunter2-sk-secret@127.0.0.1:5432/archive"
    explained = explain_configuration(
        instance_dir=instance,
        environ={"PPA_INDEX_DSN": dsn, "OPENAI_API_KEY": "sk-live-not-for-explain"},
        allow_cwd_discovery=False,
    )
    blob = json.dumps(explained)
    assert "hunter2" not in blob
    assert "sk-live" not in blob
    assert "sk-" not in blob
    assert "postgresql://" not in blob.lower()
    assert "password" not in blob.lower()
    assert explained["storage"]["index_dsn"]["name"] == "PPA_INDEX_DSN"
    secrets = current_secret_values()
    assert secrets.get("index_dsn") == dsn


def test_bound_instance_ignores_leftover_ppa_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    instance = tmp_path / "owned"
    leftover = tmp_path / "leftover"
    instance.mkdir()
    leftover.mkdir()
    _write_json(instance / "ppa.json", _v1_payload())
    monkeypatch.setenv("PPA_PATH", str(leftover))
    monkeypatch.delenv("PPA_QUERY_EMBED_CACHE_PATH", raising=False)
    monkeypatch.delenv("PPA_SERVING_INDEX_PATH", raising=False)
    config = resolve_instance_config(instance_dir=instance, allow_cwd_discovery=False)
    assert Path(config.storage.vault_path) == instance.resolve()
    with bind_instance_config(config, secrets=current_secret_values()):
        from archive_cli.commands._resolve import get_vault
        from archive_cli.config import load_archive_config

        assert get_vault() == instance.resolve()
        loaded = load_archive_config()
        assert Path(loaded.vault_path) == instance.resolve()
        assert get_index_schema() == "from_file"
        cache = get_query_embed_cache_path()
        assert str(instance.resolve()) in str(cache)
        assert leftover.name not in str(cache)
        serving = get_serving_index_path()
        assert serving == Path(config.storage.serving_index_path)


def test_cwd_unused_when_instance_dir_set(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cwd_dir = tmp_path / "cwd"
    instance = tmp_path / "inst"
    cwd_dir.mkdir()
    instance.mkdir()
    _write_json(cwd_dir / "ppa.json", _v1_payload(storage={"index_schema": "from_cwd", "index_dsn_ref": {"name": "PPA_INDEX_DSN"}}))
    _write_json(instance / "ppa.json", _v1_payload())
    monkeypatch.chdir(cwd_dir)
    config = resolve_instance_config(instance_dir=instance, environ={}, allow_cwd_discovery=True)
    assert config.storage.index_schema == "from_file"


def test_legacy_migrate_keeps_old_file_and_rollbacks(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    meta = vault / "_meta"
    meta.mkdir(parents=True)
    legacy = meta / "ppa-config.json"
    legacy.write_text(json.dumps({"merge_threshold": 95, "unknown": "kept-on-legacy"}), encoding="utf-8")
    result = migrate_legacy_configuration(vault)
    candidate = Path(result["candidate_path"])
    assert result["legacy_preserved"] is True
    assert legacy.exists()
    assert candidate.exists()
    candidate_payload = json.loads(candidate.read_text(encoding="utf-8"))
    assert candidate_payload["schema_version"] == 1
    assert candidate_payload["vault_tuning"]["merge_threshold"] == 95
    rolled = rollback_legacy_migration(vault)
    assert rolled["candidate_removed"] is True
    assert rolled["legacy_preserved"] is True
    assert not candidate.exists()
    assert legacy.exists()


def test_fingerprint_changes_with_scopes_policy_and_embeddings(tmp_path: Path) -> None:
    instance = tmp_path / "inst"
    instance.mkdir()
    base = _v1_payload()
    _write_json(instance / "ppa.json", base)
    first = resolve_instance_config(instance_dir=instance, environ={}, allow_cwd_discovery=False)
    base["scopes"][0]["sources"] = ["gmail", "slack"]
    _write_json(instance / "ppa.json", base)
    scoped = resolve_instance_config(instance_dir=instance, environ={}, allow_cwd_discovery=False)
    base["access"]["allowed_sources"] = ["gmail"]
    _write_json(instance / "ppa.json", base)
    policy = resolve_instance_config(instance_dir=instance, environ={}, allow_cwd_discovery=False)
    base["embeddings"]["dimension"] = 16
    _write_json(instance / "ppa.json", base)
    embed = resolve_instance_config(instance_dir=instance, environ={}, allow_cwd_discovery=False)
    fingerprints = {first.fingerprint(), scoped.fingerprint(), policy.fingerprint(), embed.fingerprint()}
    assert len(fingerprints) == 4


def test_two_instances_same_uid_isolated(tmp_path: Path) -> None:
    from archive_cli.store import DefaultArchiveStore
    from archive_vault.provenance import ProvenanceEntry
    from archive_vault.schema import PersonCard
    from archive_vault.vault import write_card

    class _Index:
        def __init__(self, mapping: dict[str, str]):
            self.mapping = mapping

        def read_path_for_uid(self, uid: str) -> str | None:
            return self.mapping.get(uid)

        def search(self, query: str, limit: int = 20, **_kwargs):
            return [{"card_uid": uid, "rel_path": rel} for uid, rel in list(self.mapping.items())[:limit]]

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

    def _write(vault: Path, body: str) -> None:
        (vault / "People").mkdir(parents=True, exist_ok=True)
        card = PersonCard(
            uid="hfa-person-shared",
            type="person",
            source=["test"],
            source_id="shared@example.com",
            created="2026-09-06",
            updated="2026-09-06",
            summary="shared",
        )
        write_card(vault, "People/card.md", card, body=body, provenance={"summary": ProvenanceEntry("test", "2026-09-06", "deterministic")})

    left = tmp_path / "a"
    right = tmp_path / "b"
    left.mkdir()
    right.mkdir()
    _write(left, "ARCHIVE-A")
    _write(right, "ARCHIVE-B")
    identity_a = resolve_archive_identity(left, schema_binding=schema_binding_for("ppa"))
    identity_b = resolve_archive_identity(right, schema_binding=schema_binding_for("ppa"))
    assert identity_a.archive_id != identity_b.archive_id
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
        assert "ARCHIVE-A" in str(runtime_a.read("hfa-person-shared").get("content"))
        assert "ARCHIVE-B" in str(runtime_b.read("hfa-person-shared").get("content"))
    finally:
        runtime_a.close()
        runtime_b.close()

    cfg_a = resolve_instance_config(instance_dir=left, environ={}, allow_cwd_discovery=False)
    cfg_b = resolve_instance_config(instance_dir=right, environ={}, allow_cwd_discovery=False)
    assert cfg_a.identity.archive_id == identity_a.archive_id
    assert cfg_b.identity.archive_id == identity_b.archive_id
    assert cfg_a.storage.query_embed_cache_path != cfg_b.storage.query_embed_cache_path
    store_a = DefaultArchiveStore(vault=left, index=_Index({"hfa-person-shared": "People/card.md"}))
    store_b = DefaultArchiveStore(vault=right, index=_Index({"hfa-person-shared": "People/card.md"}))
    try:
        assert store_a.runtime.identity.archive_id != store_b.runtime.identity.archive_id
    finally:
        store_a.close()
        store_b.close()


def test_validate_configuration_reports_origins(tmp_path: Path) -> None:
    instance = tmp_path / "inst"
    instance.mkdir()
    _write_json(instance / "ppa.json", _v1_payload())
    report = validate_configuration(instance_dir=instance, environ={"PPA_INDEX_SCHEMA": "env_schema"})
    assert report["ok"] is True
    assert report["index_schema"] == "env_schema"
    assert "family-2024" in report["scope_names"]
    SavedScope.from_payload({"name": "family-2024", "sources": ["gmail"]})


def test_p09_config_scenario_two_instances(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from archive_tests.acceptance.scenarios.p09_config import run_p09_config

    runtime = SimpleNamespace(root=tmp_path / "runtime")
    runtime.root.mkdir()
    result = run_p09_config(runtime)  # type: ignore[arg-type]
    assert result["status"] == "passed"
    assert result["left_archive_id"] != result["right_archive_id"]
    assert (tmp_path / "p09-config.json").exists()


def test_explain_instance_config_rejects_secret_leak(tmp_path: Path) -> None:
    instance = tmp_path / "inst"
    instance.mkdir()
    _write_json(instance / "ppa.json", _v1_payload())
    config = resolve_instance_config(
        instance_dir=instance,
        environ={"PPA_INDEX_DSN": "postgresql://u:p@localhost/db"},
        allow_cwd_discovery=False,
    )
    explained = explain_instance_config(config, secrets=current_secret_values())
    assert "postgresql://u:p@" not in json.dumps(explained)
