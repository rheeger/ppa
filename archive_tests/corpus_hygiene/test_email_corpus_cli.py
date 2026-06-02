"""Corpus-hygiene apply/rollback CLI error handling (no tracebacks on misuse)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from archive_cli.corpus_hygiene.cli import cmd_email_apply, cmd_email_rollback
from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.migrate import MigrationRunner
from archive_cli.validation_gates.constants import EXIT_BLOCKED, EXIT_REFUSED
from archive_cli.validation_gates.instance_identity import derive_archive_instance


def _minimal_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in ("People", "Finance", "Attachments", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    return vault


def _bootstrap_schema(dsn: str, vault: Path, schema: str) -> PostgresArchiveIndex:
    idx = PostgresArchiveIndex(vault=vault, dsn=dsn)
    idx.schema = schema
    with idx._connect() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {schema}")
        idx._create_schema(conn)
        runner = MigrationRunner(conn, schema)
        runner.ensure_table()
        runner.run()
    return idx


def _apply_args(**overrides: object) -> argparse.Namespace:
    defaults = {
        "decision_run_id": "missing-run",
        "vault": "",
        "instance_role": "fixture",
        "format": "json",
        "confirm_production": False,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)  # type: ignore[arg-type]


def test_apply_missing_vault_returns_blocked_json(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    monkeypatch.setenv("PPA_PATH", "/nonexistent/ppa/vault-for-cli-test")
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)

    rc = cmd_email_apply(_apply_args())

    assert rc == EXIT_BLOCKED
    out = capsys.readouterr()
    assert "Traceback" not in out.err
    payload = json.loads(out.out)
    assert payload["blocked"] is True
    assert payload["reason"] == "vault_not_found"


def test_rollback_missing_vault_returns_blocked_json(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    monkeypatch.setenv("PPA_PATH", "/nonexistent/ppa/vault-for-cli-test")
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)

    rc = cmd_email_rollback(_apply_args())

    assert rc == EXIT_BLOCKED
    out = capsys.readouterr()
    assert "Traceback" not in out.err
    payload = json.loads(out.out)
    assert payload["blocked"] is True
    assert payload["reason"] == "vault_not_found"


@pytest.mark.integration
def test_apply_unknown_decision_run_returns_refused_json(
    pgvector_dsn: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    vault = _minimal_vault(tmp_path)
    schema = "ppa_corpus_cli_apply"
    idx = _bootstrap_schema(pgvector_dsn, vault, schema)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)

    archive_instance = derive_archive_instance(
        vault_path=str(vault),
        index_dsn=pgvector_dsn,
        index_schema=schema,
        instance_role="fixture",
    )
    assert archive_instance.startswith("fixture:")

    rc = cmd_email_apply(_apply_args(vault=str(vault), decision_run_id="missing-run"))

    assert rc == EXIT_REFUSED
    out = capsys.readouterr()
    assert "Traceback" not in out.err
    payload = json.loads(out.out)
    assert payload["refused"] is True
    assert payload["reason"] == "unknown_decision_run_id"


@pytest.mark.integration
def test_rollback_unknown_decision_run_returns_refused_json(
    pgvector_dsn: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    vault = _minimal_vault(tmp_path)
    schema = "ppa_corpus_cli_rollback"
    _bootstrap_schema(pgvector_dsn, vault, schema)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)

    rc = cmd_email_rollback(_apply_args(vault=str(vault), decision_run_id="missing-run"))

    assert rc == EXIT_REFUSED
    out = capsys.readouterr()
    assert "Traceback" not in out.err
    payload = json.loads(out.out)
    assert payload["refused"] is True
    assert payload["reason"] == "unknown_decision_run_id"
