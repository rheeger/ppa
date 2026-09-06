"""P09-C: setup plan/apply, no overwrite, no silent approval."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from archive_cli.command_registry import register_product_commands
from archive_cli.commands.setup import (
    FIXTURE_PERSON_UID,
    SetupError,
    collect_guided_spec,
    detect_capabilities,
    plan_setup,
    run_setup,
)


@pytest.fixture(autouse=True)
def _unset_test_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)


def _spec(root: Path) -> dict:
    return {
        "root": str(root),
        "entity_name": "Ada Example",
        "entity_type": "person",
        "index_schema": "ppa_fixture",
        "fixture": "sample.fixture",
        "embedding_provider": "hash",
    }


def test_plan_does_not_write(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    planned = plan_setup(_spec(root))
    assert planned["apply"] is False
    assert planned["would_write"] is True
    assert not root.exists()
    assert planned["fixture_person_uid"] == FIXTURE_PERSON_UID
    assert planned["production_proven"] is False


def test_non_interactive_without_apply_is_review_only(tmp_path: Path) -> None:
    spec_path = tmp_path / "spec.json"
    root = tmp_path / "archive"
    spec_path.write_text(json.dumps(_spec(root)), encoding="utf-8")
    result = run_setup(spec_path=spec_path, apply=False, non_interactive=True)
    assert result["applied"] is False if "applied" in result else result["apply"] is False
    assert not root.exists()


def test_apply_creates_only_requested_root(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    result = run_setup(spec=_spec(root), apply=True, non_interactive=True)
    assert result["applied"] is True
    assert result["fixture_person_uid"] == FIXTURE_PERSON_UID
    assert (root / "ppa.json").is_file()
    assert (root / "People" / f"{FIXTURE_PERSON_UID}.md").is_file()
    extras = [path for path in tmp_path.iterdir() if path.name != "archive"]
    assert extras == []
    assert result["fresh"] is False
    assert result["capabilities"]["fresh"] is False


def test_existing_root_not_overwritten(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    root.mkdir()
    (root / "keep.md").write_text("stay", encoding="utf-8")
    with pytest.raises(SetupError, match="overwritten"):
        run_setup(spec=_spec(root), apply=True, non_interactive=True)
    assert (root / "keep.md").read_text(encoding="utf-8") == "stay"


def test_missing_interactive_answer_is_not_apply(tmp_path: Path) -> None:
    collected = collect_guided_spec(defaults=_spec(tmp_path / "archive"), input_fn=lambda _prompt: "")
    assert collected is None
    result = run_setup(spec=None, apply=False, non_interactive=False, input_fn=lambda _prompt: "")
    assert result["applied"] is False
    assert "approval" in result["reason"]


def test_seed_path_rejected(tmp_path: Path) -> None:
    with pytest.raises(SetupError, match="seed"):
        plan_setup(_spec(Path("/Users/rheeger/Archive/vault")))


def test_non_fixture_source_rejected(tmp_path: Path) -> None:
    spec = _spec(tmp_path / "archive")
    spec["fixture"] = "gmail-messages"
    with pytest.raises(SetupError, match="fixture-only"):
        plan_setup(spec)


def test_instance_status_never_fresh_from_manifest(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    run_setup(spec=_spec(root), apply=True, non_interactive=True)
    status = detect_capabilities(vault=root, environ={})
    assert status["fresh"] is False
    assert status["manifest_exists"] is True
    assert status["auth"]["status"] == "pending"
    assert status["warehouse"]["status"] == "pending"
    assert status["production_proven"] is False


def test_command_help_states_write_and_provider_risk() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    register_product_commands(sub)
    helps = {name: sub.choices[name].format_help() for name in sub.choices}
    assert "writes" in helps["setup"].lower() or "--apply" in helps["setup"]
    assert "no live" in helps["setup"].lower() or "accounts" in helps["setup"].lower()
    assert "read-only" in helps["config"].lower() or "secrets" in helps["config"].lower()
    assert "passphrase" in helps["backup"].lower()
    assert "fresh" in helps["instance-status"].lower()
    assert "google" in helps["connect"].lower() or "credentials" in helps["connect"].lower()
