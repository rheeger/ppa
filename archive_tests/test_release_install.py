"""P09-C: wheels install outside the repo and serve a first fixture query."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_cli.commands.setup import FIXTURE_PERSON_UID
from archive_tests.acceptance.environment import provision_isolated_runtime
from archive_tests.acceptance.p09_install_support import (
    build_or_load_release,
    install_isolated,
    release_wheels,
    run_installed,
)
from archive_tests.conftest import require_integration_enabled

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _unset_test_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)


@pytest.mark.integration
def test_clean_install_first_useful_query(tmp_path: Path, pytestconfig: pytest.Config) -> None:
    if not require_integration_enabled(pytestconfig):
        pytest.skip("P09-C install proof requires --require-integration")
    runtime = provision_isolated_runtime(tmp_path / "iso", require_integration=True, suite="p09")
    try:
        release_dir = tmp_path / "release"
        manifest = build_or_load_release(repo=REPO, output=release_dir)
        wheels = release_wheels(release_dir, manifest)
        installed = install_isolated(
            dest=tmp_path / "installed",
            wheels=wheels,
            lock=REPO / "requirements" / "runtime-py312.lock",
            repo=REPO,
        )
        assert REPO.resolve() not in Path(installed.extension_path).parents
        spec = {
            "root": str(runtime.vault),
            "entity_name": "Ada Example",
            "entity_type": "person",
            "index_schema": runtime.schema,
            "fixture": "sample.fixture",
            "embedding_provider": "hash",
        }
        spec_path = tmp_path / "spec.json"
        spec_path.write_text(json.dumps(spec), encoding="utf-8")
        env = dict(runtime.env)
        env.pop("PPA_TEST_PG_DSN", None)
        setup = run_installed(
            installed,
            ["setup", "--non-interactive", "--from", str(spec_path), "--apply"],
            extra_env=env,
        )
        if setup.returncode != 0:
            raise AssertionError(setup.stderr or setup.stdout)
        setup_payload = json.loads(setup.stdout)
        assert setup_payload["fixture_person_uid"] == FIXTURE_PERSON_UID
        assert setup_payload["fresh"] is False
        bootstrap = run_installed(installed, ["bootstrap-postgres", "--force"], extra_env=env)
        if bootstrap.returncode != 0:
            raise AssertionError(bootstrap.stderr or bootstrap.stdout)
        rebuild = run_installed(installed, ["rebuild-indexes", "--force-full-rebuild"], extra_env=env)
        if rebuild.returncode != 0:
            raise AssertionError(rebuild.stderr or rebuild.stdout)
        maintain = run_installed(installed, ["maintain"], extra_env=env)
        if maintain.returncode != 0:
            raise AssertionError(maintain.stderr or maintain.stdout)
        read = run_installed(installed, ["read", FIXTURE_PERSON_UID], extra_env=env)
        if read.returncode != 0:
            raise AssertionError(read.stderr or read.stdout)
        payload = json.loads(read.stdout)
        assert payload.get("found") is True
        assert FIXTURE_PERSON_UID in str(payload.get("content") or payload)
        assert "Ada" in str(payload.get("content") or "")
        help_out = run_installed(installed, ["setup", "--help"], extra_env=env)
        assert help_out.returncode == 0
        assert "apply" in help_out.stdout.lower()
    finally:
        runtime.cleanup()
