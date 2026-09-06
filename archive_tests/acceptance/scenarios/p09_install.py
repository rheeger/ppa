"""P09-C acceptance: installed CLI outside the checkout, first fixture query."""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import Any

from archive_cli.commands.setup import FIXTURE_PERSON_UID
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.p09_install_support import (
    build_or_load_release,
    install_isolated,
    release_wheels,
    run_installed,
)
from archive_tests.acceptance.registry import Scenario, register

REPO = Path(__file__).resolve().parents[3]


def run_p09_install(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    release_dir = Path(runtime.root).resolve().parent / "release"
    manifest = build_or_load_release(repo=REPO, output=release_dir)
    wheels = release_wheels(release_dir, manifest)
    dest = Path(tempfile.mkdtemp(prefix="ppa-p09c-install-"))
    installed = install_isolated(
        dest=dest,
        wheels=wheels,
        lock=REPO / "requirements" / "runtime-py312.lock",
        repo=REPO,
    )
    spec = {
        "root": str(runtime.vault),
        "entity_name": "Ada Example",
        "entity_type": "person",
        "index_schema": runtime.schema,
        "fixture": "sample.fixture",
        "embedding_provider": "hash",
    }
    spec_path = Path(runtime.root) / "setup-spec.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    env = dict(runtime.env)
    setup = run_installed(
        installed,
        ["setup", "--non-interactive", "--from", str(spec_path), "--apply"],
        extra_env=env,
    )
    if setup.returncode != 0:
        raise AssertionError(setup.stderr or setup.stdout)
    setup_payload = json.loads(setup.stdout)
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
    body = json.loads(read.stdout)
    if not body.get("found") or "Ada" not in str(body.get("content") or ""):
        raise AssertionError(f"first query missed fixture card: {body}")
    status = run_installed(installed, ["instance-status", "--instance-dir", str(runtime.vault)], extra_env=env)
    status_payload = json.loads(status.stdout) if status.returncode == 0 else {}
    if status_payload.get("fresh") is True:
        raise AssertionError("instance-status claimed fresh from a manifest")
    payload = {
        "id": "p09.install.clean_first_query",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "extension_path": installed.extension_path,
        "first_query_uid": FIXTURE_PERSON_UID,
        "archive_id": setup_payload.get("archive_id"),
        "wheel_hashes": installed.wheel_hashes,
        "release_sha": manifest.get("git_sha"),
        "production_proven": False,
    }
    artifact = runtime.root.parent / "p09-install.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


register(
    Scenario(
        id="p09.install.clean_first_query",
        suite="p09",
        product_guarantee="A clean wheel install outside the checkout can setup, maintain, and read a fixture card",
        proof_tier="isolated_integration",
        fixture_seed=9,
        fixture_hash="",
        prerequisites=("docker", "rust_engine"),
        expected_artifacts=("p09-install.json",),
        run=run_p09_install,
    )
)
