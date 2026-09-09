"""Installed-wheel + real MCP demonstration for PR31. Isolated fixtures only."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.p09_install_support import (
    build_or_load_release,
    install_isolated,
    release_wheels,
    run_installed,
)
from archive_tests.acceptance.p31_mcp import call_tool
from archive_tests.test_pr31_isolated_demo import _seed

REPO = Path(__file__).resolve().parents[2]


def _ensure_repair_meta(vault: Path) -> None:
    meta = vault / "_meta"
    meta.mkdir(parents=True, exist_ok=True)
    defaults = {
        "identity-map.json": "{}",
        "sync-state.json": "{}",
        "own-emails.json": "[]",
        "nicknames.json": "{}",
        "ppa-config.json": "{}",
        "llm-config.json": '{"primary": {"provider": "gemini", "model": "fixture"}}',
    }
    for name, payload in defaults.items():
        path = meta / name
        if not path.exists():
            path.write_text(payload + "\n", encoding="utf-8")


def run_installed_product_path(runtime: IsolatedRuntime) -> dict[str, Any]:
    init_vault(runtime.vault, owned_root=runtime.root)
    _ensure_repair_meta(runtime.vault)
    _seed(runtime.vault)
    preferred = REPO / "logs" / "plans" / "pr31" / "release"
    release_dir = (
        preferred
        if (preferred / "release-manifest.json").is_file()
        else Path(runtime.root).resolve().parent / "release"
    )
    manifest = build_or_load_release(repo=REPO, output=release_dir)
    wheels = release_wheels(release_dir, manifest)
    dest = Path(tempfile.mkdtemp(prefix="ppa-p31-install-"))
    installed = install_isolated(
        dest=dest,
        wheels=wheels,
        lock=REPO / "requirements" / "runtime-py312.lock",
        repo=REPO,
    )
    env = {**os.environ, **dict(runtime.env)}
    env.pop("PYTHONPATH", None)
    env.pop("PPA_TEST_PG_DSN", None)
    env.pop("PPA_CONFIG_PATH", None)
    bootstrap = run_installed(installed, ["bootstrap-postgres", "--force"], extra_env=env)
    if bootstrap.returncode != 0:
        raise AssertionError(bootstrap.stderr or bootstrap.stdout)
    rebuild = run_installed(installed, ["rebuild-indexes", "--force-full-rebuild"], extra_env=env)
    if rebuild.returncode != 0:
        raise AssertionError(rebuild.stderr or rebuild.stdout)
    maintain = run_installed(installed, ["maintain"], extra_env=env)
    if maintain.returncode != 0:
        raise AssertionError(maintain.stderr or maintain.stdout)
    dry = run_installed(installed, ["identity-repair", "merge"], extra_env=env)
    if dry.returncode != 0:
        raise AssertionError(dry.stderr or dry.stdout)
    dry_payload = json.loads(dry.stdout)
    if dry_payload.get("applied") is not False:
        raise AssertionError(f"preview mutated: {dry_payload}")
    applied = run_installed(installed, ["identity-repair", "merge", "--apply"], extra_env=env)
    if applied.returncode != 0:
        raise AssertionError(applied.stderr or applied.stdout)
    apply_payload = json.loads(applied.stdout)
    touched = {item["winner"] for item in apply_payload.get("redirected") or []} | {
        item["loser"] for item in apply_payload.get("redirected") or []
    }
    if "hfa-person-alice000001" in touched or "hfa-person-bob00000001" in touched:
        raise AssertionError(f"household namesakes merged: {apply_payload}")
    if not {"hfa-person-pat00000001", "hfa-person-stub0000001"} <= touched:
        raise AssertionError(f"stub merge missing: {apply_payload}")
    person = run_installed(installed, ["person", "+15551230000"], extra_env=env)
    if person.returncode != 0:
        raise AssertionError(person.stderr or person.stdout)
    person_payload = json.loads(person.stdout)
    if person_payload.get("status") != "ambiguous":
        raise AssertionError(f"CLI person did not stay ambiguous: {person_payload}")
    mcp = call_tool(
        [str(installed.ppa), "serve"],
        env={**installed.env, **env},
        tool="archive_person",
        arguments={"name": "+15551230000"},
        cwd=installed.root,
        python=installed.python,
    )
    text = json.dumps(mcp["result"])
    if "Ambiguous" not in text and "ambiguous" not in text.lower():
        raise AssertionError(f"MCP archive_person missed ambiguity: {mcp}")
    return {
        "id": "p31.end_to_end",
        "status": "passed",
        "public_path": "installed wheel CLI + stdio MCP JSON-RPC",
        "mcp_helper_used": False,
        "living_archive": "untouched",
        "extension_path": installed.extension_path,
        "wheel_hashes": installed.wheel_hashes,
        "release_sha": manifest.get("git_sha"),
        "cli_person_status": person_payload.get("status"),
        "mcp_tool": "archive_person",
        "applied_redirects": sorted(touched),
    }
