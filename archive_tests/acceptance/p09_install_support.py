"""Reusable isolated wheel-install helper (P09-C).

P04/P06 consume candidate wheel paths/hashes plus an owned temp root.
The installed CLI is executed from that root — never via an editable
checkout inside the repository.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from archive_tests.conftest import OWNED_ROOT_MARKER, assert_owned_test_root


class InstallProofError(RuntimeError):
    """Isolated install or installed-CLI proof failed."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class InstalledRuntime:
    root: Path
    python: Path
    ppa: Path
    extension_path: str
    wheel_hashes: dict[str, str]
    env: dict[str, str]


def assert_outside_repo(path: Path, repo: Path) -> None:
    resolved = path.resolve()
    repo_root = repo.resolve()
    if resolved == repo_root or repo_root in resolved.parents:
        raise InstallProofError(f"install root must be outside the source checkout: {resolved}")


def load_release_manifest(output: Path) -> dict[str, Any]:
    manifest = Path(output) / "release-manifest.json"
    if not manifest.is_file():
        raise InstallProofError(f"missing release manifest: {manifest}")
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise InstallProofError("release manifest must be an object")
    return payload


def install_isolated(
    *,
    dest: Path,
    wheels: list[Path],
    lock: Path,
    repo: Path,
    extra_env: Mapping[str, str] | None = None,
    python: str | None = None,
) -> InstalledRuntime:
    """Create a fresh venv, install hashed deps, then local wheels with --no-deps."""

    dest = Path(dest).resolve()
    dest.mkdir(parents=True, exist_ok=True)
    (dest / OWNED_ROOT_MARKER).write_text("ppa-p09c-owned\n", encoding="utf-8")
    assert_owned_test_root(dest)
    assert_outside_repo(dest, repo)
    venv_dir = dest / "venv"
    creator = python or (
        "/opt/homebrew/bin/python3.12" if Path("/opt/homebrew/bin/python3.12").is_file() else sys.executable
    )
    subprocess.run([creator, "-m", "venv", str(venv_dir)], check=True)
    python = venv_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    ppa = venv_dir / ("Scripts/ppa.exe" if os.name == "nt" else "bin/ppa")
    hashes = {path.name: _sha256(path) for path in wheels}
    env = {**os.environ, **dict(extra_env or {})}
    env.pop("PPA_TEST_PG_DSN", None)
    env.pop("PYTHONPATH", None)
    subprocess.run([str(python), "-m", "pip", "install", "-q", "-U", "pip"], check=True, env=env)
    subprocess.run(
        [str(python), "-m", "pip", "install", "--require-hashes", "-r", str(lock)],
        check=True,
        env=env,
    )
    wheel_args = [str(path) for path in wheels]
    subprocess.run([str(python), "-m", "pip", "install", "--no-deps", *wheel_args], check=True, env=env)
    probe = subprocess.check_output(
        [str(python), "-c", "import archive_crate, pathlib; print(pathlib.Path(archive_crate.__file__).resolve())"],
        text=True,
        env=env,
    ).strip()
    if str(repo.resolve()) in probe:
        raise InstallProofError(f"installed extension still points at the source checkout: {probe}")
    if not ppa.is_file():
        raise InstallProofError(f"installed ppa entry point missing: {ppa}")
    return InstalledRuntime(
        root=dest,
        python=python,
        ppa=ppa,
        extension_path=probe,
        wheel_hashes=hashes,
        env=env,
    )


def run_installed(
    runtime: InstalledRuntime,
    argv: list[str],
    *,
    extra_env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = {**runtime.env, **dict(extra_env or {})}
    env.pop("PPA_TEST_PG_DSN", None)
    env.pop("PYTHONPATH", None)
    return subprocess.run(
        [str(runtime.ppa), *argv],
        cwd=str(cwd or runtime.root),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _load_build_release(repo: Path):
    import importlib.util

    path = Path(repo) / "archive_scripts" / "build_release.py"
    spec = importlib.util.spec_from_file_location("ppa_build_release", path)
    if spec is None or spec.loader is None:
        raise InstallProofError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.build_release


def build_or_load_release(*, repo: Path, output: Path, python: str | None = None) -> dict[str, Any]:
    """Build wheels if the manifest is missing or SHA drifted."""

    output = Path(output)
    current = subprocess_sha(repo)
    if (output / "release-manifest.json").is_file():
        payload = load_release_manifest(output)
        if payload.get("git_sha") == current and payload.get("root_wheel") and payload.get("native_wheel"):
            return payload
    return _load_build_release(repo)(output=output, python=python)


def subprocess_sha(repo: Path) -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()


def release_wheels(output: Path, manifest: Mapping[str, Any]) -> list[Path]:
    names = [str(manifest.get("root_wheel") or ""), str(manifest.get("native_wheel") or "")]
    paths = [Path(output) / name for name in names if name]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise InstallProofError(f"missing wheels: {missing}")
    return paths
