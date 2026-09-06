"""Importable release builder (P09-C)."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _toolchain_bin() -> Path | None:
    home = Path.home() / ".rustup" / "toolchains"
    preferred = home / "1.85-aarch64-apple-darwin" / "bin"
    if preferred.is_dir():
        return preferred
    return None


def _run(argv: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(argv, cwd=cwd or REPO, check=False, capture_output=True, text=True, env=env)
    text = (result.stdout or "") + (result.stderr or "")
    if result.returncode != 0:
        raise RuntimeError(f"{argv[0]} failed ({result.returncode}): {text[-2000:]}")
    return text


def build_release(*, output: Path, python: str | None = None) -> dict[str, Any]:
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    py = python or (
        "/opt/homebrew/bin/python3.12" if Path("/opt/homebrew/bin/python3.12").is_file() else sys.executable
    )
    env = dict(os.environ)
    toolchain = _toolchain_bin()
    if toolchain is not None:
        env["PATH"] = f"{toolchain}{os.pathsep}{env.get('PATH', '')}"
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    rustc = subprocess.check_output(["rustc", "--version"], text=True, env=env).strip()
    cargo_lock = REPO / "archive_crate" / "Cargo.lock"
    runtime_lock = REPO / "requirements" / "runtime-py312.lock"
    dev_lock = REPO / "requirements" / "dev-py312.lock"

    native_out = _run(
        [
            "maturin",
            "build",
            "--release",
            "--locked",
            "--interpreter",
            py,
            "--manifest-path",
            str(REPO / "archive_crate" / "Cargo.toml"),
            "--out",
            str(output),
        ],
        env=env,
    )
    builder = sys.executable
    _run([builder, "-m", "pip", "install", "-q", "build"], env=env)
    _run([builder, "-m", "build", "--wheel", "--outdir", str(output)], env=env)

    wheels = sorted(output.glob("*.whl"))
    wheel_hashes = {path.name: sha256_file(path) for path in wheels}
    native = [name for name in wheel_hashes if name.startswith("archive_crate-")]
    root = [name for name in wheel_hashes if name.startswith("ppa-")]
    payload = {
        "schema_version": 1,
        "git_sha": sha,
        "python": subprocess.check_output([py, "--version"], text=True).strip(),
        "python_implementation": platform.python_implementation(),
        "os": platform.system(),
        "arch": platform.machine(),
        "platform": sys.platform,
        "compiler": rustc,
        "cargo_lock_sha256": sha256_file(cargo_lock) if cargo_lock.exists() else "",
        "runtime_lock_sha256": sha256_file(runtime_lock) if runtime_lock.exists() else "",
        "dev_lock_sha256": sha256_file(dev_lock) if dev_lock.exists() else "",
        "wheels": wheel_hashes,
        "root_wheel": root[0] if root else "",
        "native_wheel": native[0] if native else "",
        "verified_profile": "py312",
        "requires_python": ">=3.10",
        "production_proven": False,
        "maturin_log_tail": native_out[-500:],
    }
    (output / "release-manifest.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build PPA root + native wheels from one SHA")
    parser.add_argument("--output", required=True, help="Directory for wheels and release-manifest.json")
    parser.add_argument("--python", default="", help="Python used to build the root wheel")
    args = parser.parse_args(argv)
    payload = build_release(output=Path(args.output), python=args.python or None)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
