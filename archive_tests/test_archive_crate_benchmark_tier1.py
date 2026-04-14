"""Smoke: Tier 1 benchmark script JSON shape on the fixture vault (no --enforce on tiny trees)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

from archive_tests.fixtures import load_fixture_vault

PPA_ROOT = Path(__file__).resolve().parents[1]


def test_benchmark_tier1_script_outputs_expected_keys(tmp_path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    env = os.environ.copy()
    env["PPA_BENCHMARK_VAULT"] = str(vault)
    env.pop("PPA_BENCHMARK_SKIP_CACHE", None)
    proc = subprocess.run(
        [sys.executable, str(PPA_ROOT / "archive_scripts" / "benchmark-archive_crate-tier1.py")],
        cwd=str(PPA_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr + proc.stdout
    data = json.loads(proc.stdout)
    assert data["walk"]["python_note_count"] >= 1
    assert data["walk"]["python_over_rust_walltime"] is not None
    assert data["fingerprint"]["fingerprint_match"] is True
    assert data["cache_build"]["python_over_rust_cache_seconds"] is not None
    assert data["cache_build"]["cache_python_tier2_seconds"] is not None
    assert data["cache_build"]["cache_rust_tier2_seconds"] is not None
