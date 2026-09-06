"""P05-C acceptance: outbound calls stay inside policy; logs stay redacted."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest

from archive_engine.redaction import redact_text, safe_diagnostic_id
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_provider_egress_policy import SYN_KEY, SYN_MEDICAL, build_outbound_matrix

FORBIDDEN_NEEDLES = (
    SYN_KEY,
    SYN_MEDICAL,
    "s3cretPass",
    "sk-proj-",
    "postgresql://ppa_user:",
    "BEGIN RSA PRIVATE KEY",
)


def _output_dir(runtime: IsolatedRuntime) -> Path:
    return Path(runtime.root).resolve().parent


def _scan_secrets(root: Path, *, exclude: set[Path] | None = None) -> list[str]:
    hits: list[str] = []
    skip_names = {"runtime", "negative-secret-scan.json"}
    excluded = {path.resolve() for path in (exclude or set())}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.resolve() in excluded or path.name in skip_names:
            continue
        if any(part == "runtime" for part in path.parts):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for needle in FORBIDDEN_NEEDLES:
            if needle in text:
                hits.append(f"{path.name}:{safe_diagnostic_id(needle)}")
    return hits


def run_p05_egress(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
        monkeypatch.delenv("PPA_EGRESS_MODE", raising=False)
        matrix = build_outbound_matrix(monkeypatch)
    finally:
        monkeypatch.undo()

    output = _output_dir(runtime)
    payload = {
        "id": "p05.egress.outbound_matrix",
        "status": "passed",
        "matrix": matrix,
        "data_boundaries": "archive_docs/DATA_BOUNDARIES.md",
        "encryption_claim": "none — PPA does not encrypt at rest",
    }
    redacted = json.loads(redact_text(json.dumps(payload)))
    (output / "outbound-call-matrix.json").write_text(
        json.dumps(redacted, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output / "p05-egress.json").write_text(
        json.dumps(redacted, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    hits = _scan_secrets(output, exclude={output / "results.json", output / "evidence.json", output / "junit.xml"})
    scan = {
        "hits": hits,
        "needle_ids": [safe_diagnostic_id(item) for item in FORBIDDEN_NEEDLES],
        "ok": not hits,
    }
    (output / "negative-secret-scan.json").write_text(
        json.dumps(scan, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if hits:
        raise AssertionError(f"secret needles in evidence: {hits}")
    return {
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "matrix": matrix,
        "secret_scan_ok": True,
        "artifacts": ["outbound-call-matrix.json", "p05-egress.json", "negative-secret-scan.json"],
    }


register(
    Scenario(
        id="p05.egress.outbound_matrix",
        suite="p05",
        product_guarantee="Local-only and restricted principals cannot send denied content to a provider; logs carry no tokens or DSNs",
        proof_tier="isolated_integration",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p05-egress.json", "outbound-call-matrix.json", "negative-secret-scan.json"),
        run=run_p05_egress,
    )
)
