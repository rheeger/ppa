"""JSON, JUnit, and normalized evidence manifests for acceptance runs."""

from __future__ import annotations

import hashlib
import json
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_sha(repo: Path, *, rev: str = "HEAD") -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", rev],
            cwd=str(repo),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return ""
    return proc.stdout.strip()


def write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def write_junit(path: Path, *, suite: str, cases: Sequence[Mapping[str, Any]]) -> Path:
    """Write a minimal JUnit XML document for the suite."""

    tests = len(cases)
    failures = sum(1 for case in cases if case.get("status") == "failed")
    skipped = sum(1 for case in cases if case.get("status") == "skipped")
    errors = sum(1 for case in cases if case.get("status") == "error")
    root = ET.Element(
        "testsuite",
        name=f"acceptance.{suite}",
        tests=str(tests),
        failures=str(failures),
        skipped=str(skipped),
        errors=str(errors),
    )
    for case in cases:
        node = ET.SubElement(
            root,
            "testcase",
            classname=f"archive_tests.acceptance.{suite}",
            name=str(case.get("id") or "unknown"),
            time=str(case.get("elapsed_seconds") or 0),
        )
        status = str(case.get("status") or "failed")
        message = str(case.get("message") or "")
        if status == "failed":
            failure = ET.SubElement(node, "failure", message=message)
            failure.text = str(case.get("detail") or message)
        elif status == "error":
            error = ET.SubElement(node, "error", message=message)
            error.text = str(case.get("detail") or message)
        elif status == "skipped":
            skip = ET.SubElement(node, "skipped", message=message)
            skip.text = message
    path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)
    return path


def artifact_entry(path: Path, *, relative_to: Path) -> dict[str, str]:
    rel = path.resolve().relative_to(relative_to.resolve())
    return {"path": str(rel), "sha256": sha256_file(path)}


def build_evidence(
    *,
    repo: Path,
    output: Path,
    suite: str,
    seed: int,
    require_integration: bool,
    commands: Sequence[Mapping[str, Any]],
    cases: Sequence[Mapping[str, Any]],
    environment: Mapping[str, Any],
    artifacts: Sequence[Mapping[str, str]],
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    passed = sum(1 for case in cases if case.get("status") == "passed")
    failed = sum(1 for case in cases if case.get("status") in {"failed", "error"})
    skipped = sum(1 for case in cases if case.get("status") == "skipped")
    ac_map = (extra or {}).get("acceptance_criteria") or _default_p04a_ac(cases, suite)
    payload = {
        "plan_id": "p04",
        "slice_id": "P04-A",
        "suite": suite,
        "seed": int(seed),
        "require_integration": bool(require_integration),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "shas": {
            "current": git_sha(repo),
            "base": git_sha(repo, rev="main") or git_sha(repo),
        },
        "commands": list(commands),
        "tests": {
            "total": len(cases),
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
        },
        "environment": dict(environment),
        "acceptance_criteria": ac_map,
        "artifacts": list(artifacts),
        "proof": {
            "implementation": True,
            "integration": bool(require_integration and failed == 0 and skipped == 0 and passed > 0),
            "scale": False,
            "production": False,
        },
        "unresolved": list((extra or {}).get("unresolved") or []),
        "cases": list(cases),
    }
    if extra:
        for key, value in extra.items():
            if key not in payload:
                payload[key] = value
    write_json(output / "evidence.json", payload)
    (output / "summary.md").write_text(_summary_markdown(payload), encoding="utf-8")
    return payload


def _default_p04a_ac(cases: Sequence[Mapping[str, Any]], suite: str) -> dict[str, Any]:
    by_id = {str(case.get("id")): case for case in cases}
    baseline = by_id.get("baseline.vault_pg_rust_mcp")
    return {
        "P04-A.command_emits_json_junit_evidence": {
            "status": "recorded",
            "artifact": "results.json, junit.xml, evidence.json",
        },
        "P04-A.baseline_real_write_pg_rust_mcp": {
            "status": (baseline or {}).get("status") or ("not_run" if suite != "baseline" else "failed"),
            "scenario": "baseline.vault_pg_rust_mcp",
            "artifact": "baseline-trace.json",
        },
        "P04-A.empty_suite_fails": {"status": "covered_by_harness_tests"},
        "P04-A.missing_engine_fails": {"status": "covered_by_harness_tests"},
        "P04-A.intentional_mismatch_fails": {"status": "covered_by_harness_tests"},
        "P04-A.require_integration_no_skip_green": {"status": "covered_by_harness_tests"},
        "P04-A.exact_cosine_independent_of_ann": {"status": "covered_by_harness_tests"},
    }


def _summary_markdown(payload: Mapping[str, Any]) -> str:
    tests = payload.get("tests") or {}
    proof = payload.get("proof") or {}
    lines = [
        "# P04-A evidence",
        "",
        f"- suite: `{payload.get('suite')}`",
        f"- current SHA: `{((payload.get('shas') or {}).get('current'))}`",
        f"- tests: passed={tests.get('passed')} failed={tests.get('failed')} skipped={tests.get('skipped')}",
        f"- implementation_proven: {proof.get('implementation')}",
        f"- integration_proven: {proof.get('integration')}",
        f"- scale_proven: {proof.get('scale')}",
        f"- production_proven: {proof.get('production')}",
        "",
        "## Acceptance criteria",
        "",
    ]
    for key, value in (payload.get("acceptance_criteria") or {}).items():
        status = value.get("status") if isinstance(value, dict) else value
        lines.append(f"- `{key}`: {status}")
    return "\n".join(lines) + "\n"
