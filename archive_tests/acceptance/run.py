"""Acceptance runner: ``python -m archive_tests.acceptance.run``."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Sequence

from archive_tests.acceptance import SUITE_IDS
from archive_tests.acceptance.environment import (
    IntegrationRequiredError,
    IsolatedRuntime,
    IsolationError,
    provision_isolated_runtime,
    validate_no_production_config,
)
from archive_tests.acceptance.evidence import artifact_entry, build_evidence, write_json, write_junit
from archive_tests.acceptance.registry import load_builtin_scenarios, scenarios_for

logger = logging.getLogger("ppa.acceptance")


class RunnerError(RuntimeError):
    """Runner-level failure (empty suite, bad output path, missing proof)."""


def _configure_logging() -> None:
    from archive_engine.redaction import redacting_formatter

    root = logging.getLogger("ppa")
    if root.handlers:
        for handler in root.handlers:
            handler.setFormatter(redacting_formatter())
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(redacting_formatter())
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PPA isolated product acceptance runner")
    parser.add_argument("--suite", required=True, choices=SUITE_IDS, help="Suite id (baseline, release, p01-p10, p31)")
    parser.add_argument("--output", required=True, help="Directory for JSON, JUnit, and evidence")
    parser.add_argument(
        "--require-integration",
        action="store_true",
        help="Missing Docker or native engine is a failure, not a skip",
    )
    parser.add_argument("--scale-profile", default="", help="Reserved scale profile name (P04-B+)")
    parser.add_argument("--seed", type=int, default=0, help="Fixture seed")
    return parser.parse_args(argv)


def validate_output_path(raw: str, *, repo: Path) -> Path:
    """Reject existing files and paths that look like production/seed trees."""

    if not raw or not str(raw).strip():
        raise RunnerError("output path is required")
    path = Path(raw).expanduser()
    if path.exists() and path.is_file():
        raise RunnerError(f"output path is an existing file, not a directory: {path}")
    text = str(path)
    forbidden = ("hf-archives-seed", "/Archive/seed/", "/Users/rheeger/Archive/vault")
    if any(marker in text for marker in forbidden):
        raise RunnerError(f"refusing output path that looks like a seed/production tree: {path}")
    try:
        resolved = path.resolve()
    except OSError as exc:
        raise RunnerError(f"output path is not usable: {path}: {exc}") from exc
    return resolved


def _run_scenario(scenario, runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    try:
        result = dict(scenario.run(runtime))
        result.setdefault("id", scenario.id)
        result.setdefault("status", "passed")
        result.setdefault("elapsed_seconds", round(time.monotonic() - started, 3))
        return result
    except Exception as exc:
        logger.exception("scenario_failed id=%s", scenario.id)
        return {
            "id": scenario.id,
            "status": "failed",
            "message": str(exc),
            "detail": str(exc),
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }


def run_suite(
    *,
    suite: str,
    output: Path,
    require_integration: bool,
    seed: int = 0,
    scale_profile: str = "",
    repo: Path | None = None,
) -> dict[str, Any]:
    """Provision isolation, execute ``suite``, and write evidence. Raises on hard failures."""

    repo = repo or Path(__file__).resolve().parents[2]
    load_builtin_scenarios()
    try:
        scenarios = scenarios_for(suite)
    except ValueError as exc:
        raise RunnerError(str(exc)) from exc
    if not scenarios:
        raise RunnerError(f"suite {suite!r} has zero scenarios")

    # CI integration sets a job-level shared DSN; the suite still owns its own.
    os.environ.pop("PPA_TEST_PG_DSN", None)
    validate_no_production_config()
    output.mkdir(parents=True, exist_ok=True)
    runtime_root = output / "runtime"
    commands: list[dict[str, Any]] = [
        {
            "argv": [
                sys.executable,
                "-m",
                "archive_tests.acceptance.run",
                "--suite",
                suite,
                "--output",
                str(output),
                *(["--require-integration"] if require_integration else []),
            ]
        }
    ]
    runtime: IsolatedRuntime | None = None
    cases: list[dict[str, Any]] = []
    extra: dict[str, Any] = {"scale_profile": scale_profile}
    try:
        runtime = provision_isolated_runtime(
            runtime_root,
            require_integration=require_integration,
            seed=seed,
            suite=suite,
        )
        runtime.apply()
        extra["engine"] = runtime.engine
        extra["schema"] = runtime.schema
        extra["container_name"] = runtime.container_name
        if scale_profile:
            os.environ["PPA_SCALE_PROFILE"] = scale_profile
            setattr(runtime, "scale_profile", scale_profile)
        for scenario in scenarios:
            logger.info("scenario_start id=%s suite=%s", scenario.id, suite)
            case = _run_scenario(scenario, runtime)
            cases.append(case)
            if case.get("id") == "baseline.vault_pg_rust_mcp":
                write_json(output / "baseline-trace.json", {k: v for k, v in case.items() if k not in {"mcp_read"}})
            if case.get("id") == "p04.frozen_corpus":
                write_json(output / "corpus-trace.json", case)
            if case.get("id") == "p04.crash_matrix":
                write_json(output / "crash-matrix.json", case)
            if case.get("id") == "p04.privacy_restore":
                write_json(output / "privacy-restore.json", case)
                receipt = case.get("receipt_path")
                if receipt:
                    src = Path(str(receipt))
                    dest = output / "restore-receipt.json"
                    if src.is_file():
                        dest.write_bytes(src.read_bytes())
            if case.get("id") == "release.integrated_gate":
                write_json(output / "release-evidence.json", case)
    except IntegrationRequiredError:
        raise
    except IsolationError:
        raise
    finally:
        if runtime is not None:
            try:
                runtime.restore()
            finally:
                runtime.cleanup()

    results_path = write_json(
        output / "results.json",
        {"suite": suite, "seed": seed, "cases": cases, "ok": all(c.get("status") == "passed" for c in cases)},
    )
    junit_path = write_junit(output / "junit.xml", suite=suite, cases=cases)
    artifacts = [
        artifact_entry(results_path, relative_to=output),
        artifact_entry(junit_path, relative_to=output),
    ]
    for name in (
        "baseline-trace.json",
        "corpus-trace.json",
        "crash-matrix.json",
        "privacy-restore.json",
        "restore-receipt.json",
        "release-evidence.json",
    ):
        trace = output / name
        if trace.is_file():
            artifacts.append(artifact_entry(trace, relative_to=output))
    evidence = build_evidence(
        repo=repo,
        output=output,
        suite=suite,
        seed=seed,
        require_integration=require_integration,
        commands=commands,
        cases=cases,
        environment={
            "engine": extra.get("engine"),
            "schema": extra.get("schema"),
            "container_name": extra.get("container_name"),
            "embedding_provider": "hash",
        },
        artifacts=artifacts,
        extra=extra,
    )
    artifacts.append(artifact_entry(output / "evidence.json", relative_to=output))
    evidence["artifacts"] = artifacts
    write_json(output / "evidence.json", evidence)
    if any(case.get("status") != "passed" for case in cases):
        raise RunnerError("one or more required scenarios failed")
    return evidence


def main(argv: Sequence[str] | None = None) -> int:
    _configure_logging()
    args = parse_args(argv)
    repo = Path(__file__).resolve().parents[2]
    try:
        output = validate_output_path(args.output, repo=repo)
        run_suite(
            suite=args.suite,
            output=output,
            require_integration=bool(args.require_integration),
            seed=int(args.seed),
            scale_profile=str(args.scale_profile or ""),
            repo=repo,
        )
    except RunnerError as exc:
        logger.error("runner_failed %s", exc)
        return 1
    except IntegrationRequiredError as exc:
        logger.error("require_integration_failed %s", exc)
        return 1
    except IsolationError as exc:
        logger.error("isolation_failed %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
