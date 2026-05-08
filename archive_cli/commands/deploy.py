"""Phase 9 deployment orchestration."""

from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

from ..errors import PpaError
from ..migrate import MigrationRunner
from ..store import DefaultArchiveStore
from .embedding_cache import import_embedding_cache
from .geocode import geocode_places
from .health_check import run_deployment_checks, run_structural_checks
from .latency_check import run_latency_check
from .preflight import run_preflight

StepStatus = Literal["pending", "running", "passed", "failed", "skipped"]
DeployStatus = Literal["pending", "success", "failed", "partial"]


@dataclass
class DeployStep:
    name: str
    status: StepStatus = "pending"
    elapsed_ms: int = 0
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class DeployResult:
    steps: list[DeployStep] = field(default_factory=list)
    overall_status: DeployStatus = "pending"
    total_elapsed_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "steps": [s.to_dict() for s in self.steps],
            "overall_status": self.overall_status,
            "total_elapsed_ms": self.total_elapsed_ms,
        }


DEPLOY_STEPS = [
    "preflight",
    "migrate",
    "rebuild",
    "geocode-places",
    "restore-embeddings",
    "verify",
    "restart-mcp",
]


@dataclass(frozen=True)
class RebuildTuning:
    workers: int
    batch_size: int
    commit_interval: int
    max_pending_batches: int
    flush_max_total_rows: int
    flush_max_chunks: int
    flush_max_edges: int
    flush_max_bytes: int

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def _available_memory_bytes() -> int:
    meminfo = Path("/proc/meminfo")
    try:
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                parts = line.split()
                if len(parts) >= 2:
                    return int(parts[1]) * 1024
    except (OSError, ValueError):
        pass

    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        available_pages = os.sysconf("SC_AVPHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return 0
    try:
        return int(page_size) * int(available_pages)
    except (TypeError, ValueError):
        return 0


def _env_int(name: str) -> int | None:
    value = os.environ.get(name, "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise PpaError(f"{name} must be an integer") from exc


def _rebuild_tuning(*, workers: int, available_memory_bytes: int | None = None) -> RebuildTuning:
    """Choose bounded rebuild chunking from host memory, with env overrides."""
    available = _available_memory_bytes() if available_memory_bytes is None else available_memory_bytes
    available_gib = available / (1024**3) if available > 0 else 0
    safe_workers = max(1, workers)

    if available_gib >= 24:
        batch_size = min(max(1000, safe_workers * 1000), 5000)
        commit_interval = batch_size
        max_pending = min(max(safe_workers + 2, 4), 8)
        flush_rows = 200_000
        flush_chunks = 80_000
        flush_edges = 200_000
        flush_bytes = 512 * 1024 * 1024
    elif available_gib >= 12:
        batch_size = min(max(500, safe_workers * 500), 2500)
        commit_interval = batch_size * 2
        max_pending = min(max(safe_workers + 1, 3), 6)
        flush_rows = 100_000
        flush_chunks = 40_000
        flush_edges = 100_000
        flush_bytes = 256 * 1024 * 1024
    else:
        batch_size = 250
        commit_interval = 1000
        max_pending = min(max(safe_workers + 1, 2), 5)
        flush_rows = 25_000
        flush_chunks = 10_000
        flush_edges = 25_000
        flush_bytes = 64 * 1024 * 1024

    return RebuildTuning(
        workers=safe_workers,
        batch_size=_env_int("PPA_REBUILD_BATCH_SIZE") or batch_size,
        commit_interval=_env_int("PPA_REBUILD_COMMIT_INTERVAL") or commit_interval,
        max_pending_batches=_env_int("PPA_MATERIALIZE_MAX_PENDING_BATCHES") or max_pending,
        flush_max_total_rows=_env_int("PPA_REBUILD_FLUSH_MAX_TOTAL_ROWS") or flush_rows,
        flush_max_chunks=_env_int("PPA_REBUILD_FLUSH_MAX_CHUNKS") or flush_chunks,
        flush_max_edges=_env_int("PPA_REBUILD_FLUSH_MAX_EDGES") or flush_edges,
        flush_max_bytes=_env_int("PPA_REBUILD_FLUSH_MAX_BYTES") or flush_bytes,
    )


def _skip_before(step: str | None) -> set[str]:
    if not step:
        return set()
    if step not in DEPLOY_STEPS:
        raise PpaError(f"Unknown deploy step: {step}")
    return set(DEPLOY_STEPS[: DEPLOY_STEPS.index(step)])


def run_deploy(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    dry_run: bool = False,
    skip_to: str | None = None,
    workers: int = 4,
    embedding_cache_dir: str = "",
    timeout_hours: float = 5.0,
) -> DeployResult:
    """Run the Arnold-side Phase 9 deployment sequence.

    Vault rsync is operator-side and is intentionally not performed here.
    """
    started = time.monotonic()
    result = DeployResult()
    skipped = _skip_before(skip_to)

    def run_step(name: str, fn) -> DeployStep:
        step = DeployStep(name=name)
        result.steps.append(step)
        if name in skipped:
            step.status = "skipped"
            return step
        step.status = "running"
        t0 = time.monotonic()
        try:
            if dry_run and name != "preflight":
                step.status = "skipped"
                step.details = {"dry_run": True}
                return step
            details = fn()
            step.status = "passed"
            step.details = details if isinstance(details, dict) else {"result": details}
        except Exception as exc:  # noqa: BLE001 - deployment must capture all failures
            step.status = "failed"
            step.error = str(exc)
            logger.exception("deploy_step_failed step=%s", name)
        finally:
            step.elapsed_ms = int((time.monotonic() - t0) * 1000)
        return step

    dsn = os.environ.get("PPA_INDEX_DSN", "")
    schema = str(getattr(store.index, "schema", os.environ.get("PPA_INDEX_SCHEMA", "ppa")))
    vault = Path(store.vault)

    step = run_step("preflight", lambda: run_preflight(vault_path=vault, dsn=dsn, schema=schema).to_dict())
    if step.status == "failed" or step.details.get("errors"):
        result.overall_status = "failed"
        result.total_elapsed_ms = int((time.monotonic() - started) * 1000)
        return result

    def migrate() -> dict[str, Any]:
        with store.index._connect() as conn:  # noqa: SLF001
            migration_result = MigrationRunner(conn, schema).run()
        return asdict(migration_result)

    def rebuild() -> dict[str, Any]:
        # Phase 9 runs on Arnold's production VM. Size Rust materialization and
        # COPY flushes from host memory, while keeping explicit caps on in-flight
        # batches so a fast producer cannot outrun the database load path.
        tuning = _rebuild_tuning(workers=workers)
        os.environ["PPA_MATERIALIZE_MAX_PENDING_BATCHES"] = str(tuning.max_pending_batches)
        os.environ["PPA_REBUILD_FLUSH_MAX_TOTAL_ROWS"] = str(tuning.flush_max_total_rows)
        os.environ["PPA_REBUILD_FLUSH_MAX_CHUNKS"] = str(tuning.flush_max_chunks)
        os.environ["PPA_REBUILD_FLUSH_MAX_EDGES"] = str(tuning.flush_max_edges)
        os.environ["PPA_REBUILD_FLUSH_MAX_BYTES"] = str(tuning.flush_max_bytes)
        logger.info("phase9_rebuild_tuning %s", tuning.to_dict())
        rebuild_result = store.rebuild(
            force_full=True,
            workers=tuning.workers,
            executor_kind="thread",
            batch_size=tuning.batch_size,
            commit_interval=tuning.commit_interval,
            progress_every=10_000,
        )
        rebuild_result["tuning"] = tuning.to_dict()
        return rebuild_result

    def restore_embeddings() -> dict[str, Any]:
        cache = embedding_cache_dir or os.environ.get("PPA_EMBEDDING_RECOVERY_CACHE_DIR", "")
        if not cache:
            raise PpaError("PPA_EMBEDDING_RECOVERY_CACHE_DIR is required")
        result = import_embedding_cache(input_dir=cache, logger=logger)
        with store.index._connect() as conn:  # noqa: SLF001
            conn.execute("SET statement_timeout = '0'")
            store.index._ensure_embeddings_vector_index(conn)  # noqa: SLF001
            conn.commit()
        result["vector_index_ensured"] = True
        return result

    def verify() -> dict[str, Any]:
        with store.index._connect() as conn:  # noqa: SLF001
            conn.execute("SET statement_timeout = '10min'")
            structural = run_structural_checks(conn, schema)
            deployment = run_deployment_checks(conn, schema, vault)
        latency = run_latency_check(store.index)
        failed_latency = [r.to_dict() for r in latency if not r.passed]
        ok = structural.ok and deployment.ok and not failed_latency
        if not ok:
            raise PpaError("post-deployment verification failed")
        return {
            "structural_ok": structural.ok,
            "deployment": deployment.to_dict() if hasattr(deployment, "to_dict") else deployment.__dict__,
            "latency": [r.to_dict() for r in latency],
        }

    steps = [
        ("migrate", migrate),
        ("rebuild", rebuild),
        ("geocode-places", lambda: geocode_places(store=store)),
        ("restore-embeddings", restore_embeddings),
        ("verify", verify),
        ("restart-mcp", _restart_mcp),
    ]
    for name, fn in steps:
        step = run_step(name, fn)
        if step.status == "failed":
            break

    result.total_elapsed_ms = int((time.monotonic() - started) * 1000)
    result.overall_status = "success" if all(s.status in {"passed", "skipped"} for s in result.steps) else "failed"
    return result


def _restart_mcp() -> dict[str, Any]:
    try:
        subprocess.run(["sudo", "systemctl", "restart", "ppa-mcp"], check=True, capture_output=True, text=True)
        return {"restarted": True}
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        return {"restarted": False, "manual_restart_required": True, "error": str(exc)}
