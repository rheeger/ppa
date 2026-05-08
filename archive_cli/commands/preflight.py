"""Pre-flight checks for Phase 9 deployment."""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

from ..health import run_health_checks
from ..index_config import get_index_dsn, get_index_schema


@dataclass
class PreflightResult:
    health_ok: bool = False
    vault_file_count: int = 0
    vault_size_gb: float = 0.0
    srv_disk_gb_available: float = 0.0
    srv_disk_gb_required_estimate: float = 0.0
    srv_disk_ok: bool = False
    root_disk_gb_available: float = 0.0
    root_disk_ok: bool = False
    pg_version: str = ""
    pg_version_ok: bool = False
    current_card_count: int = 0
    pending_migrations: list[str] = field(default_factory=list)
    archive_crate_available: bool = False
    archive_crate_version: str = ""
    model_provider_available: bool = False
    model_provider_name: str = ""
    backup_timer_enabled: bool = False
    backup_volume_ok: bool = False
    vault_io_throughput_mbps: float = 0.0
    vault_io_ok: bool = False
    embedding_recovery_cache_present: bool = False
    embedding_recovery_cache_dir: str = ""
    embedding_recovery_cache_file_count: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    elapsed_ms: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__, ok=self.ok)


def run_preflight(
    *,
    vault_path: Path,
    dsn: str | None = None,
    schema: str | None = None,
    estimated_index_size_gb: float = 80.0,
) -> PreflightResult:
    started = time.monotonic()
    result = PreflightResult()
    resolved_dsn = dsn or get_index_dsn()
    resolved_schema = schema or get_index_schema()

    _check_disk_space(vault_path, estimated_index_size_gb, result)
    _check_pg_version(resolved_dsn, result)
    _check_vault(vault_path, result)
    _check_postgres_state(resolved_dsn, resolved_schema, result)
    _check_archive_crate(result)
    _check_model_provider(result)
    _check_health(vault_path, resolved_dsn, resolved_schema, result)
    _check_vault_io(vault_path, result)
    _check_backup_coverage(resolved_dsn, result)
    _check_embedding_recovery_cache(result)

    result.elapsed_ms = int((time.monotonic() - started) * 1000)
    return result


def _check_disk_space(vault_path: Path, estimate_gb: float, result: PreflightResult) -> None:
    pgdata = Path(os.environ.get("PPA_PGDATA", "/srv/hfa-secure/postgres"))
    check_path = pgdata if pgdata.exists() else vault_path
    if check_path.exists():
        usage = shutil.disk_usage(check_path)
        result.srv_disk_gb_available = usage.free / (1024**3)
        result.srv_disk_gb_required_estimate = estimate_gb
        if result.srv_disk_gb_available < estimate_gb:
            result.errors.append(
                f"{check_path} has {result.srv_disk_gb_available:.1f}GB free; need {estimate_gb:.1f}GB"
            )
        elif result.srv_disk_gb_available < estimate_gb * 1.5:
            result.warnings.append(
                f"{check_path} has {result.srv_disk_gb_available:.1f}GB free; recommended {estimate_gb * 1.5:.1f}GB"
            )
            result.srv_disk_ok = True
        else:
            result.srv_disk_ok = True
    root_usage = shutil.disk_usage("/")
    result.root_disk_gb_available = root_usage.free / (1024**3)
    if result.root_disk_gb_available < 5:
        result.errors.append(f"/ has {result.root_disk_gb_available:.1f}GB free; need >=5GB")
    else:
        result.root_disk_ok = True


def _check_pg_version(dsn: str, result: PreflightResult) -> None:
    if not dsn:
        result.errors.append("PPA_INDEX_DSN is not set")
        return
    try:
        with psycopg.connect(dsn) as conn:
            version = str(conn.execute("SHOW server_version").fetchone()[0])
        result.pg_version = version
        result.pg_version_ok = version.startswith("17.")
        if not result.pg_version_ok:
            result.errors.append(f"Postgres version is {version}; expected 17.x")
    except Exception as exc:
        result.errors.append(f"Could not check Postgres version: {exc}")


def _check_vault(vault_path: Path, result: PreflightResult) -> None:
    if not vault_path.exists():
        result.errors.append(f"Vault path does not exist: {vault_path}")
        return
    try:
        import archive_crate

        files = archive_crate.walk_vault(str(vault_path))
        result.vault_file_count = len(files)
    except Exception:
        result.vault_file_count = sum(1 for _ in vault_path.rglob("*.md"))
    try:
        out = subprocess.run(["du", "-sb", str(vault_path)], capture_output=True, text=True, timeout=300)
        result.vault_size_gb = int(out.stdout.split()[0]) / (1024**3)
    except Exception:
        pass


def _check_postgres_state(dsn: str, schema: str, result: PreflightResult) -> None:
    if not dsn:
        return
    from ..migrate import MigrationRunner

    try:
        with psycopg.connect(dsn) as conn:
            try:
                result.current_card_count = int(conn.execute(f"SELECT COUNT(*) FROM {schema}.cards").fetchone()[0])
            except Exception:
                result.current_card_count = 0
            status = MigrationRunner(conn, schema).status()
            result.pending_migrations = [str(v) for v in status.get("pending_versions", [])]
    except Exception as exc:
        result.warnings.append(f"Could not read Postgres state: {exc}")


def _check_archive_crate(result: PreflightResult) -> None:
    try:
        import archive_crate

        result.archive_crate_available = True
        result.archive_crate_version = str(getattr(archive_crate, "__version__", "unknown"))
    except ImportError as exc:
        result.errors.append(f"archive_crate is required for Phase 9: {exc}")


def _check_model_provider(result: PreflightResult) -> None:
    try:
        from ..providers import resolve_provider
    except ImportError as exc:
        result.errors.append(f"archive_cli.providers missing even though Phase 8 is complete: {exc}")
        return
    provider = resolve_provider()
    if provider is None:
        result.warnings.append("PPA_ENRICHMENT_MODEL is unset; LLM maintenance tasks will be skipped")
        return
    result.model_provider_name = type(provider).__name__
    result.model_provider_available = bool(provider.is_available())
    if not result.model_provider_available:
        result.warnings.append(f"Model provider {result.model_provider_name} configured but unavailable")


def _check_health(vault_path: Path, dsn: str, schema: str, result: PreflightResult) -> None:
    report = run_health_checks(vault_path=vault_path, dsn=dsn, schema=schema)
    result.health_ok = bool(report.get("ok"))
    if not result.health_ok:
        for name, check in report.get("checks", {}).items():
            if isinstance(check, dict) and not check.get("ok"):
                result.warnings.append(f"health.{name}: {check.get('error', 'failed')}")


def _check_vault_io(vault_path: Path, result: PreflightResult) -> None:
    test_file = vault_path / ".ppa_io_test"
    try:
        data = os.urandom(1024 * 1024)
        t0 = time.monotonic()
        test_file.write_bytes(data)
        _ = test_file.read_bytes()
        elapsed = time.monotonic() - t0
        throughput = 2.0 / elapsed
        result.vault_io_throughput_mbps = round(throughput, 1)
        result.vault_io_ok = throughput > 10
        if not result.vault_io_ok:
            result.warnings.append(f"Vault I/O throughput is {throughput:.1f} MB/s")
    except OSError as exc:
        result.warnings.append(f"Could not test vault I/O: {exc}")
    finally:
        test_file.unlink(missing_ok=True)


def _check_backup_coverage(dsn: str, result: PreflightResult) -> None:
    try:
        out = subprocess.run(["systemctl", "is-enabled", "ppa-backup.timer"], capture_output=True, text=True, timeout=5)
        result.backup_timer_enabled = out.stdout.strip() == "enabled"
    except Exception:
        result.backup_timer_enabled = False
    if not result.backup_timer_enabled:
        result.warnings.append("ppa-backup.timer is not enabled or systemctl is unavailable")

    backup_path = Path("/mnt/user/backups/hfa-encrypted")
    if not backup_path.exists() or not dsn:
        return
    try:
        with psycopg.connect(dsn) as conn:
            db_size_gb = int(conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]) / (1024**3)
        avail_gb = shutil.disk_usage(backup_path).free / (1024**3)
        result.backup_volume_ok = avail_gb > db_size_gb * 1.5
        if not result.backup_volume_ok:
            result.warnings.append(f"Backup volume {avail_gb:.1f}GB free; DB size {db_size_gb:.1f}GB")
    except Exception as exc:
        result.warnings.append(f"Could not check backup volume: {exc}")


def _check_embedding_recovery_cache(result: PreflightResult) -> None:
    configured = os.environ.get("PPA_EMBEDDING_RECOVERY_CACHE_DIR", "").strip()
    if not configured:
        result.errors.append("PPA_EMBEDDING_RECOVERY_CACHE_DIR is not set")
        return
    cache_dir = Path(configured).expanduser()
    if not cache_dir.exists():
        result.errors.append(f"Embedding recovery cache does not exist: {cache_dir}")
        return
    required = ["MANIFEST.json", "embeddings.tsv", "embeddings.tsv.rows", "embeddings.tsv.sha256"]
    missing = [name for name in required if not (cache_dir / name).exists()]
    if missing:
        result.errors.append(f"Embedding recovery cache missing files: {', '.join(missing)}")
        return
    result.embedding_recovery_cache_present = True
    result.embedding_recovery_cache_dir = str(cache_dir)
    result.embedding_recovery_cache_file_count = len(list(cache_dir.iterdir()))
