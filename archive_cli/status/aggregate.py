"""Production status aggregation for Section F (read-only)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from archive_cli.config import load_archive_config
from archive_cli.ppa_engine import ppa_engine
from archive_cli.validation_gates.constants import SECTION_F_COMPLETION_STATE
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_sync.processors.status import status_payload as processor_status_payload
from archive_sync.processors.state_store import ProcessorStateStore
from archive_sync.source_updaters.declarations import iter_declaration_templates
from archive_sync.source_updaters.snapshot import status_payload_for_declarations
from archive_sync.source_updaters.state_store import SourceUpdaterStateStore

from .corpus_summary import query_corpus_summary, query_email_hygiene_summary
from .readiness import V3ReadinessResult, evaluate_v3_readiness


@dataclass
class ProductionStatusContext:
    vault_path: str
    index_schema: str
    archive_instance: str
    engine_mode: str
    conn: Any | None = None
    blocked: bool = False
    blocked_reason: str = ""
    blocked_message: str = ""


def _table_exists(conn: Any, schema: str, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (schema, table_name),
    ).fetchone()
    return row is not None


def _meta_value(conn: Any, schema: str, key: str) -> str:
    if not _table_exists(conn, schema, "meta"):
        return ""
    try:
        row = conn.execute(
            f"SELECT value FROM {schema}.meta WHERE key = %s",
            (key,),
        ).fetchone()
    except Exception:
        return ""
    if row is None:
        return ""
    if isinstance(row, dict):
        return str(row.get("value") or "")
    return str(row[0] or "")


def _linker_health(conn: Any, schema: str) -> dict[str, Any]:
    if not _table_exists(conn, schema, "link_jobs"):
        return {"table_exists": False, "pending": 0, "failed": 0, "completed": 0}
    counts: dict[str, int] = {}
    rows = conn.execute(
        f"""
        SELECT status, COUNT(*) AS n
        FROM {schema}.link_jobs
        GROUP BY status
        """
    ).fetchall()
    for row in rows:
        status = str(row["status"] if isinstance(row, dict) else row[0] or "unknown")
        counts[status] = int(row["n"] if isinstance(row, dict) else row[1])
    return {
        "table_exists": True,
        "pending": counts.get("pending", 0),
        "failed": counts.get("failed", 0),
        "completed": counts.get("completed", 0),
        "by_status": counts,
        "suppressed_excluded": True,
    }


def _flatten_source_entries(sources_payload: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for item in sources_payload.get("sources") or []:
        state = dict(item.get("state") or {})
        decl = item.get("declaration") or {}
        batch = dict(state.get("last_batch_summary") or {})
        entries.append(
            {
                "source_key": state.get("source_key") or decl.get("source_key"),
                "source_type": state.get("source_type") or decl.get("source_type"),
                "enabled": state.get("enabled", decl.get("enabled", True)),
                "state": state.get("staleness_state") or "never_synced",
                "last_success_at": state.get("last_success_at"),
                "last_attempt_at": state.get("last_attempt_at"),
                "cursor_summary": item.get("cursor_summary") or "",
                "observed_last_run": batch.get("observed", 0),
                "promoted_last_run": batch.get("promoted", 0),
                "suppressed_last_run": batch.get("suppressed", 0),
                "quarantined_last_run": batch.get("quarantined", 0),
                "deleted_last_run": batch.get("deleted_or_tombstoned", 0),
                "last_error": state.get("last_error") or "",
            }
        )
    return entries


def _overall_archive_status(
    *,
    blocked: bool,
    v3_ready: bool,
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> str:
    if blocked:
        return "blocked"
    if errors:
        return "failed"
    if not v3_ready or warnings:
        return "degraded"
    return "healthy"


def build_production_status(
    *,
    store: Any,
    archive_instance: str,
    conn: Any | None = None,
    schema: str = "ppa",
    require_production_soak: bool = True,
    include_index_status: bool = True,
) -> dict[str, Any]:
    """Aggregate Sections B/D/E/G state into one machine-readable payload."""

    vault_path = str(getattr(store, "vault", "") or "")
    engine_mode = ppa_engine()
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    meta_path_sources = Path(vault_path) / "_meta" / "source-updaters.json"
    meta_path_processors = Path(vault_path) / "_meta" / "processors.json"
    source_store = SourceUpdaterStateStore(conn, schema, meta_path=meta_path_sources) if conn else SourceUpdaterStateStore(
        None,
        meta_path=meta_path_sources,
    )
    processor_store = ProcessorStateStore(conn, schema, meta_path=meta_path_processors) if conn else ProcessorStateStore(
        None,
        meta_path=meta_path_processors,
    )
    if conn is not None:
        try:
            source_store.ensure_tables()
            processor_store.ensure_tables()
        except Exception:
            pass

    sources_payload = status_payload_for_declarations(
        list(iter_declaration_templates()),
        source_store,
        vault_path=vault_path,
        archive_instance=archive_instance,
        engine_mode=engine_mode,
    )
    processors_payload = processor_status_payload(
        processor_store,
        archive_instance=archive_instance,
        engine_mode=engine_mode,
    )

    corpus: dict[str, Any] = {}
    email_hygiene: dict[str, Any] = {}
    validation_gates: dict[str, Any] = {"runs": [], "passed_gates": []}
    v3_readiness: V3ReadinessResult | None = None

    if conn is not None:
        registry = GateRegistry(conn, schema)
        registry.ensure_table()
        runs = registry.list_runs(archive_instance=archive_instance, limit=50)
        validation_gates = {
            "runs": [run.to_dict() for run in runs],
            "passed_gates": [run.gate for run in runs if run.status == "passed"],
        }
        corpus = query_corpus_summary(conn, schema)
        email_hygiene = query_email_hygiene_summary(conn, schema)
        v3_readiness = evaluate_v3_readiness(
            registry=registry,
            conn=conn,
            schema=schema,
            archive_instance=archive_instance,
            sources_payload=sources_payload,
            processors_payload=processors_payload,
            require_production_soak=require_production_soak,
        )
    else:
        warnings.append(
            {
                "category": "archive",
                "message": "index connection unavailable; gate/corpus/readiness sections omitted",
            }
        )

    for source in _flatten_source_entries(sources_payload):
        state = str(source.get("state") or "")
        if state in ("failed", "blocked"):
            errors.append(
                {
                    "category": "sources",
                    "source_key": source.get("source_key"),
                    "state": state,
                    "message": source.get("last_error") or state,
                }
            )
        elif state in ("stale", "never_synced"):
            warnings.append(
                {
                    "category": "sources",
                    "source_key": source.get("source_key"),
                    "state": state,
                    "message": source.get("last_error") or state,
                }
            )

    proc_totals = processors_payload.get("totals") or {}
    if int(proc_totals.get("failed") or 0) > 0:
        errors.append(
            {
                "category": "processors",
                "message": "processor failures present",
                "failed_count": proc_totals.get("failed"),
            }
        )

    embeddings: dict[str, Any] = {}
    if include_index_status and hasattr(store, "embedding_status"):
        try:
            embeddings = store.embedding_status()
        except Exception as exc:
            warnings.append({"category": "embeddings", "message": str(exc)})

    linkers: dict[str, Any] = {}
    if conn is not None:
        linkers = _linker_health(conn, schema)

    maintenance = {
        "last_maintenance_at": _meta_value(conn, schema, "last_maintenance_at") if conn else "",
        "report_root": "logs/maintenance",
    }

    cfg = load_archive_config()
    index_status: dict[str, Any] = {}
    if include_index_status and hasattr(store, "status"):
        try:
            index_status = store.status()
        except Exception as exc:
            warnings.append({"category": "archive", "message": str(exc)})

    ready = bool(v3_readiness.ready) if v3_readiness is not None else False
    payload = {
        "completion_state": SECTION_F_COMPLETION_STATE,
        "archive": {
            "instance": archive_instance,
            "vault_path": vault_path,
            "schema": schema,
            "engine_mode": engine_mode,
            "status": _overall_archive_status(
                blocked=False,
                v3_ready=ready,
                errors=errors,
                warnings=warnings,
            ),
            "index_dsn_configured": bool(cfg.index_dsn),
            "last_maintenance_at": maintenance.get("last_maintenance_at"),
            "index_status": index_status,
        },
        "sources": _flatten_source_entries(sources_payload),
        "corpus": corpus,
        "email_hygiene": email_hygiene,
        "processors": processors_payload.get("processors") or [],
        "processor_totals": processors_payload.get("totals") or {},
        "embeddings": embeddings,
        "linkers": linkers,
        "maintenance": maintenance,
        "validation_gates": validation_gates,
        "v3_readiness": v3_readiness.to_dict() if v3_readiness else {"ready": False, "failed_checks": ["index_unavailable"]},
        "errors": errors,
        "warnings": warnings,
    }
    return payload


def build_blocked_status(
    *,
    reason: str,
    message: str,
    archive_instance: str = "",
    vault_path: str = "",
) -> dict[str, Any]:
    """Structured blocked payload when vault/config/database is unavailable."""

    return {
        "completion_state": SECTION_F_COMPLETION_STATE,
        "blocked": True,
        "reason": reason,
        "message": message,
        "archive": {
            "instance": archive_instance,
            "vault_path": vault_path,
            "status": "blocked",
        },
        "sources": [],
        "corpus": {},
        "email_hygiene": {},
        "processors": [],
        "embeddings": {},
        "linkers": {},
        "maintenance": {},
        "validation_gates": {},
        "v3_readiness": {"ready": False, "failed_checks": ["blocked"], "blocking_reasons": [reason]},
        "errors": [{"category": "blocked", "reason": reason, "message": message}],
        "warnings": [],
    }
