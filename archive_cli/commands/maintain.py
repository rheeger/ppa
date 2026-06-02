"""Maintenance automation -- sequences existing operations to keep the system current.

A single CLI command (ppa maintain) that sequences:
1. Tail ingestion ledger for new entries since last maintenance
2. Auto-extract new emails via Phase 2 extractor registry
3. Entity resolution for newly extracted derived cards
4. Incremental rebuild to index new cards
5. Coverage report with all metrics
6. Update maintenance watermark

Each step is independently idempotent and failure-isolated.
Steps with missing upstream dependencies are skipped gracefully via _try_import().
"""

from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..store import DefaultArchiveStore


def _try_import(module_path: str) -> Any | None:
    try:
        return importlib.import_module(module_path)
    except ImportError:
        return None


def _table_missing(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return "does not exist" in msg or "undefined_table" in msg


def _get_watermark(conn: Any, schema: str) -> str:
    try:
        row = conn.execute(
            f"SELECT value FROM {schema}.meta WHERE key = %s",
            ("last_maintenance_at",),
        ).fetchone()
    except Exception as exc:
        if _table_missing(exc):
            return ""
        raise
    if row is None:
        return ""
    if isinstance(row, dict):
        return str(row.get("value") or "")
    return str(row[0] or "")


def _tail_ingestion_log(conn: Any, schema: str, watermark: str) -> list[dict[str, Any]]:
    if watermark:
        rows = conn.execute(
            f"SELECT card_uid, action, source_adapter, logged_at "
            f"FROM {schema}.ingestion_log "
            f"WHERE logged_at > %s ORDER BY logged_at ASC",
            (watermark,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT card_uid, action, source_adapter, logged_at FROM {schema}.ingestion_log ORDER BY logged_at ASC"
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        if isinstance(r, dict):
            out.append(
                {
                    "card_uid": str(r.get("card_uid", "")),
                    "action": str(r.get("action", "")),
                    "source_adapter": str(r.get("source_adapter", "")),
                    "logged_at": r.get("logged_at"),
                }
            )
        else:
            out.append(
                dict(
                    zip(
                        ("card_uid", "action", "source_adapter", "logged_at"),
                        r,
                        strict=False,
                    )
                )
            )
    return out


def _update_watermark(conn: Any, schema: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        f"INSERT INTO {schema}.meta (key, value) VALUES (%s, %s) "
        f"ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        ("last_maintenance_at", now),
    )
    conn.commit()


def _enrichment_queue_depth(conn: Any, schema: str) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) AS c FROM {schema}.enrichment_queue WHERE status = %s", ("pending",)
    ).fetchone()
    if isinstance(row, dict):
        return int(row.get("c") or 0)
    return int(row[0] or 0)


def _retrieval_gaps_since(conn: Any, schema: str, watermark: str) -> int:
    if watermark:
        row = conn.execute(
            f"SELECT COUNT(*) AS c FROM {schema}.retrieval_gaps WHERE detected_at > %s",
            (watermark,),
        ).fetchone()
    else:
        row = conn.execute(f"SELECT COUNT(*) AS c FROM {schema}.retrieval_gaps").fetchone()
    if isinstance(row, dict):
        return int(row.get("c") or 0)
    return int(row[0] or 0)


@dataclass
class MaintenanceReport:
    started_at: str = ""
    completed_at: str = ""
    new_cards_ingested: int = 0
    cards_extracted: int = 0
    entities_resolved: int = 0
    cards_rebuilt: int = 0
    enrichment_queue_depth: int = 0
    retrieval_gaps_since_last: int = 0
    source_updater_snapshots: int = 0
    processor_status_snapshots: int = 0
    errors: list[dict[str, str]] = field(default_factory=list)
    skipped_steps: list[str] = field(default_factory=list)
    nothing_to_do: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}


def _record_source_updater_snapshots(store: DefaultArchiveStore, schema: str) -> int:
    """Read vault cursors into source_updater_state; does not run adapters."""

    from pathlib import Path

    from archive_sync.source_updaters.declarations import iter_declaration_templates
    from archive_sync.source_updaters.snapshot import snapshot_all_declarations
    from archive_sync.source_updaters.state_store import SourceUpdaterStateStore

    meta_path = Path(store.vault) / "_meta" / "source-updaters.json"
    try:
        with store.index._connect() as conn:
            state_store = SourceUpdaterStateStore(conn, schema, meta_path=meta_path)
            state_store.ensure_tables()
            records = snapshot_all_declarations(
                state_store,
                list(iter_declaration_templates()),
                vault_path=str(store.vault),
            )
            conn.commit()
            return len(records)
    except Exception:
        state_store = SourceUpdaterStateStore(None, meta_path=meta_path)
        records = snapshot_all_declarations(
            state_store,
            list(iter_declaration_templates()),
            vault_path=str(store.vault),
        )
        return len(records)


def _record_processor_status_snapshots(store: DefaultArchiveStore, schema: str) -> int:
    """Seed processor_state from declarations; does not run processors."""

    from pathlib import Path

    from archive_sync.processors.declarations import iter_processor_declarations
    from archive_sync.processors.state_store import ProcessorStateRecord, ProcessorStateStore

    meta_path = Path(store.vault) / "_meta" / "processors.json"
    try:
        with store.index._connect() as conn:
            state_store = ProcessorStateStore(conn, schema, meta_path=meta_path)
            state_store.ensure_tables()
            count = 0
            for decl in iter_processor_declarations():
                existing = state_store.get_state(decl.processor_key)
                if existing is None:
                    state_store.upsert_state(
                        ProcessorStateRecord(
                            processor_key=decl.processor_key,
                            processor_version=decl.processor_version,
                            enabled=decl.enabled,
                        )
                    )
                    count += 1
            conn.commit()
            return count
    except Exception:
        state_store = ProcessorStateStore(None, meta_path=meta_path)
        count = 0
        for decl in iter_processor_declarations():
            existing = state_store.get_state(decl.processor_key)
            if existing is None:
                state_store.upsert_state(
                    ProcessorStateRecord(
                        processor_key=decl.processor_key,
                        processor_version=decl.processor_version,
                        enabled=decl.enabled,
                    )
                )
                count += 1
        return count


def run_maintenance(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    dry_run: bool = False,
    record_source_status: bool = False,
    record_processor_status: bool = False,
) -> MaintenanceReport:
    report = MaintenanceReport()
    report.started_at = datetime.now(timezone.utc).isoformat()
    idx = store.index
    schema = str(getattr(idx, "schema", "ppa"))

    if record_source_status and not dry_run:
        try:
            report.source_updater_snapshots = _record_source_updater_snapshots(store, schema)
        except Exception as exc:
            logger.exception("maintain_source_updater_snapshot_failed")
            report.errors.append({"step": "source_updater_snapshot", "error": str(exc)})
    elif record_source_status:
        report.skipped_steps.append("source_updater_snapshot (dry-run)")

    if record_processor_status and not dry_run:
        try:
            report.processor_status_snapshots = _record_processor_status_snapshots(store, schema)
        except Exception as exc:
            logger.exception("maintain_processor_status_snapshot_failed")
            report.errors.append({"step": "processor_status_snapshot", "error": str(exc)})
    elif record_processor_status:
        report.skipped_steps.append("processor_status_snapshot (dry-run)")

    from ..providers import resolve_provider

    try:
        # Re-read env on each maintenance run (CLI/timers), not the long-lived MCP cache.
        provider = resolve_provider(refresh=True)
        if provider is not None:
            if not provider.is_available():
                logger.warning(
                    "provider_unavailable name=%s model=%s -- LLM-dependent steps will be skipped",
                    provider.name,
                    provider.model,
                )
                report.skipped_steps.append("llm_tasks (provider unavailable)")
        else:
            logger.info("no_provider_configured -- LLM-dependent steps will be skipped")
            report.skipped_steps.append("llm_tasks (PPA_ENRICHMENT_MODEL unset)")
    except ValueError as exc:
        logger.error("provider_resolve_failed error=%s", exc)
        report.errors.append({"step": "resolve_provider", "error": str(exc)})

    watermark = ""
    new_rows: list[dict[str, Any]] = []
    try:
        with idx._connect() as conn:
            watermark = _get_watermark(conn, schema)
            try:
                new_rows = _tail_ingestion_log(conn, schema, watermark)
            except Exception as exc:
                if _table_missing(exc):
                    report.skipped_steps.append("ingestion_log missing")
                    report.completed_at = datetime.now(timezone.utc).isoformat()
                    return report
                raise
    except Exception as exc:
        logger.exception("maintain_tail_failed")
        report.errors.append({"step": "tail_ingestion_log", "error": str(exc)})
        report.completed_at = datetime.now(timezone.utc).isoformat()
        return report

    if not new_rows:
        report.nothing_to_do = True
        report.completed_at = datetime.now(timezone.utc).isoformat()
        return report

    report.new_cards_ingested = len(new_rows)
    created_n = sum(1 for r in new_rows if r.get("action") == "created")

    reg_mod = _try_import("archive_sync.extractors.registry")
    if reg_mod is None:
        report.skipped_steps.append("auto_extract (extractor registry import failed)")
    elif created_n <= 0:
        report.skipped_steps.append("auto_extract (no created entries)")
    else:
        try:
            from archive_sync.extractors.runner import ExtractionRunner

            runner = ExtractionRunner(
                str(store.vault),
                registry=reg_mod.build_default_registry(),
                dry_run=dry_run,
                limit=min(created_n, 10_000),
            )
            metrics = runner.run()
            report.cards_extracted = int(getattr(metrics, "extracted_cards", 0) or 0)
        except Exception as exc:
            logger.exception("maintain_extract_failed")
            report.errors.append({"step": "auto_extract", "error": str(exc)})

    er_mod = _try_import("archive_sync.extractors.entity_resolution")
    if er_mod is None:
        report.skipped_steps.append("entity_resolution (module import failed)")
    else:
        try:
            res = er_mod.run_entity_resolution(str(store.vault), dry_run=dry_run)
            report.entities_resolved = int(
                (res.get("places_created") or 0)
                + (res.get("places_merged") or 0)
                + (res.get("orgs_created") or 0)
                + (res.get("orgs_merged") or 0)
                + (res.get("persons_linked") or 0)
            )
        except Exception as exc:
            logger.exception("maintain_entity_resolution_failed")
            report.errors.append({"step": "entity_resolution", "error": str(exc)})

    if dry_run:
        report.skipped_steps.append("incremental_rebuild (dry-run)")
        report.skipped_steps.append("watermark_update (dry-run)")
    else:
        try:
            counts = store.rebuild()
            report.cards_rebuilt = int(counts.get("cards", 0) or 0)
        except Exception as exc:
            logger.exception("maintain_rebuild_failed")
            report.errors.append({"step": "incremental_rebuild", "error": str(exc)})

    try:
        with idx._connect() as conn:
            try:
                report.enrichment_queue_depth = _enrichment_queue_depth(conn, schema)
            except Exception as exc:
                if _table_missing(exc):
                    report.skipped_steps.append("enrichment_queue (table missing)")
                else:
                    raise
            try:
                report.retrieval_gaps_since_last = _retrieval_gaps_since(conn, schema, watermark)
            except Exception as exc:
                if _table_missing(exc):
                    report.skipped_steps.append("retrieval_gaps (table missing)")
                else:
                    raise
            if not dry_run:
                try:
                    _update_watermark(conn, schema)
                except Exception as exc:
                    logger.exception("maintain_watermark_failed")
                    report.errors.append({"step": "watermark", "error": str(exc)})
    except Exception as exc:
        logger.exception("maintain_coverage_failed")
        report.errors.append({"step": "coverage_report", "error": str(exc)})

    report.completed_at = datetime.now(timezone.utc).isoformat()
    return report
