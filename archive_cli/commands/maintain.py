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
import os
from collections.abc import Iterable
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
    source_updater_runs: int = 0
    source_updater_reports: list[dict[str, Any]] = field(default_factory=list)
    source_updater_partial: bool = False
    processor_status_snapshots: int = 0
    processor_runs: int = 0
    processor_reports: list[dict[str, Any]] = field(default_factory=list)
    processor_output_count: int = 0
    junk_attachments_purged: int = 0
    file_duplicates_linked: int = 0
    file_identity: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, str]] = field(default_factory=list)
    skipped_steps: list[str] = field(default_factory=list)
    nothing_to_do: bool = False
    serving_index: dict[str, Any] = field(default_factory=dict)
    publish_uids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}


def _normalize_uids(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        uid = str(raw or "").strip()
        if uid and uid not in seen:
            seen.add(uid)
            out.append(uid)
    return out


def _uids_from_processor_reports(reports: list[dict[str, Any]]) -> list[str]:
    collected: list[str] = []
    for report in reports:
        inner = report.get("report") or report
        for uid in inner.get("output_uids") or []:
            collected.append(uid)
        for item in report.get("item_results") or inner.get("item_results") or []:
            if item.get("input_uid"):
                collected.append(item["input_uid"])
            for uid in item.get("output_uids") or []:
                collected.append(uid)
    return _normalize_uids(collected)


def collect_maintain_publish_uids(report: MaintenanceReport, vault: Any | None = None) -> list[str]:
    """Concrete UIDs this maintain run wrote. Maintain is publisher of truth."""

    from archive_sync.processors.dirty_io import dirty_uids_from_source_reports

    collected: list[str] = list(report.publish_uids)
    collected.extend(dirty_uids_from_source_reports(report.source_updater_reports or []))
    collected.extend(_uids_from_processor_reports(report.processor_reports or []))
    if vault is not None:
        from archive_cli.serving_index import read_dirty_uids

        collected.extend(read_dirty_uids(vault))
    return _normalize_uids(collected)


def _extend_publish_uids(report: MaintenanceReport, uids: Iterable[Any]) -> None:
    report.publish_uids = _normalize_uids(list(report.publish_uids) + list(uids))


def _maybe_embed_pending(store: Any, report: MaintenanceReport, logger: logging.Logger, *, dry_run: bool) -> None:
    if dry_run or not getattr(store, "embed_pending", None):
        return
    from archive_cli.index_store import PostgresArchiveIndex

    if not isinstance(getattr(store, "index", None), PostgresArchiveIndex):
        return
    if int(report.cards_rebuilt or 0) <= 0 and report.nothing_to_do:
        return
    try:
        report.serving_index.setdefault("embed_pending", store.embed_pending(limit=0))
    except Exception as exc:
        logger.exception("maintain_embed_pending_failed")
        report.errors.append({"step": "embed_pending", "error": str(exc)})


def _finish_maintain(
    store: Any, report: MaintenanceReport, logger: logging.Logger, *, dry_run: bool
) -> MaintenanceReport:
    _maybe_embed_pending(store, report, logger, dry_run=dry_run)
    _publish_serving_index(store, report, logger, dry_run=dry_run)
    report.completed_at = datetime.now(timezone.utc).isoformat()
    return report


def _publish_serving_index(store: Any, report: MaintenanceReport, logger: logging.Logger, *, dry_run: bool) -> None:
    """Sole publisher of ACTIVE. Failure leaves the last good generation in place."""
    if dry_run:
        report.skipped_steps.append("serving_index_publish (dry-run)")
        return
    from archive_cli.index_store import PostgresArchiveIndex
    from archive_cli.serving_index import publish_serving_index, serving_index_status

    if not isinstance(getattr(store, "index", None), PostgresArchiveIndex):
        report.skipped_steps.append("serving_index_publish (no warehouse)")
        return
    status = serving_index_status(store.vault)
    ready = bool(status.get("serving_index_ready"))
    active_gid = str(status.get("serving_index_generation") or "")
    concrete = collect_maintain_publish_uids(report, store.vault)
    cards_rebuilt = int(report.cards_rebuilt or 0)
    ingested = int(report.new_cards_ingested or 0)
    publish_required = bool(concrete) or cards_rebuilt > 0 or ingested > 0
    if ready and not publish_required:
        logger.info("serving_index_publish skip-only-when-clean keep_generation=%s", active_gid)
        report.skipped_steps.append("serving_index_publish (clean)")
        report.serving_index = status
        return
    if not concrete and publish_required:
        error = "publish_required_without_uids"
        logger.error(
            "serving_index_publish failed error=%s cards_rebuilt=%s ingested=%s",
            error,
            cards_rebuilt,
            ingested,
        )
        report.errors.append({"step": "serving_index_publish", "error": error})
        report.serving_index = {"ok": False, "error": error, **status}
        return
    try:
        logger.info("serving_index_publish incremental uids=%s", len(concrete))
        result = publish_serving_index(store, logger=logger, dirty_uids=concrete)
        report.serving_index = result
        if result.get("skipped"):
            error = f"serving_index_publish_skipped:{result.get('skipped')}"
            logger.error("serving_index_publish failed error=%s uids=%s", error, len(concrete))
            report.errors.append({"step": "serving_index_publish", "error": error})
            return
        if not result.get("ok"):
            logger.error("serving_index_refresh_failed")
            report.errors.append(
                {
                    "step": "serving_index_publish",
                    "error": str(result.get("error") or "serving_index_refresh_failed"),
                }
            )
            return
        logger.info(
            "serving_index_publish incremental uids=%s generation=%s",
            len(concrete),
            result.get("generation"),
        )
    except Exception as exc:
        logger.exception("serving_index_refresh_failed")
        report.errors.append({"step": "serving_index_publish", "error": str(exc)})
        report.serving_index = {"ok": False, "error": str(exc)}


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


def _run_source_updaters(
    store: DefaultArchiveStore,
    schema: str,
    *,
    apply: bool,
    source_keys: list[str] | None = None,
    max_items: int | None = None,
    catch_up: bool = False,
    strict: bool = False,
    logger: logging.Logger,
) -> tuple[int, list[dict[str, Any]], bool]:
    """Execute enabled source updaters (Section D Phase 2). Isolates per-source failures."""

    from pathlib import Path

    from archive_cli.config import load_archive_config
    from archive_cli.ppa_engine import ppa_engine
    from archive_cli.validation_gates.constants import GATE_SYNTHETIC_FIXTURES
    from archive_cli.validation_gates.instance_identity import derive_archive_instance
    from archive_sync.source_updaters.runner import default_maintain_source_keys, run_source_updaters
    from archive_sync.source_updaters.state_store import SourceUpdaterStateStore

    repo_root = Path(__file__).resolve().parents[2]
    meta_path = Path(store.vault) / "_meta" / "source-updaters.json"
    cfg = load_archive_config()
    archive_instance = derive_archive_instance(
        vault_path=str(store.vault),
        index_dsn=cfg.index_dsn,
        index_schema=schema,
    )
    keys = list(source_keys or [])
    if not keys:
        account = (os.environ.get("GOOGLE_ACCOUNT") or "").strip()
        otter = (os.environ.get("OTTER_ACCOUNT") or account).strip()
        accounts = (account,) if account else ()
        otter_accounts = (otter,) if otter else ()
        keys = default_maintain_source_keys(
            gmail_accounts=accounts,
            calendar_accounts=accounts,
            otter_accounts=otter_accounts,
        )
    if not keys:
        logger.info("run_source_updaters skipped: no executable source keys configured")
        return 0, [], False

    try:
        with store.index._connect() as conn:
            state_store = SourceUpdaterStateStore(conn, schema, meta_path=meta_path)
            state_store.ensure_tables()
            multi = run_source_updaters(
                source_keys=keys,
                vault_path=str(store.vault),
                apply=apply,
                archive_instance=archive_instance,
                engine_mode=ppa_engine(),
                ladder_gate=GATE_SYNTHETIC_FIXTURES,
                repo_root=repo_root,
                state_store=state_store,
                max_items=max_items,
                catch_up=catch_up,
                strict=strict,
            )
            conn.commit()
    except Exception:
        state_store = SourceUpdaterStateStore(None, meta_path=meta_path)
        multi = run_source_updaters(
            source_keys=keys,
            vault_path=str(store.vault),
            apply=apply,
            archive_instance=archive_instance,
            engine_mode=ppa_engine(),
            ladder_gate=GATE_SYNTHETIC_FIXTURES,
            repo_root=repo_root,
            state_store=state_store,
            max_items=max_items,
            catch_up=catch_up,
            strict=strict,
        )
    partial = multi.completion_state == "partial"
    return len(multi.reports), [r.to_dict() for r in multi.reports], partial


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


def _run_file_hygiene(
    store: DefaultArchiveStore,
    *,
    apply: bool,
    extra_dirty_uids: set[str] | None = None,
    logger: logging.Logger,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Purge slipped junk attachments, then hash-link missing duplicate peers.

    Linking is cache-wide (reuse existing shas, Rust-hash the rest) so historical
    unlinked copies are found. Dirty hints are not used as a write allowlist.
    """

    from pathlib import Path

    from archive_sync.file_identity import run_file_duplicate_linking
    from archive_sync.junk_attachments import run_junk_attachment_purge

    vault = Path(store.vault)
    hint = {str(uid).strip() for uid in (extra_dirty_uids or set()) if str(uid).strip()}
    logger.info("maintain file hygiene start apply=%s dirty_hint=%s", apply, len(hint))
    purge = run_junk_attachment_purge(vault, dry_run=not apply, store=store)
    purged = {str(uid).strip() for uid in (purge.get("purged_uids") or []) if str(uid).strip()}
    link = run_file_duplicate_linking(
        vault,
        dry_run=not apply,
        incremental=True,
        exclude_uids=purged or None,
    )
    dirty = [
        str(uid).strip()
        for uid in list(purge.get("dirty_uids") or []) + list(link.get("dirty_uids") or [])
        if str(uid).strip()
    ]
    logger.info(
        "maintain file hygiene done purged=%s linked=%s dirty=%s",
        purge.get("purged"),
        link.get("cards_linked"),
        len(set(dirty)),
    )
    return purge, link, sorted(set(dirty))


def _run_processors(
    store: DefaultArchiveStore,
    schema: str,
    *,
    apply: bool,
    dirty_uids_path: str = "",
    extra_dirty_uids: list[str] | None = None,
    source_updater_reports: list[dict[str, Any]] | None = None,
    processor_keys: list[str] | None = None,
    allow_full_embedding: bool = False,
    allow_all_linkers: bool = False,
    allow_broad_llm: bool = False,
    logger: logging.Logger,
) -> tuple[int, list[dict[str, Any]], int]:
    """Execute processor DAG on dirty UIDs (Section E Phase 2)."""

    from pathlib import Path

    from archive_cli.config import load_archive_config
    from archive_cli.ppa_engine import ppa_engine
    from archive_cli.validation_gates.constants import GATE_SYNTHETIC_FIXTURES
    from archive_cli.validation_gates.instance_identity import derive_archive_instance
    from archive_sync.processors.runner import run_processors
    from archive_sync.processors.state_store import ProcessorStateStore

    repo_root = Path(__file__).resolve().parents[2]
    meta_path = Path(store.vault) / "_meta" / "processors.json"
    cfg = load_archive_config()
    archive_instance = derive_archive_instance(
        vault_path=str(store.vault),
        index_dsn=cfg.index_dsn,
        index_schema=schema,
    )

    try:
        with store.index._connect() as conn:
            state_store = ProcessorStateStore(conn, schema, meta_path=meta_path)
            state_store.ensure_tables()
            keys = list(processor_keys or [])
            result = run_processors(
                dirty_uids_path=Path(dirty_uids_path) if dirty_uids_path else None,
                dirty_uids=list(extra_dirty_uids or []),
                source_updater_reports=source_updater_reports,
                vault_path=str(store.vault),
                store=store,
                state_store=state_store,
                processor_keys=keys or None,
                apply=apply,
                dry_run=not apply,
                allow_full_embedding=allow_full_embedding,
                allow_all_linkers=allow_all_linkers,
                allow_broad_llm=allow_broad_llm,
                archive_instance=archive_instance,
                engine_mode=ppa_engine(),
                ladder_gate=GATE_SYNTHETIC_FIXTURES,
                repo_root=repo_root,
            )
            conn.commit()
    except Exception:
        state_store = ProcessorStateStore(None, meta_path=meta_path)
        keys = list(processor_keys or [])
        result = run_processors(
            dirty_uids_path=Path(dirty_uids_path) if dirty_uids_path else None,
            dirty_uids=list(extra_dirty_uids or []),
            source_updater_reports=source_updater_reports,
            vault_path=str(store.vault),
            store=store,
            state_store=state_store,
            processor_keys=keys or None,
            apply=apply,
            dry_run=not apply,
            allow_full_embedding=allow_full_embedding,
            allow_all_linkers=allow_all_linkers,
            allow_broad_llm=allow_broad_llm,
            archive_instance=archive_instance,
            engine_mode=ppa_engine(),
            ladder_gate=GATE_SYNTHETIC_FIXTURES,
            repo_root=repo_root,
        )
    logger.info(
        "run_processors executed=%s stale=%s skipped=%s outputs=%s",
        result.executed,
        result.report.stale_count,
        result.report.skipped_count,
        result.report.output_count,
    )
    return 1, [result.to_dict()], int(result.report.output_count or 0)


def _cards_rebuilt_from_processor_reports(reports: list[dict[str, Any]]) -> int:
    """Parse materialization card count from processor warnings."""

    import re

    best = 0
    for report in reports:
        inner = report.get("report") or report
        for warning in inner.get("warnings") or []:
            match = re.search(r"materialization incremental rebuild cards=(\d+)", str(warning))
            if match:
                best = max(best, int(match.group(1)))
        for item in report.get("item_results") or inner.get("item_results") or []:
            if item.get("processor_key") == "materialization" and item.get("status") == "complete":
                best = max(best, 1)
    return best


def _processor_materialization_failed(reports: list[dict[str, Any]]) -> bool:
    for report in reports:
        inner = report.get("report") or report
        for err in inner.get("errors") or []:
            if "materialization" in str(err).lower():
                return True
    return False


def run_maintenance(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    dry_run: bool = False,
    record_source_status: bool = False,
    record_processor_status: bool = False,
    run_source_updaters: bool = False,
    source_updater_keys: list[str] | None = None,
    apply_source_updaters: bool = False,
    source_updater_max_items: int | None = None,
    source_updater_catch_up: bool = False,
    source_updater_strict: bool = False,
    run_processors: bool = False,
    apply_processors: bool = False,
    dirty_uids_path: str = "",
    processor_keys: list[str] | None = None,
    allow_full_embedding: bool = False,
    allow_all_linkers: bool = False,
    allow_broad_llm: bool = False,
) -> MaintenanceReport:
    report = MaintenanceReport()
    report.started_at = datetime.now(timezone.utc).isoformat()
    if not os.environ.get("PYTEST_CURRENT_TEST"):
        try:
            from archive_cli.vault_cache_runtime import install_process_reuse

            install_process_reuse()
        except Exception:
            logger.exception("maintain_vault_cache_process_reuse_failed")
    idx = store.index
    schema = str(getattr(idx, "schema", "ppa"))

    if run_source_updaters:
        try:
            # Default dry-run unless explicitly applying source updaters.
            apply = bool(apply_source_updaters) and not dry_run
            count, payloads, partial = _run_source_updaters(
                store,
                schema,
                apply=apply,
                source_keys=source_updater_keys,
                max_items=source_updater_max_items,
                catch_up=source_updater_catch_up,
                strict=source_updater_strict,
                logger=logger,
            )
            report.source_updater_runs = count
            report.source_updater_reports = payloads
            report.source_updater_partial = partial
            from archive_sync.processors.dirty_io import dirty_uids_from_source_reports

            _extend_publish_uids(report, dirty_uids_from_source_reports(payloads))
            if apply and not dry_run:
                try:
                    from archive_cli.vault_cache_runtime import rebuild_vault_cache_after_writes

                    rebuild_vault_cache_after_writes(store.vault, tier=2, progress_every=5000)
                except Exception:
                    logger.exception("maintain_vault_cache_rebuild_after_updaters_failed")
            if partial and not source_updater_strict:
                report.skipped_steps.append("source_updater_hard_fail (partial success; use --strict to fail)")
            elif not apply:
                report.skipped_steps.append("source_updater_cursor_commit (dry-run)")
        except Exception as exc:
            logger.exception("maintain_run_source_updaters_failed")
            report.errors.append({"step": "run_source_updaters", "error": str(exc)})

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

    hygiene_dirty: list[str] = []
    if run_processors or run_source_updaters:
        try:
            from archive_sync.processors.dirty_io import dirty_uids_from_source_reports

            hint = set(dirty_uids_from_source_reports(report.source_updater_reports or []))
            apply_hygiene = (bool(apply_processors) or bool(apply_source_updaters)) and not dry_run
            purge, link, hygiene_dirty = _run_file_hygiene(
                store,
                apply=apply_hygiene,
                extra_dirty_uids=hint,
                logger=logger,
            )
            report.junk_attachments_purged = int(purge.get("purged") or 0)
            report.file_duplicates_linked = int(link.get("cards_linked") or 0)
            report.file_identity = {
                "cards_scanned": link.get("cards_scanned"),
                "hashes_reused": link.get("hashes_reused"),
                "hashes_computed": link.get("hashes_computed"),
                "groups": link.get("groups"),
                "incremental": True,
            }
            _extend_publish_uids(report, hygiene_dirty)
            if apply_hygiene and (report.junk_attachments_purged or report.file_duplicates_linked):
                try:
                    from archive_cli.vault_cache_runtime import mark_vault_written

                    mark_vault_written(store.vault, uids=hygiene_dirty)
                except Exception:
                    logger.debug("maintain mark_vault_written after hygiene failed", exc_info=True)
            if not apply_hygiene:
                report.skipped_steps.append("file_hygiene (dry-run)")
        except Exception as exc:
            logger.exception("maintain_file_hygiene_failed")
            report.errors.append({"step": "file_hygiene", "error": str(exc)})

    if run_processors:
        try:
            apply = bool(apply_processors) and not dry_run
            count, payloads, outputs = _run_processors(
                store,
                schema,
                apply=apply,
                dirty_uids_path=dirty_uids_path,
                extra_dirty_uids=hygiene_dirty,
                source_updater_reports=report.source_updater_reports or None,
                processor_keys=processor_keys,
                allow_full_embedding=allow_full_embedding,
                allow_all_linkers=allow_all_linkers,
                allow_broad_llm=allow_broad_llm,
                logger=logger,
            )
            report.processor_runs = count
            report.processor_reports = payloads
            report.processor_output_count = outputs
            _extend_publish_uids(report, _uids_from_processor_reports(payloads))
            if not apply:
                report.skipped_steps.append("processor_execution (dry-run)")
        except Exception as exc:
            logger.exception("maintain_run_processors_failed")
            report.errors.append({"step": "run_processors", "error": str(exc)})

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
                    return _finish_maintain(store, report, logger, dry_run=dry_run)
                raise
    except Exception as exc:
        logger.exception("maintain_tail_failed")
        report.errors.append({"step": "tail_ingestion_log", "error": str(exc)})
        return _finish_maintain(store, report, logger, dry_run=dry_run)

    if not new_rows:
        processors_applied = bool(run_processors) and bool(apply_processors) and not dry_run
        if hygiene_dirty and not dry_run and not processors_applied:
            try:
                counts = store.rebuild(force_full=False, uid_allowlist=set(hygiene_dirty))
                report.cards_rebuilt = int(counts.get("cards_materialized") or counts.get("cards") or 0)
                _extend_publish_uids(report, hygiene_dirty)
            except Exception as exc:
                logger.exception("maintain_rebuild_failed")
                report.errors.append({"step": "incremental_rebuild", "error": str(exc)})
        report.nothing_to_do = (
            not hygiene_dirty
            and not report.source_updater_runs
            and not report.processor_runs
            and report.junk_attachments_purged == 0
            and report.file_duplicates_linked == 0
        )
        return _finish_maintain(store, report, logger, dry_run=dry_run)

    report.new_cards_ingested = len(new_rows)
    created_n = sum(1 for r in new_rows if r.get("action") == "created")
    tailed_uids = {str(row.get("card_uid") or "").strip() for row in new_rows if str(row.get("card_uid") or "").strip()}
    _extend_publish_uids(report, tailed_uids)
    created_uids = {
        str(row.get("card_uid") or "").strip()
        for row in new_rows
        if row.get("action") == "created" and str(row.get("card_uid") or "").strip()
    }

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
                uid_allowlist=created_uids,
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
            res = er_mod.run_entity_resolution(
                str(store.vault),
                dry_run=dry_run,
                uid_allowlist=tailed_uids,
            )
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
            processors_applied = bool(run_processors) and bool(apply_processors)
            rebuild_uids = set(tailed_uids)
            if hygiene_dirty and not processors_applied:
                rebuild_uids.update(hygiene_dirty)
            already = _cards_rebuilt_from_processor_reports(report.processor_reports)
            if processors_applied and not _processor_materialization_failed(report.processor_reports):
                logger.info(
                    "maintain skip second rematerialize processors_already_rebuilt cards=%s tailed_uids=%s",
                    already,
                    len(rebuild_uids),
                )
                report.cards_rebuilt = already or len(rebuild_uids)
                _extend_publish_uids(report, rebuild_uids)
                report.skipped_steps.append("incremental_rebuild (processors already rematerialized allowlist)")
            else:
                counts = store.rebuild(force_full=False, uid_allowlist=rebuild_uids)
                report.cards_rebuilt = int(counts.get("cards_materialized") or counts.get("cards") or 0)
                _extend_publish_uids(report, rebuild_uids)
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

    return _finish_maintain(store, report, logger, dry_run=dry_run)
