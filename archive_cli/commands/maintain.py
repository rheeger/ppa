"""Living-archive maintain: pull, process dirty cards, publish search.

``ppa maintain --apply`` is the user loop. It pulls incremental updates from
connected sources, runs dirty-only processors (extract, enrich, rematerialize,
reuse leftover embeddings, embed leftovers, incremental links), then publishes
one complete serving generation. Dry-run prints the same steps with no writes.

Bare ``ppa maintain`` without ``--apply`` still tails the ingestion ledger for
compatibility. Nightly is a thin wrapper of ``--apply``, not a second pipeline.
"""

from __future__ import annotations

import importlib
import logging
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..store import DefaultArchiveStore

_LOG = logging.getLogger("ppa.maintain")


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
    mark = str(watermark or "").strip()
    if not mark:
        # Empty watermark used to SELECT the whole ledger. On this seed that is
        # millions of rows and rematerializes the vault.
        _LOG.warning("ingestion_log tail skipped: last_maintenance_at is empty")
        return []
    rows = conn.execute(
        f"SELECT card_uid, action, source_adapter, logged_at "
        f"FROM {schema}.ingestion_log "
        f"WHERE logged_at > %s ORDER BY logged_at ASC",
        (mark,),
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


APPLY_LOOP_STEPS = ("pull", "process", "publish")
FAILED_SOURCE_STATUSES = frozenset({"failed", "blocked"})


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
    journal_watermark: int = 0
    materialized_watermark: int = 0
    published_watermark: int = 0
    eligible_checkpoint: int = 0
    source_cursors: dict[str, Any] = field(default_factory=dict)
    pending_gaps: list[int] = field(default_factory=list)
    failed_revision_uids: list[str] = field(default_factory=list)
    publication: dict[str, Any] = field(default_factory=dict)
    maintenance_run_id: str = ""
    apply_loop: bool = False
    planned_steps: list[str] = field(default_factory=list)
    cards_pulled: int = 0
    cards_written: int = 0
    cards_enriched: int = 0
    embeddings_embedded: int = 0
    embeddings_reused: int = 0
    published_generation: str = ""
    pending_embeddings: int = 0
    failed_sources: list[str] = field(default_factory=list)
    provider_reason: str = ""
    ok: bool = True
    human_summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}


def apply_provider_failure_reason() -> str:
    """One-line reason the apply loop cannot run. Empty means providers are ok.

    Unset enrichment is not a hard fail (deterministic extract still runs).
    A configured enrichment or paid embedding provider that is down is a fail.
    Hash embeddings are always available.
    """

    from archive_cli.embedding_provider import DEFAULT_EMBEDDING_PROVIDER
    from archive_cli.index_config import _ppa_env

    embed_name = (_ppa_env("PPA_EMBEDDING_PROVIDER", default=DEFAULT_EMBEDDING_PROVIDER) or "hash").lower()
    if embed_name == "openai" and not (os.environ.get("OPENAI_API_KEY") or "").strip():
        return "OPENAI_API_KEY missing; apply cannot embed dirty cards"
    if embed_name not in {"", "hash", "openai"}:
        return f"unsupported embedding provider {embed_name}; apply cannot embed dirty cards"
    raw = (os.environ.get("PPA_ENRICHMENT_MODEL") or "").strip()
    if not raw:
        return ""
    try:
        from archive_cli.providers import resolve_provider

        provider = resolve_provider(refresh=True)
    except ValueError as exc:
        return str(exc)
    if provider is None:
        return "PPA_ENRICHMENT_MODEL is set but no provider resolved"
    if not provider.is_available():
        return f"enrichment provider unavailable: {provider.name} {provider.model}"
    return ""


def failed_source_keys(source_reports: list[dict[str, Any]] | None) -> list[str]:
    out: list[str] = []
    for report in source_reports or []:
        key = str(report.get("source_key") or "").strip()
        status = str(report.get("status") or "").strip()
        if key and status in FAILED_SOURCE_STATUSES:
            out.append(key)
    return out


def format_maintain_human_summary(report: MaintenanceReport) -> str:
    """Short human report. A person should be able to read it once."""

    mode = "dry-run" if "serving_index_publish (dry-run)" in report.skipped_steps else "apply"
    lines = [f"Maintain {mode}"]
    if report.planned_steps:
        lines.append("Steps: " + ", ".join(report.planned_steps) + ".")
    if mode == "dry-run":
        lines.append("Would pull connected sources, process dirty cards, and publish a complete search generation.")
        lines.append("No writes.")
        if report.failed_sources:
            lines.append("Failed sources: " + ", ".join(report.failed_sources) + ".")
        if report.provider_reason:
            lines.append(report.provider_reason + ".")
        return "\n".join(lines)
    source_runs = int(report.source_updater_runs or 0)
    lines.append(f"Pulled {report.cards_pulled} cards from {source_runs} sources.")
    if report.failed_sources:
        lines.append("Failed sources: " + ", ".join(report.failed_sources) + ".")
    lines.append(
        f"Wrote {report.cards_written} cards. Extracted {report.cards_extracted}. Enriched {report.cards_enriched}."
    )
    lines.append(
        f"Embedded {report.embeddings_embedded} new vectors, reused {report.embeddings_reused} by content hash."
    )
    if report.published_generation:
        lines.append(f"Published generation {report.published_generation}.")
    elif report.publication.get("skipped") == "clean":
        lines.append("Publish skipped: nothing new to serve.")
    elif report.publication.get("error"):
        lines.append(f"Publish failed: {report.publication.get('error')}.")
    else:
        lines.append("No new generation published.")
    lines.append(f"Pending embeddings: {report.pending_embeddings}.")
    if report.provider_reason:
        lines.append(report.provider_reason + ".")
        lines.append("Result: incomplete because a required provider is down.")
    elif report.failed_sources:
        lines.append("Result: incomplete because a live source failed.")
    elif report.errors:
        first = report.errors[0]
        lines.append(f"Result: incomplete ({first.get('step')}: {first.get('error')}).")
    elif report.ok:
        lines.append("Result: ok.")
    else:
        lines.append("Result: incomplete.")
    return "\n".join(lines)


def living_loop_ok(report: MaintenanceReport) -> bool:
    if report.errors:
        return False
    if report.failed_sources:
        return False
    publication = report.publication or {}
    if publication.get("dry_run"):
        return True
    if publication.get("ok") is False and publication.get("skipped") != "clean":
        return False
    return True


def apply_living_loop_counts(report: MaintenanceReport) -> None:
    """Fill the human-report counters from receipts this run already collected."""

    from archive_sync.processors.constants import PROCESSOR_EMAIL_THREAD_ENRICHMENT
    from archive_sync.processors.dirty_io import dirty_uids_from_source_reports

    pulled = dirty_uids_from_source_reports(report.source_updater_reports or [])
    report.cards_pulled = len(pulled) if pulled else int(report.new_cards_ingested or 0)
    report.cards_written = max(int(report.processor_output_count or 0), int(report.cards_extracted or 0))
    enriched = 0
    for item in _iter_processor_items(report.processor_reports or []):
        key = str(item.get("processor_key") or "")
        if key != PROCESSOR_EMAIL_THREAD_ENRICHMENT:
            continue
        status = str(item.get("status") or "")
        if status == "complete" and not item.get("already_current") and not item.get("valid_no_output"):
            enriched += 1
    report.cards_enriched = enriched
    embedded = 0
    reused = 0
    pending = 0
    for proc in report.processor_reports or []:
        inner = proc.get("report") or proc
        for warning in inner.get("warnings") or []:
            text = str(warning)
            match = re.search(r"embedded=(\d+)", text)
            if match:
                embedded = max(embedded, int(match.group(1)))
            match = re.search(r"reused_by_content=(\d+)", text)
            if match:
                reused = max(reused, int(match.group(1)))
            else:
                match = re.search(r"\breused=(\d+)", text)
                if match:
                    reused = max(reused, int(match.group(1)))
            match = re.search(r"pending_after=(\d+)", text)
            if match:
                pending = max(pending, int(match.group(1)))
    report.embeddings_embedded = embedded
    report.embeddings_reused = reused
    report.pending_embeddings = pending
    report.failed_sources = failed_source_keys(report.source_updater_reports)
    publication = report.publication or report.serving_index or {}
    report.published_generation = str(
        publication.get("generation_id") or publication.get("generation") or ""
    )


def finalize_living_report(report: MaintenanceReport) -> None:
    apply_living_loop_counts(report)
    report.ok = living_loop_ok(report)
    report.human_summary = format_maintain_human_summary(report)


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
            if item.get("already_current") or item.get("skip_reason") == "already_current":
                continue
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


_FAILED_ITEM_STATUSES = frozenset(
    {
        "failed",
        "retryable_failure",
        "permanent_failure",
    }
)
_PENDING_ITEM_STATUSES = frozenset(
    {
        "pending",
        "blocked_dependency",
        "blocked_provider",
        "stale",
    }
)


def read_freshness_watermarks(vault: Any) -> dict[str, Any]:
    """Journal / materialized / published watermarks. Source cursors are not served freshness."""

    from pathlib import Path

    from archive_engine.changes import CONSUMER_PUBLICATION, CONSUMER_WAREHOUSE
    from archive_vault.change_journal import ChangeJournal

    empty = {
        "journal_watermark": 0,
        "materialized_watermark": 0,
        "published_watermark": 0,
        "pending_gaps": [],
        "checkpoint": {},
    }
    if vault is None:
        return empty
    try:
        from archive_vault.change_journal import JOURNAL_REL_PATH

        root = Path(vault)
        if not root.is_dir() or not (root / JOURNAL_REL_PATH).is_file():
            return empty
        with ChangeJournal(vault) as journal:
            checkpoint = journal.checkpoint()
            value = checkpoint.get("value") if isinstance(checkpoint, dict) else {}
            if not isinstance(value, dict):
                value = {}
            warehouse = journal.consumer_cursor(CONSUMER_WAREHOUSE)
            publication = journal.consumer_cursor(CONSUMER_PUBLICATION)
            journal_hw = int(value.get("high_watermark") or 0)
            gaps = sorted({int(item) for item in (*warehouse.gaps, *publication.gaps)})
            return {
                "journal_watermark": journal_hw,
                "materialized_watermark": int(warehouse.high_watermark or 0),
                "published_watermark": int(publication.high_watermark or 0),
                "pending_gaps": gaps,
                "checkpoint": checkpoint if isinstance(checkpoint, dict) else {},
            }
    except Exception:
        return empty


def source_cursor_progress(source_reports: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Per-source cursor after persist. Never treat this as globally served freshness."""

    out: dict[str, Any] = {}
    for report in source_reports or []:
        key = str(report.get("source_key") or "").strip()
        if not key:
            continue
        out[key] = {
            "cursor_before": report.get("cursor_before") or {},
            "cursor_after": report.get("cursor_after") or {},
            "status": report.get("status") or "",
            "dirty_card_uids": list(report.get("dirty_card_uids") or []),
        }
    return out


def _iter_processor_items(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for report in reports:
        inner = report.get("report") or report
        items.extend(report.get("item_results") or inner.get("item_results") or [])
    return items


_SERVE_BLOCKING_PROCESSORS = frozenset(
    {
        "materialization",
        "email_typed_extraction",
    }
)


def failed_required_uids(reports: list[dict[str, Any]]) -> list[str]:
    """UIDs whose required processor work failed. Must not be claimed served.

    Linker / embedding failures stay pending gaps. They do not hide a
    materialized card from publish.
    """

    failed: list[str] = []
    for item in _iter_processor_items(reports):
        if str(item.get("processor_key") or "") not in _SERVE_BLOCKING_PROCESSORS:
            continue
        status = str(item.get("status") or item.get("receipt_status") or "")
        if status not in _FAILED_ITEM_STATUSES:
            continue
        if item.get("input_uid"):
            failed.append(item["input_uid"])
        for uid in item.get("output_uids") or []:
            failed.append(uid)
    return _normalize_uids(failed)


def pending_revision_uids(reports: list[dict[str, Any]]) -> list[str]:
    pending: list[str] = []
    for item in _iter_processor_items(reports):
        if str(item.get("processor_key") or "") not in _SERVE_BLOCKING_PROCESSORS:
            continue
        status = str(item.get("status") or item.get("receipt_status") or "")
        if status not in _PENDING_ITEM_STATUSES:
            continue
        if item.get("input_uid"):
            pending.append(item["input_uid"])
        for uid in item.get("output_uids") or []:
            pending.append(uid)
    return _normalize_uids(pending)


def counts_from_processor_reports(reports: list[dict[str, Any]]) -> dict[str, int]:
    """Count summaries from real receipts, not invented totals."""

    from archive_sync.processors.constants import (
        INPUT_STATUS_COMPLETE,
        PROCESSOR_EMAIL_TYPED_EXTRACTION,
        PROCESSOR_ENTITY_RESOLUTION,
        PROCESSOR_MATERIALIZATION,
    )

    extracted = 0
    resolved = 0
    rebuilt = 0
    outputs = 0
    for item in _iter_processor_items(reports):
        status = str(item.get("status") or "")
        key = str(item.get("processor_key") or "")
        output_uids = [uid for uid in (item.get("output_uids") or []) if uid]
        receipt = item.get("receipt") or {}
        receipt_outputs = receipt.get("outputs") if isinstance(receipt, dict) else None
        if receipt_outputs:
            output_uids = _normalize_uids(
                list(output_uids) + [str(row.get("uid") or "") for row in receipt_outputs if isinstance(row, dict)]
            )
        if status == INPUT_STATUS_COMPLETE and not item.get("valid_no_output"):
            outputs += len(output_uids)
            if key == PROCESSOR_EMAIL_TYPED_EXTRACTION:
                extracted += len(output_uids) or (0 if item.get("already_current") else 1)
            elif key == PROCESSOR_ENTITY_RESOLUTION:
                resolved += len(output_uids) or (0 if item.get("already_current") else 1)
            elif key == PROCESSOR_MATERIALIZATION and not item.get("already_current"):
                rebuilt += 1
    rebuilt = max(rebuilt, _cards_rebuilt_from_processor_reports(reports))
    return {
        "cards_extracted": extracted,
        "entities_resolved": resolved,
        "cards_rebuilt": rebuilt,
        "processor_output_count": outputs,
    }


def apply_processor_counts(report: MaintenanceReport) -> None:
    counts = counts_from_processor_reports(report.processor_reports or [])
    report.cards_extracted = int(counts["cards_extracted"])
    report.entities_resolved = int(counts["entities_resolved"])
    report.cards_rebuilt = int(counts["cards_rebuilt"])
    if not report.processor_output_count:
        report.processor_output_count = int(counts["processor_output_count"])


def apply_freshness_watermarks(report: MaintenanceReport, vault: Any) -> dict[str, Any]:
    marks = read_freshness_watermarks(vault)
    report.journal_watermark = int(marks.get("journal_watermark") or 0)
    report.materialized_watermark = int(marks.get("materialized_watermark") or 0)
    report.published_watermark = int(marks.get("published_watermark") or 0)
    report.pending_gaps = list(marks.get("pending_gaps") or [])
    report.source_cursors = source_cursor_progress(report.source_updater_reports)
    return marks


def eligible_checkpoint_for_report(store: Any, report: MaintenanceReport) -> dict[str, Any]:
    """Eligible contiguous checkpoint. Failed required work is not claimed served."""

    marks = apply_freshness_watermarks(report, getattr(store, "vault", None))
    failed = failed_required_uids(report.processor_reports or [])
    pending = pending_revision_uids(report.processor_reports or [])
    report.failed_revision_uids = failed
    journal_hw = int(marks.get("journal_watermark") or 0)
    materialized = int(marks.get("materialized_watermark") or 0)
    published = int(marks.get("published_watermark") or 0)
    gaps = [int(item) for item in (marks.get("pending_gaps") or [])]
    prefix = materialized if materialized > 0 else journal_hw
    if gaps:
        prefix = min(prefix, min(gaps) - 1) if min(gaps) > 0 else prefix
    prefix = max(prefix, 0)
    if failed:
        eligible = min(prefix, published) if published > 0 else 0
    else:
        eligible = prefix
    report.eligible_checkpoint = int(eligible)
    concrete = collect_maintain_publish_uids(report, getattr(store, "vault", None))
    blocked = set(failed) | set(pending)
    eligible_uids = [uid for uid in concrete if uid not in blocked]
    return {
        "high_watermark": int(eligible),
        "journal_watermark": journal_hw,
        "materialized_watermark": materialized,
        "published_watermark": published,
        "dirty_uids": eligible_uids,
        "failed_uids": failed,
        "pending_uids": pending,
        "pending_gaps": gaps,
        "checkpoint": marks.get("checkpoint") or {},
    }


def _finish_maintain(
    store: Any, report: MaintenanceReport, logger: logging.Logger, *, dry_run: bool
) -> MaintenanceReport:
    apply_processor_counts(report)
    provider_blocked = any(item.get("step") == "apply_providers" for item in report.errors)
    if provider_blocked and not dry_run:
        report.publication = {"ok": False, "error": report.provider_reason or "apply_providers"}
    else:
        _publish_serving_index(store, report, logger, dry_run=dry_run)
    report.completed_at = datetime.now(timezone.utc).isoformat()
    finalize_living_report(report)
    return report


def _publish_serving_index(store: Any, report: MaintenanceReport, logger: logging.Logger, *, dry_run: bool) -> None:
    """Publish only the eligible checkpoint through ``archive_engine.publication.publish``."""

    plan = eligible_checkpoint_for_report(store, report)
    if dry_run:
        report.skipped_steps.append("serving_index_publish (dry-run)")
        report.publication = {
            "ok": False,
            "dry_run": True,
            "eligible_checkpoint": plan["high_watermark"],
        }
        return
    from archive_cli.index_store import PostgresArchiveIndex
    from archive_cli.serving_index import serving_index_status

    if not isinstance(getattr(store, "index", None), PostgresArchiveIndex):
        report.skipped_steps.append("serving_index_publish (no warehouse)")
        return
    status = serving_index_status(store.vault)
    ready = bool(status.get("serving_index_ready"))
    active_gid = str(status.get("serving_index_generation") or "")
    concrete = list(plan["dirty_uids"])
    cards_rebuilt = int(report.cards_rebuilt or 0)
    ingested = int(report.new_cards_ingested or 0)
    publish_required = bool(concrete) or cards_rebuilt > 0 or ingested > 0
    if ready and not publish_required:
        logger.info("serving_index_publish skip-only-when-clean keep_generation=%s", active_gid)
        report.skipped_steps.append("serving_index_publish (clean)")
        report.serving_index = status
        report.publication = {"ok": True, "skipped": "clean", **status}
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
        report.publication = {"ok": False, "error": error}
        return
    if report.failed_revision_uids and plan["high_watermark"] > int(plan["published_watermark"] or 0):
        error = "eligible_checkpoint_past_failed_revision"
        logger.error(
            "serving_index_publish refused error=%s failed=%s eligible=%s published=%s",
            error,
            report.failed_revision_uids,
            plan["high_watermark"],
            plan["published_watermark"],
        )
        report.errors.append({"step": "serving_index_publish", "error": error})
        report.publication = {"ok": False, "error": error, "eligible_checkpoint": plan["high_watermark"]}
        report.serving_index = {"ok": False, "error": error, **status}
        return
    try:
        from archive_engine.publication import publish

        logger.info(
            "serving_index_publish incremental uids=%s eligible_checkpoint=%s",
            len(concrete),
            plan["high_watermark"],
        )
        receipt = publish(
            {"high_watermark": plan["high_watermark"], "checkpoint": plan.get("checkpoint")},
            {
                "vault": store.vault,
                "store": store,
                "dirty_uids": concrete,
                "mode": "incremental",
            },
        )
        payload = receipt.to_payload()
        report.publication = payload
        report.serving_index = {
            "ok": receipt.ok,
            "generation": receipt.generation_id,
            "generation_id": receipt.generation_id,
            "acked_watermark": receipt.acked_watermark,
            "eligible_checkpoint": receipt.eligible_checkpoint,
            "unresolved_gaps": list(receipt.unresolved_gaps),
            "error": receipt.error,
            **payload,
        }
        if not receipt.ok:
            logger.error("serving_index_refresh_failed error=%s", receipt.error)
            report.errors.append(
                {
                    "step": "serving_index_publish",
                    "error": str(receipt.error or "serving_index_refresh_failed"),
                }
            )
            return
        report.published_watermark = int(receipt.acked_watermark or report.published_watermark)
        report.eligible_checkpoint = int(receipt.eligible_checkpoint or plan["high_watermark"])
        from archive_cli.serving_index import ack_dirty_uids

        ack_dirty_uids(store.vault, concrete)
        logger.info(
            "serving_index_publish incremental uids=%s generation=%s acked=%s",
            len(concrete),
            receipt.generation_id,
            receipt.acked_watermark,
        )
    except Exception as exc:
        logger.exception("serving_index_refresh_failed")
        report.errors.append({"step": "serving_index_publish", "error": str(exc)})
        report.serving_index = {"ok": False, "error": str(exc)}
        report.publication = {"ok": False, "error": str(exc)}


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
        conn = store.index._connect()
        try:
            if hasattr(conn, "autocommit"):
                conn.autocommit = True
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
                default_processor_decision="typed_extraction",
                archive_instance=archive_instance,
                engine_mode=ppa_engine(),
                ladder_gate=GATE_SYNTHETIC_FIXTURES,
                repo_root=repo_root,
            )
        finally:
            conn.close()
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
            default_processor_decision="typed_extraction",
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
            if (
                item.get("processor_key") == "materialization"
                and item.get("status") == "complete"
                and not item.get("already_current")
            ):
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
    apply_loop: bool = False,
) -> MaintenanceReport:
    report = MaintenanceReport()
    report.started_at = datetime.now(timezone.utc).isoformat()
    report.maintenance_run_id = datetime.now(timezone.utc).strftime("maintain-%Y%m%dT%H%M%S%fZ")
    report.apply_loop = bool(apply_loop)
    if dirty_uids_path:
        from pathlib import Path

        from archive_sync.processors.dirty_io import load_dirty_uids

        file_uids = load_dirty_uids(Path(dirty_uids_path))
        _extend_publish_uids(report, file_uids)
        logger.info("maintain dirty_uids_path uids=%s path=%s", len(file_uids), dirty_uids_path)
    if apply_loop:
        report.planned_steps = list(APPLY_LOOP_STEPS)
        run_source_updaters = True
        run_processors = True
        apply_source_updaters = not dry_run
        apply_processors = not dry_run
        logger.info("maintain apply_loop dry_run=%s steps=%s", dry_run, ",".join(APPLY_LOOP_STEPS))
        if not dry_run:
            reason = apply_provider_failure_reason()
            if reason:
                report.provider_reason = reason
                report.errors.append({"step": "apply_providers", "error": reason})
                logger.error("maintain apply refused: %s", reason)
                return _finish_maintain(store, report, logger, dry_run=False)
    if not os.environ.get("PYTEST_CURRENT_TEST"):
        try:
            from archive_cli.vault_cache_runtime import install_process_reuse

            install_process_reuse()
        except Exception:
            logger.exception("maintain_vault_cache_process_reuse_failed")
        if apply_loop and not dry_run:
            try:
                from archive_cli.serving_index import schedule_serving_handle_warm

                schedule_serving_handle_warm(store.vault)
            except Exception:
                logger.exception("maintain_serving_handle_warm_failed")
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

    leftover_dirty: list[str] = []
    try:
        from pathlib import Path

        from archive_cli.serving_index import read_dirty_uids

        leftover_dirty = read_dirty_uids(Path(store.vault))
        if leftover_dirty:
            logger.info("maintain leftover serving-index dirty uids=%s", len(leftover_dirty))
            _extend_publish_uids(report, leftover_dirty)
    except Exception:
        logger.debug("maintain read leftover dirty uids failed", exc_info=True)

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
                    new_rows = []
                else:
                    raise
    except Exception as exc:
        logger.exception("maintain_tail_failed")
        report.errors.append({"step": "tail_ingestion_log", "error": str(exc)})
        new_rows = []

    tailed_uids = _normalize_uids(row.get("card_uid") for row in new_rows)
    if new_rows and not apply_loop:
        report.new_cards_ingested = len(new_rows)
        _extend_publish_uids(report, tailed_uids)
        report.skipped_steps.append("auto_extract (routed through processor DAG)")
        report.skipped_steps.append("entity_resolution (routed through processor DAG)")
        report.skipped_steps.append("incremental_rebuild (routed through processor DAG)")

    from archive_engine.thread_projection import drain_pending, pending_thread_uids

    thread_uids = pending_thread_uids(store.vault)
    # Apply loop dirty set is this run's source UIDs (passed via source
    # reports), hygiene, leftover serving-index dirty, and pending threads.
    # The warehouse ingestion_log is a historical ledger. Unioning it here
    # rematerialized 1.39 million cards on the living seed.
    processor_tail = [] if apply_loop else tailed_uids
    if apply_loop and tailed_uids:
        logger.info(
            "maintain apply_loop omitting ingestion_log tail from processors uids=%s",
            len(tailed_uids),
        )
    scheduler_uids = _normalize_uids(list(hygiene_dirty) + leftover_dirty + processor_tail + thread_uids)
    logger.info(
        "maintain processor dirty hygiene=%s leftover=%s tail=%s threads=%s scheduled=%s apply_loop=%s",
        len(hygiene_dirty),
        len(leftover_dirty),
        len(processor_tail),
        len(thread_uids),
        len(scheduler_uids),
        apply_loop,
    )
    should_run_processors = bool(run_processors) or bool(scheduler_uids) or bool(dirty_uids_path)
    # Explicit --run-processors honours --apply-processors. Legacy ingestion-log
    # maintain (no processor flags) still applies unless this is a dry-run.
    apply_scheduler = (bool(apply_processors) or (not run_processors and bool(scheduler_uids))) and not dry_run

    if should_run_processors:
        try:
            count, payloads, outputs = _run_processors(
                store,
                schema,
                apply=apply_scheduler,
                dirty_uids_path=dirty_uids_path,
                extra_dirty_uids=scheduler_uids,
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
            apply_processor_counts(report)
            if apply_scheduler:
                try:
                    from archive_cli.vault_cache_runtime import (
                        mark_vault_written,
                        rebuild_vault_cache_after_writes,
                    )

                    written = _uids_from_processor_reports(payloads)
                    mark_vault_written(store.vault, uids=written)
                    rebuild_vault_cache_after_writes(store.vault, tier=2)
                except Exception:
                    logger.debug("maintain vault-cache refresh after processors failed", exc_info=True)
            if not apply_scheduler:
                report.skipped_steps.append("processor_execution (dry-run)")
        except Exception as exc:
            logger.exception("maintain_run_processors_failed")
            report.errors.append({"step": "run_processors", "error": str(exc)})
    else:
        report.skipped_steps.append("processor_execution (no dirty work)")

    if thread_uids and not dry_run:
        try:
            from archive_cli.commands.identity_repair import _frontmatter_rows

            rows = _frontmatter_rows(store.vault, types=["imessage_thread", "imessage_message"])
            drained = drain_pending(store.vault, rows)
            report.skipped_steps.append(f"thread_projection drained={drained.get('drained', 0)}")
        except Exception as exc:
            logger.exception("maintain_thread_projection_failed")
            report.errors.append({"step": "thread_projection", "error": str(exc)})

    report.nothing_to_do = (
        not scheduler_uids
        and not report.source_updater_runs
        and not report.processor_runs
        and report.junk_attachments_purged == 0
        and report.file_duplicates_linked == 0
        and report.new_cards_ingested == 0
    )

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
            if dry_run:
                report.skipped_steps.append("watermark_update (dry-run)")
            else:
                try:
                    _update_watermark(conn, schema)
                except Exception as exc:
                    logger.exception("maintain_watermark_failed")
                    report.errors.append({"step": "watermark", "error": str(exc)})
    except Exception as exc:
        logger.exception("maintain_coverage_failed")
        report.errors.append({"step": "coverage_report", "error": str(exc)})

    return _finish_maintain(store, report, logger, dry_run=dry_run)
