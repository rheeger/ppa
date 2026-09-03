"""Source updater execution — run adapters under the Section D contract (Phase 2)."""

from __future__ import annotations

import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from archive_sync.adapters.base import BaseAdapter, IngestResult
from archive_vault.schema import PersonCard
from archive_vault.sync_state import load_sync_state

from .batch import (
    SourceUpdaterBatchSummary,
    SourceUpdaterRunReport,
    batch_summary_from_skip_details,
    commit_cursor_after_persisted,
)
from .constants import (
    ADAPTER_VERSION_DEFAULT,
    CONTACTS_EXPORT_SCOPES,
    EXECUTABLE_ADAPTER_SOURCE_IDS,
    EXPORT_ADAPTER_SOURCE_IDS,
    PARKED_ADAPTER_SOURCE_IDS,
    RUN_STATUS_BLOCKED,
    RUN_STATUS_FAILED,
    RUN_STATUS_PARTIAL,
    RUN_STATUS_SUCCESS,
    SECTION_D_EXECUTION_STATE,
)
from .cursor_io import read_adapter_cursor
from .declarations import (
    SourceUpdaterDeclaration,
    declaration_for_adapter_source_id,
    expand_declarations,
)
from .report import write_source_updater_report
from .state_store import SourceUpdaterStateStore

logger = logging.getLogger("ppa.source_updaters")

_AUTH_BLOCKED_RE = re.compile(
    r"(auth|oauth|token|credential|permission|forbidden|unauthorized|access.?denied|not\s+authorized|invalid_scope)",
    re.IGNORECASE,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_source_key(source_key: str) -> tuple[str, str]:
    """Split ``adapter_source_id:scope`` into parts."""

    key = source_key.strip()
    if ":" not in key:
        raise ValueError(f"source_key must be adapter:scope, got {source_key!r}")
    adapter_source_id, scope = key.split(":", 1)
    adapter_source_id = adapter_source_id.strip()
    scope = scope.strip()
    if not adapter_source_id or not scope:
        raise ValueError(f"source_key must be adapter:scope, got {source_key!r}")
    if "<" in scope or scope in {"account", "source_label"}:
        raise ValueError(f"source_key scope is a template placeholder: {source_key!r}")
    return adapter_source_id, scope


def resolve_declaration(source_key: str) -> SourceUpdaterDeclaration:
    adapter_source_id, scope = parse_source_key(source_key)
    lookup_id = "apple-health" if adapter_source_id == "health" else adapter_source_id
    decl = declaration_for_adapter_source_id(lookup_id, scope=scope)
    contacts_export = lookup_id == "contacts" and scope.strip().lower() in CONTACTS_EXPORT_SCOPES
    is_executable = lookup_id in EXECUTABLE_ADAPTER_SOURCE_IDS and not contacts_export
    if not is_executable:
        known_export = (
            adapter_source_id in EXPORT_ADAPTER_SOURCE_IDS
            or lookup_id in EXPORT_ADAPTER_SOURCE_IDS
            or contacts_export
            or decl is not None
        )
        if known_export:
            raise ValueError(
                f"Source {source_key!r} is declared but not executable in Phase 2 "
                f"(supported: {', '.join(sorted(EXECUTABLE_ADAPTER_SOURCE_IDS))})"
            )
        raise ValueError(f"No source updater declaration for adapter {adapter_source_id!r}")
    if decl is None:
        raise ValueError(f"No source updater declaration for adapter {adapter_source_id!r}")
    return decl


# CLI --max-items → adapter ingest kwarg.
MAX_ITEMS_INGEST_KWARGS: dict[str, str] = {
    "gmail-messages": "max_threads",
    "calendar-events": "max_events",
    "imessage": "max_messages",
    "otter-transcripts": "max_meetings",
    "file-libraries": "max_files",
    "photos": "max_assets",
    "beeper": "max_threads",
    "github-history": "max_items",
    "gmail-correspondents": "max_messages",
    "contacts": "max_items",
}


def apply_max_items_kwarg(
    adapter_source_id: str,
    ingest_kwargs: dict[str, Any],
    max_items: int | None,
) -> dict[str, Any]:
    """Map ``--max-items`` onto the adapter ingest kwarg, if any."""

    if max_items is None:
        return ingest_kwargs
    key = MAX_ITEMS_INGEST_KWARGS.get(adapter_source_id)
    if key is not None:
        ingest_kwargs[key] = max_items
    return ingest_kwargs


def build_adapter(adapter_source_id: str) -> BaseAdapter:
    if adapter_source_id == "gmail-messages":
        from archive_sync.adapters.gmail_messages import GmailMessagesAdapter

        return GmailMessagesAdapter()
    if adapter_source_id == "calendar-events":
        from archive_sync.adapters.calendar_events import CalendarEventsAdapter

        return CalendarEventsAdapter()
    if adapter_source_id == "imessage":
        from archive_sync.adapters.imessage import IMessageAdapter

        return IMessageAdapter()
    if adapter_source_id == "otter-transcripts":
        from archive_sync.adapters.otter_transcripts import OtterTranscriptsAdapter

        return OtterTranscriptsAdapter()
    if adapter_source_id == "file-libraries":
        from archive_sync.adapters.file_libraries import FileLibrariesAdapter

        return FileLibrariesAdapter()
    if adapter_source_id == "photos":
        from archive_sync.adapters.photos import PhotosAdapter

        return PhotosAdapter()
    if adapter_source_id == "beeper":
        from archive_sync.adapters.beeper import BeeperAdapter

        return BeeperAdapter()
    if adapter_source_id == "contacts":
        from archive_sync.adapters.contacts import ContactsAdapter

        return ContactsAdapter()
    if adapter_source_id == "github-history":
        from archive_sync.adapters.github_history import GitHubHistoryAdapter

        return GitHubHistoryAdapter()
    if adapter_source_id == "gmail-correspondents":
        from archive_sync.adapters.gmail_correspondents import GmailCorrespondentsAdapter

        return GmailCorrespondentsAdapter()
    raise ValueError(f"No executable adapter for {adapter_source_id!r}")


def _resolve_github_stage_dir(stage_dir: str | None) -> str:
    """CLI ``--stage-dir`` wins over ``PPA_GITHUB_STAGE_DIR`` / ``HFA_GITHUB_STAGE_DIR``."""

    explicit = (stage_dir or "").strip()
    if explicit:
        return explicit
    return (os.environ.get("PPA_GITHUB_STAGE_DIR") or os.environ.get("HFA_GITHUB_STAGE_DIR") or "").strip()


def adapter_ingest_kwargs(
    decl: SourceUpdaterDeclaration,
    *,
    apply: bool,
    catch_up: bool = False,
    stage_dir: str | None = None,
) -> dict[str, Any]:
    """Build kwargs passed to ``adapter.ingest`` for a declaration."""

    _, scope = parse_source_key(decl.source_key)
    adapter_id = decl.adapter_source_id
    kwargs: dict[str, Any] = {}
    if adapter_id == "gmail-messages":
        kwargs["account_email"] = scope
        kwargs["gmail_promotion_gate"] = True
        kwargs["extract_attachment_text"] = True
        if catch_up:
            # Reset page cursor so threads.list starts at newest mail.
            # Keep the promotion gate on; history_id quick-update stays cheap.
            kwargs["catch_up"] = True
            kwargs["quick_update"] = True
        return kwargs
    if adapter_id == "calendar-events":
        kwargs["account_email"] = scope
        kwargs["calendar_id"] = "primary"
        return kwargs
    if adapter_id == "imessage":
        kwargs["source_label"] = scope
        snapshot_dir = (
            os.environ.get("IMESSAGE_SNAPSHOT_DIR") or os.environ.get("PPA_IMESSAGE_SNAPSHOT_DIR") or ""
        ).strip()
        if snapshot_dir:
            kwargs["snapshot_dir"] = snapshot_dir
        return kwargs
    if adapter_id == "otter-transcripts":
        kwargs["account_email"] = scope
        return kwargs
    if adapter_id == "file-libraries":
        kwargs["roots"] = [scope]
        return kwargs
    if adapter_id == "photos":
        kwargs["source_label"] = scope
        return kwargs
    if adapter_id == "beeper":
        from archive_sync.adapters.beeper import IMESSAGE_BEEPER_ACCOUNT_PREFIXES

        # Helga-Pataki BlueBubbles bridge: iMessage stays the Messages snapshot source.
        kwargs["exclude_account_prefixes"] = list(IMESSAGE_BEEPER_ACCOUNT_PREFIXES)
        return kwargs
    if adapter_id == "contacts":
        kwargs["sources"] = ["google"]
        if "@" in scope:
            kwargs["account_email"] = scope
        return kwargs
    if adapter_id == "github-history":
        resolved_stage = _resolve_github_stage_dir(stage_dir)
        if resolved_stage:
            kwargs["stage_dir"] = resolved_stage
        if catch_up:
            kwargs["catch_up"] = True
        return kwargs
    if adapter_id == "gmail-correspondents":
        kwargs["account_email"] = scope
        return kwargs
    kwargs["account_email"] = scope
    return kwargs


def classify_run_exception(exc: BaseException) -> str:
    """Return run status for an exception (``blocked`` vs ``failed``)."""

    message = str(exc)
    if _AUTH_BLOCKED_RE.search(message) or _AUTH_BLOCKED_RE.search(type(exc).__name__):
        return RUN_STATUS_BLOCKED
    return RUN_STATUS_FAILED


def _commit_state_store(state_store: SourceUpdaterStateStore | None) -> None:
    """Flush per-source so a later kill still leaves last-run rows."""

    if state_store is None:
        return
    conn = getattr(state_store, "_conn", None)
    if conn is None:
        return
    try:
        conn.commit()
    except Exception:
        logger.exception("source updater state commit failed")


def _install_dirty_uid_tracker(adapter: BaseAdapter, dirty: list[str]) -> Callable[[], None]:
    """Track persisted card UIDs for downstream processors.

    Non-person cards: UID from ``to_card`` (dry-run + apply) and ``after_card_write``.
    Person cards: only ``after_card_write``, which ingest calls with the persisted
    host card after merge/create. Conflicts write nothing and are not tracked.
    """

    original_to_card = adapter.to_card
    original_after = adapter.after_card_write
    seen: set[str] = set()

    def _track(uid: object) -> None:
        text = str(uid or "").strip()
        if text and text not in seen:
            seen.add(text)
            dirty.append(text)

    def tracking_to_card(item: dict[str, Any]):
        card, provenance, body = original_to_card(item)
        if not isinstance(card, PersonCard):
            _track(getattr(card, "uid", ""))
        return card, provenance, body

    def tracking_after(
        vault_path,
        card,
        rel_path,
        *,
        raw_item,
        action,
        **kwargs,
    ):
        _track(getattr(card, "uid", ""))
        return original_after(
            vault_path,
            card,
            rel_path,
            raw_item=raw_item,
            action=action,
            **kwargs,
        )

    adapter.to_card = tracking_to_card  # type: ignore[method-assign]
    adapter.after_card_write = tracking_after  # type: ignore[method-assign]

    def restore() -> None:
        adapter.to_card = original_to_card  # type: ignore[method-assign]
        adapter.after_card_write = original_after  # type: ignore[method-assign]

    return restore


def batch_summary_from_ingest(
    result: IngestResult,
    *,
    dirty_card_uids: list[str] | None = None,
    default_active_policy: str = "",
) -> SourceUpdaterBatchSummary:
    """Map ``IngestResult`` into a committed batch summary."""

    skip = dict(result.skip_details or {})
    summary = batch_summary_from_skip_details(
        skip,
        observed=0,
        updated=int(result.merged or 0),
        dirty_card_uids=dirty_card_uids,
    )
    # Non-promotion sources: created cards are promoted/active by default.
    if summary.promoted == 0 and summary.suppressed == 0 and summary.quarantined == 0:
        summary.promoted = int(result.created or 0)
        if summary.updated == 0:
            summary.updated = int(result.merged or 0)
    if summary.observed <= 0:
        summary.observed = (
            summary.promoted
            + summary.suppressed
            + summary.quarantined
            + summary.updated
            + summary.unchanged
            + int(result.skipped or 0)
        )
    if dirty_card_uids:
        summary.dirty_card_uids = list(dirty_card_uids)
    return summary


@dataclass
class SourceUpdaterRunResult:
    report: SourceUpdaterRunReport
    exit_hint: int = 0  # 0 success, 1 runtime, 4 blocked
    completion_state: str = SECTION_D_EXECUTION_STATE


@dataclass
class SourceUpdaterMultiRunResult:
    reports: list[SourceUpdaterRunReport] = field(default_factory=list)
    exit_code: int = 0
    completion_state: str = SECTION_D_EXECUTION_STATE

    def to_dict(self) -> dict[str, Any]:
        return {
            "completion_state": self.completion_state,
            "exit_code": self.exit_code,
            "runs": [r.to_dict() for r in self.reports],
        }


def run_source_updater(
    *,
    source_key: str,
    vault_path: str | Path,
    apply: bool = False,
    archive_instance: str = "",
    engine_mode: str = "",
    ladder_gate: str = "synthetic_fixtures",
    run_id: str = "",
    repo_root: Path | None = None,
    state_store: SourceUpdaterStateStore | None = None,
    adapter: BaseAdapter | None = None,
    decision_run_id: str = "",
    max_items: int | None = None,
    catch_up: bool = False,
    stage_dir: str | None = None,
    strict: bool = False,
) -> SourceUpdaterRunResult:
    """Run one source updater. Dry-run by default; ``apply`` persists and advances cursor."""

    vault = Path(vault_path)
    started = _utc_now_iso()
    run_id = run_id.strip() or f"su-{uuid.uuid4().hex[:12]}"

    try:
        decl = resolve_declaration(source_key)
    except ValueError as exc:
        report = SourceUpdaterRunReport(
            run_id=run_id,
            source_key=source_key,
            source_type="unknown",
            archive_instance=archive_instance,
            status=RUN_STATUS_FAILED,
            errors=[str(exc)],
            started_at=started,
            completed_at=_utc_now_iso(),
            engine_mode=engine_mode,
            ladder_gate=ladder_gate,
        )
        return SourceUpdaterRunResult(report=report, exit_hint=2)

    adapter_obj = adapter or build_adapter(decl.adapter_source_id)
    ingest_kwargs = adapter_ingest_kwargs(decl, apply=apply, catch_up=catch_up, stage_dir=stage_dir)
    if (
        decl.adapter_source_id == "gmail-messages"
        and state_store is not None
        and getattr(state_store, "_conn", None) is not None
        and getattr(state_store, "_schema", "")
    ):
        ingest_kwargs["promotion_db_conn"] = state_store._conn
        ingest_kwargs["promotion_db_schema"] = state_store._schema
        ingest_kwargs["promotion_decision_run_id"] = decision_run_id or run_id
    # Gmail: keep gmail_promotion_gate=True. Volume is bounded by max_threads;
    # vault presence for the gate uses the vault-scan cache, not a full markdown
    # walk. Catch-up must not disable the gate either — including uncapped runs.
    apply_max_items_kwarg(decl.adapter_source_id, ingest_kwargs, max_items)

    cursor_key = adapter_obj.get_cursor_key(**ingest_kwargs)
    cursor_before = dict(load_sync_state(vault).get(cursor_key, {}) or {})
    if not cursor_before:
        # Fall back to adapter_source_id cursor for fixtures that store under short key.
        cursor_before = read_adapter_cursor(vault, decl.adapter_source_id)

    dirty: list[str] = []
    restore = _install_dirty_uid_tracker(adapter_obj, dirty)
    dry_run = not apply
    warnings: list[str] = []
    if dry_run:
        warnings.append("dry_run: cursor will not advance; cards will not be written")
    if decl.adapter_source_id == "gmail-messages" and ingest_kwargs.get("gmail_promotion_gate") is True:
        warnings.append("gmail_promotion_gate=true")
        if max_items is not None:
            warnings.append(f"max_threads={max_items} (bounded fetch; promotion gate remains on)")
    if catch_up and decl.adapter_source_id == "gmail-messages":
        warnings.append("catch_up: gmail page cursor reset (newest-first; history_id skip kept)")

    logger.info(
        "source updater start source_key=%s apply=%s max_items=%s catch_up=%s stage_dir=%s cursor_key=%s",
        decl.source_key,
        apply,
        max_items if max_items is not None else "none",
        catch_up,
        ingest_kwargs.get("stage_dir") or "none",
        cursor_key,
    )
    ingest_started = time.perf_counter()
    from archive_sync.transient_retry import call_with_transient_retry

    try:
        result = call_with_transient_retry(
            lambda: adapter_obj.ingest(str(vault), dry_run=dry_run, **ingest_kwargs),
            logger=logger,
            label=f"source updater ingest source_key={decl.source_key}",
            attempts=5,
        )
    except Exception as exc:
        restore()
        elapsed = time.perf_counter() - ingest_started
        status = classify_run_exception(exc)
        logger.exception(
            "source updater failed source_key=%s status=%s elapsed=%.1fs error=%s",
            decl.source_key,
            status,
            elapsed,
            exc,
        )
        report = SourceUpdaterRunReport(
            run_id=run_id,
            source_key=decl.source_key,
            source_type=decl.source_type,
            archive_instance=archive_instance,
            status=status,
            cursor_before=dict(cursor_before),
            cursor_after=dict(cursor_before),
            errors=[f"{type(exc).__name__}: {exc}"],
            warnings=warnings,
            started_at=started,
            completed_at=_utc_now_iso(),
            engine_mode=engine_mode,
            ladder_gate=ladder_gate,
            decision_run_id=decision_run_id,
            adapter_version=decl.adapter_version or ADAPTER_VERSION_DEFAULT,
            policy_version=decl.promotion_policy_version,
        )
        if repo_root is not None:
            write_source_updater_report(repo_root, report)
        if state_store is not None:
            state_store.record_run(report)
            _commit_state_store(state_store)
        return SourceUpdaterRunResult(
            report=report,
            exit_hint=4 if status == RUN_STATUS_BLOCKED else 1,
        )
    finally:
        restore()

    cursor_after_live = dict(load_sync_state(vault).get(cursor_key, {}) or {})
    side_effects_persisted = apply and len(result.errors) == 0
    # On apply, BaseAdapter already committed cursor when safe. Report what is on disk.
    # On dry-run / errors, force cursor_after == cursor_before for the contract report.
    if side_effects_persisted:
        cursor_after = cursor_after_live or commit_cursor_after_persisted(
            side_effects_persisted=True,
            cursor_before=cursor_before,
            cursor_patch=cursor_after_live,
        )
    else:
        cursor_after = commit_cursor_after_persisted(
            side_effects_persisted=False,
            cursor_before=cursor_before,
            cursor_patch=cursor_after_live,
        )

    batch = batch_summary_from_ingest(
        result,
        dirty_card_uids=dirty,
        default_active_policy=decl.default_active_policy,
    )
    status = RUN_STATUS_SUCCESS
    if result.errors:
        status = RUN_STATUS_PARTIAL if (result.created or result.merged or batch.promoted) else RUN_STATUS_FAILED
        warnings.append(f"ingest reported {len(result.errors)} item error(s)")

    report = SourceUpdaterRunReport(
        run_id=run_id,
        source_key=decl.source_key,
        source_type=decl.source_type,
        archive_instance=archive_instance,
        status=status,
        cursor_before=dict(cursor_before),
        cursor_after=dict(cursor_after),
        batch=batch,
        errors=list(result.errors),
        warnings=warnings,
        started_at=started,
        completed_at=_utc_now_iso(),
        engine_mode=engine_mode,
        ladder_gate=ladder_gate,
        decision_run_id=decision_run_id,
        adapter_version=decl.adapter_version or ADAPTER_VERSION_DEFAULT,
        policy_version=decl.promotion_policy_version,
    )
    if repo_root is not None:
        write_source_updater_report(repo_root, report)
    if state_store is not None:
        state_store.record_run(report)
        _commit_state_store(state_store)

    if (result.created or result.merged) and apply:
        try:
            from archive_cli.vault_cache_runtime import mark_vault_written

            mark_vault_written(vault_path)
        except Exception:
            logger.debug("source updater mark_vault_written failed", exc_info=True)

    elapsed = time.perf_counter() - ingest_started
    logger.info(
        "source updater done source_key=%s status=%s created=%s merged=%s errors=%s dirty_uids=%s elapsed=%.1fs",
        decl.source_key,
        status,
        result.created,
        result.merged,
        len(result.errors),
        len(dirty),
        elapsed,
    )

    exit_hint = 0 if status in (RUN_STATUS_SUCCESS, RUN_STATUS_PARTIAL) else 1
    return SourceUpdaterRunResult(report=report, exit_hint=exit_hint)


def run_source_updaters(
    *,
    source_keys: list[str],
    vault_path: str | Path,
    apply: bool = False,
    archive_instance: str = "",
    engine_mode: str = "",
    ladder_gate: str = "synthetic_fixtures",
    repo_root: Path | None = None,
    state_store: SourceUpdaterStateStore | None = None,
    adapter_factory: Callable[[str], BaseAdapter] | None = None,
    max_items: int | None = None,
    catch_up: bool = False,
    stage_dir: str | None = None,
    strict: bool = False,
) -> SourceUpdaterMultiRunResult:
    """Run multiple sources with failure isolation."""

    multi = SourceUpdaterMultiRunResult()
    worst = 0
    ok_statuses = {RUN_STATUS_SUCCESS, RUN_STATUS_PARTIAL}
    logger.info(
        "source updaters run start count=%d apply=%s max_items=%s sources=%s",
        len(source_keys),
        apply,
        max_items if max_items is not None else "none",
        ",".join(source_keys),
    )
    for index, source_key in enumerate(source_keys, start=1):
        logger.info("source updaters step %d/%d source_key=%s", index, len(source_keys), source_key)
        adapter = None
        if adapter_factory is not None:
            try:
                adapter = adapter_factory(parse_source_key(source_key)[0])
            except Exception:
                adapter = None
        one = run_source_updater(
            source_key=source_key,
            vault_path=vault_path,
            apply=apply,
            archive_instance=archive_instance,
            engine_mode=engine_mode,
            ladder_gate=ladder_gate,
            repo_root=repo_root,
            state_store=state_store,
            adapter=adapter,
            max_items=max_items,
            catch_up=catch_up,
            stage_dir=stage_dir,
            strict=strict,
        )
        multi.reports.append(one.report)
        if one.exit_hint > worst:
            # Prefer blocked (4) over runtime (1) when any blocked; validation (2) over success.
            if one.exit_hint == 4:
                worst = 4
            elif one.exit_hint == 2 and worst not in (4,):
                worst = 2
            elif one.exit_hint == 1 and worst == 0:
                worst = 1
    successes = sum(1 for report in multi.reports if report.status in ok_statuses)
    failures = len(multi.reports) - successes
    if worst == 1 and not strict and successes > 0:
        multi.exit_code = 0
        multi.completion_state = "partial"
        logger.warning(
            "source updaters partial success ok=%s failed=%s strict=false — continuing maintain",
            successes,
            failures,
        )
    else:
        multi.exit_code = worst
    logger.info(
        "source updaters run done count=%d exit_code=%s completion=%s statuses=%s",
        len(multi.reports),
        multi.exit_code,
        multi.completion_state,
        ",".join(f"{r.source_key}:{r.status}" for r in multi.reports),
    )
    return multi


def default_maintain_source_keys(
    *,
    gmail_accounts: tuple[str, ...] = (),
    calendar_accounts: tuple[str, ...] = (),
    otter_accounts: tuple[str, ...] = (),
) -> list[str]:
    """Live (non-parked) sources maintain should run when --run-source-updaters is set.

    Photos and Apple Health stay parked. Gmail/Calendar/Otter/correspondents are
    included only when concrete accounts are supplied (placeholders are skipped).
    """

    decls = expand_declarations(
        gmail_accounts=gmail_accounts,
        calendar_accounts=calendar_accounts,
        otter_accounts=otter_accounts,
    )
    return [
        d.source_key
        for d in decls
        if d.enabled
        and d.adapter_source_id in EXECUTABLE_ADAPTER_SOURCE_IDS
        and d.adapter_source_id not in PARKED_ADAPTER_SOURCE_IDS
        and "<" not in d.source_key
    ]
