"""Source updater execution — run adapters under the Section D contract (Phase 2)."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from archive_sync.adapters.base import BaseAdapter, IngestResult
from archive_vault.sync_state import load_sync_state

from .batch import (
    SourceUpdaterBatchSummary,
    SourceUpdaterRunReport,
    batch_summary_from_skip_details,
    commit_cursor_after_persisted,
)
from .constants import (
    ADAPTER_VERSION_DEFAULT,
    EXECUTABLE_ADAPTER_SOURCE_IDS,
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

_AUTH_BLOCKED_RE = re.compile(
    r"(auth|oauth|token|credential|permission|forbidden|unauthorized|access.?denied|not\s+authorized)",
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
    decl = declaration_for_adapter_source_id(adapter_source_id, scope=scope)
    if decl is None:
        raise ValueError(f"No source updater declaration for adapter {adapter_source_id!r}")
    if adapter_source_id not in EXECUTABLE_ADAPTER_SOURCE_IDS:
        raise ValueError(
            f"Source {source_key!r} is declared but not executable in Phase 2 "
            f"(supported: {', '.join(sorted(EXECUTABLE_ADAPTER_SOURCE_IDS))})"
        )
    return decl


def build_adapter(adapter_source_id: str) -> BaseAdapter:
    if adapter_source_id == "gmail-messages":
        from archive_sync.adapters.gmail_messages import GmailMessagesAdapter

        return GmailMessagesAdapter()
    if adapter_source_id == "calendar-events":
        from archive_sync.adapters.calendar_events import CalendarEventsAdapter

        return CalendarEventsAdapter()
    raise ValueError(f"No executable adapter for {adapter_source_id!r}")


def adapter_ingest_kwargs(decl: SourceUpdaterDeclaration, *, apply: bool) -> dict[str, Any]:
    """Build kwargs passed to ``adapter.ingest`` for a declaration."""

    _, scope = parse_source_key(decl.source_key)
    kwargs: dict[str, Any] = {"account_email": scope}
    if decl.adapter_source_id == "gmail-messages":
        kwargs["gmail_promotion_gate"] = True
    if decl.adapter_source_id == "calendar-events":
        kwargs.setdefault("calendar_id", "primary")
    return kwargs


def classify_run_exception(exc: BaseException) -> str:
    """Return run status for an exception (``blocked`` vs ``failed``)."""

    message = str(exc)
    if _AUTH_BLOCKED_RE.search(message) or _AUTH_BLOCKED_RE.search(type(exc).__name__):
        return RUN_STATUS_BLOCKED
    return RUN_STATUS_FAILED


def _install_dirty_uid_tracker(adapter: BaseAdapter, dirty: list[str]) -> Callable[[], None]:
    """Track card UIDs from to_card (dry-run + apply) and after_card_write (apply)."""

    original_to_card = adapter.to_card
    original_after = adapter.after_card_write

    def tracking_to_card(item: dict[str, Any]):
        card, provenance, body = original_to_card(item)
        uid = str(getattr(card, "uid", "") or "").strip()
        if uid and uid not in dirty:
            dirty.append(uid)
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
        uid = str(getattr(card, "uid", "") or "").strip()
        if uid and uid not in dirty:
            dirty.append(uid)
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
    ingest_kwargs = adapter_ingest_kwargs(decl, apply=apply)
    if max_items is not None:
        if decl.adapter_source_id == "gmail-messages":
            ingest_kwargs["max_threads"] = max_items
            # Keep gmail_promotion_gate=True (from adapter_ingest_kwargs). Volume
            # is bounded by max_threads; vault presence for the gate uses the
            # vault-scan cache, not a full markdown walk.
        elif decl.adapter_source_id == "calendar-events":
            ingest_kwargs["max_events"] = max_items

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

    try:
        result = adapter_obj.ingest(str(vault), dry_run=dry_run, **ingest_kwargs)
    except Exception as exc:
        restore()
        status = classify_run_exception(exc)
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
) -> SourceUpdaterMultiRunResult:
    """Run multiple sources with failure isolation."""

    multi = SourceUpdaterMultiRunResult()
    worst = 0
    for source_key in source_keys:
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
    multi.exit_code = worst
    return multi


def default_maintain_source_keys(
    *,
    gmail_accounts: tuple[str, ...] = (),
    calendar_accounts: tuple[str, ...] = (),
) -> list[str]:
    """Sources maintain should run when --run-source-updaters is set."""

    decls = expand_declarations(gmail_accounts=gmail_accounts, calendar_accounts=calendar_accounts)
    keys = [
        d.source_key
        for d in decls
        if d.enabled and d.adapter_source_id in EXECUTABLE_ADAPTER_SOURCE_IDS and "<" not in d.source_key
    ]
    if keys:
        return keys
    # No configured accounts — nothing executable without placeholders.
    return []
