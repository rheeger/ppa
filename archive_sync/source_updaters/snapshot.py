"""Snapshot source status from vault cursors without running sync."""

from __future__ import annotations

from typing import Any

from .batch import SourceUpdaterBatchSummary
from .constants import (
    ADAPTER_VERSION_DEFAULT,
    EXPORT_ADAPTER_SOURCE_IDS,
    PARKED_ADAPTER_SOURCE_IDS,
    RUN_STATUS_SUCCESS,
)
from .cursor_io import read_adapter_cursor, summarize_cursor_payload
from .declarations import SourceUpdaterDeclaration, declaration_for_adapter_source_id
from .state_store import SourceUpdaterStateRecord, SourceUpdaterStateStore


def is_template_source_key(source_key: str) -> bool:
    """True for declaration placeholders such as ``gmail-messages:<account>``."""

    key = (source_key or "").strip()
    if not key:
        return False
    if "<" in key or ">" in key:
        return True
    if ":" not in key:
        return False
    scope = key.split(":", 1)[1].strip().lower()
    return scope in {"account", "source_label", "<account>", "<source_label>"}


def adapter_id_from_source_key(source_key: str) -> str:
    key = (source_key or "").strip()
    if ":" not in key:
        return key
    return key.split(":", 1)[0].strip()


def is_parked_source_key(source_key: str) -> bool:
    adapter = adapter_id_from_source_key(source_key)
    if adapter == "health":
        adapter = "apple-health"
    return adapter in PARKED_ADAPTER_SOURCE_IDS or adapter in EXPORT_ADAPTER_SOURCE_IDS


def is_required_freshness_source(source_key: str, *, enabled: bool = True) -> bool:
    """Live, non-template, non-parked streams that can fail READY."""

    if not source_key or not enabled:
        return False
    if is_template_source_key(source_key):
        return False
    if is_parked_source_key(source_key):
        return False
    return True


def resolve_status_declarations(
    declarations: list[SourceUpdaterDeclaration],
    states: dict[str, SourceUpdaterStateRecord] | None = None,
) -> list[SourceUpdaterDeclaration]:
    """Prefer concrete live keys over ``<account>`` templates."""

    live_keys = [key for key in (states or {}) if key and not is_template_source_key(key)]
    by_adapter: dict[str, list[str]] = {}
    for key in live_keys:
        by_adapter.setdefault(adapter_id_from_source_key(key), []).append(key)

    out: list[SourceUpdaterDeclaration] = []
    seen: set[str] = set()

    def _append(decl: SourceUpdaterDeclaration | None) -> None:
        if decl is None or not decl.source_key or decl.source_key in seen:
            return
        out.append(decl)
        seen.add(decl.source_key)

    for decl in declarations:
        adapter = decl.adapter_source_id
        matches = list(by_adapter.get(adapter, []))
        if adapter == "apple-health":
            matches = matches or list(by_adapter.get("health", []))
        if matches:
            for key in matches:
                scope = key.split(":", 1)[1] if ":" in key else ""
                lookup = "health" if adapter == "apple-health" else adapter
                _append(declaration_for_adapter_source_id(lookup, scope=scope))
            continue
        if not is_template_source_key(decl.source_key):
            _append(decl)

    for key in live_keys:
        if key in seen:
            continue
        adapter, _, scope = key.partition(":")
        _append(declaration_for_adapter_source_id(adapter, scope=scope))
    return out


def snapshot_declaration_state(
    store: SourceUpdaterStateStore,
    decl: SourceUpdaterDeclaration,
    *,
    vault_path: str,
    run_id: str = "",
) -> SourceUpdaterStateRecord:
    """Read cursor from sync-state only; never calls adapter fetch."""

    cursor = read_adapter_cursor(vault_path, decl.adapter_source_id)
    batch = SourceUpdaterBatchSummary()
    skip = cursor.get("skip_details") if isinstance(cursor.get("skip_details"), dict) else {}
    if skip:
        from .batch import batch_summary_from_skip_details

        batch = batch_summary_from_skip_details(skip)
    record = store.get_state(decl.source_key) or SourceUpdaterStateRecord(
        source_key=decl.source_key,
        source_type=decl.source_type,
        enabled=decl.enabled,
    )
    record.cursor_payload = cursor
    record.adapter_version = decl.adapter_version or ADAPTER_VERSION_DEFAULT
    record.policy_version = decl.promotion_policy_version
    record.last_batch_summary = batch.to_dict()
    if run_id:
        record.last_run_id = run_id
    if cursor.get("last_sync"):
        record.last_success_at = str(cursor.get("last_sync"))
    store.upsert_state(record, last_run_status=RUN_STATUS_SUCCESS if cursor else "")
    return record


def snapshot_all_declarations(
    store: SourceUpdaterStateStore,
    declarations: list[SourceUpdaterDeclaration],
    *,
    vault_path: str,
) -> list[SourceUpdaterStateRecord]:
    """Snapshot each declaration; one failure does not block others."""

    out: list[SourceUpdaterStateRecord] = []
    for decl in declarations:
        if not decl.enabled:
            continue
        try:
            out.append(snapshot_declaration_state(store, decl, vault_path=vault_path))
        except Exception:
            continue
    return out


def status_payload_for_declarations(
    declarations: list[SourceUpdaterDeclaration],
    store: SourceUpdaterStateStore,
    *,
    vault_path: str,
    archive_instance: str,
    engine_mode: str,
) -> dict[str, Any]:
    """Build JSON status payload without running source sync."""

    states = {s.source_key: s for s in store.list_state()}
    sources: list[dict[str, Any]] = []
    for decl in resolve_status_declarations(declarations, states):
        state = states.get(decl.source_key)
        cursor = read_adapter_cursor(vault_path, decl.adapter_source_id)
        entry: dict[str, Any] = {
            "declaration": decl.to_dict(),
            "cursor_summary": summarize_cursor_payload(cursor),
            "vault_cursor_keys": sorted(cursor.keys()) if cursor else [],
        }
        if state:
            entry["state"] = state.to_dict()
        else:
            entry["state"] = {
                "source_key": decl.source_key,
                "staleness_state": "never_synced",
                "cursor_payload": cursor,
            }
        sources.append(entry)
    return {
        "completion_state": "source_updater_status_read",
        "archive_instance": archive_instance,
        "engine_mode": engine_mode,
        "vault_path": vault_path,
        "sources": sources,
    }
