"""Snapshot source status from vault cursors without running sync."""

from __future__ import annotations

from typing import Any

from .batch import SourceUpdaterBatchSummary
from .constants import ADAPTER_VERSION_DEFAULT, RUN_STATUS_SUCCESS
from .cursor_io import read_adapter_cursor, summarize_cursor_payload
from .declarations import SourceUpdaterDeclaration, expand_declarations
from .state_store import SourceUpdaterStateRecord, SourceUpdaterStateStore


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
    for decl in declarations:
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
