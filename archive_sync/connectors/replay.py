"""Connector replay, cursor lifecycle, and thread-freshness (P08-C).

Cursor candidates become durable only after persist. Expired tokens are an
explicit state, not a silent full-mailbox reset. Provider tombstones record
source state; archive-forget is a separate intent. Burst keys are invalidated
when a resolver is attached; otherwise message/thread UIDs are dirtied and
burst freshness is ``unknown``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol

from archive_engine.contracts import AccessContext, AffectedContext, ArchiveIdentity
from archive_engine.errors import IncompatibleContractError, IncompatibleStateError
from archive_sync.connectors.contracts import CanonicalWriter, ConnectorRunResult
from archive_sync.connectors.runtime import execute_connector
from archive_vault.paths import resolve_contained_path

logger = logging.getLogger("ppa.connectors")

CURSOR_VERSION_V1 = "1"
CURSOR_VERSION_V2 = "2"
P01_BURST_ALGORITHM = "p01b1-burst-1"
BURST_FRESHNESS_UNKNOWN = "unknown"
BURST_FRESHNESS_INVALIDATED = "invalidated"
CURSOR_ACTIVE = "active"
CURSOR_EXPIRED = "expired"
CURSOR_PENDING_CATCH_UP = "pending_catch_up"
CURSOR_INCOMPATIBLE = "incompatible"
RETENTION_PROVIDER_TOMBSTONE = "provider_tombstone"
RETENTION_ARCHIVE_FORGET = "archive_forget"
PENDING_SCOPES_REL = "_meta/connector-pending-scopes.json"
CHECKPOINT_REL = "_meta/connector-cursors.json"
FIXTURE_CATCH_UP_BUDGET = 16

CursorStatus = Literal["active", "expired", "pending_catch_up", "incompatible"]
BurstFreshness = Literal["unknown", "invalidated"]


class BurstKeyResolver(Protocol):
    """Optional P01-B1 burst invalidation. Absence is unknown, not fresh."""

    def burst_keys_for(
        self,
        *,
        thread_uid: str,
        changed_message_ids: Sequence[str],
        content_hashes: Sequence[str] = (),
    ) -> tuple[str, ...]: ...


@dataclass(frozen=True)
class CursorState:
    version: str
    status: CursorStatus
    payload: Mapping[str, object]
    catch_up_estimate: int = 0
    reason: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "cursor_version": self.version,
            "status": self.status,
            "payload": dict(self.payload),
            "catch_up_estimate": self.catch_up_estimate,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ThreadEvent:
    """Reply/edit/delete against one thread. Not a mailbox walk."""

    parent_thread_id: str
    changed_message_ids: tuple[str, ...]
    provider_revision: str
    account_scope: str
    event_identity: str
    kind: str = "reply"
    requires_full_thread_refetch: bool = False
    tombstone: bool = False


@dataclass(frozen=True)
class PendingScope:
    source: str
    account_scope: str
    thread_id: str
    event_identity: str
    status: str = "pending"
    resolver_capability: str = ""
    scheduled: bool = False

    def to_payload(self) -> dict[str, object]:
        return {
            "source": self.source,
            "account_scope": self.account_scope,
            "thread_id": self.thread_id,
            "event_identity": self.event_identity,
            "status": self.status,
            "resolver_capability": self.resolver_capability,
            "scheduled": self.scheduled,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> PendingScope:
        return cls(
            source=str(payload.get("source") or ""),
            account_scope=str(payload.get("account_scope") or ""),
            thread_id=str(payload.get("thread_id") or ""),
            event_identity=str(payload.get("event_identity") or ""),
            status=str(payload.get("status") or "pending"),
            resolver_capability=str(payload.get("resolver_capability") or ""),
            scheduled=bool(payload.get("scheduled")),
        )


@dataclass
class LifecycleResult:
    run: ConnectorRunResult | None
    cursor: CursorState
    dirty_uids: tuple[str, ...]
    burst_freshness: BurstFreshness
    burst_keys: tuple[str, ...]
    pending_scopes: tuple[PendingScope, ...]
    retention: str
    rolled_back: bool = False
    interrupted: bool = False
    cards_preserved: bool = True
    scheduled_event_identities: tuple[str, ...] = ()
    unrelated_uids: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        return {
            "cursor": self.cursor.to_payload(),
            "dirty_uids": list(self.dirty_uids),
            "burst_freshness": self.burst_freshness,
            "burst_keys": list(self.burst_keys),
            "pending_scopes": [item.to_payload() for item in self.pending_scopes],
            "retention": self.retention,
            "rolled_back": self.rolled_back,
            "interrupted": self.interrupted,
            "cards_preserved": self.cards_preserved,
            "scheduled_event_identities": list(self.scheduled_event_identities),
            "unrelated_uids": list(self.unrelated_uids),
            "uids": list(self.run.uids) if self.run is not None else [],
            "created_count": self.run.created_count if self.run is not None else 0,
        }


@dataclass
class ConnectorCheckpoint:
    last_safe_cursor: dict[str, object] = field(default_factory=dict)
    last_safe_version: str = CURSOR_VERSION_V1
    connector_version: str = "1.0.0"


def compatible_burst_key(message_ids: Sequence[str], content_hashes: Sequence[str]) -> str:
    """P01-B1 burst-key identity. Uses the live module when present."""

    try:
        from archive_cli.conversation_bursts import burst_key_for

        return burst_key_for(message_ids, content_hashes)
    except ImportError:
        payload = json.dumps(
            {
                "algorithm": P01_BURST_ALGORITHM,
                "content_hashes": list(content_hashes),
                "message_ids": list(message_ids),
            },
            sort_keys=True,
        )
        return "burst-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def inspect_cursor(cursor: Mapping[str, object] | None) -> CursorState:
    raw = dict(cursor or {})
    version = str(raw.get("cursor_version") or CURSOR_VERSION_V1)
    if raw.get("expired") is True or str(raw.get("status") or "") == CURSOR_EXPIRED:
        return CursorState(
            version=version,
            status=CURSOR_EXPIRED,
            payload=raw,
            catch_up_estimate=int(raw.get("catch_up_estimate") or FIXTURE_CATCH_UP_BUDGET),
            reason=str(raw.get("reason") or "provider token expired"),
        )
    token = str(raw.get("page_token") or raw.get("sync_token") or raw.get("history_id") or "")
    if token.upper() == "EXPIRED":
        return CursorState(
            version=version,
            status=CURSOR_EXPIRED,
            payload=raw,
            catch_up_estimate=FIXTURE_CATCH_UP_BUDGET,
            reason="provider token expired",
        )
    return CursorState(version=version, status=CURSOR_ACTIVE, payload=raw)


def migrate_cursor(
    cursor: Mapping[str, object] | None,
    *,
    from_version: str | None = None,
    to_version: str = CURSOR_VERSION_V2,
    strategy: str = "migrate",
) -> CursorState:
    """Explicit cursor migration. Expired tokens do not reset to the beginning."""

    state = inspect_cursor(cursor)
    if state.status == CURSOR_EXPIRED:
        if strategy != "bounded_replay":
            return CursorState(
                version=state.version,
                status=CURSOR_EXPIRED,
                payload=dict(state.payload),
                catch_up_estimate=state.catch_up_estimate,
                reason=state.reason or "expired cursor is not a silent reset",
            )
        if state.catch_up_estimate > FIXTURE_CATCH_UP_BUDGET:
            raise IncompatibleStateError(
                f"catch-up estimate {state.catch_up_estimate} exceeds fixture budget {FIXTURE_CATCH_UP_BUDGET}"
            )
        payload = {key: value for key, value in state.payload.items() if key not in {"expired", "status"}}
        payload["cursor_version"] = to_version
        payload["bounded_replay"] = True
        return CursorState(
            version=to_version,
            status=CURSOR_PENDING_CATCH_UP,
            payload=payload,
            catch_up_estimate=state.catch_up_estimate,
            reason="bounded expired-cursor replay",
        )
    source_version = from_version or state.version
    payload = dict(state.payload)
    if source_version == to_version:
        payload.setdefault("cursor_version", to_version)
        return CursorState(version=to_version, status=CURSOR_ACTIVE, payload=payload)
    if source_version == CURSOR_VERSION_V1 and to_version == CURSOR_VERSION_V2:
        migrated = {
            "cursor_version": CURSOR_VERSION_V2,
            "history_id": payload.get("history_id"),
            "page_token": payload.get("page_token"),
            "page_index": payload.get("page_index") or 0,
            "sync_token": payload.get("sync_token"),
        }
        return CursorState(version=CURSOR_VERSION_V2, status=CURSOR_ACTIVE, payload=migrated)
    if strategy == "safe_replay":
        return CursorState(
            version=source_version,
            status=CURSOR_PENDING_CATCH_UP,
            payload=payload,
            catch_up_estimate=FIXTURE_CATCH_UP_BUDGET,
            reason=f"no migration from {source_version} to {to_version}",
        )
    raise IncompatibleContractError(f"no cursor migration from {source_version} to {to_version}")


def compare_provider_revision(left: str, right: str) -> int:
    """Latest authoritative source revision wins. Equal revisions are duplicates."""

    if left == right:
        return 0
    try:
        return (int(left) > int(right)) - (int(left) < int(right))
    except (TypeError, ValueError):
        return (left > right) - (left < right)


def select_latest_events(events: Sequence[ThreadEvent]) -> tuple[ThreadEvent, ...]:
    """Drop duplicates and older out-of-order revisions for the same identity."""

    latest: dict[str, ThreadEvent] = {}
    for event in events:
        key = event.event_identity
        previous = latest.get(key)
        if previous is None or compare_provider_revision(event.provider_revision, previous.provider_revision) >= 0:
            latest[key] = event
    return tuple(latest[key] for key in latest)


def retention_for(*, tombstone: bool, archive_forget: bool) -> str:
    if archive_forget:
        return RETENTION_ARCHIVE_FORGET
    if tombstone:
        return RETENTION_PROVIDER_TOMBSTONE
    return ""


def apply_provider_tombstone(card: Mapping[str, object]) -> dict[str, object]:
    """Record provider deletion without erasing the historical card."""

    out = dict(card)
    out["provider_deleted"] = True
    out["retention"] = RETENTION_PROVIDER_TOMBSTONE
    return out


def archive_forget_requires_intent(*, intent: bool) -> None:
    if not intent:
        raise IncompatibleStateError("archive-forget requires explicit archival intent")


def dirty_uids_for_thread_event(
    event: ThreadEvent,
    *,
    thread_uid: str,
    message_uids: Sequence[str],
) -> tuple[str, ...]:
    seen: list[str] = []
    for uid in (thread_uid, *message_uids):
        if uid and uid not in seen:
            seen.append(uid)
    return tuple(seen)


def burst_freshness_for(
    *,
    resolver: BurstKeyResolver | None,
    thread_uid: str,
    changed_message_ids: Sequence[str],
    content_hashes: Sequence[str] = (),
) -> tuple[BurstFreshness, tuple[str, ...]]:
    if resolver is None:
        return BURST_FRESHNESS_UNKNOWN, ()
    keys = tuple(
        resolver.burst_keys_for(
            thread_uid=thread_uid,
            changed_message_ids=changed_message_ids,
            content_hashes=content_hashes,
        )
    )
    return BURST_FRESHNESS_INVALIDATED, keys


class FixtureBurstResolver:
    """Deterministic burst-key fixture. Does not scan a mailbox."""

    def burst_keys_for(
        self,
        *,
        thread_uid: str,
        changed_message_ids: Sequence[str],
        content_hashes: Sequence[str] = (),
    ) -> tuple[str, ...]:
        hashes = tuple(content_hashes) or tuple(changed_message_ids)
        return (compatible_burst_key(tuple(changed_message_ids) or (thread_uid,), hashes),)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_pending_scopes(vault: Path) -> list[PendingScope]:
    target = resolve_contained_path(vault, PENDING_SCOPES_REL, purpose="read")
    payload = _read_json(target)
    return [PendingScope.from_payload(item) for item in payload.get("scopes") or []]


def store_pending_scopes(vault: Path, scopes: Sequence[PendingScope]) -> None:
    target = resolve_contained_path(vault, PENDING_SCOPES_REL, purpose="write", create_parents=True)
    _write_json(target, {"scopes": [item.to_payload() for item in scopes]})


def load_checkpoint(vault: Path, connector_id: str) -> ConnectorCheckpoint:
    target = resolve_contained_path(vault, CHECKPOINT_REL, purpose="read")
    payload = _read_json(target)
    row = payload.get(connector_id) or {}
    return ConnectorCheckpoint(
        last_safe_cursor=dict(row.get("last_safe_cursor") or {}),
        last_safe_version=str(row.get("last_safe_version") or CURSOR_VERSION_V1),
        connector_version=str(row.get("connector_version") or "1.0.0"),
    )


def store_checkpoint(vault: Path, connector_id: str, checkpoint: ConnectorCheckpoint) -> None:
    target = resolve_contained_path(vault, CHECKPOINT_REL, purpose="write", create_parents=True)
    payload = _read_json(target)
    payload[connector_id] = {
        "last_safe_cursor": dict(checkpoint.last_safe_cursor),
        "last_safe_version": checkpoint.last_safe_version,
        "connector_version": checkpoint.connector_version,
    }
    _write_json(target, payload)


def persist_pending_scope(vault: Path, scope: PendingScope) -> tuple[PendingScope, ...]:
    scopes = load_pending_scopes(vault)
    identities = {item.event_identity for item in scopes}
    if scope.event_identity not in identities:
        scopes.append(scope)
    store_pending_scopes(vault, scopes)
    return tuple(scopes)


def attach_resolver(
    vault: Path,
    resolver: BurstKeyResolver,
    *,
    capability: str = "p01b1-burst-1",
) -> tuple[PendingScope, ...]:
    """Schedule each pending scope once. Does not walk the mailbox."""

    updated: list[PendingScope] = []
    for scope in load_pending_scopes(vault):
        if scope.scheduled:
            updated.append(scope)
            continue
        resolver.burst_keys_for(
            thread_uid=scope.thread_id,
            changed_message_ids=(scope.event_identity,),
        )
        updated.append(
            PendingScope(
                source=scope.source,
                account_scope=scope.account_scope,
                thread_id=scope.thread_id,
                event_identity=scope.event_identity,
                status="scheduled",
                resolver_capability=capability,
                scheduled=True,
            )
        )
    store_pending_scopes(vault, updated)
    return tuple(updated)


def validate_upgrade(current_version: str, next_version: str, *, compatible: Sequence[str]) -> None:
    if next_version not in compatible and next_version != current_version:
        raise IncompatibleContractError(
            f"incompatible connector_version {next_version!r}; compatible={list(compatible)}"
        )


def rollback_checkpoint(vault: Path, connector_id: str) -> ConnectorCheckpoint:
    """Restore the last safe cursor. Never deletes canonical cards."""

    return load_checkpoint(vault, connector_id)


class FaultAfterPersist:
    def __init__(self, after: int) -> None:
        self.after = after
        self.seen = 0

    def __call__(self) -> None:
        self.seen += 1
        if self.seen >= self.after:
            raise IncompatibleStateError("injected interrupt after persist")


class LifecycleRunner:
    """Replay/upgrade/rollback surface over ``execute_connector``."""

    def __init__(
        self,
        connector_id: str,
        *,
        vault: Path,
        identity: ArchiveIdentity,
        access: AccessContext,
        writer: CanonicalWriter,
        burst_resolver: BurstKeyResolver | None = None,
    ) -> None:
        self.connector_id = connector_id
        self.vault = Path(vault)
        self.identity = identity
        self.access = access
        self.writer = writer
        self.burst_resolver = burst_resolver

    def run(
        self,
        *,
        cursor: Mapping[str, object] | None = None,
        run_id: str = "p08c",
        events: Sequence[ThreadEvent] = (),
        thread_uid: str = "",
        message_uids: Sequence[str] = (),
        unrelated_uids: Sequence[str] = (),
        to_cursor_version: str = CURSOR_VERSION_V2,
        cursor_strategy: str = "migrate",
        compatible_versions: Sequence[str] = ("1.0.0", "1.1.0"),
        next_connector_version: str = "",
        archive_forget: bool = False,
        fault_after: int | None = None,
        context: Mapping[str, object] | None = None,
        execute: Callable[..., ConnectorRunResult] = execute_connector,
    ) -> LifecycleResult:
        checkpoint = load_checkpoint(self.vault, self.connector_id)
        if next_connector_version:
            try:
                validate_upgrade(checkpoint.connector_version, next_connector_version, compatible=compatible_versions)
            except IncompatibleContractError:
                safe = rollback_checkpoint(self.vault, self.connector_id)
                return LifecycleResult(
                    run=None,
                    cursor=CursorState(
                        version=safe.last_safe_version,
                        status=CURSOR_INCOMPATIBLE,
                        payload=dict(safe.last_safe_cursor),
                        reason="incompatible connector version",
                    ),
                    dirty_uids=(),
                    burst_freshness=BURST_FRESHNESS_UNKNOWN,
                    burst_keys=(),
                    pending_scopes=tuple(load_pending_scopes(self.vault)),
                    retention="",
                    rolled_back=True,
                    cards_preserved=True,
                    unrelated_uids=tuple(unrelated_uids),
                )
        migrated = migrate_cursor(
            cursor if cursor is not None else checkpoint.last_safe_cursor,
            to_version=to_cursor_version,
            strategy=cursor_strategy,
        )
        if migrated.status == CURSOR_EXPIRED:
            return LifecycleResult(
                run=None,
                cursor=migrated,
                dirty_uids=(),
                burst_freshness=BURST_FRESHNESS_UNKNOWN,
                burst_keys=(),
                pending_scopes=tuple(load_pending_scopes(self.vault)),
                retention="",
                unrelated_uids=tuple(unrelated_uids),
            )
        latest = select_latest_events(events)
        dirty = (
            dirty_uids_for_thread_event(
                latest[0],
                thread_uid=thread_uid,
                message_uids=message_uids,
            )
            if latest
            else tuple(uid for uid in (thread_uid, *message_uids) if uid)
        )
        freshness, burst_keys = burst_freshness_for(
            resolver=self.burst_resolver,
            thread_uid=thread_uid,
            changed_message_ids=tuple(mid for event in latest for mid in event.changed_message_ids),
        )
        scopes = list(load_pending_scopes(self.vault))
        if latest and self.burst_resolver is None:
            for event in latest:
                scopes = list(
                    persist_pending_scope(
                        self.vault,
                        PendingScope(
                            source="gmail.thread",
                            account_scope=event.account_scope,
                            thread_id=event.parent_thread_id,
                            event_identity=event.event_identity,
                        ),
                    )
                )
        tombstone = any(event.tombstone for event in latest)
        if archive_forget:
            archive_forget_requires_intent(intent=True)
        retention = retention_for(tombstone=tombstone, archive_forget=archive_forget)
        wrapped = self.writer
        if fault_after is not None:
            wrapped = _FaultingWriter(self.writer, FaultAfterPersist(fault_after))
        try:
            run = execute(
                self.connector_id,
                identity=self.identity,
                access=self.access,
                writer=wrapped,
                cursor=dict(migrated.payload),
                run_id=run_id,
                context=context,
            )
        except IncompatibleStateError as exc:
            if "interrupt" not in str(exc):
                raise
            return LifecycleResult(
                run=None,
                cursor=CursorState(
                    version=checkpoint.last_safe_version,
                    status=CURSOR_ACTIVE,
                    payload=dict(checkpoint.last_safe_cursor),
                    reason="interrupted before cursor commit",
                ),
                dirty_uids=dirty,
                burst_freshness=freshness,
                burst_keys=burst_keys,
                pending_scopes=tuple(scopes),
                retention=retention,
                interrupted=True,
                cards_preserved=True,
                unrelated_uids=tuple(unrelated_uids),
            )
        store_checkpoint(
            self.vault,
            self.connector_id,
            ConnectorCheckpoint(
                last_safe_cursor=dict(run.committed_cursor or migrated.payload),
                last_safe_version=migrated.version,
                connector_version=next_connector_version
                or checkpoint.connector_version
                or run.manifest.connector_version,
            ),
        )
        scheduled = tuple(item.event_identity for item in scopes if item.scheduled)
        return LifecycleResult(
            run=run,
            cursor=CursorState(
                version=migrated.version, status=CURSOR_ACTIVE, payload=dict(run.committed_cursor or {})
            ),
            dirty_uids=dirty or tuple(run.uids),
            burst_freshness=freshness,
            burst_keys=burst_keys,
            pending_scopes=tuple(scopes),
            retention=retention,
            scheduled_event_identities=scheduled,
            unrelated_uids=tuple(unrelated_uids),
        )


class _FaultingWriter:
    def __init__(self, inner: CanonicalWriter, fault: FaultAfterPersist) -> None:
        self.inner = inner
        self.fault = fault
        self.write_attempts = 0
        self.uid_to_rel: dict[str, str] = getattr(inner, "uid_to_rel", {})

    def write_canonical(self, proposal, *, identity, access):
        result = self.inner.write_canonical(proposal, identity=identity, access=access)
        self.write_attempts += 1
        self.uid_to_rel = getattr(self.inner, "uid_to_rel", self.uid_to_rel)
        self.fault()
        return result


def affected_context_for(uid: str, revision: str, related: Sequence[str], *, resolved: bool) -> AffectedContext:
    return AffectedContext(
        uid=uid,
        revision=revision,
        related_uids=tuple(related),
        status="resolved" if resolved else "reconciliation_pending",
    )


__all__ = [
    "BURST_FRESHNESS_INVALIDATED",
    "BURST_FRESHNESS_UNKNOWN",
    "CHECKPOINT_REL",
    "CURSOR_EXPIRED",
    "CURSOR_VERSION_V1",
    "CURSOR_VERSION_V2",
    "FIXTURE_CATCH_UP_BUDGET",
    "FixtureBurstResolver",
    "LifecycleResult",
    "LifecycleRunner",
    "PENDING_SCOPES_REL",
    "PendingScope",
    "RETENTION_ARCHIVE_FORGET",
    "RETENTION_PROVIDER_TOMBSTONE",
    "ThreadEvent",
    "apply_provider_tombstone",
    "archive_forget_requires_intent",
    "attach_resolver",
    "compatible_burst_key",
    "dirty_uids_for_thread_event",
    "inspect_cursor",
    "load_pending_scopes",
    "migrate_cursor",
    "select_latest_events",
]
