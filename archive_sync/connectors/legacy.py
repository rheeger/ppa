"""Bridge existing Gmail/calendar adapters onto the connector SDK.

Parsing stays in the adapters. This module supplies manifests, fetch/normalize
wrappers, and a correction-aware writer. No warehouse SQL. No live Google.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from archive_engine.contracts import AccessContext, ArchiveIdentity
from archive_sync.adapter_contracts import AdapterSpec
from archive_sync.adapters.base import BaseAdapter
from archive_sync.connectors.contracts import (
    LEGACY_IDENTITY_RECIPE,
    SDK_VERSION,
    CanonicalProposal,
    ConnectorManifest,
    FetchedBatch,
    FetchedRecord,
    PersistResult,
    SourceObjectIdentity,
    require_access,
)
from archive_sync.connectors.registry import get_connector_factory, register_connector
from archive_sync.connectors.runtime import execute_connector
from archive_sync.source_updaters.constants import (
    CURSOR_ETAG,
    CURSOR_HISTORY_ID,
    CURSOR_PAGE_TOKEN,
    CURSOR_SYNC_TOKEN,
    DEFAULT_ACTIVE_ALL,
    DEFAULT_ACTIVE_PROMOTION_GATED,
    GMAIL_POLICY_VERSION,
    SOURCE_TYPE_CALENDAR,
    SOURCE_TYPE_GMAIL,
)
from archive_sync.source_updaters.declarations import SourceUpdaterDeclaration
from archive_vault.paths import resolve_contained_path
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import validate_card_strict

logger = logging.getLogger("ppa.connectors")

GMAIL_CONNECTOR_ID = "gmail-messages"
CALENDAR_CONNECTOR_ID = "calendar-events"
SDK_SOURCES = frozenset({GMAIL_CONNECTOR_ID, CALENDAR_CONNECTOR_ID})


def _gmail_manifest() -> ConnectorManifest:
    return ConnectorManifest(
        connector_id=GMAIL_CONNECTOR_ID,
        connector_version="1.0.0",
        sdk_version=SDK_VERSION,
        min_engine_version=1,
        max_engine_version=1,
        min_card_contract_version=1,
        max_card_contract_version=1,
        supported_sources=("gmail-messages", "gmail.thread", "gmail.message", "gmail.attachment"),
        supported_account_scopes=("*",),
        emitted_card_types=("email_thread", "email_message", "email_attachment"),
        deterministic_fields_owned=(
            "subject",
            "snippet",
            "sent_at",
            "participants",
            "participant_emails",
            "attachments",
            "calendar_events",
            "gmail_thread_id",
            "gmail_message_id",
            "attachment_id",
            "account_email",
        ),
        identity_recipe=LEGACY_IDENTITY_RECIPE,
        cursor_schema="history_id+page_token",
        cursor_version="1",
        delete_policy="provider_tombstone",
        retention_policy="retain_until_archive_forget",
        freshness_capability="polling",
        freshness_interval="history",
        rate_limit="provider",
        batch_limit=100000,
        auth_capabilities=("oauth",),
        egress_capabilities=("gmail.readonly",),
        fixture_id="gmail-legacy",
        fixture_version="1",
    )


def _calendar_manifest() -> ConnectorManifest:
    return ConnectorManifest(
        connector_id=CALENDAR_CONNECTOR_ID,
        connector_version="1.0.0",
        sdk_version=SDK_VERSION,
        min_engine_version=1,
        max_engine_version=1,
        min_card_contract_version=1,
        max_card_contract_version=1,
        supported_sources=("calendar-events", "calendar.event"),
        supported_account_scopes=("*",),
        emitted_card_types=("calendar_event",),
        deterministic_fields_owned=(
            "summary",
            "title",
            "start_at",
            "end_at",
            "timezone",
            "status",
            "organizer_email",
            "attendee_emails",
            "calendar_id",
            "event_id",
            "account_email",
        ),
        identity_recipe=LEGACY_IDENTITY_RECIPE,
        cursor_schema="sync_token+page_token+etag",
        cursor_version="1",
        delete_policy="provider_tombstone",
        retention_policy="retain_until_archive_forget",
        freshness_capability="polling",
        freshness_interval="sync_token",
        rate_limit="provider",
        batch_limit=100000,
        auth_capabilities=("oauth",),
        egress_capabilities=("calendar.readonly",),
        fixture_id="calendar-legacy",
        fixture_version="1",
    )


def manifest_for_source(source_id: str) -> ConnectorManifest:
    if source_id == GMAIL_CONNECTOR_ID:
        return _gmail_manifest()
    if source_id == CALENDAR_CONNECTOR_ID:
        return _calendar_manifest()
    raise KeyError(f"no legacy manifest for {source_id}")


def adapter_spec_for(source_id: str) -> AdapterSpec:
    manifest = manifest_for_source(source_id)
    if source_id == GMAIL_CONNECTOR_ID:
        return AdapterSpec(
            adapter_name="GmailMessagesAdapter",
            source_id=source_id,
            emitted_card_types=manifest.emitted_card_types,
            deterministic_fields_owned=(
                "subject",
                "snippet",
                "sent_at",
                "participants",
                "participant_emails",
                "attachments",
                "calendar_events",
            ),
            identity_keys=("source_id", "gmail_thread_id", "gmail_message_id", "attachment_id"),
            external_id_fields=("source_id", "gmail_thread_id", "gmail_message_id", "message_id_header", "attachment_id"),
            relationship_fields=("thread", "messages", "attachments", "calendar_events", "people", "orgs"),
            supports_incremental_cursor=True,
        )
    return AdapterSpec(
        adapter_name="CalendarEventsAdapter",
        source_id=source_id,
        emitted_card_types=manifest.emitted_card_types,
        deterministic_fields_owned=(
            "summary",
            "title",
            "start_at",
            "end_at",
            "timezone",
            "status",
            "organizer_email",
            "attendee_emails",
        ),
        identity_keys=("source_id", "calendar_id", "event_id", "ical_uid"),
        external_id_fields=("source_id", "calendar_id", "event_id", "event_etag", "ical_uid"),
        relationship_fields=("people", "orgs", "source_messages", "source_threads", "meeting_transcripts"),
        supports_incremental_cursor=True,
    )


def declaration_for_gmail(account: str = "<account>") -> SourceUpdaterDeclaration:
    return SourceUpdaterDeclaration(
        source_key=f"gmail-messages:{account}",
        source_type=SOURCE_TYPE_GMAIL,
        adapter_name="GmailMessagesAdapter",
        adapter_source_id=GMAIL_CONNECTOR_ID,
        promotion_policy_version=GMAIL_POLICY_VERSION,
        cursor_kind=CURSOR_HISTORY_ID,
        cursor_kinds=(CURSOR_HISTORY_ID, CURSOR_PAGE_TOKEN),
        supports_incremental=True,
        supports_deletes=True,
        default_active_policy=DEFAULT_ACTIVE_PROMOTION_GATED,
    )


def declaration_for_calendar(account: str = "<account>") -> SourceUpdaterDeclaration:
    return SourceUpdaterDeclaration(
        source_key=f"calendar-events:{account}",
        source_type=SOURCE_TYPE_CALENDAR,
        adapter_name="CalendarEventsAdapter",
        adapter_source_id=CALENDAR_CONNECTOR_ID,
        cursor_kind=CURSOR_SYNC_TOKEN,
        cursor_kinds=(CURSOR_SYNC_TOKEN, CURSOR_PAGE_TOKEN, CURSOR_ETAG),
        supports_incremental=True,
        supports_deletes=True,
        default_active_policy=DEFAULT_ACTIVE_ALL,
    )


def _build_gmail() -> BaseAdapter:
    from archive_sync.adapters.gmail_messages import GmailMessagesAdapter

    return GmailMessagesAdapter()


def _build_calendar() -> BaseAdapter:
    from archive_sync.adapters.calendar_events import CalendarEventsAdapter

    return CalendarEventsAdapter()


def adapter_for_source(source_id: str) -> BaseAdapter:
    if source_id == GMAIL_CONNECTOR_ID:
        return _build_gmail()
    if source_id == CALENDAR_CONNECTOR_ID:
        return _build_calendar()
    raise KeyError(f"no SDK adapter for {source_id}")


class LegacyAdapterConnector:
    """SDK surface over an existing adapter. ``to_card`` / ``fetch`` stay authoritative."""

    def __init__(self, source_id: str, adapter: BaseAdapter | None = None) -> None:
        self.source_id = source_id
        self.adapter = adapter or adapter_for_source(source_id)
        self.vault_path = ""
        self.kwargs: dict[str, Any] = {}

    def bind_context(self, context: Mapping[str, object]) -> None:
        self.vault_path = str(context.get("vault_path") or "")
        self.kwargs = {key: value for key, value in context.items() if key != "vault_path"}

    def manifest(self) -> ConnectorManifest:
        return manifest_for_source(self.source_id)

    def fetch(
        self,
        *,
        cursor: Mapping[str, object],
        identity: ArchiveIdentity,
        access: AccessContext,
    ) -> FetchedBatch:
        require_access(identity=identity, access=access)
        cursor_before = dict(cursor)
        batches = list(self.adapter.fetch_batches(self.vault_path, dict(cursor_before), **self.kwargs))
        items: list[dict[str, Any]] = []
        cursor_after = dict(cursor_before)
        for batch in batches:
            items.extend(batch.items)
            if batch.cursor_patch and batch.commit_cursor:
                cursor_after.update(batch.cursor_patch)
        records = tuple(
            FetchedRecord(
                record_ref=str(
                    item.get("source_id")
                    or item.get("message_id")
                    or item.get("thread_id")
                    or item.get("event_id")
                    or index
                ),
                event_identity=str(item.get("kind") or self.source_id) + f":{index}",
                payload=item,
            )
            for index, item in enumerate(items)
        )
        return FetchedBatch(
            batch_id=f"{self.source_id}:{len(records)}",
            source=self.source_id,
            account_scope=str(self.kwargs.get("account_email") or "sample-fixture"),
            cursor_before=cursor_before,
            cursor_after_candidate=cursor_after,
            records=records,
        )

    def normalize(
        self,
        batch: FetchedBatch,
        *,
        identity: ArchiveIdentity,
        access: AccessContext,
    ) -> tuple[CanonicalProposal, ...]:
        require_access(identity=identity, access=access)
        proposals: list[CanonicalProposal] = []
        vault = Path(self.vault_path)
        for record in batch.records:
            card, provenance, body = self.adapter.to_card(dict(record.payload))
            source_id = str(card.source_id)
            if ":" not in source_id:
                raise ValueError(f"legacy card source_id must be account-scoped, got {source_id!r}")
            account_scope, provider_object_id = source_id.split(":", 1)
            uid_source = str((card.source or [self.source_id])[0])
            source_identity = SourceObjectIdentity.from_parts(
                archive=identity,
                source=uid_source,
                account_scope=account_scope,
                provider_object_id=provider_object_id,
            )
            rel_path = self.adapter._card_rel_path(vault, card)
            proposals.append(
                CanonicalProposal(
                    identity=source_identity,
                    card_type=str(card.type),
                    rel_path=str(rel_path),
                    card=card.model_dump(mode="python"),
                    body=body,
                    provenance={
                        field: {"source": entry.source, "date": entry.date, "method": entry.method}
                        for field, entry in provenance.items()
                    },
                    supporting_source_ids=(source_id,),
                    provider_revision=str(getattr(card, "message_body_sha", "") or getattr(card, "event_body_sha", "") or ""),
                )
            )
        return tuple(proposals)


class CorrectionAwareAdapterWriter:
    """Persist through adapter merge/write so P07 overrides survive replay."""

    def __init__(self, adapter: BaseAdapter, vault: Path, *, run_id: str = "") -> None:
        self.adapter = adapter
        self.vault = Path(vault)
        self.run_id = run_id
        self.write_attempts = 0
        self.uid_to_rel: dict[str, str] = {}

    def write_canonical(
        self,
        proposal: CanonicalProposal,
        *,
        identity: ArchiveIdentity,
        access: AccessContext,
    ) -> PersistResult:
        require_access(identity=identity, access=access)
        self.write_attempts += 1
        uid = str(proposal.card.get("uid") or "")
        target = resolve_contained_path(self.vault, proposal.rel_path, purpose="write", create_parents=True)
        existed = target.is_file()
        before = hashlib.sha256(target.read_bytes()).hexdigest() if existed else ""
        card = validate_card_strict(dict(proposal.card))
        provenance = {
            field: ProvenanceEntry(
                source=str(value.get("source") or self.adapter.source_id),
                date=str(value.get("date") or ""),
                method=str(value.get("method") or "deterministic"),
            )
            if isinstance(value, Mapping)
            else value
            for field, value in proposal.provenance.items()
        }
        if existed:
            self.adapter.merge_card(self.vault, Path(proposal.rel_path), card, proposal.body, provenance)
        else:
            self.adapter._write_canonical_card(
                self.vault,
                proposal.rel_path,
                card,
                proposal.body,
                provenance,
                run_id=self.run_id,
            )
        written = resolve_contained_path(self.vault, proposal.rel_path, purpose="read")
        revision = hashlib.sha256(written.read_bytes()).hexdigest()
        self.uid_to_rel[uid] = proposal.rel_path
        return PersistResult(
            uid=uid,
            rel_path=proposal.rel_path,
            revision=revision,
            created=not existed,
            duplicate=existed and before == revision,
        )


def run_legacy_connector(
    source_id: str,
    *,
    identity: ArchiveIdentity,
    access: AccessContext,
    vault: Path,
    cursor: Mapping[str, object] | None = None,
    adapter: BaseAdapter | None = None,
    run_id: str = "p08b",
    **fetch_kwargs: Any,
):
    connector = LegacyAdapterConnector(source_id, adapter=adapter)
    connector.bind_context({"vault_path": str(vault), **fetch_kwargs})
    writer = CorrectionAwareAdapterWriter(connector.adapter, Path(vault), run_id=run_id)

    def _factory() -> LegacyAdapterConnector:
        return connector

    from archive_sync.connectors import registry as registry_mod

    previous = registry_mod._FACTORIES.get(source_id)
    registry_mod._FACTORIES[source_id] = _factory
    try:
        result = execute_connector(
            source_id,
            identity=identity,
            access=access,
            writer=writer,
            cursor=cursor,
            run_id=run_id,
            context={"vault_path": str(vault), **fetch_kwargs},
            pending_p02_wiring="wired",
            pending_p03_wiring="wired",
        )
    finally:
        if previous is not None:
            registry_mod._FACTORIES[source_id] = previous
        elif source_id in registry_mod._FACTORIES and registry_mod._FACTORIES[source_id] is _factory:
            registry_mod._FACTORIES[source_id] = lambda sid=source_id: LegacyAdapterConnector(sid)
    return result


def _gmail_factory() -> LegacyAdapterConnector:
    return LegacyAdapterConnector(GMAIL_CONNECTOR_ID)


def _calendar_factory() -> LegacyAdapterConnector:
    return LegacyAdapterConnector(CALENDAR_CONNECTOR_ID)


register_connector(GMAIL_CONNECTOR_ID, _gmail_factory)
register_connector(CALENDAR_CONNECTOR_ID, _calendar_factory)


def known_legacy_connectors() -> tuple[str, ...]:
    return tuple(sorted(SDK_SOURCES))


__all__ = [
    "CALENDAR_CONNECTOR_ID",
    "GMAIL_CONNECTOR_ID",
    "SDK_SOURCES",
    "CorrectionAwareAdapterWriter",
    "LegacyAdapterConnector",
    "adapter_for_source",
    "adapter_spec_for",
    "declaration_for_calendar",
    "declaration_for_gmail",
    "get_connector_factory",
    "manifest_for_source",
    "run_legacy_connector",
]
