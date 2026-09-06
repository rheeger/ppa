"""Fixture-backed sample connector.

No live provider calls. Two accounts share one provider object ID so the
identity recipe can prove they stay distinct.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from archive_engine.contracts import AccessContext, ArchiveIdentity
from archive_sync.connectors.contracts import (
    IDENTITY_RECIPE,
    SDK_VERSION,
    CanonicalProposal,
    ConnectorManifest,
    FetchedBatch,
    FetchedRecord,
    SourceObjectIdentity,
    card_prefix_for,
    require_access,
)
from archive_sync.connectors.registry import register_connector

SAMPLE_CONNECTOR_ID = "sample.fixture"
SAMPLE_SOURCE = "sample"
SAMPLE_FIXTURE_ID = "sample-page-1"
SAMPLE_FIXTURE_VERSION = "1"
SAMPLE_PROVIDER_OBJECT_ID = "sample-msg-001"
SAMPLE_ACCOUNT_ALPHA = "acct-alpha@example.test"
SAMPLE_ACCOUNT_BETA = "acct-beta@example.test"
SAMPLE_SUBJECT = "Sample connector evidence"
SAMPLE_BODY = (
    "A fixture-backed source object becomes a typed email_message card. "
    "Account scope is part of identity, so the same provider ID in another "
    "account is a different card."
)

SAMPLE_OWNED_FIELDS = (
    "summary",
    "tags",
    "gmail_message_id",
    "gmail_thread_id",
    "account_email",
    "thread",
    "direction",
    "from_name",
    "from_email",
    "to_emails",
    "sent_at",
    "subject",
    "snippet",
    "message_body_sha",
)

_FIXTURE_RECORDS: tuple[dict[str, str | tuple[str, ...]], ...] = (
    {
        "provider_object_id": SAMPLE_PROVIDER_OBJECT_ID,
        "account_scope": SAMPLE_ACCOUNT_ALPHA,
        "thread_id": "sample-thr-001",
        "subject": SAMPLE_SUBJECT,
        "snippet": "Fixture source object for the sample connector SDK.",
        "body": SAMPLE_BODY,
        "from_email": "sender@example.test",
        "from_name": "Sample Sender",
        "to_emails": (SAMPLE_ACCOUNT_ALPHA,),
        "sent_at": "2026-09-06T15:04:00Z",
        "created": "2026-09-06",
        "direction": "inbound",
    },
    {
        "provider_object_id": SAMPLE_PROVIDER_OBJECT_ID,
        "account_scope": SAMPLE_ACCOUNT_BETA,
        "thread_id": "sample-thr-001",
        "subject": SAMPLE_SUBJECT,
        "snippet": "Same provider object ID, different account.",
        "body": SAMPLE_BODY,
        "from_email": "sender@example.test",
        "from_name": "Sample Sender",
        "to_emails": (SAMPLE_ACCOUNT_BETA,),
        "sent_at": "2026-09-06T15:05:00Z",
        "created": "2026-09-06",
        "direction": "inbound",
    },
)


def sample_manifest() -> ConnectorManifest:
    return ConnectorManifest(
        connector_id=SAMPLE_CONNECTOR_ID,
        connector_version="1.0.0",
        sdk_version=SDK_VERSION,
        min_engine_version=1,
        max_engine_version=1,
        min_card_contract_version=1,
        max_card_contract_version=1,
        supported_sources=(SAMPLE_SOURCE,),
        supported_account_scopes=(SAMPLE_ACCOUNT_ALPHA, SAMPLE_ACCOUNT_BETA),
        emitted_card_types=("email_message",),
        deterministic_fields_owned=SAMPLE_OWNED_FIELDS,
        identity_recipe=IDENTITY_RECIPE,
        cursor_schema="page_token+page_index",
        cursor_version="1",
        delete_policy="provider_tombstone",
        retention_policy="retain_until_archive_forget",
        freshness_capability="import-only",
        freshness_interval="fixture",
        rate_limit="fixture",
        batch_limit=16,
        auth_capabilities=("fixture",),
        egress_capabilities=(),
        fixture_id=SAMPLE_FIXTURE_ID,
        fixture_version=SAMPLE_FIXTURE_VERSION,
        event_handler="",
    )


def sample_rel_path(uid: str, created: str, sent_at: str = "") -> str:
    month_source = sent_at or created
    year_month = month_source[:7] if len(month_source) >= 7 else created[:7]
    return f"Email/{year_month}/{uid}.md"


class SampleConnector:
    """In-process fixture page. Replay of the empty cursor returns the same records."""

    def manifest(self) -> ConnectorManifest:
        return sample_manifest()

    def fetch(
        self,
        *,
        cursor: Mapping[str, object],
        identity: ArchiveIdentity,
        access: AccessContext,
    ) -> FetchedBatch:
        require_access(identity=identity, access=access)
        cursor_before = {
            "page_token": str(cursor.get("page_token") or ""),
            "page_index": int(cursor.get("page_index") or 0),
        }
        if cursor_before["page_token"] == "done":
            return FetchedBatch(
                batch_id=SAMPLE_FIXTURE_ID,
                source=SAMPLE_SOURCE,
                account_scope="sample-fixture",
                cursor_before=cursor_before,
                cursor_after_candidate={"page_token": "done", "page_index": 1},
                records=(),
            )
        records = tuple(
            FetchedRecord(
                record_ref=f"{row['account_scope']}:{row['provider_object_id']}",
                event_identity=f"{SAMPLE_FIXTURE_ID}:{row['account_scope']}:{row['provider_object_id']}",
                payload=dict(row),
            )
            for row in _FIXTURE_RECORDS
        )
        return FetchedBatch(
            batch_id=SAMPLE_FIXTURE_ID,
            source=SAMPLE_SOURCE,
            account_scope="sample-fixture",
            cursor_before=cursor_before,
            cursor_after_candidate={"page_token": "done", "page_index": 1},
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
        for record in batch.records:
            payload = record.payload
            account_scope = str(payload.get("account_scope") or "")
            provider_object_id = str(payload.get("provider_object_id") or "")
            created = str(payload.get("created") or "2026-09-06")
            sent_at = str(payload.get("sent_at") or "")
            subject = str(payload.get("subject") or SAMPLE_SUBJECT)
            snippet = str(payload.get("snippet") or "")
            body = str(payload.get("body") or SAMPLE_BODY)
            thread_id = str(payload.get("thread_id") or "")
            to_emails = tuple(str(item) for item in _as_seq(payload.get("to_emails")))
            source_identity = SourceObjectIdentity.from_parts(
                archive=identity,
                source=SAMPLE_SOURCE,
                account_scope=account_scope,
                provider_object_id=provider_object_id,
            )
            uid = source_identity.derive_uid(card_prefix_for("email_message"))
            body_sha = hashlib.sha256(body.encode("utf-8")).hexdigest()
            card: dict[str, object] = {
                "uid": uid,
                "type": "email_message",
                "source": [SAMPLE_SOURCE],
                "source_id": source_identity.source_id,
                "created": created,
                "updated": created,
                "summary": subject,
                "tags": ["p08-sample", "synthetic"],
                "gmail_message_id": provider_object_id,
                "gmail_thread_id": thread_id,
                "account_email": account_scope,
                "thread": "",
                "direction": str(payload.get("direction") or ""),
                "from_name": str(payload.get("from_name") or ""),
                "from_email": str(payload.get("from_email") or ""),
                "to_emails": list(to_emails),
                "sent_at": sent_at,
                "subject": subject,
                "snippet": snippet,
                "message_body_sha": body_sha,
            }
            provenance = {
                field: {"source": SAMPLE_SOURCE, "date": created, "method": "deterministic"}
                for field in SAMPLE_OWNED_FIELDS
                if card.get(field) not in ("", [], None, 0)
            }
            proposals.append(
                CanonicalProposal(
                    identity=source_identity,
                    card_type="email_message",
                    rel_path=sample_rel_path(uid, created, sent_at),
                    card=card,
                    body=body,
                    provenance=provenance,
                    supporting_source_ids=(source_identity.source_id,),
                    provider_revision=body_sha,
                )
            )
        return tuple(proposals)


def _as_seq(value: object) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)):
        return (value,)
    return tuple(value)


def _sample_factory() -> SampleConnector:
    return SampleConnector()


register_connector(SAMPLE_CONNECTOR_ID, _sample_factory)
