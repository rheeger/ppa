"""Contributor template connector. Fixture-backed. No live provider."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from archive_engine.contracts import AccessContext, ArchiveIdentity
from archive_sync.connectors.contracts import (
    CanonicalProposal,
    FetchedBatch,
    FetchedRecord,
    SourceObjectIdentity,
    card_prefix_for,
    parse_manifest,
    require_access,
)
from archive_sync.connectors.registry import known_connectors, register_connector

PACKAGE = Path(__file__).resolve().parent
SOURCE = "example"


def _load_json(name: str) -> dict[str, Any]:
    return json.loads((PACKAGE / name).read_text(encoding="utf-8"))


def manifest():
    return parse_manifest(_load_json("manifest.json"))


def _records() -> tuple[dict[str, Any], ...]:
    payload = _load_json("fixtures.json")
    return tuple(item for item in payload.get("records") or [] if isinstance(item, dict))


def _rel_path(uid: str, created: str, sent_at: str = "") -> str:
    month_source = sent_at or created
    year_month = month_source[:7] if len(month_source) >= 7 else created[:7]
    return f"Email/{year_month}/{uid}.md"


class ExampleContributorConnector:
    def manifest(self):
        return manifest()

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
        resolved = manifest()
        if cursor_before["page_token"] == "done":
            return FetchedBatch(
                batch_id=resolved.fixture_id,
                source=SOURCE,
                account_scope="contributor-fixture",
                cursor_before=cursor_before,
                cursor_after_candidate={"page_token": "done", "page_index": 1},
                records=(),
            )
        records = tuple(
            FetchedRecord(
                record_ref=f"{row['account_scope']}:{row['provider_object_id']}",
                event_identity=f"{resolved.fixture_id}:{row['account_scope']}:{row['provider_object_id']}",
                payload=dict(row),
            )
            for row in _records()
        )
        return FetchedBatch(
            batch_id=resolved.fixture_id,
            source=SOURCE,
            account_scope="contributor-fixture",
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
        resolved = manifest()
        proposals: list[CanonicalProposal] = []
        for record in batch.records:
            payload = record.payload
            account_scope = str(payload.get("account_scope") or "")
            provider_object_id = str(payload.get("provider_object_id") or "")
            created = str(payload.get("created") or "2026-09-06")
            sent_at = str(payload.get("sent_at") or "")
            subject = str(payload.get("subject") or "")
            body = str(payload.get("body") or "")
            source_identity = SourceObjectIdentity.from_parts(
                archive=identity,
                source=SOURCE,
                account_scope=account_scope,
                provider_object_id=provider_object_id,
            )
            uid = source_identity.derive_uid(card_prefix_for("email_message"))
            body_sha = hashlib.sha256(body.encode("utf-8")).hexdigest()
            card: dict[str, object] = {
                "uid": uid,
                "type": "email_message",
                "source": [SOURCE],
                "source_id": source_identity.source_id,
                "created": created,
                "updated": created,
                "summary": subject,
                "tags": ["p08-contributor", "synthetic"],
                "gmail_message_id": provider_object_id,
                "gmail_thread_id": str(payload.get("thread_id") or ""),
                "account_email": account_scope,
                "thread": "",
                "direction": str(payload.get("direction") or ""),
                "from_name": str(payload.get("from_name") or ""),
                "from_email": str(payload.get("from_email") or ""),
                "to_emails": list(payload.get("to_emails") or []),
                "sent_at": sent_at,
                "subject": subject,
                "snippet": str(payload.get("snippet") or ""),
                "message_body_sha": body_sha,
            }
            provenance = {
                field: {"source": SOURCE, "date": created, "method": "deterministic"}
                for field in resolved.deterministic_fields_owned
                if card.get(field) not in ("", [], None, 0)
            }
            proposals.append(
                CanonicalProposal(
                    identity=source_identity,
                    card_type="email_message",
                    rel_path=_rel_path(uid, created, sent_at),
                    card=card,
                    body=body,
                    provenance=provenance,
                    supporting_source_ids=(source_identity.source_id,),
                    provider_revision=body_sha,
                )
            )
        return tuple(proposals)


def factory() -> ExampleContributorConnector:
    return ExampleContributorConnector()


CONNECTOR_ID = "example.contributor"
if CONNECTOR_ID not in known_connectors():
    register_connector(CONNECTOR_ID, factory)
