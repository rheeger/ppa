"""Opaque versioned query cursors (P10-A).

A cursor is bound to archive identity, predicate/order/page semantics,
access-policy fingerprint, and serving/warehouse snapshot. Changing any of
those invalidates the cursor instead of silently skipping or repeating rows.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any, Mapping

from archive_engine.errors import CursorInvalidError

CURSOR_VERSION = 1
CURSOR_KIND = "p10a.keyset.v1"


def _canonical(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def cursor_signing_key(*, archive_id: str, snapshot: str, policy_fingerprint: str) -> bytes:
    material = "\0".join((CURSOR_KIND, archive_id, snapshot, policy_fingerprint))
    return hashlib.sha256(material.encode("utf-8")).digest()


def _sign(body: Mapping[str, Any], key: bytes) -> str:
    return hmac.new(key, _canonical(body), hashlib.sha256).hexdigest()


@dataclass(frozen=True)
class QueryCursor:
    """Keyset cursor. ``order_value`` may be empty when the last row's order field was null."""

    version: int
    archive_id: str
    snapshot: str
    policy_fingerprint: str
    predicate_fingerprint: str
    order_field: str
    order_direction: str
    last_order_value: str
    last_uid: str
    last_order_null: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "kind": CURSOR_KIND,
            "archive_id": self.archive_id,
            "snapshot": self.snapshot,
            "policy_fingerprint": self.policy_fingerprint,
            "predicate_fingerprint": self.predicate_fingerprint,
            "order_field": self.order_field,
            "order_direction": self.order_direction,
            "last_order_value": self.last_order_value,
            "last_uid": self.last_uid,
            "last_order_null": self.last_order_null,
        }

    def encode(self) -> str:
        body = self.to_payload()
        key = cursor_signing_key(
            archive_id=self.archive_id,
            snapshot=self.snapshot,
            policy_fingerprint=self.policy_fingerprint,
        )
        envelope = {"body": body, "mac": _sign(body, key)}
        raw = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(token: str) -> QueryCursor:
    """Decode and integrity-check a cursor. Does not bind to the current request."""

    if not token or not str(token).strip():
        raise CursorInvalidError("cursor is empty")
    try:
        raw = base64.urlsafe_b64decode(str(token).encode("ascii"))
        envelope = json.loads(raw.decode("utf-8"))
    except (ValueError, json.JSONDecodeError, UnicodeError) as exc:
        raise CursorInvalidError("cursor is malformed") from exc
    if not isinstance(envelope, dict):
        raise CursorInvalidError("cursor is malformed")
    body = envelope.get("body")
    mac = envelope.get("mac")
    if not isinstance(body, dict) or not isinstance(mac, str) or not mac:
        raise CursorInvalidError("cursor is malformed")
    version = body.get("version")
    if version != CURSOR_VERSION:
        raise CursorInvalidError(f"unsupported cursor version: {version}")
    if body.get("kind") != CURSOR_KIND:
        raise CursorInvalidError("unsupported cursor kind")
    archive_id = str(body.get("archive_id") or "")
    snapshot = str(body.get("snapshot") or "")
    policy = str(body.get("policy_fingerprint") or "")
    if not archive_id or not snapshot or not policy:
        raise CursorInvalidError("cursor is missing binding fields")
    key = cursor_signing_key(archive_id=archive_id, snapshot=snapshot, policy_fingerprint=policy)
    expected = _sign(body, key)
    if not hmac.compare_digest(expected, mac):
        raise CursorInvalidError("cursor integrity check failed")
    last_uid = str(body.get("last_uid") or "")
    if not last_uid:
        raise CursorInvalidError("cursor is missing last_uid")
    direction = str(body.get("order_direction") or "asc")
    if direction not in {"asc", "desc"}:
        raise CursorInvalidError("cursor order_direction is invalid")
    return QueryCursor(
        version=CURSOR_VERSION,
        archive_id=archive_id,
        snapshot=snapshot,
        policy_fingerprint=policy,
        predicate_fingerprint=str(body.get("predicate_fingerprint") or ""),
        order_field=str(body.get("order_field") or "uid"),
        order_direction=direction,
        last_order_value=str(body.get("last_order_value") or ""),
        last_uid=last_uid,
        last_order_null=bool(body.get("last_order_null") or False),
    )


def bind_cursor(
    cursor: QueryCursor,
    *,
    archive_id: str,
    snapshot: str,
    policy_fingerprint: str,
    predicate_fingerprint: str,
    order_field: str,
    order_direction: str,
) -> QueryCursor:
    """Reject a cursor that does not match the current request binding."""

    if cursor.archive_id != archive_id:
        raise CursorInvalidError("cursor archive_id does not match this archive")
    if cursor.snapshot != snapshot:
        raise CursorInvalidError("cursor snapshot is stale; restart the query")
    if cursor.policy_fingerprint != policy_fingerprint:
        raise CursorInvalidError("cursor policy changed; restart the query")
    if cursor.predicate_fingerprint != predicate_fingerprint:
        raise CursorInvalidError("cursor predicate does not match this request")
    if cursor.order_field != order_field or cursor.order_direction != order_direction:
        raise CursorInvalidError("cursor order does not match this request")
    return cursor
