"""Source updater declaration registry (read without running sync)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from .constants import (
    ADAPTER_VERSION_DEFAULT,
    CURSOR_ETAG,
    CURSOR_HASH,
    CURSOR_HISTORY_ID,
    CURSOR_MODIFIED_AT,
    CURSOR_PAGE_TOKEN,
    CURSOR_ROWID,
    CURSOR_SYNC_TOKEN,
    DEFAULT_ACTIVE_ALL,
    DEFAULT_ACTIVE_METADATA_GATED,
    DEFAULT_ACTIVE_POLICIES,
    DEFAULT_ACTIVE_PROMOTION_GATED,
    GMAIL_POLICY_VERSION,
    SOURCE_TYPE_CALENDAR,
    SOURCE_TYPE_GMAIL,
    SOURCE_TYPE_HEALTH,
    SOURCE_TYPE_IMESSAGE,
    SOURCE_TYPE_PHOTOS,
)


@dataclass(frozen=True)
class SourceUpdaterDeclaration:
    source_key: str
    source_type: str
    adapter_name: str
    adapter_source_id: str
    adapter_version: str = ADAPTER_VERSION_DEFAULT
    promotion_policy_version: str = ""
    cursor_kind: str = ""
    cursor_kinds: tuple[str, ...] = ()
    supports_incremental: bool = True
    supports_deletes: bool = False
    supports_webhook: bool = False
    requires_polling: bool = True
    default_active_policy: str = DEFAULT_ACTIVE_ALL
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        kinds = self.cursor_kinds or ((self.cursor_kind,) if self.cursor_kind else ())
        return {
            "source_key": self.source_key,
            "source_type": self.source_type,
            "adapter_name": self.adapter_name,
            "adapter_source_id": self.adapter_source_id,
            "adapter_version": self.adapter_version,
            "promotion_policy_version": self.promotion_policy_version,
            "cursor_kind": self.cursor_kind,
            "cursor_kinds": list(kinds),
            "supports_incremental": self.supports_incremental,
            "supports_deletes": self.supports_deletes,
            "supports_webhook": self.supports_webhook,
            "requires_polling": self.requires_polling,
            "default_active_policy": self.default_active_policy,
            "enabled": self.enabled,
        }


def _gmail_template(account: str = "<account>") -> SourceUpdaterDeclaration:
    return SourceUpdaterDeclaration(
        source_key=f"gmail-messages:{account}",
        source_type=SOURCE_TYPE_GMAIL,
        adapter_name="GmailMessagesAdapter",
        adapter_source_id="gmail-messages",
        promotion_policy_version=GMAIL_POLICY_VERSION,
        cursor_kind=CURSOR_HISTORY_ID,
        cursor_kinds=(CURSOR_HISTORY_ID, CURSOR_PAGE_TOKEN),
        supports_incremental=True,
        supports_deletes=True,
        default_active_policy=DEFAULT_ACTIVE_PROMOTION_GATED,
    )


def _calendar_template(account: str = "<account>") -> SourceUpdaterDeclaration:
    return SourceUpdaterDeclaration(
        source_key=f"calendar-events:{account}",
        source_type=SOURCE_TYPE_CALENDAR,
        adapter_name="CalendarEventsAdapter",
        adapter_source_id="calendar-events",
        cursor_kind=CURSOR_SYNC_TOKEN,
        cursor_kinds=(CURSOR_SYNC_TOKEN, CURSOR_PAGE_TOKEN, CURSOR_ETAG),
        supports_incremental=True,
        supports_deletes=True,
        default_active_policy=DEFAULT_ACTIVE_ALL,
    )


def _imessage_template(label: str = "local") -> SourceUpdaterDeclaration:
    return SourceUpdaterDeclaration(
        source_key=f"imessage:{label}",
        source_type=SOURCE_TYPE_IMESSAGE,
        adapter_name="IMessageAdapter",
        adapter_source_id="imessage",
        cursor_kind=CURSOR_ROWID,
        cursor_kinds=(CURSOR_ROWID,),
        supports_incremental=True,
        supports_deletes=False,
        default_active_policy=DEFAULT_ACTIVE_ALL,
    )


def _photos_template(label: str = "local") -> SourceUpdaterDeclaration:
    return SourceUpdaterDeclaration(
        source_key=f"photos:{label}",
        source_type=SOURCE_TYPE_PHOTOS,
        adapter_name="PhotosAdapter",
        adapter_source_id="photos",
        cursor_kind=CURSOR_MODIFIED_AT,
        cursor_kinds=(CURSOR_MODIFIED_AT, CURSOR_HASH),
        supports_incremental=True,
        supports_deletes=True,
        default_active_policy=DEFAULT_ACTIVE_METADATA_GATED,
    )


def _health_template(label: str = "apple-health") -> SourceUpdaterDeclaration:
    return SourceUpdaterDeclaration(
        source_key=f"health:{label}",
        source_type=SOURCE_TYPE_HEALTH,
        adapter_name="AppleHealthAdapter",
        adapter_source_id="apple-health",
        cursor_kind=CURSOR_HASH,
        cursor_kinds=(CURSOR_HASH,),
        supports_incremental=True,
        supports_deletes=False,
        default_active_policy=DEFAULT_ACTIVE_ALL,
    )


_DECLARATION_TEMPLATES: tuple[SourceUpdaterDeclaration, ...] = (
    _gmail_template(),
    _calendar_template(),
    _imessage_template(),
    _photos_template(),
    _health_template(),
)

_ADAPTER_TO_TEMPLATE: dict[str, SourceUpdaterDeclaration] = {
    decl.adapter_source_id: decl for decl in _DECLARATION_TEMPLATES
}


def iter_declaration_templates() -> tuple[SourceUpdaterDeclaration, ...]:
    return _DECLARATION_TEMPLATES


def declaration_for_adapter_source_id(adapter_source_id: str, *, scope: str = "") -> SourceUpdaterDeclaration | None:
    template = _ADAPTER_TO_TEMPLATE.get(adapter_source_id)
    if template is None:
        return None
    scope = scope.strip() or "<account>"
    if adapter_source_id == "gmail-messages":
        return _gmail_template(scope)
    if adapter_source_id == "calendar-events":
        return _calendar_template(scope)
    if adapter_source_id == "imessage":
        return _imessage_template(scope or "local")
    if adapter_source_id == "photos":
        return _photos_template(scope or "local")
    if adapter_source_id == "apple-health":
        return _health_template(scope or "apple-health")
    return template


def expand_declarations(
    *,
    gmail_accounts: tuple[str, ...] = (),
    calendar_accounts: tuple[str, ...] = (),
    imessage_label: str = "local",
    photos_label: str = "local",
    health_label: str = "apple-health",
    include_templates: bool = False,
) -> list[SourceUpdaterDeclaration]:
    """Expand registry entries for configured accounts (no sync)."""

    out: list[SourceUpdaterDeclaration] = []
    if include_templates:
        out.extend(_DECLARATION_TEMPLATES)
    for account in gmail_accounts:
        acct = account.strip()
        if acct:
            out.append(_gmail_template(acct))
    for account in calendar_accounts:
        acct = account.strip()
        if acct:
            out.append(_calendar_template(acct))
    out.append(_imessage_template(imessage_label))
    out.append(_photos_template(photos_label))
    out.append(_health_template(health_label))
    return out


def validate_declaration(decl: SourceUpdaterDeclaration) -> list[str]:
    errors: list[str] = []
    if not decl.source_key.strip():
        errors.append("source_key is required")
    if not decl.source_type.strip():
        errors.append("source_type is required")
    if not decl.adapter_name.strip():
        errors.append("adapter_name is required")
    if not decl.adapter_source_id.strip():
        errors.append("adapter_source_id is required")
    if decl.default_active_policy not in DEFAULT_ACTIVE_POLICIES:
        errors.append(f"unknown default_active_policy: {decl.default_active_policy}")
    kinds = decl.cursor_kinds or ((decl.cursor_kind,) if decl.cursor_kind else ())
    if not kinds:
        errors.append("cursor_kind or cursor_kinds is required")
    if ":" not in decl.source_key and decl.source_type not in (SOURCE_TYPE_HEALTH,):
        errors.append("source_key should include scope suffix (type:scope)")
    return errors


def validate_all_declarations(declarations: Iterable[SourceUpdaterDeclaration] | None = None) -> dict[str, list[str]]:
    decls = list(declarations) if declarations is not None else list(_DECLARATION_TEMPLATES)
    return {d.source_key: validate_declaration(d) for d in decls if validate_declaration(d)}
