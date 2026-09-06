"""Versioned connector SDK contracts.

Connectors emit sourced facts with account-scoped identity. They do not write
inferred links onto source fields, and they do not commit a cursor before a
durable canonical write. Shared engine records are imported, not redefined.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from archive_engine.contracts import AccessContext, ArchiveIdentity, ChangeRecord, OutputReceipt
from archive_engine.errors import AccessDeniedError, IncompatibleContractError, IncompatibleStateError
from archive_vault.uid import generate_uid

SDK_VERSION = "1"
IDENTITY_RECIPE = "archive_id+source+account_scope+provider_object_id"
LEGACY_IDENTITY_RECIPE = "source+account_scope+provider_object_id"
CARD_PREFIX_BY_TYPE = {
    "email_message": "email-message",
    "email_thread": "email-thread",
    "email_attachment": "email-attachment",
    "calendar_event": "calendar-event",
}

FRESHNESS_CAPABILITIES = frozenset({"polling", "event-capable", "import-only"})
DELETE_POLICIES = frozenset({"provider_tombstone", "archive_forget"})
RETENTION_POLICIES = frozenset({"retain_until_archive_forget", "provider_tombstone_only"})

_SECRET_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "client_secret",
        "dsn",
        "password",
        "passphrase",
        "private_key",
        "refresh_token",
        "secret",
        "token",
    }
)


def _as_str(value: object, *, field: str) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    raise IncompatibleContractError(f"{field} must be a string")


def _as_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise IncompatibleContractError(f"{field} must be an integer")
    return value


def _as_str_tuple(value: object, *, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise IncompatibleContractError(f"{field} must be a list of strings")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise IncompatibleContractError(f"{field} items must be strings")
        out.append(item)
    return tuple(out)


def _as_mapping(value: object, *, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise IncompatibleContractError(f"{field} must be an object")
    return value


def _require_nonempty(value: str, *, field: str) -> str:
    if not value.strip():
        raise IncompatibleContractError(f"{field} is required")
    return value.strip()


def _reject_secret_keys(payload: Mapping[str, object], *, field: str) -> None:
    for key in payload:
        lowered = str(key).strip().lower().replace("-", "_")
        if lowered in _SECRET_KEYS:
            raise IncompatibleContractError(f"{field} must not contain secret key {key!r}")


@dataclass(frozen=True)
class SourceObjectIdentity:
    """Account-scoped source identity. Same provider ID in two accounts stays distinct."""

    archive_id: str
    source: str
    account_scope: str
    provider_object_id: str

    @property
    def source_id(self) -> str:
        return f"{self.account_scope}:{self.provider_object_id}"

    def derive_uid(self, card_prefix: str, recipe: str = IDENTITY_RECIPE) -> str:
        if recipe == LEGACY_IDENTITY_RECIPE:
            material = f"{self.account_scope}:{self.provider_object_id}"
        else:
            material = f"{self.archive_id}:{self.account_scope}:{self.provider_object_id}"
        return generate_uid(card_prefix, self.source, material)

    def recipe_mismatch(self) -> bool:
        return not (self.archive_id.strip() and self.account_scope.strip() and self.provider_object_id.strip())

    def to_payload(self) -> dict[str, str]:
        return {
            "archive_id": self.archive_id,
            "source": self.source,
            "account_scope": self.account_scope,
            "provider_object_id": self.provider_object_id,
            "source_id": self.source_id,
            "recipe": IDENTITY_RECIPE,
        }

    @classmethod
    def from_parts(
        cls,
        *,
        archive: ArchiveIdentity,
        source: str,
        account_scope: str,
        provider_object_id: str,
    ) -> SourceObjectIdentity:
        return cls(
            archive_id=_require_nonempty(archive.archive_id, field="archive_id"),
            source=_require_nonempty(source, field="source"),
            account_scope=_require_nonempty(account_scope, field="account_scope"),
            provider_object_id=_require_nonempty(provider_object_id, field="provider_object_id"),
        )


@dataclass(frozen=True)
class ConnectorManifest:
    """Authoritative connector registration. Validated before any write."""

    connector_id: str
    connector_version: str
    sdk_version: str
    min_engine_version: int
    max_engine_version: int
    min_card_contract_version: int
    max_card_contract_version: int
    supported_sources: tuple[str, ...]
    supported_account_scopes: tuple[str, ...]
    emitted_card_types: tuple[str, ...]
    deterministic_fields_owned: tuple[str, ...]
    identity_recipe: str
    cursor_schema: str
    cursor_version: str
    delete_policy: str
    retention_policy: str
    freshness_capability: str
    freshness_interval: str
    rate_limit: str
    batch_limit: int
    auth_capabilities: tuple[str, ...]
    egress_capabilities: tuple[str, ...]
    fixture_id: str
    fixture_version: str
    event_handler: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "connector_id": self.connector_id,
            "connector_version": self.connector_version,
            "sdk_version": self.sdk_version,
            "min_engine_version": self.min_engine_version,
            "max_engine_version": self.max_engine_version,
            "min_card_contract_version": self.min_card_contract_version,
            "max_card_contract_version": self.max_card_contract_version,
            "supported_sources": list(self.supported_sources),
            "supported_account_scopes": list(self.supported_account_scopes),
            "emitted_card_types": list(self.emitted_card_types),
            "deterministic_fields_owned": list(self.deterministic_fields_owned),
            "identity_recipe": self.identity_recipe,
            "cursor_schema": self.cursor_schema,
            "cursor_version": self.cursor_version,
            "delete_policy": self.delete_policy,
            "retention_policy": self.retention_policy,
            "freshness_capability": self.freshness_capability,
            "freshness_interval": self.freshness_interval,
            "rate_limit": self.rate_limit,
            "batch_limit": self.batch_limit,
            "auth_capabilities": list(self.auth_capabilities),
            "egress_capabilities": list(self.egress_capabilities),
            "fixture_id": self.fixture_id,
            "fixture_version": self.fixture_version,
            "event_handler": self.event_handler,
        }


@dataclass(frozen=True)
class FetchedRecord:
    record_ref: str
    event_identity: str
    payload: Mapping[str, object]


@dataclass(frozen=True)
class FetchedBatch:
    batch_id: str
    source: str
    account_scope: str
    cursor_before: Mapping[str, object]
    cursor_after_candidate: Mapping[str, object]
    records: tuple[FetchedRecord, ...]


@dataclass(frozen=True)
class CanonicalProposal:
    identity: SourceObjectIdentity
    card_type: str
    rel_path: str
    card: Mapping[str, object]
    body: str
    provenance: Mapping[str, object]
    supporting_source_ids: tuple[str, ...]
    provider_revision: str


@dataclass(frozen=True)
class PersistResult:
    uid: str
    rel_path: str
    revision: str
    created: bool
    duplicate: bool


@dataclass(frozen=True)
class ConnectorRunResult:
    connector_id: str
    manifest: ConnectorManifest
    batch_id: str
    proposals: tuple[CanonicalProposal, ...]
    persists: tuple[PersistResult, ...]
    changes: tuple[ChangeRecord, ...]
    committed_cursor: Mapping[str, object] | None
    proposed_cursor: Mapping[str, object]
    receipts: tuple[OutputReceipt, ...] = ()
    pending_p02_wiring: str = "pending"
    pending_p03_wiring: str = "pending"
    dirty_uids: tuple[str, ...] = ()
    burst_freshness: str = "unknown"
    burst_keys: tuple[str, ...] = ()
    pending_scopes: tuple[Mapping[str, object], ...] = ()
    cursor_status: str = "active"

    @property
    def uids(self) -> tuple[str, ...]:
        return tuple(item.uid for item in self.persists)

    @property
    def created_count(self) -> int:
        return sum(1 for item in self.persists if item.created)


class Connector(Protocol):
    """Fetch/normalize surface. Runtime owns validation, write, and cursor commit."""

    def manifest(self) -> ConnectorManifest: ...

    def fetch(
        self,
        *,
        cursor: Mapping[str, object],
        identity: ArchiveIdentity,
        access: AccessContext,
    ) -> FetchedBatch: ...

    def normalize(
        self,
        batch: FetchedBatch,
        *,
        identity: ArchiveIdentity,
        access: AccessContext,
    ) -> tuple[CanonicalProposal, ...]: ...


class CanonicalWriter(Protocol):
    """Injected contained writer. Runtime never inspects a concrete store type."""

    def write_canonical(
        self,
        proposal: CanonicalProposal,
        *,
        identity: ArchiveIdentity,
        access: AccessContext,
    ) -> PersistResult: ...


def card_prefix_for(card_type: str) -> str:
    try:
        return CARD_PREFIX_BY_TYPE[card_type]
    except KeyError as exc:
        raise IncompatibleContractError(f"unsupported emitted card type: {card_type}") from exc


def parse_manifest(payload: Mapping[str, object]) -> ConnectorManifest:
    """Validate a manifest mapping before any connector import or write."""

    _reject_secret_keys(payload, field="manifest")
    sdk_version = _require_nonempty(_as_str(payload.get("sdk_version"), field="sdk_version"), field="sdk_version")
    if sdk_version != SDK_VERSION:
        raise IncompatibleContractError(f"unsupported sdk_version {sdk_version!r}; expected {SDK_VERSION}")
    min_engine = _as_int(payload.get("min_engine_version"), field="min_engine_version")
    max_engine = _as_int(payload.get("max_engine_version"), field="max_engine_version")
    if min_engine > max_engine:
        raise IncompatibleContractError("min_engine_version must be <= max_engine_version")
    from archive_engine.contracts import CONTRACT_SERIALIZATION_VERSION

    if not (min_engine <= CONTRACT_SERIALIZATION_VERSION <= max_engine):
        raise IncompatibleContractError(
            f"engine contract {CONTRACT_SERIALIZATION_VERSION} outside [{min_engine}, {max_engine}]"
        )
    min_card = _as_int(payload.get("min_card_contract_version"), field="min_card_contract_version")
    max_card = _as_int(payload.get("max_card_contract_version"), field="max_card_contract_version")
    if min_card > max_card:
        raise IncompatibleContractError("min_card_contract_version must be <= max_card_contract_version")
    if not (min_card <= 1 <= max_card):
        raise IncompatibleContractError(f"card contract 1 outside [{min_card}, {max_card}]")
    emitted = _as_str_tuple(payload.get("emitted_card_types"), field="emitted_card_types")
    if not emitted:
        raise IncompatibleContractError("emitted_card_types is required")
    for card_type in emitted:
        card_prefix_for(card_type)
    identity_recipe = _require_nonempty(
        _as_str(payload.get("identity_recipe"), field="identity_recipe"),
        field="identity_recipe",
    )
    if "account_scope" not in identity_recipe:
        raise IncompatibleContractError("identity_recipe must include account_scope")
    freshness = _require_nonempty(
        _as_str(payload.get("freshness_capability"), field="freshness_capability"),
        field="freshness_capability",
    )
    if freshness not in FRESHNESS_CAPABILITIES:
        raise IncompatibleContractError(f"freshness_capability must be one of {sorted(FRESHNESS_CAPABILITIES)}")
    if freshness == "live":
        raise IncompatibleContractError("import-only connectors must not declare live freshness")
    delete_policy = _require_nonempty(_as_str(payload.get("delete_policy"), field="delete_policy"), field="delete_policy")
    if delete_policy not in DELETE_POLICIES:
        raise IncompatibleContractError(f"delete_policy must be one of {sorted(DELETE_POLICIES)}")
    retention = _require_nonempty(
        _as_str(payload.get("retention_policy"), field="retention_policy"),
        field="retention_policy",
    )
    if retention not in RETENTION_POLICIES:
        raise IncompatibleContractError(f"retention_policy must be one of {sorted(RETENTION_POLICIES)}")
    batch_limit = _as_int(payload.get("batch_limit"), field="batch_limit")
    if batch_limit <= 0:
        raise IncompatibleContractError("batch_limit must be a positive integer")
    return ConnectorManifest(
        connector_id=_require_nonempty(_as_str(payload.get("connector_id"), field="connector_id"), field="connector_id"),
        connector_version=_require_nonempty(
            _as_str(payload.get("connector_version"), field="connector_version"),
            field="connector_version",
        ),
        sdk_version=sdk_version,
        min_engine_version=min_engine,
        max_engine_version=max_engine,
        min_card_contract_version=min_card,
        max_card_contract_version=max_card,
        supported_sources=_as_str_tuple(payload.get("supported_sources"), field="supported_sources"),
        supported_account_scopes=_as_str_tuple(payload.get("supported_account_scopes"), field="supported_account_scopes"),
        emitted_card_types=emitted,
        deterministic_fields_owned=_as_str_tuple(
            payload.get("deterministic_fields_owned"),
            field="deterministic_fields_owned",
        ),
        identity_recipe=identity_recipe,
        cursor_schema=_require_nonempty(_as_str(payload.get("cursor_schema"), field="cursor_schema"), field="cursor_schema"),
        cursor_version=_require_nonempty(
            _as_str(payload.get("cursor_version"), field="cursor_version"),
            field="cursor_version",
        ),
        delete_policy=delete_policy,
        retention_policy=retention,
        freshness_capability=freshness,
        freshness_interval=_require_nonempty(
            _as_str(payload.get("freshness_interval"), field="freshness_interval"),
            field="freshness_interval",
        ),
        rate_limit=_require_nonempty(_as_str(payload.get("rate_limit"), field="rate_limit"), field="rate_limit"),
        batch_limit=batch_limit,
        auth_capabilities=_as_str_tuple(payload.get("auth_capabilities"), field="auth_capabilities"),
        egress_capabilities=_as_str_tuple(payload.get("egress_capabilities"), field="egress_capabilities"),
        fixture_id=_require_nonempty(_as_str(payload.get("fixture_id"), field="fixture_id"), field="fixture_id"),
        fixture_version=_require_nonempty(
            _as_str(payload.get("fixture_version"), field="fixture_version"),
            field="fixture_version",
        ),
        event_handler=_as_str(payload.get("event_handler"), field="event_handler"),
    )


def validate_manifest(manifest: ConnectorManifest) -> ConnectorManifest:
    return parse_manifest(manifest.to_payload())


def require_access(*, identity: ArchiveIdentity, access: AccessContext) -> AccessContext:
    if access.archive_id != identity.archive_id:
        raise IncompatibleStateError("AccessContext.archive_id must match ArchiveIdentity.archive_id")
    if access.deny:
        raise AccessDeniedError(access.deny_reason or "access denied")
    return access


def validate_proposal(proposal: CanonicalProposal, manifest: ConnectorManifest) -> None:
    if proposal.card_type not in manifest.emitted_card_types:
        raise IncompatibleContractError(f"proposal card type {proposal.card_type!r} is not emitted by this connector")
    if proposal.identity.source not in manifest.supported_sources:
        raise IncompatibleContractError(f"source {proposal.identity.source!r} is not supported by this connector")
    if proposal.identity.recipe_mismatch():
        raise IncompatibleContractError("proposal identity is missing account scope")
    recipe = manifest.identity_recipe
    if recipe == IDENTITY_RECIPE:
        owned = set(manifest.deterministic_fields_owned)
        required = {"uid", "type", "source", "source_id", "created", "updated"}
        extra = [key for key in proposal.card if key not in owned and key not in required]
        if extra:
            raise IncompatibleContractError(f"proposal sets fields not owned by connector: {sorted(extra)}")
    uid = proposal.identity.derive_uid(card_prefix_for(proposal.card_type), recipe)
    if str(proposal.card.get("uid") or "") != uid:
        raise IncompatibleContractError("proposal uid does not match account-scoped identity recipe")
    if str(proposal.card.get("source_id") or "") != proposal.identity.source_id:
        raise IncompatibleContractError("proposal source_id must be account_scope:provider_object_id")
    if not proposal.rel_path or proposal.rel_path.startswith("/") or ".." in proposal.rel_path.split("/"):
        raise IncompatibleContractError("proposal rel_path must be a contained relative path")
