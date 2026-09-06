"""Frozen shared records for P01/P03/P08 and later consumers.

These types are the program shared-interface table. Consumers add behavior in
their own modules; they must not redefine these records. Flexible JSON is
allowed only at the transport serialization boundary via the versioned
envelope helpers below — the records themselves are immutable dataclasses.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from archive_engine.errors import IncompatibleContractError

CONTRACT_SERIALIZATION_VERSION = 1
CHUNK_EVIDENCE_REF_VERSION = 1
UNKNOWN = "unknown"

EvidenceKind = Literal["source_reported", "derived", "proposed_link", "unknown"]
RunResult = Literal["pass", "fail", "skip"]
OutputStatus = Literal["completed", "pending", "failed", "skipped", "blocked", "dependency_unmet"]
SpanRepresentation = Literal["canonical_body_utf8"]

EVIDENCE_KINDS: frozenset[str] = frozenset(
    {"source_reported", "derived", "proposed_link", "unknown"}
)
RUN_RESULTS: frozenset[str] = frozenset({"pass", "fail", "skip"})
OUTPUT_STATUSES: frozenset[str] = frozenset(
    {"completed", "pending", "failed", "skipped", "blocked", "dependency_unmet"}
)
_SECRET_ENV_KEYS = frozenset(
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


def _as_bool(value: object, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    raise IncompatibleContractError(f"{field} must be a boolean")


def _as_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise IncompatibleContractError(f"{field} must be an integer")
    return value


def _as_optional_int(value: object, *, field: str) -> int | None:
    if value is None:
        return None
    return _as_int(value, field=field)


def _as_optional_float(value: object, *, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise IncompatibleContractError(f"{field} must be a number or null")
    return float(value)


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


def _as_object_list(value: object, *, field: str) -> tuple[Mapping[str, object], ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise IncompatibleContractError(f"{field} must be a list")
    out: list[Mapping[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise IncompatibleContractError(f"{field} items must be objects")
        out.append(item)
    return tuple(out)


def _require_nonempty(value: str, *, field: str) -> str:
    if not value.strip():
        raise IncompatibleContractError(f"{field} is required")
    return value


def _evidence_kind(value: object, *, field: str = "evidence_kind") -> EvidenceKind:
    raw = _as_str(value, field=field) or UNKNOWN
    if raw not in EVIDENCE_KINDS:
        raise IncompatibleContractError(f"{field} must be one of {sorted(EVIDENCE_KINDS)}")
    return raw  # type: ignore[return-value]


def _reject_secret_keys(pairs: tuple[tuple[str, str], ...], *, field: str) -> None:
    for key, _value in pairs:
        lowered = key.strip().lower().replace("-", "_")
        if lowered in _SECRET_ENV_KEYS:
            raise IncompatibleContractError(f"{field} must not contain secret key {key!r}")


def _pairs_from_mapping(value: object, *, field: str) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        out: list[tuple[str, str]] = []
        for key, item in value.items():
            if not isinstance(key, str) or not isinstance(item, str):
                raise IncompatibleContractError(f"{field} must be string-to-string")
            out.append((key, item))
        return tuple(out)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise IncompatibleContractError(f"{field} must be an object or list of pairs")
    out = []
    for item in value:
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes)) or len(item) != 2:
            raise IncompatibleContractError(f"{field} pairs must be [name, value]")
        name, raw = item[0], item[1]
        if not isinstance(name, str) or not isinstance(raw, str):
            raise IncompatibleContractError(f"{field} pairs must be strings")
        out.append((name, raw))
    return tuple(out)


def _pairs_to_dict(pairs: tuple[tuple[str, str], ...]) -> dict[str, str]:
    return {key: value for key, value in pairs}


@dataclass(frozen=True)
class ArchiveIdentity:
    """Persistent archive identity. Never infer from a display name."""

    archive_id: str
    canonical_root: str
    schema_binding: str

    def to_payload(self) -> dict[str, str]:
        return {
            "archive_id": self.archive_id,
            "canonical_root": self.canonical_root,
            "schema_binding": self.schema_binding,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> ArchiveIdentity:
        return cls(
            archive_id=_require_nonempty(_as_str(payload.get("archive_id"), field="archive_id"), field="archive_id"),
            canonical_root=_require_nonempty(
                _as_str(payload.get("canonical_root"), field="canonical_root"),
                field="canonical_root",
            ),
            schema_binding=_require_nonempty(
                _as_str(payload.get("schema_binding"), field="schema_binding"),
                field="schema_binding",
            ),
        )


@dataclass(frozen=True)
class EmbeddingSpec:
    """Identity of a vector/cache/generation space. Mixed spaces must not rank together."""

    provider_namespace: str
    model: str
    model_revision: str
    dimension: int
    metric: str
    normalization: str
    chunk_schema: str

    def to_payload(self) -> dict[str, object]:
        return {
            "provider_namespace": self.provider_namespace,
            "model": self.model,
            "model_revision": self.model_revision,
            "dimension": self.dimension,
            "metric": self.metric,
            "normalization": self.normalization,
            "chunk_schema": self.chunk_schema,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> EmbeddingSpec:
        dimension = _as_int(payload.get("dimension"), field="dimension")
        if dimension <= 0:
            raise IncompatibleContractError("dimension must be a positive integer")
        return cls(
            provider_namespace=_require_nonempty(
                _as_str(payload.get("provider_namespace"), field="provider_namespace"),
                field="provider_namespace",
            ),
            model=_require_nonempty(_as_str(payload.get("model"), field="model"), field="model"),
            model_revision=_require_nonempty(
                _as_str(payload.get("model_revision"), field="model_revision"),
                field="model_revision",
            ),
            dimension=dimension,
            metric=_require_nonempty(_as_str(payload.get("metric"), field="metric"), field="metric"),
            normalization=_require_nonempty(
                _as_str(payload.get("normalization"), field="normalization"),
                field="normalization",
            ),
            chunk_schema=_require_nonempty(
                _as_str(payload.get("chunk_schema"), field="chunk_schema"),
                field="chunk_schema",
            ),
        )


@dataclass(frozen=True)
class ChangeRecord:
    """Event-spine mutation. This is not 'dirty UIDs for search only'."""

    archive_id: str
    sequence: int
    mutation_id: str
    uid: str
    operation: str
    before_revision: str
    after_revision: str
    source: str
    account: str
    run_id: str
    committed: bool

    def to_payload(self) -> dict[str, object]:
        return {
            "archive_id": self.archive_id,
            "sequence": self.sequence,
            "mutation_id": self.mutation_id,
            "uid": self.uid,
            "operation": self.operation,
            "before_revision": self.before_revision,
            "after_revision": self.after_revision,
            "source": self.source,
            "account": self.account,
            "run_id": self.run_id,
            "committed": self.committed,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> ChangeRecord:
        sequence = _as_int(payload.get("sequence"), field="sequence")
        if sequence < 0:
            raise IncompatibleContractError("sequence must be >= 0")
        return cls(
            archive_id=_require_nonempty(_as_str(payload.get("archive_id"), field="archive_id"), field="archive_id"),
            sequence=sequence,
            mutation_id=_require_nonempty(_as_str(payload.get("mutation_id"), field="mutation_id"), field="mutation_id"),
            uid=_require_nonempty(_as_str(payload.get("uid"), field="uid"), field="uid"),
            operation=_require_nonempty(_as_str(payload.get("operation"), field="operation"), field="operation"),
            before_revision=_as_str(payload.get("before_revision"), field="before_revision"),
            after_revision=_as_str(payload.get("after_revision"), field="after_revision"),
            source=_as_str(payload.get("source"), field="source"),
            account=_as_str(payload.get("account"), field="account"),
            run_id=_as_str(payload.get("run_id"), field="run_id"),
            committed=_as_bool(payload.get("committed"), field="committed"),
        )


@dataclass(frozen=True)
class ChangeBatch:
    """Inclusive high-watermark batch. ``consumer_name`` is required."""

    high_watermark: int
    consumer_name: str
    records: tuple[ChangeRecord, ...] = ()
    record_ref: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "high_watermark": self.high_watermark,
            "consumer_name": self.consumer_name,
            "records": [record.to_payload() for record in self.records],
            "record_ref": self.record_ref,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> ChangeBatch:
        watermark = _as_int(payload.get("high_watermark"), field="high_watermark")
        if watermark < 0:
            raise IncompatibleContractError("high_watermark must be >= 0")
        consumer = _require_nonempty(
            _as_str(payload.get("consumer_name"), field="consumer_name"),
            field="consumer_name",
        )
        records = tuple(
            ChangeRecord.from_payload(item) for item in _as_object_list(payload.get("records"), field="records")
        )
        return cls(
            high_watermark=watermark,
            consumer_name=consumer,
            records=records,
            record_ref=_as_str(payload.get("record_ref"), field="record_ref"),
        )


@dataclass(frozen=True)
class OutputRevision:
    """One compiler output at a specific revision, for later invalidation."""

    uid: str
    revision: str

    def to_payload(self) -> dict[str, str]:
        return {"uid": self.uid, "revision": self.revision}

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> OutputRevision:
        return cls(
            uid=_require_nonempty(_as_str(payload.get("uid"), field="uid"), field="uid"),
            revision=_require_nonempty(_as_str(payload.get("revision"), field="revision"), field="revision"),
        )


@dataclass(frozen=True)
class OutputReceipt:
    """Revision-specific processor receipt. Completion lists actual outputs."""

    processor: str
    processor_version: str
    input_uid: str
    input_revision: str
    status: OutputStatus
    outputs: tuple[OutputRevision, ...] = ()
    chunk_keys: tuple[str, ...] = ()
    embedding_spec: EmbeddingSpec | None = None
    error_reason: str = ""
    dependency_reason: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "processor": self.processor,
            "processor_version": self.processor_version,
            "input_uid": self.input_uid,
            "input_revision": self.input_revision,
            "status": self.status,
            "outputs": [item.to_payload() for item in self.outputs],
            "chunk_keys": list(self.chunk_keys),
            "embedding_spec": None if self.embedding_spec is None else self.embedding_spec.to_payload(),
            "error_reason": self.error_reason,
            "dependency_reason": self.dependency_reason,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> OutputReceipt:
        status = _as_str(payload.get("status"), field="status")
        if status not in OUTPUT_STATUSES:
            raise IncompatibleContractError(f"status must be one of {sorted(OUTPUT_STATUSES)}")
        spec_raw = payload.get("embedding_spec")
        spec = None if spec_raw is None else EmbeddingSpec.from_payload(_as_mapping(spec_raw, field="embedding_spec"))
        return cls(
            processor=_require_nonempty(_as_str(payload.get("processor"), field="processor"), field="processor"),
            processor_version=_require_nonempty(
                _as_str(payload.get("processor_version"), field="processor_version"),
                field="processor_version",
            ),
            input_uid=_require_nonempty(_as_str(payload.get("input_uid"), field="input_uid"), field="input_uid"),
            input_revision=_require_nonempty(
                _as_str(payload.get("input_revision"), field="input_revision"),
                field="input_revision",
            ),
            status=status,  # type: ignore[arg-type]
            outputs=tuple(
                OutputRevision.from_payload(item) for item in _as_object_list(payload.get("outputs"), field="outputs")
            ),
            chunk_keys=_as_str_tuple(payload.get("chunk_keys"), field="chunk_keys"),
            embedding_spec=spec,
            error_reason=_as_str(payload.get("error_reason"), field="error_reason"),
            dependency_reason=_as_str(payload.get("dependency_reason"), field="dependency_reason"),
        )


@dataclass(frozen=True)
class ServingEdge:
    """Edge trust fields. Absent values are unknown, not 'everything is trusted'."""

    method: str = UNKNOWN
    confidence: float | None = None
    evidence_uids: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        return {
            "method": self.method or UNKNOWN,
            "confidence": self.confidence,
            "evidence_uids": list(self.evidence_uids),
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> ServingEdge:
        method = _as_str(payload.get("method"), field="method") or UNKNOWN
        return cls(
            method=method,
            confidence=_as_optional_float(payload.get("confidence"), field="confidence"),
            evidence_uids=_as_str_tuple(payload.get("evidence_uids"), field="evidence_uids"),
        )


@dataclass(frozen=True)
class ServingManifest:
    """Immutable generation descriptor. P01 owns vector/trust payload semantics."""

    format_version: str
    registry_version: str
    snapshot_id: str
    source_watermark: int
    embedding_spec: EmbeddingSpec
    artifact_checksums: tuple[tuple[str, str], ...]
    logical_counts: tuple[tuple[str, int], ...]
    parent_generation: str
    base_generation: str
    validation_summary: str
    edge_defaults: ServingEdge = ServingEdge()

    def to_payload(self) -> dict[str, object]:
        return {
            "format_version": self.format_version,
            "registry_version": self.registry_version,
            "snapshot_id": self.snapshot_id,
            "source_watermark": self.source_watermark,
            "embedding_spec": self.embedding_spec.to_payload(),
            "artifact_checksums": _pairs_to_dict(self.artifact_checksums),
            "logical_counts": {name: count for name, count in self.logical_counts},
            "parent_generation": self.parent_generation,
            "base_generation": self.base_generation,
            "validation_summary": self.validation_summary,
            "edge_defaults": self.edge_defaults.to_payload(),
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> ServingManifest:
        counts_raw = payload.get("logical_counts")
        counts: list[tuple[str, int]] = []
        if counts_raw is None:
            counts = []
        elif isinstance(counts_raw, Mapping):
            for key, value in counts_raw.items():
                if not isinstance(key, str):
                    raise IncompatibleContractError("logical_counts keys must be strings")
                counts.append((key, _as_int(value, field=f"logical_counts.{key}")))
        else:
            raise IncompatibleContractError("logical_counts must be an object")
        edge_raw = payload.get("edge_defaults")
        edge = ServingEdge() if edge_raw is None else ServingEdge.from_payload(_as_mapping(edge_raw, field="edge_defaults"))
        return cls(
            format_version=_require_nonempty(
                _as_str(payload.get("format_version"), field="format_version"),
                field="format_version",
            ),
            registry_version=_require_nonempty(
                _as_str(payload.get("registry_version"), field="registry_version"),
                field="registry_version",
            ),
            snapshot_id=_require_nonempty(
                _as_str(payload.get("snapshot_id"), field="snapshot_id"),
                field="snapshot_id",
            ),
            source_watermark=_as_int(payload.get("source_watermark"), field="source_watermark"),
            embedding_spec=EmbeddingSpec.from_payload(
                _as_mapping(payload.get("embedding_spec"), field="embedding_spec")
            ),
            artifact_checksums=_pairs_from_mapping(payload.get("artifact_checksums"), field="artifact_checksums"),
            logical_counts=tuple(counts),
            parent_generation=_as_str(payload.get("parent_generation"), field="parent_generation"),
            base_generation=_as_str(payload.get("base_generation"), field="base_generation"),
            validation_summary=_as_str(payload.get("validation_summary"), field="validation_summary"),
            edge_defaults=edge,
        )


@dataclass(frozen=True)
class AccessContext:
    """Per-request access. Cannot be omitted on an internal engine call.

    ``deny=True`` is explicit. Empty allow-lists with ``deny=False`` mean the
    configured trusted-local context (unrestricted at this layer). P05-B fills
    source/domain lists; mixed-source derived records deny if any required
    source is denied.
    """

    archive_id: str
    principal: str
    profile: str
    allowed_tools: tuple[str, ...] = ()
    allowed_sources: tuple[str, ...] = ()
    allowed_domains: tuple[str, ...] = ()
    egress_policy_revision: str = "unspecified"
    deny: bool = False
    deny_reason: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "archive_id": self.archive_id,
            "principal": self.principal,
            "profile": self.profile,
            "allowed_tools": list(self.allowed_tools),
            "allowed_sources": list(self.allowed_sources),
            "allowed_domains": list(self.allowed_domains),
            "egress_policy_revision": self.egress_policy_revision,
            "deny": self.deny,
            "deny_reason": self.deny_reason,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> AccessContext:
        return cls(
            archive_id=_require_nonempty(_as_str(payload.get("archive_id"), field="archive_id"), field="archive_id"),
            principal=_require_nonempty(_as_str(payload.get("principal"), field="principal"), field="principal"),
            profile=_require_nonempty(_as_str(payload.get("profile"), field="profile"), field="profile"),
            allowed_tools=_as_str_tuple(payload.get("allowed_tools"), field="allowed_tools"),
            allowed_sources=_as_str_tuple(payload.get("allowed_sources"), field="allowed_sources"),
            allowed_domains=_as_str_tuple(payload.get("allowed_domains"), field="allowed_domains"),
            egress_policy_revision=_as_str(payload.get("egress_policy_revision"), field="egress_policy_revision")
            or "unspecified",
            deny=_as_bool(payload.get("deny"), field="deny") if "deny" in payload else False,
            deny_reason=_as_str(payload.get("deny_reason"), field="deny_reason"),
        )


@dataclass(frozen=True)
class SourceSpan:
    """Typed source span over exact UTF-8 body bytes (half-open, zero-based)."""

    source_uid: str
    source_message_id: str
    source_revision: str
    representation: str
    start_byte: int
    end_byte: int

    def to_payload(self) -> dict[str, object]:
        return {
            "source_uid": self.source_uid,
            "source_message_id": self.source_message_id,
            "source_revision": self.source_revision,
            "representation": self.representation,
            "start_byte": self.start_byte,
            "end_byte": self.end_byte,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> SourceSpan:
        start = _as_int(payload.get("start_byte"), field="start_byte")
        end = _as_int(payload.get("end_byte"), field="end_byte")
        if start < 0 or end < start:
            raise IncompatibleContractError("source span offsets must be half-open and non-negative")
        representation = _as_str(payload.get("representation"), field="representation") or "canonical_body_utf8"
        return cls(
            source_uid=_require_nonempty(_as_str(payload.get("source_uid"), field="source_uid"), field="source_uid"),
            source_message_id=_as_str(payload.get("source_message_id"), field="source_message_id"),
            source_revision=_require_nonempty(
                _as_str(payload.get("source_revision"), field="source_revision"),
                field="source_revision",
            ),
            representation=representation,
            start_byte=start,
            end_byte=end,
        )


@dataclass(frozen=True)
class MessageEvidenceRef:
    """Optional per-message burst member. Missing parent list means unavailable."""

    message_id: str
    source_revision: str
    spans: tuple[SourceSpan, ...] = ()

    def to_payload(self) -> dict[str, object]:
        return {
            "message_id": self.message_id,
            "source_revision": self.source_revision,
            "spans": [span.to_payload() for span in self.spans],
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> MessageEvidenceRef:
        return cls(
            message_id=_require_nonempty(_as_str(payload.get("message_id"), field="message_id"), field="message_id"),
            source_revision=_require_nonempty(
                _as_str(payload.get("source_revision"), field="source_revision"),
                field="source_revision",
            ),
            spans=tuple(SourceSpan.from_payload(item) for item in _as_object_list(payload.get("spans"), field="spans")),
        )


@dataclass(frozen=True)
class ChunkEvidenceRef:
    """Versioned chunk evidence. P01-B freezes the serving JSON/Rust round trip."""

    version: int
    archive_id: str
    card_uid: str
    chunk_id: str
    chunk_schema_version: str
    algorithm_version: str
    evidence_kind: EvidenceKind = UNKNOWN
    lineage_complete: bool = False
    parent_thread: str = ""
    source_revisions: tuple[str, ...] = ()
    source_spans: tuple[SourceSpan, ...] = ()
    span_unavailable: bool = True
    message_refs: tuple[MessageEvidenceRef, ...] = ()
    message_refs_available: bool = False

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "version": self.version,
            "archive_id": self.archive_id,
            "card_uid": self.card_uid,
            "chunk_id": self.chunk_id,
            "chunk_schema_version": self.chunk_schema_version,
            "algorithm_version": self.algorithm_version,
            "evidence_kind": self.evidence_kind,
            "lineage_complete": self.lineage_complete,
            "parent_thread": self.parent_thread,
            "source_revisions": list(self.source_revisions),
            "span_unavailable": self.span_unavailable,
            "message_refs_available": self.message_refs_available,
        }
        if not self.span_unavailable:
            payload["source_spans"] = [span.to_payload() for span in self.source_spans]
        if self.message_refs_available:
            payload["message_refs"] = [item.to_payload() for item in self.message_refs]
        return payload

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> ChunkEvidenceRef:
        version = _as_int(payload.get("version"), field="version")
        if version != CHUNK_EVIDENCE_REF_VERSION:
            raise IncompatibleContractError(f"unsupported ChunkEvidenceRef version: {version}")
        spans_present = "source_spans" in payload
        refs_present = "message_refs" in payload
        span_unavailable = (
            _as_bool(payload.get("span_unavailable"), field="span_unavailable")
            if "span_unavailable" in payload
            else not spans_present
        )
        message_available = (
            _as_bool(payload.get("message_refs_available"), field="message_refs_available")
            if "message_refs_available" in payload
            else refs_present
        )
        return cls(
            version=version,
            archive_id=_require_nonempty(_as_str(payload.get("archive_id"), field="archive_id"), field="archive_id"),
            card_uid=_require_nonempty(_as_str(payload.get("card_uid"), field="card_uid"), field="card_uid"),
            chunk_id=_require_nonempty(_as_str(payload.get("chunk_id"), field="chunk_id"), field="chunk_id"),
            chunk_schema_version=_require_nonempty(
                _as_str(payload.get("chunk_schema_version"), field="chunk_schema_version"),
                field="chunk_schema_version",
            ),
            algorithm_version=_require_nonempty(
                _as_str(payload.get("algorithm_version"), field="algorithm_version"),
                field="algorithm_version",
            ),
            evidence_kind=_evidence_kind(payload.get("evidence_kind")),
            lineage_complete=_as_bool(payload.get("lineage_complete"), field="lineage_complete")
            if "lineage_complete" in payload
            else False,
            parent_thread=_as_str(payload.get("parent_thread"), field="parent_thread"),
            source_revisions=_as_str_tuple(payload.get("source_revisions"), field="source_revisions"),
            source_spans=tuple(
                SourceSpan.from_payload(item) for item in _as_object_list(payload.get("source_spans"), field="source_spans")
            ),
            span_unavailable=span_unavailable,
            message_refs=tuple(
                MessageEvidenceRef.from_payload(item)
                for item in _as_object_list(payload.get("message_refs"), field="message_refs")
            ),
            message_refs_available=message_available,
        )


@dataclass(frozen=True)
class EvidenceEnvelope:
    """Query/evidence wrapper. Distinguishes source-reported vs derived vs proposed."""

    generation: str
    watermark: int
    evidence_refs: tuple[ChunkEvidenceRef, ...] = ()
    coverage: str = UNKNOWN
    freshness: str = UNKNOWN
    complete: bool = False
    truncated: bool = False
    confidence_reason: str = ""
    errors: tuple[str, ...] = ()
    method: str = UNKNOWN
    corpus_state: str = UNKNOWN
    provenance: str = UNKNOWN
    evidence_kind: EvidenceKind = UNKNOWN
    pipeline_version: str = ""
    query: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "generation": self.generation,
            "watermark": self.watermark,
            "evidence_refs": [item.to_payload() for item in self.evidence_refs],
            "coverage": self.coverage,
            "freshness": self.freshness,
            "complete": self.complete,
            "truncated": self.truncated,
            "confidence_reason": self.confidence_reason,
            "errors": list(self.errors),
            "method": self.method,
            "corpus_state": self.corpus_state,
            "provenance": self.provenance,
            "evidence_kind": self.evidence_kind,
            "pipeline_version": self.pipeline_version,
            "query": self.query,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> EvidenceEnvelope:
        return cls(
            generation=_require_nonempty(_as_str(payload.get("generation"), field="generation"), field="generation"),
            watermark=_as_int(payload.get("watermark"), field="watermark"),
            evidence_refs=tuple(
                ChunkEvidenceRef.from_payload(item)
                for item in _as_object_list(payload.get("evidence_refs"), field="evidence_refs")
            ),
            coverage=_as_str(payload.get("coverage"), field="coverage") or UNKNOWN,
            freshness=_as_str(payload.get("freshness"), field="freshness") or UNKNOWN,
            complete=_as_bool(payload.get("complete"), field="complete") if "complete" in payload else False,
            truncated=_as_bool(payload.get("truncated"), field="truncated") if "truncated" in payload else False,
            confidence_reason=_as_str(payload.get("confidence_reason"), field="confidence_reason"),
            errors=_as_str_tuple(payload.get("errors"), field="errors"),
            method=_as_str(payload.get("method"), field="method") or UNKNOWN,
            corpus_state=_as_str(payload.get("corpus_state"), field="corpus_state") or UNKNOWN,
            provenance=_as_str(payload.get("provenance"), field="provenance") or UNKNOWN,
            evidence_kind=_evidence_kind(payload.get("evidence_kind")),
            pipeline_version=_as_str(payload.get("pipeline_version"), field="pipeline_version"),
            query=_as_str(payload.get("query"), field="query"),
        )


@dataclass(frozen=True)
class ArtifactHash:
    path: str
    sha256: str

    def to_payload(self) -> dict[str, str]:
        return {"path": self.path, "sha256": self.sha256}

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> ArtifactHash:
        return cls(
            path=_require_nonempty(_as_str(payload.get("path"), field="path"), field="path"),
            sha256=_require_nonempty(_as_str(payload.get("sha256"), field="sha256"), field="sha256"),
        )


@dataclass(frozen=True)
class RunEvidence:
    """Isolated proof record. Environment maps must not carry secrets."""

    commit_sha: str
    base_sha: str
    scenario_ids: tuple[str, ...]
    environment: tuple[tuple[str, str], ...]
    versions: tuple[tuple[str, str], ...]
    commands: tuple[str, ...]
    started_at: str
    finished_at: str
    result: RunResult
    artifacts: tuple[ArtifactHash, ...] = ()
    skip_reason: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "commit_sha": self.commit_sha,
            "base_sha": self.base_sha,
            "scenario_ids": list(self.scenario_ids),
            "environment": _pairs_to_dict(self.environment),
            "versions": _pairs_to_dict(self.versions),
            "commands": list(self.commands),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": self.result,
            "artifacts": [item.to_payload() for item in self.artifacts],
            "skip_reason": self.skip_reason,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> RunEvidence:
        result = _as_str(payload.get("result"), field="result")
        if result not in RUN_RESULTS:
            raise IncompatibleContractError(f"result must be one of {sorted(RUN_RESULTS)}")
        environment = _pairs_from_mapping(payload.get("environment"), field="environment")
        versions = _pairs_from_mapping(payload.get("versions"), field="versions")
        _reject_secret_keys(environment, field="environment")
        _reject_secret_keys(versions, field="versions")
        return cls(
            commit_sha=_require_nonempty(_as_str(payload.get("commit_sha"), field="commit_sha"), field="commit_sha"),
            base_sha=_require_nonempty(_as_str(payload.get("base_sha"), field="base_sha"), field="base_sha"),
            scenario_ids=_as_str_tuple(payload.get("scenario_ids"), field="scenario_ids"),
            environment=environment,
            versions=versions,
            commands=_as_str_tuple(payload.get("commands"), field="commands"),
            started_at=_require_nonempty(_as_str(payload.get("started_at"), field="started_at"), field="started_at"),
            finished_at=_require_nonempty(_as_str(payload.get("finished_at"), field="finished_at"), field="finished_at"),
            result=result,  # type: ignore[arg-type]
            artifacts=tuple(
                ArtifactHash.from_payload(item) for item in _as_object_list(payload.get("artifacts"), field="artifacts")
            ),
            skip_reason=_as_str(payload.get("skip_reason"), field="skip_reason"),
        )


@dataclass(frozen=True)
class AffectedContext:
    """Optional burst/reconciliation scope. Absence means pending, not success."""

    uid: str
    revision: str
    related_uids: tuple[str, ...] = ()
    status: Literal["resolved", "reconciliation_pending"] = "reconciliation_pending"

    def to_payload(self) -> dict[str, object]:
        return {
            "uid": self.uid,
            "revision": self.revision,
            "related_uids": list(self.related_uids),
            "status": self.status,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> AffectedContext:
        status = _as_str(payload.get("status"), field="status") or "reconciliation_pending"
        if status not in {"resolved", "reconciliation_pending"}:
            raise IncompatibleContractError("status must be resolved or reconciliation_pending")
        return cls(
            uid=_require_nonempty(_as_str(payload.get("uid"), field="uid"), field="uid"),
            revision=_as_str(payload.get("revision"), field="revision"),
            related_uids=_as_str_tuple(payload.get("related_uids"), field="related_uids"),
            status=status,  # type: ignore[arg-type]
        )


@dataclass(frozen=True)
class ExactReadResult:
    """Compatible exact-read envelope used by CLI and MCP."""

    path_or_uid: str
    content: str
    found: bool
    rel_path: str = ""
    include_rel_path: bool = False

    def to_store_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "path_or_uid": self.path_or_uid,
            "content": self.content,
            "found": self.found,
        }
        if self.include_rel_path:
            payload["rel_path"] = self.rel_path
        return payload


_RECORD_TYPES: dict[str, type] = {
    "ArchiveIdentity": ArchiveIdentity,
    "EmbeddingSpec": EmbeddingSpec,
    "ChangeRecord": ChangeRecord,
    "ChangeBatch": ChangeBatch,
    "OutputRevision": OutputRevision,
    "OutputReceipt": OutputReceipt,
    "ServingEdge": ServingEdge,
    "ServingManifest": ServingManifest,
    "AccessContext": AccessContext,
    "SourceSpan": SourceSpan,
    "MessageEvidenceRef": MessageEvidenceRef,
    "ChunkEvidenceRef": ChunkEvidenceRef,
    "EvidenceEnvelope": EvidenceEnvelope,
    "ArtifactHash": ArtifactHash,
    "RunEvidence": RunEvidence,
    "AffectedContext": AffectedContext,
}


def dump_contract(record: object) -> dict[str, object]:
    """Versioned JSON envelope for a frozen record."""

    name = type(record).__name__
    if name not in _RECORD_TYPES:
        raise IncompatibleContractError(f"not a shared contract: {name}")
    to_payload = getattr(record, "to_payload", None)
    if to_payload is None:
        raise IncompatibleContractError(f"{name} cannot be serialized")
    return {
        "contract": name,
        "version": CONTRACT_SERIALIZATION_VERSION,
        "payload": to_payload(),
    }


def load_contract(data: Mapping[str, object], expected: type | None = None) -> object:
    """Load a versioned envelope. Unknown versions and names are rejected."""

    name = _as_str(data.get("contract"), field="contract")
    version = data.get("version")
    if version != CONTRACT_SERIALIZATION_VERSION:
        raise IncompatibleContractError(f"unsupported contract serialization version: {version!r}")
    if name not in _RECORD_TYPES:
        raise IncompatibleContractError(f"unknown contract: {name}")
    cls = _RECORD_TYPES[name]
    if expected is not None and cls is not expected:
        raise IncompatibleContractError(f"expected {expected.__name__}, got {name}")
    return cls.from_payload(_as_mapping(data.get("payload"), field="payload"))
