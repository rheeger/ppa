"""PPA engine package.

P06-A owns shared contracts and the exact-read service.
P07 owns ``recovery_manifest`` and ``recovery`` as sibling modules — this
init does not register CLI commands (P09 owns parser wiring).

Sibling modules remain importable as ``archive_engine.<name>`` even when they
are not re-exported here.
"""

from __future__ import annotations

from importlib import import_module
from pathlib import Path

from archive_engine.contracts import (
    AccessContext,
    AffectedContext,
    ArchiveIdentity,
    ArtifactHash,
    ChangeBatch,
    ChangeRecord,
    ChunkEvidenceRef,
    EmbeddingSpec,
    EvidenceEnvelope,
    ExactReadResult,
    MessageEvidenceRef,
    OutputReceipt,
    OutputRevision,
    RunEvidence,
    ServingEdge,
    ServingManifest,
    SourceSpan,
    dump_contract,
    load_contract,
)
from archive_engine.errors import (
    AccessDeniedError,
    CapabilityUnavailableError,
    EgressDeniedError,
    EngineError,
    IncompatibleContractError,
    IncompatibleStateError,
    RetryableEngineError,
)
from archive_engine.runtime import ArchiveRuntime
from archive_engine.service import ArchiveEngineService

__all__ = [
    "AccessContext",
    "AccessDeniedError",
    "AffectedContext",
    "ArchiveEngineService",
    "ArchiveIdentity",
    "ArchiveRuntime",
    "ArtifactHash",
    "CapabilityUnavailableError",
    "EgressDeniedError",
    "ChangeBatch",
    "ChangeRecord",
    "ChunkEvidenceRef",
    "EmbeddingSpec",
    "EngineError",
    "EvidenceEnvelope",
    "ExactReadResult",
    "IncompatibleContractError",
    "IncompatibleStateError",
    "MessageEvidenceRef",
    "OutputReceipt",
    "OutputRevision",
    "RetryableEngineError",
    "RunEvidence",
    "ServingEdge",
    "ServingManifest",
    "SourceSpan",
    "dump_contract",
    "load_contract",
]

_SIBLING_MODULES = frozenset(
    {
        "access",
        "changes",
        "config",
        "corrections",
        "egress",
        "execution_mode",
        "publication",
        "recovery",
        "recovery_manifest",
        "redaction",
        "runtime",
        "scopes",
    }
)


def __getattr__(name: str) -> object:
    if name in _SIBLING_MODULES:
        return import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    names = set(__all__)
    package_dir = Path(__file__).resolve().parent
    for sibling in _SIBLING_MODULES:
        if (package_dir / f"{sibling}.py").exists():
            names.add(sibling)
    return sorted(names)
