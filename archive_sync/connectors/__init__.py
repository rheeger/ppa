"""Versioned connector SDK (P08-A/B).

Gmail and calendar register through ``legacy``. Dispatch looks up a factory
by connector id — do not add source-specific branches in core handler code.
"""

from __future__ import annotations

from archive_sync.connectors import legacy as _legacy
from archive_sync.connectors.contracts import (
    SDK_VERSION,
    CanonicalProposal,
    CanonicalWriter,
    Connector,
    ConnectorManifest,
    ConnectorRunResult,
    FetchedBatch,
    SourceObjectIdentity,
    parse_manifest,
)
from archive_sync.connectors.registry import get_connector_factory, known_connectors, register_connector
from archive_sync.connectors.runtime import ContainedVaultWriter, execute_connector, execute_from_manifest
from archive_sync.connectors.sample import SAMPLE_CONNECTOR_ID, SampleConnector

__all__ = [
    "SDK_VERSION",
    "SAMPLE_CONNECTOR_ID",
    "CanonicalProposal",
    "CanonicalWriter",
    "Connector",
    "ConnectorManifest",
    "ConnectorRunResult",
    "ContainedVaultWriter",
    "FetchedBatch",
    "SampleConnector",
    "SourceObjectIdentity",
    "execute_connector",
    "execute_from_manifest",
    "get_connector_factory",
    "known_connectors",
    "parse_manifest",
    "register_connector",
]

_ = _legacy
