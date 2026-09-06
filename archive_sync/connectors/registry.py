"""Connector factory registry.

Contributors add a factory. Runtime looks up by id. There is no source-specific
dispatch conditional in the SDK.
"""

from __future__ import annotations

from collections.abc import Callable

from archive_sync.connectors.contracts import Connector

ConnectorFactory = Callable[[], Connector]

_FACTORIES: dict[str, ConnectorFactory] = {}


def register_connector(connector_id: str, factory: ConnectorFactory) -> None:
    key = connector_id.strip()
    if not key:
        raise ValueError("connector_id is required")
    if key in _FACTORIES:
        raise ValueError(f"duplicate connector registration: {key}")
    _FACTORIES[key] = factory


def get_connector_factory(connector_id: str) -> ConnectorFactory:
    try:
        return _FACTORIES[connector_id]
    except KeyError as exc:
        raise KeyError(f"unknown connector {connector_id!r}") from exc


def known_connectors() -> tuple[str, ...]:
    return tuple(sorted(_FACTORIES))
