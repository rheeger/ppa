"""Typed engine errors.

Distinguish retryable failure, denied access, incompatible state, and
unavailable capability. Transport layers (CLI/MCP) format these; the engine
does not import those transports.
"""

from __future__ import annotations


class EngineError(Exception):
    """Base for archive_engine failures."""


class RetryableEngineError(EngineError):
    """Transient failure that a caller may retry."""


class AccessDeniedError(EngineError):
    """Explicit deny from AccessContext or a contained-path rejection.

    Exact-read compatibility still returns a not-found envelope for denied
    vault paths (P05-A). Raise this when a caller asks the engine to treat
    deny as an error rather than a miss.
    """


class EgressDeniedError(AccessDeniedError):
    """Provider destination or payload is not permitted for the current policy.

    Raised before transport. Callers must not fall back to another destination.
    """


class IncompatibleStateError(EngineError):
    """Schema, contract version, or identity binding does not match."""


class CapabilityUnavailableError(EngineError):
    """Optional port or backend is not attached (not a success)."""


class IncompatibleContractError(IncompatibleStateError):
    """Serialized shared record is missing fields or has an unknown version."""


class QueryValidationError(IncompatibleContractError):
    """Unknown field, operator, type, or over-budget predicate AST."""


class CursorInvalidError(IncompatibleStateError):
    """Cursor integrity, snapshot, policy, or predicate fingerprint mismatch."""


class ConfigError(IncompatibleStateError):
    """Instance configuration is malformed, unknown-critical, or incompatible."""


class PublisherBusyError(RetryableEngineError):
    """Another process holds the publication lease."""


class StaleContextError(IncompatibleStateError):
    """Generation offsets no longer match the canonical file revision."""


class SpanRequiredError(IncompatibleStateError):
    """A span-required request cannot quote because the source span is unavailable."""
