"""Typed exception hierarchy for PPA operations.

Commands raise these instead of returning error strings. This allows the CLI
to catch and exit(1) with a message, and the MCP layer to catch and return
a JSON error payload. The previous pattern of returning "Vault not found" as
a plain string made it impossible for callers to distinguish errors from results.
"""

from __future__ import annotations


class PpaError(Exception):
    """Base for all PPA errors."""


class VaultNotFoundError(PpaError):
    """Vault path missing or not a directory."""


class IndexUnavailableError(PpaError):
    """Index or store could not be constructed (e.g. missing DSN, DB error)."""


class EmbeddingError(PpaError):
    """Embedding provider or vector operation failed."""


class SeedLinksDisabledError(PpaError):
    """Seed link operations when PPA_SEED_LINKS_ENABLED is off."""


class InvalidInputError(PpaError):
    """Caller input could not be parsed or validated."""


class ExtractionError(PpaError):
    """Email extraction pipeline error."""


class EntityResolutionError(PpaError):
    """Entity resolution error."""
