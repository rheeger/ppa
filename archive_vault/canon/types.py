"""Typed identifier parse results. Pure — no I/O, alias ownership, or merges."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

IdentifierKind = Literal["phone", "email", "handle", "url", "unknown"]
IdentifierValidity = Literal["valid", "unknown", "invalid", "opaque"]

CANON_IDENTIFIER_VERSION = "p31.1"


@dataclass(frozen=True, slots=True)
class IdentifierResult:
    kind: IdentifierKind
    original: str
    canonical: str
    validity: IdentifierValidity
    reason: str = ""
    region: str = ""
    extension: str = ""
    canon_version: str = CANON_IDENTIFIER_VERSION

    @property
    def available(self) -> bool:
        return self.validity == "valid" and bool(self.canonical)
