"""PPA commands layer — the single source of truth for all PPA operations.

Both the MCP server (server.py) and CLI (__main__.py) are thin wrappers
over this package. Every command function:
- Takes explicit dependencies (store, index, logger) — no global state
- Returns a dict — callers decide formatting (JSON, text, etc.)
- Raises PpaError subclasses — callers decide error presentation
- Logs timing and key parameters via the Act 1 logging subsystem

This package exists because server.py and __main__.py were "siblings" — both
reaching into store/index independently with different error handling and output
formatting. The commands layer makes them "layers" over a shared foundation.
"""

from __future__ import annotations

from . import (admin, confidence, explain, graph, query, read, search,
               seed_links, status)

__all__ = [
    "admin",
    "confidence",
    "explain",
    "graph",
    "query",
    "read",
    "search",
    "seed_links",
    "status",
]
