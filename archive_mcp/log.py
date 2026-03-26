"""Centralized logging configuration for PPA.

All PPA modules use the 'ppa.*' logger namespace. Output goes to stderr
exclusively — stdout is reserved for MCP JSON-RPC in serve mode and for
CLI command output (JSON) in non-serve mode. This separation is critical:
any print() to stdout during MCP serve will corrupt the protocol stream.

Usage:
    from archive_mcp.log import configure_logging
    configure_logging(verbose=True)  # call once at startup in __main__.py

All modules should use:
    import logging
    logger = logging.getLogger("ppa.<module_name>")
"""

from __future__ import annotations

import logging
import sys
from typing import Final

_PPA_LOGGER_NAME: Final = "ppa"
_CONFIGURED = False


def configure_logging(verbose: bool = False) -> None:
    """Attach stderr logging for the ``ppa`` namespace.

    Idempotent: repeated calls do not duplicate handlers.
    ``verbose=True`` sets the ``ppa`` logger to DEBUG; otherwise INFO.
    """
    global _CONFIGURED
    root_ppa = logging.getLogger(_PPA_LOGGER_NAME)
    root_ppa.setLevel(logging.DEBUG if verbose else logging.INFO)
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s %(message)s"))
    root_ppa.addHandler(handler)
    root_ppa.propagate = False
    _CONFIGURED = True
