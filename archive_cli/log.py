"""Centralized logging configuration for PPA.

All PPA modules use the 'ppa.*' logger namespace. Output goes to stderr
exclusively — stdout is reserved for MCP JSON-RPC in serve mode and for
CLI command output (JSON) in non-serve mode. This separation is critical:
any print() to stdout during MCP serve will corrupt the protocol stream.

Usage:
    from archive_cli.log import configure_logging
    configure_logging(verbose=True)  # call once at startup in __main__.py

All modules should use:
    import logging
    logger = logging.getLogger("ppa.<module_name>")
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Final

from archive_engine.redaction import redacting_formatter

_PPA_LOGGER_NAME: Final = "ppa"
_CONFIGURED = False
_FILE_HANDLER_PATH: Path | None = None


class _FlushingFileHandler(logging.FileHandler):
    """Flush after each record so long-running jobs show up in ``tail -f`` / editors immediately."""

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        self.flush()


def configure_logging(verbose: bool = False) -> None:
    """Attach stderr logging for the ``ppa`` namespace.

    Idempotent: repeated calls do not duplicate handlers.
    ``verbose=True`` sets the ``ppa`` logger to DEBUG; otherwise INFO.
    """
    global _CONFIGURED
    root_ppa = logging.getLogger(_PPA_LOGGER_NAME)
    root_ppa.setLevel(logging.DEBUG if verbose else logging.INFO)
    if _CONFIGURED:
        ensure_redacting_handlers()
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(redacting_formatter())
    root_ppa.addHandler(handler)
    root_ppa.propagate = False
    _CONFIGURED = True
    ensure_redacting_handlers()


def attach_file_log(path: Path) -> None:
    """Append duplicate ``ppa.*`` log lines to *path* (same format as stderr).

    Idempotent per path: calling twice with the same path does not add a second handler.
    Parent directories are created when missing.
    """
    global _FILE_HANDLER_PATH
    path = Path(path).resolve()
    if _FILE_HANDLER_PATH == path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    root_ppa = logging.getLogger(_PPA_LOGGER_NAME)
    handler = _FlushingFileHandler(path, encoding="utf-8")
    handler.setFormatter(redacting_formatter())
    root_ppa.addHandler(handler)
    _FILE_HANDLER_PATH = path
    ensure_redacting_handlers()


def ensure_redacting_handlers() -> None:
    """Upgrade existing ``ppa.*`` handlers so secrets cannot leak through an older formatter."""

    root_ppa = logging.getLogger(_PPA_LOGGER_NAME)
    formatter = redacting_formatter()
    for handler in root_ppa.handlers:
        handler.setFormatter(formatter)
