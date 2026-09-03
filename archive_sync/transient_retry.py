"""Shared retry helpers for provider / network transient failures."""

from __future__ import annotations

import logging
import re
import socket
import time
import urllib.error
from collections.abc import Callable
from typing import TypeVar

_T = TypeVar("_T")

_TRANSIENT_RE = re.compile(
    r"(broken pipe|connection reset|connection aborted|timed out|timeout|"
    r"deadlock detected|could not serialize|server closed the connection|"
    r"temporary failure|try again|rate.?limit|503|502|504)",
    re.IGNORECASE,
)


def is_transient_error(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, TimeoutError)):
        return True
    if isinstance(exc, (urllib.error.URLError, socket.timeout)):
        return True
    if _TRANSIENT_RE.search(str(exc)) or _TRANSIENT_RE.search(type(exc).__name__):
        return True
    cause = getattr(exc, "__cause__", None)
    if cause is not None and cause is not exc:
        return is_transient_error(cause)
    return False


def call_with_transient_retry(
    fn: Callable[[], _T],
    *,
    logger: logging.Logger,
    label: str,
    attempts: int = 5,
    base_delay_s: float = 1.0,
    max_delay_s: float = 30.0,
) -> _T:
    last_exc: BaseException | None = None
    for attempt in range(1, max(int(attempts), 1) + 1):
        try:
            return fn()
        except BaseException as exc:
            last_exc = exc
            if attempt >= attempts or not is_transient_error(exc):
                raise
            delay = min(max_delay_s, base_delay_s * (2 ** (attempt - 1)))
            logger.warning(
                "%s transient error attempt=%s/%s delay=%.1fs error=%s: %s",
                label,
                attempt,
                attempts,
                delay,
                type(exc).__name__,
                exc,
            )
            time.sleep(delay)
    assert last_exc is not None
    raise last_exc
