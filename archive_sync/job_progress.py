"""Elapsed / ETA log lines for long extract and maintain jobs."""

from __future__ import annotations

import logging
import time


def fmt_elapsed(seconds: float) -> str:
    total = int(round(max(0.0, seconds)))
    m, s = divmod(total, 60)
    return f"{m}:{s:02d}"


def log_progress(
    log: logging.Logger,
    prefix: str,
    i: int,
    n: int,
    t0: float,
    extra: str = "",
) -> None:
    elapsed = time.monotonic() - t0
    rate = i / elapsed if elapsed > 0 else 0.0
    remain = (n - i) / rate if rate > 0 else 0.0
    pct = (100.0 * i / n) if n else 100.0
    log.info(
        "%s %s/%s (%.1f%%) elapsed=%s eta_remaining=%s rate_per_s=%.2f %s",
        prefix,
        i,
        n,
        pct,
        fmt_elapsed(elapsed),
        fmt_elapsed(remain),
        rate,
        extra,
    )
