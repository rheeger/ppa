"""Consistent CLI logging helpers for archive-sync workflows."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime


def log_cli_step(source_id: str, step_number: int, total_steps: int, title: str, detail: str = "") -> None:
    timestamp = datetime.now().isoformat(timespec="seconds")
    suffix = f" {detail}" if detail else ""
    print(f"[{timestamp}] {source_id}: step {step_number}/{total_steps} {title}{suffix}", flush=True)


@dataclass(slots=True)
class CliProgressReporter:
    source_id: str
    step_number: int
    total_steps: int
    stage: str
    total_items: int
    progress_every: int
    enabled: bool
    min_interval_seconds: float = 5.0
    started_at: float = field(default_factory=time.time)
    last_log_at: float = field(default_factory=time.time)
    last_logged_count: int = 0

    def _should_log(self, count: int) -> bool:
        if not self.enabled or count <= 0:
            return False
        if self.progress_every and count - self.last_logged_count >= self.progress_every:
            return True
        return (time.time() - self.last_log_at) >= self.min_interval_seconds

    def _progress_bar(self, count: int, width: int = 24) -> str:
        if self.total_items <= 0:
            return "[" + ("." * width) + "]"
        ratio = min(max(count / self.total_items, 0.0), 1.0)
        filled = int(ratio * width)
        return "[" + ("#" * filled) + ("." * (width - filled)) + "]"

    def _emit(self, message: str, count: int) -> None:
        self.last_log_at = time.time()
        self.last_logged_count = count
        timestamp = datetime.now().isoformat(timespec="seconds")
        print(f"[{timestamp}] {self.source_id}: {message}", flush=True)

    def update(self, count: int, *, extra: str = "") -> None:
        if not self._should_log(count):
            return
        total_label = self.total_items if self.total_items > 0 else "?"
        percent = (count / self.total_items * 100.0) if self.total_items > 0 else 0.0
        elapsed = time.time() - self.started_at
        rate = count / elapsed if elapsed > 0 else 0.0
        remaining = max(self.total_items - count, 0) if self.total_items > 0 else 0
        eta = (remaining / rate) if rate > 0 and self.total_items > 0 else 0.0
        suffix = f" {extra}" if extra else ""
        self._emit(
            f"step {self.step_number}/{self.total_steps} {self.stage} "
            f"{self._progress_bar(count)} {count}/{total_label} ({percent:.1f}%) "
            f"elapsed={elapsed:.1f}s rate={rate:.1f}/s eta={eta:.1f}s{suffix}",
            count,
        )

    def complete(self, count: int, *, extra: str = "") -> None:
        if not self.enabled:
            return
        total_label = self.total_items if self.total_items > 0 else count
        elapsed = time.time() - self.started_at
        suffix = f" {extra}" if extra else ""
        self._emit(
            f"step {self.step_number}/{self.total_steps} {self.stage} "
            f"{self._progress_bar(total_label if isinstance(total_label, int) else count)} "
            f"complete {count}/{total_label} elapsed={elapsed:.1f}s{suffix}",
            count,
        )
