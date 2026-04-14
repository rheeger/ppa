#!/usr/bin/env python3
"""Run multiple Calendar extraction windows in parallel."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _build_windows(start_year: int, end_year: int, years_per_window: int) -> list[tuple[str, str, str]]:
    windows: list[tuple[str, str, str]] = []
    year = start_year
    while year <= end_year:
        window_end = min(year + years_per_window, end_year + 1)
        label = f"{year}" if window_end == year + 1 else f"{year}-{window_end - 1}"
        time_min = f"{year}-01-01T00:00:00Z"
        time_max = f"{window_end}-01-01T00:00:00Z"
        windows.append((label, time_min, time_max))
        year = window_end
    return windows


def _window_complete(output_root: Path, label: str) -> bool:
    state_path = output_root / label / "_meta" / "extract-state.json"
    if not state_path.exists():
        return False
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    return bool(payload.get("complete"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Parallel Calendar extraction by date windows")
    parser.add_argument("--account-email", default="rheeger@gmail.com")
    parser.add_argument("--calendar-id", default="primary")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--start-year", type=int, default=2004)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--years-per-window", type=int, default=1)
    parser.add_argument("--max-concurrent", type=int, default=4)
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument("--python-bin", default=sys.executable)
    args = parser.parse_args()

    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    windows = deque(
        (label, time_min, time_max)
        for label, time_min, time_max in _build_windows(args.start_year, args.end_year, args.years_per_window)
    )
    inflight: dict[str, subprocess.Popen[str]] = {}

    while windows or inflight:
        while windows and len(inflight) < max(1, args.max_concurrent):
            label, time_min, time_max = windows.popleft()
            if _window_complete(output_root, label):
                print(json.dumps({"status": "skip-complete", "label": label}, indent=2), flush=True)
                continue
            output_dir = output_root / label
            output_dir.mkdir(parents=True, exist_ok=True)
            env = os.environ.copy()
            env["OPENCLAW_GOOGLE_TOKEN_CACHE_DIR"] = str(output_dir / "_meta" / "token-cache")
            command = [
                args.python_bin,
                str(REPO_ROOT / "scripts" / "ppa-calendar-extract.py"),
                "--account-email",
                args.account_email,
                "--calendar-id",
                args.calendar_id,
                "--output-dir",
                str(output_dir),
                "--time-min",
                time_min,
                "--time-max",
                time_max,
                "--page-size",
                str(args.page_size),
            ]
            proc = subprocess.Popen(
                command,
                cwd=str(REPO_ROOT),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            inflight[label] = proc
            print(
                json.dumps(
                    {"status": "started", "label": label, "time_min": time_min, "time_max": time_max, "pid": proc.pid},
                    indent=2,
                ),
                flush=True,
            )

        time.sleep(2)
        completed: list[str] = []
        for label, proc in inflight.items():
            code = proc.poll()
            if code is None:
                continue
            stderr = (proc.stderr.read() if proc.stderr is not None else "").strip()
            payload = {"status": "finished", "label": label, "exit_code": code}
            if stderr:
                payload["stderr"] = stderr[-500:]
            print(json.dumps(payload, indent=2), flush=True)
            completed.append(label)
            if code != 0:
                return code
        for label in completed:
            inflight.pop(label, None)

    print(json.dumps({"status": "all-complete"}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
