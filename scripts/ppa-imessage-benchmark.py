#!/usr/bin/env python3
"""Benchmark Apple Messages processing across worker counts."""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from pathlib import Path

from archive_sync.adapters.imessage import IMessageAdapter


def _workers_list(value: str) -> list[int]:
    parsed = [int(part.strip()) for part in value.split(",") if part.strip()]
    return [worker for worker in parsed if worker > 0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark iMessage processing worker counts")
    parser.add_argument("--vault", default=os.environ.get("PPA_PATH", str(Path.home() / "Archive" / "tests" / "hf-archives-imessage-scratch")))
    parser.add_argument("--snapshot-dir", required=True)
    parser.add_argument("--source-label", default="local-messages")
    parser.add_argument("--max-messages", type=int, default=20000)
    parser.add_argument("--workers", default="1,4,8", help="Comma-separated worker counts")
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--mode", choices=("fetch", "dry-run"), default="fetch")
    args = parser.parse_args()

    adapter = IMessageAdapter()
    worker_counts = _workers_list(args.workers)
    if not worker_counts:
        raise SystemExit("No valid worker counts provided")

    for worker_count in worker_counts:
        durations: list[float] = []
        produced: list[int] = []
        for _ in range(max(1, args.repeats)):
            started = time.perf_counter()
            if args.mode == "fetch":
                items = adapter.fetch(
                    args.vault,
                    {},
                    snapshot_dir=args.snapshot_dir,
                    source_label=args.source_label,
                    max_messages=args.max_messages,
                    workers=worker_count,
                )
                produced.append(len(items))
            else:
                result = adapter.ingest(
                    args.vault,
                    dry_run=True,
                    snapshot_dir=args.snapshot_dir,
                    source_label=args.source_label,
                    max_messages=args.max_messages,
                    workers=worker_count,
                )
                produced.append(result.created + result.merged + result.conflicted + result.skipped)
            durations.append(time.perf_counter() - started)
        mean_duration = statistics.mean(durations)
        print(
            f"workers={worker_count} mode={args.mode} repeats={len(durations)} "
            f"mean_seconds={mean_duration:.4f} min_seconds={min(durations):.4f} "
            f"max_seconds={max(durations):.4f} produced={produced[0] if produced else 0}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
