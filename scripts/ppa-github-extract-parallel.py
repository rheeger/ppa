#!/usr/bin/env python3
"""Run staged GitHub extraction with bounded parallel workers."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from archive_sync.adapters.github_history import GitHubHistoryAdapter


def main() -> int:
    parser = argparse.ArgumentParser(description="Parallel GitHub archive extraction using gh")
    parser.add_argument(
        "--vault", default=os.environ.get("PPA_PATH", str(Path.home() / "Archive" / "production" / "hf-archives"))
    )
    parser.add_argument("--stage-dir", required=True)
    parser.add_argument("--max-repos", type=int, default=None)
    parser.add_argument("--max-commits-per-repo", type=int, default=None)
    parser.add_argument("--max-threads-per-repo", type=int, default=None)
    parser.add_argument("--max-messages-per-thread", type=int, default=None)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--progress-every", type=int, default=10)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    manifest = GitHubHistoryAdapter().stage_history(
        args.vault,
        args.stage_dir,
        max_repos=args.max_repos,
        max_commits_per_repo=args.max_commits_per_repo,
        max_threads_per_repo=args.max_threads_per_repo,
        max_messages_per_thread=args.max_messages_per_thread,
        workers=args.workers,
        progress_every=args.progress_every,
        verbose=args.verbose,
    )
    counts = manifest.get("counts", {})
    print(
        "ppa-github-extract-parallel:"
        f" repos={counts.get('repos', 0)}"
        f" commits={counts.get('commits', 0)}"
        f" threads={counts.get('threads', 0)}"
        f" messages={counts.get('messages', 0)}"
        f" failures={len(manifest.get('failures', []))}"
        f" elapsed_s={manifest.get('elapsed_seconds', 0)}"
    )
    return 0 if not manifest.get("failures") else 1


if __name__ == "__main__":
    raise SystemExit(main())
