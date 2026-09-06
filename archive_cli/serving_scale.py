"""Honest serving-index scale probes. Missing envelope is blocked, not waived."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from archive_cli.index_config import (
    get_serving_index_max_rss_mb,
    get_serving_train_memory_mb,
    get_serving_train_sample,
)

SCALE_PROFILE_MILLION = "million_vector"
PRODUCTION_VECTOR_DIM = 1536
MILLION_VECTOR_N = 1_000_000
MAX_NLIST = 4096


def physical_memory_bytes() -> int:
    if sys.platform == "darwin":
        try:
            return int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip())
        except (OSError, subprocess.CalledProcessError, ValueError):
            return 0
    try:
        pages = int(os.sysconf("SC_PHYS_PAGES"))
        page = int(os.sysconf("SC_PAGE_SIZE"))
        return pages * page
    except (OSError, ValueError, AttributeError):
        return 0


def estimate_vector_envelope(*, n: int, dimension: int) -> dict[str, Any]:
    """Byte envelope for embeddings + IVF artifacts. No latency/recall claims."""

    nlist = min(max(int(n**0.5), 1), MAX_NLIST)
    embeddings = n * dimension * 4
    centroids = nlist * dimension * 4
    assignments = n * 4
    train_sample = min(get_serving_train_sample(), n)
    train_sample_bytes = train_sample * dimension * 4
    query_rss = embeddings + centroids + assignments
    sidecar_headroom = 2 * 1024 * 1024 * 1024
    train_rss = embeddings + train_sample_bytes + centroids + sidecar_headroom
    disk = embeddings + centroids + assignments + sidecar_headroom
    return {
        "n": n,
        "dimension": dimension,
        "nlist": nlist,
        "embeddings_bytes": embeddings,
        "centroids_bytes": centroids,
        "assignments_bytes": assignments,
        "train_sample": train_sample,
        "train_sample_bytes": train_sample_bytes,
        "sidecar_headroom_bytes": sidecar_headroom,
        "query_rss_bytes": query_rss,
        "train_rss_bytes": train_rss,
        "disk_bytes": disk,
    }


def probe_million_vector_scale(*, root: Path | None = None) -> dict[str, Any]:
    """Decide whether the production-dim million-vector profile can run.

    Dimension 8 is algorithmic evidence only and is not production RSS proof.
    This probe never fabricates Recall@k or latency.
    """

    envelope = estimate_vector_envelope(n=MILLION_VECTOR_N, dimension=PRODUCTION_VECTOR_DIM)
    rss_cap_mb = get_serving_index_max_rss_mb()
    train_cap_mb = get_serving_train_memory_mb()
    rss_cap = rss_cap_mb * 1024 * 1024
    train_cap = train_cap_mb * 1024 * 1024
    phys = physical_memory_bytes()
    disk_root = root or Path.cwd()
    disk_free = shutil.disk_usage(disk_root).free
    reasons: list[str] = []
    if envelope["train_rss_bytes"] > train_cap:
        reasons.append(
            f"train_rss_bytes={envelope['train_rss_bytes']} exceeds PPA_SERVING_TRAIN_MEMORY_MB={train_cap_mb}"
        )
    if envelope["query_rss_bytes"] > rss_cap:
        reasons.append(
            f"query_rss_bytes={envelope['query_rss_bytes']} exceeds PPA_SERVING_INDEX_MAX_RSS_MB={rss_cap_mb}"
        )
    if phys and envelope["train_rss_bytes"] > phys:
        reasons.append(f"train_rss_bytes={envelope['train_rss_bytes']} exceeds physical_memory_bytes={phys}")
    if envelope["disk_bytes"] > disk_free:
        reasons.append(f"disk_bytes={envelope['disk_bytes']} exceeds disk_free_bytes={disk_free}")
    return {
        "profile": SCALE_PROFILE_MILLION,
        "status": "blocked" if reasons else "runnable",
        "executed": False,
        "production_proven": False,
        "algorithmic_only": False,
        "reasons": reasons,
        "envelope": envelope,
        "constraints": {
            "serving_index_max_rss_mb": rss_cap_mb,
            "serving_train_memory_mb": train_cap_mb,
            "physical_memory_bytes": phys,
            "disk_free_bytes": disk_free,
            "disk_root": str(disk_root),
        },
        "note": (
            "Million-vector production proof requires dimension 1536. "
            "Missing envelope is blocked, not waived. Recall/latency were not invented."
        ),
    }
