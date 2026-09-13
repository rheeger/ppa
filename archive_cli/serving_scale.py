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


def parse_darwin_vm_stat(text: str) -> int:
    """Estimate unused RAM from ``vm_stat``. Free + inactive + speculative + purgeable."""

    page_size = 4096
    counts: dict[str, int] = {}
    for raw in text.splitlines():
        line = raw.strip()
        lower = line.lower()
        if "page size of" in lower:
            parts = lower.replace(".", " ").split()
            for idx, part in enumerate(parts):
                if part.isdigit() and idx > 0 and parts[idx - 1] == "of":
                    page_size = int(part)
                    break
            continue
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        digits = "".join(ch for ch in value if ch.isdigit())
        if digits:
            counts[name.strip().lower()] = int(digits)
    pages = (
        counts.get("pages free", 0)
        + counts.get("pages inactive", 0)
        + counts.get("pages speculative", 0)
        + counts.get("pages purgeable", 0)
    )
    return pages * page_size


def available_memory_bytes() -> int:
    """Currently unused RAM. 0 means the host did not report a number."""

    meminfo = Path("/proc/meminfo")
    try:
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                parts = line.split()
                if len(parts) >= 2:
                    return int(parts[1]) * 1024
    except (OSError, ValueError):
        pass
    if sys.platform == "darwin":
        try:
            text = subprocess.check_output(["vm_stat"], text=True)
        except (OSError, subprocess.CalledProcessError):
            text = ""
        parsed = parse_darwin_vm_stat(text) if text else 0
        if parsed:
            return parsed
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        available_pages = int(os.sysconf("SC_AVPHYS_PAGES"))
    except (AttributeError, OSError, TypeError, ValueError):
        return 0
    if page_size <= 0 or available_pages <= 0:
        return 0
    return page_size * available_pages


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
