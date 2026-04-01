"""Stratified transitive-closure seed vault slicer for integration tests."""

from __future__ import annotations

import json
import logging
import math
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hfa.vault import extract_wikilinks, iter_note_paths, read_note_file

from .benchmark import _orphan_metrics, _stable_score
from .features import RELATIONSHIP_FIELDS
from .vault_cache import VaultScanCache

logger = logging.getLogger("ppa.slice")


def _format_mins_secs(seconds: float) -> str:
    """Format *seconds* as ``M:SS`` (minutes : zero-padded seconds) for ETAs and elapsed."""
    if not math.isfinite(seconds) or seconds < 0:
        return "?"
    total = int(round(seconds))
    m, s = divmod(total, 60)
    return f"{m}:{s:02d}"


@dataclass
class SliceConfig:
    vault_commit: str = ""
    snapshot_date: str = ""
    seed_uids_by_type: dict[str, list[str]] = field(default_factory=dict)
    cluster_cap: int = 200
    min_cards_per_type: int = 1
    target_percent: float = 5.0


@dataclass
class SliceResult:
    total_source_cards: int = 0
    selected_card_count: int = 0
    cards_by_type: dict[str, int] = field(default_factory=dict)
    orphaned_wikilinks: int = 0
    config: SliceConfig = field(default_factory=SliceConfig)


def _note_uid(vault: Path, rel_path: Path) -> str:
    note = read_note_file(vault / rel_path, vault_root=vault)
    return str(note.frontmatter.get("uid", "")).strip()


def _rel_paths_by_uid(vault: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for rel in iter_note_paths(vault):
        uid = _note_uid(vault, rel)
        if uid:
            out[uid] = rel
    return out


def _iter_relationship_refs(frontmatter: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for key in RELATIONSHIP_FIELDS:
        if key not in frontmatter:
            continue
        val = frontmatter[key]
        if isinstance(val, str):
            refs.append(val)
        elif isinstance(val, list):
            refs.extend(str(x) for x in val)
    return refs


def _transitive_closure(
    vault: Path,
    seeds: set[str],
    *,
    rel_by_uid: dict[str, Path],
    cluster_cap: int,
) -> set[str] | None:
    """BFS closure; return None if cluster_cap would be exceeded."""
    seen: set[str] = set()
    stack = list(seeds)
    while stack:
        uid = stack.pop()
        if uid in seen:
            continue
        if len(seen) >= cluster_cap:
            return None
        seen.add(uid)
        rel = rel_by_uid.get(uid)
        if rel is None:
            continue
        note = read_note_file(vault / rel, vault_root=vault)
        fm = note.frontmatter
        for ref in _iter_relationship_refs(dict(fm)):
            s = ref.strip()
            if s.startswith("[[") and s.endswith("]]"):
                slug = s[2:-2].split("|", 1)[0].strip()
                for cand_uid, cpath in rel_by_uid.items():
                    if cpath.stem == slug or slug.replace(" ", "-").lower() == cpath.stem.lower():
                        if cand_uid not in seen:
                            stack.append(cand_uid)
            elif s.startswith("hfa-"):
                if s not in seen:
                    stack.append(s)
        for slug in extract_wikilinks(note.body):
            for cand_uid, cpath in rel_by_uid.items():
                if cpath.stem == slug or slug.replace(" ", "-").lower() == cpath.stem.lower():
                    if cand_uid not in seen:
                        stack.append(cand_uid)
    return seen


def slice_seed_vault(
    source_vault: Path,
    output_dir: Path,
    config: SliceConfig,
    *,
    progress_every: int = 5000,
    no_cache: bool = False,
) -> SliceResult:
    """Produce a relationally complete vault slice.

    Logs to ``ppa.slice`` (stderr via :mod:`archive_mcp.log`). For large vaults,
    set *progress_every* lower (e.g. 2000) for more frequent scan/copy lines.
    """
    source_vault = Path(source_vault)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    t_scan = time.monotonic()
    logger.info(
        "slice-seed start source=%s target_percent=%s cluster_cap=%s progress_every=%s",
        source_vault.resolve(),
        config.target_percent,
        config.cluster_cap,
        progress_every,
    )
    t_cache = time.monotonic()
    cache = VaultScanCache.build_or_load(
        source_vault,
        tier=1,
        progress_every=progress_every,
        no_cache=no_cache,
    )
    by_type = {t: [Path(p) for p in paths] for t, paths in cache.rel_paths_by_type().items()}
    rel_by_uid = {uid: Path(p) for uid, p in cache.uid_to_rel_path().items()}
    uid_by_path = {Path(p): uid for p, uid in cache.rel_path_to_uid().items()}
    n_total = cache.note_count()
    note_count = n_total
    scan_s = time.monotonic() - t_scan
    cache_elapsed = time.monotonic() - t_cache
    logger.info(
        "slice-seed scan cache_hit=%s notes=%d elapsed=%.1fs",
        cache.is_cache_hit,
        n_total,
        cache_elapsed,
    )
    total = sum(len(v) for v in by_type.values())
    logger.info(
        "slice-seed scan_complete notes_read=%d typed_cards=%d unique_uids=%d elapsed=%s",
        note_count,
        total,
        len(rel_by_uid),
        _format_mins_secs(scan_s),
    )

    seeds: set[str] = set()
    for card_type, paths in by_type.items():
        n = max(
            config.min_cards_per_type,
            min(len(paths), max(1, int(len(paths) * (config.target_percent / 100.0)))),
        )
        n = min(n, len(paths))
        ordered = sorted(paths, key=lambda p: _stable_score(p))
        if config.seed_uids_by_type.get(card_type):
            want = {u for u in config.seed_uids_by_type[card_type] if u in rel_by_uid}
            seeds |= want
        else:
            seeds |= {uid_by_path[p] for p in ordered[:n] if uid_by_path.get(p)}

    logger.info("slice-seed seeds_selected count=%d (before closure)", len(seeds))

    closure_raw = _transitive_closure(
        source_vault,
        seeds,
        rel_by_uid=rel_by_uid,
        cluster_cap=config.cluster_cap,
    )
    hit_cap = closure_raw is None
    expanded = closure_raw if closure_raw is not None else set(seeds)
    if not expanded:
        expanded = set(seeds)

    logger.info("slice-seed closure cards_in_slice=%d hit_cluster_cap=%s", len(expanded), hit_cap)

    t_copy = time.monotonic()
    expanded_list = list(expanded)
    n_copy = len(expanded_list)
    for i, uid in enumerate(expanded_list):
        if progress_every > 0 and n_copy and (i + 1) % progress_every == 0:
            elapsed = time.monotonic() - t_copy
            done = i + 1
            pct = 100.0 * done / n_copy
            rate = done / elapsed if elapsed > 0 else 0.0
            remaining = n_copy - done
            eta_sec = remaining / rate if rate > 0 else float("nan")
            logger.info(
                "slice-seed copy files=%d/%d (%.1f%%) elapsed=%s eta_remaining=%s rate_files_per_s=%.1f",
                done,
                n_copy,
                pct,
                _format_mins_secs(elapsed),
                _format_mins_secs(eta_sec),
                rate,
            )
        rel = rel_by_uid.get(uid)
        if rel is None:
            continue
        src = source_vault / rel
        dst = output_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    copy_s = time.monotonic() - t_copy
    logger.info("slice-seed copy_complete files=%d elapsed=%s", n_copy, _format_mins_secs(copy_s))

    # Minimal _meta for downstream tools
    meta = output_dir / "_meta"
    meta.mkdir(parents=True, exist_ok=True)
    (meta / "identity-map.json").write_text("{}")
    (meta / "sync-state.json").write_text("{}")
    (meta / "benchmark-sample.json").write_text(
        json.dumps({"source_vault": str(source_vault.resolve())}),
        encoding="utf-8",
    )

    counts: dict[str, int] = {}
    for uid in expanded:
        rel = rel_by_uid.get(uid)
        if rel is None:
            continue
        fm = cache.frontmatter_for_rel_path(rel.as_posix())
        t = str(fm.get("type", "") or "unknown")
        counts[t] = counts.get(t, 0) + 1

    om = _orphan_metrics(output_dir)
    logger.info(
        "slice-seed done selected_card_count=%d orphaned_wikilinks=%d total_wall=%s",
        len(expanded),
        int(om.get("orphaned_wikilinks", 0)),
        _format_mins_secs(time.monotonic() - t_scan),
    )
    return SliceResult(
        total_source_cards=total,
        selected_card_count=len(expanded),
        cards_by_type=counts,
        orphaned_wikilinks=int(om.get("orphaned_wikilinks", 0)),
        config=config,
    )


def load_slice_config(path: Path) -> SliceConfig:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return SliceConfig(
        vault_commit=str(data.get("vault_commit", "") or ""),
        snapshot_date=str(data.get("snapshot_date", "") or ""),
        seed_uids_by_type={str(k): list(v) for k, v in (data.get("seed_uids_by_type") or {}).items()},
        cluster_cap=int(data.get("cluster_cap", 200)),
        min_cards_per_type=int(data.get("min_cards_per_type", 1)),
        target_percent=float(data.get("target_percent", 5.0)),
    )


def build_slice_docker_image(slice_dir: Path, tag: str) -> str:
    """Build a Docker image containing the slice vault for CI."""
    slice_dir = Path(slice_dir).resolve()
    with tempfile.TemporaryDirectory() as build_ctx:
        build_path = Path(build_ctx)
        dockerfile = build_path / "Dockerfile"
        dockerfile.write_text(
            "FROM busybox:latest\nCOPY slice/ /vault/\nVOLUME /vault\n",
            encoding="utf-8",
        )
        shutil.copytree(slice_dir, build_path / "slice")
        subprocess.run(
            ["docker", "build", "-t", tag, "."],
            cwd=str(build_path),
            check=True,
        )
    return tag
