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

from .benchmark import (_orphan_metrics, _orphan_metrics_from_frontmatters,
                        _stable_score)
from .features import iter_string_values
from .index_config import get_primary_user_uid
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
    primary_user_uid: str = ""


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


def _normalize_slug(value: str) -> str:
    return value.strip().replace(" ", "-").lower()


def _build_uid_by_stem(vault: Path, rel_by_uid: dict[str, Path]) -> dict[str, str]:
    """Fallback for non-cached paths (fixture vaults, etc.)."""
    out: dict[str, str] = {}
    for uid, rel_path in rel_by_uid.items():
        stem = rel_path.stem.strip()
        if not stem:
            continue
        out.setdefault(stem, uid)
        out.setdefault(_normalize_slug(stem), uid)
        try:
            note = read_note_file(vault / rel_path, vault_root=vault)
        except OSError:
            continue
        frontmatter = dict(note.frontmatter)
        summary = str(frontmatter.get("summary", "") or "").strip()
        if summary:
            out.setdefault(summary, uid)
            out.setdefault(_normalize_slug(summary), uid)
        if str(frontmatter.get("type", "") or "") == "person":
            for alias in frontmatter.get("aliases", []) or []:
                alias_text = str(alias).strip()
                if alias_text:
                    out.setdefault(alias_text, uid)
                    out.setdefault(_normalize_slug(alias_text), uid)
            for email in frontmatter.get("emails", []) or []:
                email_text = str(email).strip()
                if email_text:
                    out.setdefault(email_text, uid)
    return out


def _resolve_ref_to_uid(ref: str, *, rel_by_uid: dict[str, Path], uid_by_stem: dict[str, str]) -> str | None:
    text = ref.strip()
    if not text:
        return None
    if text in rel_by_uid:
        return text
    if text.startswith("[[") and text.endswith("]]"):
        text = text[2:-2].split("|", 1)[0].strip()
        if not text:
            return None
    return uid_by_stem.get(text) or uid_by_stem.get(_normalize_slug(text))


def _iter_note_refs(frontmatter: dict[str, Any], body: str) -> list[str]:
    refs: list[str] = []
    for value in frontmatter.values():
        for text in iter_string_values(value):
            s = text.strip()
            if not s:
                continue
            if s.startswith("[[") and s.endswith("]]"):
                refs.append(s[2:-2].split("|", 1)[0].strip())
            elif s.startswith("hfa-"):
                refs.append(s)
    refs.extend(extract_wikilinks(body))
    return refs


def _closure_single_seed(
    vault: Path,
    seed_uid: str,
    *,
    rel_by_uid: dict[str, Path],
    uid_by_stem: dict[str, str],
    cluster_cap: int,
    already_included: set[str],
    enforce_cluster_cap: bool = True,
    frontmatter_by_uid: dict[str, dict[str, Any]] | None = None,
) -> set[str] | None:
    """BFS closure from a single seed; return None if cluster_cap is exceeded.

    When *frontmatter_by_uid* is provided, frontmatter refs are resolved from
    the in-memory cache (no disk I/O). Body wikilinks are skipped in cached mode
    — the dangling-reference pass catches any residual orphans.
    """
    seen: set[str] = set()
    stack = [seed_uid]
    while stack:
        uid = stack.pop()
        if uid in seen or uid in already_included:
            continue
        if enforce_cluster_cap and len(seen) >= cluster_cap:
            return None
        seen.add(uid)
        rel = rel_by_uid.get(uid)
        if rel is None:
            continue
        if frontmatter_by_uid is not None and uid in frontmatter_by_uid:
            fm = frontmatter_by_uid[uid]
            for ref in _iter_note_refs(fm, ""):
                ref_uid = _resolve_ref_to_uid(ref, rel_by_uid=rel_by_uid, uid_by_stem=uid_by_stem)
                if ref_uid and ref_uid not in seen and ref_uid not in already_included:
                    stack.append(ref_uid)
        else:
            note = read_note_file(vault / rel, vault_root=vault)
            for ref in _iter_note_refs(dict(note.frontmatter), note.body):
                ref_uid = _resolve_ref_to_uid(ref, rel_by_uid=rel_by_uid, uid_by_stem=uid_by_stem)
                if ref_uid and ref_uid not in seen and ref_uid not in already_included:
                    stack.append(ref_uid)
    return seen


def _resolve_dangling_references(
    source_vault: Path,
    accumulated: set[str],
    rel_by_uid: dict[str, Path],
    uid_by_stem: dict[str, str],
    *,
    max_rounds: int = 3,
    frontmatter_by_uid: dict[str, dict[str, Any]] | None = None,
) -> tuple[set[str], int]:
    added_total = 0
    for round_idx in range(1, max_rounds + 1):
        added_this_round: set[str] = set()
        for uid in sorted(accumulated):
            rel = rel_by_uid.get(uid)
            if rel is None:
                continue
            if frontmatter_by_uid is not None and uid in frontmatter_by_uid:
                fm = frontmatter_by_uid[uid]
                refs = _iter_note_refs(fm, "")
            else:
                note = read_note_file(source_vault / rel, vault_root=source_vault)
                refs = _iter_note_refs(dict(note.frontmatter), note.body)
            for ref in refs:
                ref_uid = _resolve_ref_to_uid(ref, rel_by_uid=rel_by_uid, uid_by_stem=uid_by_stem)
                if ref_uid and ref_uid not in accumulated:
                    added_this_round.add(ref_uid)
        if not added_this_round:
            logger.info("slice-seed dangling_resolve round=%d added=0 — stable", round_idx)
            break
        accumulated |= added_this_round
        added_total += len(added_this_round)
        logger.info(
            "slice-seed dangling_resolve round=%d added=%d total=%d",
            round_idx,
            len(added_this_round),
            len(accumulated),
        )
    return accumulated, added_total


def slice_seed_vault(
    source_vault: Path,
    output_dir: Path,
    config: SliceConfig,
    *,
    progress_every: int = 5000,
    no_cache: bool = False,
    dangling_rounds: int = 3,
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
    cache_elapsed = time.monotonic() - t_cache
    logger.info(
        "slice-seed cache loaded cache_hit=%s elapsed=%.1fs",
        cache.is_cache_hit,
        cache_elapsed,
    )

    t_lookup = time.monotonic()
    n_total = cache.note_count()
    use_fast_path = cache.is_cache_hit or n_total > 10_000
    if use_fast_path:
        by_type_str, rel_by_uid_str, uid_by_path_str, uid_by_stem, frontmatter_by_uid = (
            cache.slice_lookup_tables(progress_every=100_000)
        )
        by_type = {t: [Path(p) for p in paths] for t, paths in by_type_str.items()}
        rel_by_uid: dict[str, Path] = {uid: Path(p) for uid, p in rel_by_uid_str.items()}
        uid_by_path: dict[str | Path, str] = {}
        for rp_s, uid_s in uid_by_path_str.items():
            uid_by_path[rp_s] = uid_s
            uid_by_path[Path(rp_s)] = uid_s
    else:
        by_type = {t: [Path(p) for p in paths] for t, paths in cache.rel_paths_by_type().items()}
        rel_by_uid = {uid: Path(p) for uid, p in cache.uid_to_rel_path().items()}
        uid_by_path = {Path(p): uid for p, uid in cache.rel_path_to_uid().items()}
        uid_by_stem = _build_uid_by_stem(source_vault, rel_by_uid)
        frontmatter_by_uid = None

    lookup_s = time.monotonic() - t_lookup
    total = sum(len(v) for v in by_type.values())
    logger.info(
        "slice-seed scan_complete notes=%d typed_cards=%d unique_uids=%d lookup_elapsed=%s total_scan=%s",
        n_total,
        total,
        len(rel_by_uid),
        _format_mins_secs(lookup_s),
        _format_mins_secs(time.monotonic() - t_scan),
    )

    primary_user_uid = str(config.primary_user_uid or get_primary_user_uid() or "").strip()
    expanded: set[str] = set()
    selected_seeds: set[str] = set()
    dropped_seeds: set[str] = set()
    seed_count = 0
    t_closure = time.monotonic()

    def _accept_seed(seed_uid: str, *, mandatory: bool = False) -> bool:
        nonlocal seed_count, expanded, selected_seeds, dropped_seeds
        if seed_uid in selected_seeds:
            return True
        closure = _closure_single_seed(
            source_vault,
            seed_uid,
            rel_by_uid=rel_by_uid,
            uid_by_stem=uid_by_stem,
            cluster_cap=config.cluster_cap,
            already_included=expanded,
            enforce_cluster_cap=not mandatory,
            frontmatter_by_uid=frontmatter_by_uid,
        )
        if closure is None:
            dropped_seeds.add(seed_uid)
            return False
        expanded |= closure
        selected_seeds.add(seed_uid)
        seed_count += 1
        return True

    if primary_user_uid and primary_user_uid in rel_by_uid:
        logger.info("slice-seed primary_user_uid=%s (guaranteed anchor)", primary_user_uid)
        if not _accept_seed(primary_user_uid, mandatory=True):
            logger.warning("slice-seed primary_user_uid=%s exceeded cluster_cap but was retained", primary_user_uid)

    type_idx = 0
    total_types = len(by_type)
    for card_type in sorted(by_type):
        type_idx += 1
        paths = by_type[card_type]
        desired = max(
            config.min_cards_per_type,
            min(len(paths), max(1, int(len(paths) * (config.target_percent / 100.0)))),
        )
        desired = min(desired, len(paths))
        if config.seed_uids_by_type.get(card_type):
            candidates = [uid for uid in config.seed_uids_by_type[card_type] if uid in rel_by_uid]
        else:
            ordered = sorted(paths, key=lambda p: _stable_score(p))
            candidates = [uid_by_path[p] for p in ordered if uid_by_path.get(p)]
        accepted = 0
        for candidate_uid in candidates:
            if accepted >= desired:
                break
            if candidate_uid == primary_user_uid:
                accepted += 1
                continue
            if _accept_seed(candidate_uid):
                accepted += 1
        elapsed_closure = time.monotonic() - t_closure
        logger.info(
            "slice-seed type_done [%d/%d] type=%s desired=%d accepted=%d seeds_total=%d expanded=%d dropped=%d elapsed=%s",
            type_idx,
            total_types,
            card_type,
            desired,
            accepted,
            seed_count,
            len(expanded),
            len(dropped_seeds),
            _format_mins_secs(elapsed_closure),
        )

    logger.info(
        "slice-seed seeds_selected count=%d expanded=%d (before dangling resolution) closure_elapsed=%s",
        seed_count,
        len(expanded),
        _format_mins_secs(time.monotonic() - t_closure),
    )

    if not expanded:
        expanded = set(selected_seeds)
    expanded, dangling_added = _resolve_dangling_references(
        source_vault,
        expanded,
        rel_by_uid,
        uid_by_stem,
        max_rounds=max(0, int(dangling_rounds)),
        frontmatter_by_uid=frontmatter_by_uid,
    )
    logger.info(
        "slice-seed closure cards_in_slice=%d dropped_seeds=%d dangling_added=%d total_closure=%s",
        len(expanded),
        len(dropped_seeds),
        dangling_added,
        _format_mins_secs(time.monotonic() - t_closure),
    )

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

    # Pre-build tier-2 cache so downstream rebuild-indexes hits immediately
    t_precache = time.monotonic()
    logger.info("slice-seed pre-building tier-2 vault cache for output dir")
    VaultScanCache.build_or_load(
        output_dir, tier=2, progress_every=progress_every, no_cache=False,
    )
    logger.info(
        "slice-seed tier-2 cache ready elapsed=%s",
        _format_mins_secs(time.monotonic() - t_precache),
    )

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
        if frontmatter_by_uid and uid in frontmatter_by_uid:
            t = str(frontmatter_by_uid[uid].get("type", "") or "unknown")
        else:
            rel = rel_by_uid.get(uid)
            if rel is None:
                continue
            fm = cache.frontmatter_for_rel_path(rel.as_posix())
            t = str(fm.get("type", "") or "unknown")
        counts[t] = counts.get(t, 0) + 1

    t_orphan = time.monotonic()
    if frontmatter_by_uid:
        slice_fm = {uid: frontmatter_by_uid[uid] for uid in expanded if uid in frontmatter_by_uid}
        slice_rel = {uid: rel_by_uid[uid] for uid in expanded if uid in rel_by_uid}
        om = _orphan_metrics_from_frontmatters(slice_rel, slice_fm)
    else:
        om = _orphan_metrics(output_dir)
    orphaned = int(om.get("orphaned_wikilinks", 0))
    logger.info("slice-seed orphan_metrics orphaned=%d elapsed=%s", orphaned, _format_mins_secs(time.monotonic() - t_orphan))
    log_fn = logger.warning if orphaned > 0 else logger.info
    log_fn(
        "slice-seed done selected_card_count=%d orphaned_wikilinks=%d total_wall=%s",
        len(expanded),
        orphaned,
        _format_mins_secs(time.monotonic() - t_scan),
    )
    return SliceResult(
        total_source_cards=total,
        selected_card_count=len(expanded),
        cards_by_type=counts,
        orphaned_wikilinks=orphaned,
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
        primary_user_uid=str(data.get("primary_user_uid", "") or ""),
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
