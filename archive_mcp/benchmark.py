"""Benchmark helpers for archive rebuild performance work."""

from __future__ import annotations

import hashlib
import heapq
import json
import math
import os
import resource
import shutil
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from hfa.vault import ParsedNoteRecord, extract_wikilinks, iter_note_paths, read_note_file

from .index_store import PostgresArchiveIndex, get_index_dsn
from .seed_links import compute_link_quality_gate, run_seed_link_backfill
from .vault_cache import VaultScanCache

DEFAULT_BENCHMARK_SOURCE_VAULT = Path(
    os.environ.get(
        "PPA_BENCHMARK_SOURCE_VAULT",
        "/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127",
    )
)
BENCHMARK_PROFILES: dict[str, dict[str, Any]] = {
    "local-laptop": {
        "workers": min(max(os.cpu_count() or 4, 4), 8),
        "batch_size": 1000,
        "commit_interval": 4000,
        "progress_every": 1000,
        "executor_kind": "process",
    },
    "vm-large": {
        "workers": min(max((os.cpu_count() or 8) * 2, 8), 32),
        "batch_size": 2000,
        "commit_interval": 10000,
        "progress_every": 5000,
        "executor_kind": "process",
    },
}


def resolve_benchmark_profile(
    profile: str,
    *,
    workers: int | None = None,
    batch_size: int | None = None,
    commit_interval: int | None = None,
    progress_every: int | None = None,
    executor_kind: str | None = None,
) -> dict[str, Any]:
    resolved = dict(BENCHMARK_PROFILES.get(profile, BENCHMARK_PROFILES["local-laptop"]))
    if workers is not None:
        resolved["workers"] = workers
    if batch_size is not None:
        resolved["batch_size"] = batch_size
    if commit_interval is not None:
        resolved["commit_interval"] = commit_interval
    if progress_every is not None:
        resolved["progress_every"] = progress_every
    if executor_kind is not None:
        resolved["executor_kind"] = executor_kind
    resolved["profile"] = profile
    return resolved


def _note_group(note: ParsedNoteRecord) -> str:
    top_level = note.rel_path.parts[0] if note.rel_path.parts else "root"
    card_type = str(note.frontmatter.get("type", "") or "unknown")
    return f"{top_level}:{card_type}"


def _copy_note(source_vault: Path, target_vault: Path, rel_path: Path) -> None:
    source = source_vault / rel_path
    target = target_vault / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def _stable_score(path: Path) -> int:
    digest = hashlib.sha1(path.as_posix().encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def _top_level(rel_path: Path) -> str:
    return rel_path.parts[0] if rel_path.parts else "root"


def _sample_row_sort_key(rel_path: Path) -> tuple[int, int, str]:
    stem = rel_path.stem
    has_hash_suffix = (
        1 if len(stem) >= 9 and stem[-9] == "-" and all(ch in "0123456789abcdef" for ch in stem[-8:]) else 0
    )
    return (has_hash_suffix, len(rel_path.as_posix()), rel_path.as_posix())


def _dedupe_selected_paths_by_uid(source_vault: Path, selected: set[Path]) -> tuple[set[Path], dict[str, list[str]]]:
    by_uid: dict[str, list[Path]] = defaultdict(list)
    anonymous: set[Path] = set()
    for rel_path in selected:
        try:
            note = read_note_file(source_vault / rel_path, vault_root=source_vault)
        except FileNotFoundError:
            continue
        uid = str(note.frontmatter.get("uid", "")).strip()
        if not uid:
            anonymous.add(rel_path)
            continue
        by_uid[uid].append(rel_path)
    deduped: set[Path] = set(anonymous)
    collisions: dict[str, list[str]] = {}
    for uid, paths in by_uid.items():
        preferred = min(paths, key=_sample_row_sort_key)
        deduped.add(preferred)
        if len(paths) > 1:
            collisions[uid] = sorted(path.as_posix() for path in paths)
    return deduped, collisions


def _load_benchmark_manifest(vault: Path) -> dict[str, Any]:
    path = vault / "_meta" / "benchmark-sample.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _select_candidate_paths(
    *,
    source_vault: Path,
    oversample_limit: int,
) -> tuple[dict[str, Path], list[Path], dict[str, int]]:
    slug_map: dict[str, Path] = {}
    heaps: dict[str, list[tuple[int, str]]] = defaultdict(list)
    counts_by_top_level: dict[str, int] = defaultdict(int)

    path_cache = VaultScanCache.build_or_load(source_vault, tier=1, progress_every=0)
    for rel_path in (Path(p) for p in path_cache.all_rel_paths()):
        top_level = _top_level(rel_path)
        score = _stable_score(rel_path)
        counts_by_top_level[top_level] += 1
        slug_map[rel_path.stem] = rel_path
        heap = heaps[top_level]
        entry = (-score, rel_path.as_posix())
        if len(heap) < oversample_limit:
            heapq.heappush(heap, entry)
        elif entry > heap[0]:
            heapq.heapreplace(heap, entry)

    candidates = sorted({Path(rel_path) for heap in heaps.values() for _score, rel_path in heap})
    return slug_map, candidates, dict(counts_by_top_level)


def build_benchmark_sample(
    *,
    source_vault: str | Path,
    output_vault: str | Path,
    per_group_limit: int = 200,
    max_notes: int = 5000,
    neighborhood_hops: int = 1,
    oversample_factor: int = 8,
    sample_percent: float = 0.0,
) -> dict[str, Any]:
    source_vault = Path(source_vault or DEFAULT_BENCHMARK_SOURCE_VAULT)
    output_vault = Path(output_vault)
    output_vault.mkdir(parents=True, exist_ok=True)
    if sample_percent < 0:
        raise ValueError("sample_percent must be >= 0")
    if sample_percent > 10:
        raise ValueError("sample_percent must be <= 10")

    selected_by_group: dict[str, list[Path]] = defaultdict(list)
    note_links: dict[Path, list[str]] = {}
    group_counts: dict[str, int] = defaultdict(int)
    selected_total = 0
    oversample_limit = max(per_group_limit * max(oversample_factor, 1), per_group_limit)
    slug_map, candidate_paths, counts_by_top_level = _select_candidate_paths(
        source_vault=source_vault,
        oversample_limit=oversample_limit,
    )
    total_notes = sum(counts_by_top_level.values())
    percent_note_limit = 0
    if sample_percent > 0:
        percent_note_limit = max(1, int(math.ceil(total_notes * (sample_percent / 100.0))))
        max_notes = min(max_notes, percent_note_limit) if max_notes > 0 else percent_note_limit

    for rel_path in candidate_paths:
        try:
            note = read_note_file(source_vault / rel_path, vault_root=source_vault)
        except FileNotFoundError:
            continue
        group_key = _note_group(note)
        if group_counts[group_key] < per_group_limit and selected_total < max_notes:
            selected_by_group[group_key].append(note.rel_path)
            group_counts[group_key] += 1
            selected_total += 1
            note_links[note.rel_path] = extract_wikilinks(note.content)

    selected: set[Path] = {path for items in selected_by_group.values() for path in items}
    if neighborhood_hops > 0 and selected:
        frontier = set(selected)
        for _ in range(neighborhood_hops):
            next_frontier: set[Path] = set()
            for rel_path in list(frontier):
                links = note_links.get(rel_path)
                if links is None:
                    try:
                        links = extract_wikilinks((source_vault / rel_path).read_text(encoding="utf-8"))
                    except FileNotFoundError:
                        links = []
                    note_links[rel_path] = links
                for slug in links:
                    target = slug_map.get(slug)
                    if target is None or target in selected or len(selected) >= max_notes:
                        continue
                    selected.add(target)
                    next_frontier.add(target)
            frontier = next_frontier
            if not frontier or len(selected) >= max_notes:
                break

    selected, duplicate_uid_collisions = _dedupe_selected_paths_by_uid(source_vault, selected)

    for rel_path in sorted(selected):
        _copy_note(source_vault, output_vault, rel_path)

    meta_dir = output_vault / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "source_vault": str(source_vault),
        "output_vault": str(output_vault),
        "source_note_count": total_notes,
        "selected_note_count": len(selected),
        "per_group_limit": per_group_limit,
        "max_notes": max_notes,
        "neighborhood_hops": neighborhood_hops,
        "oversample_factor": oversample_factor,
        "sample_percent": sample_percent,
        "percent_note_limit": percent_note_limit,
        "duplicate_uid_collision_count": len(duplicate_uid_collisions),
        "duplicate_uid_collisions": duplicate_uid_collisions,
        "counts_by_top_level": counts_by_top_level,
        "groups": {key: len(value) for key, value in sorted(selected_by_group.items())},
    }
    (meta_dir / "benchmark-sample.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def benchmark_rebuild(
    *,
    vault: str | Path,
    schema: str,
    profile: str = "local-laptop",
    workers: int | None = None,
    batch_size: int | None = None,
    commit_interval: int | None = None,
    progress_every: int | None = None,
    executor_kind: str | None = None,
) -> dict[str, Any]:
    dsn = get_index_dsn()
    if not dsn:
        raise RuntimeError("PPA_INDEX_DSN is required")

    config = resolve_benchmark_profile(
        profile,
        workers=workers,
        batch_size=batch_size,
        commit_interval=commit_interval,
        progress_every=progress_every,
        executor_kind=executor_kind,
    )
    index = PostgresArchiveIndex(Path(vault), dsn=dsn)
    index.schema = schema
    result = index.rebuild_with_metrics(
        workers=config["workers"],
        batch_size=config["batch_size"],
        commit_interval=config["commit_interval"],
        progress_every=config["progress_every"],
        executor_kind=config["executor_kind"],
    )
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "vault": str(vault),
        "schema": schema,
        "profile": profile,
        "config": config,
        "counts": result.counts,
        "metrics": result.metrics,
        "max_rss": int(usage.ru_maxrss),
        "pid": os.getpid(),
    }


def _count_cards_with_nonempty_field(
    vault: Path,
    top_level: str,
    field_name: str,
    *,
    cache: VaultScanCache | None = None,
) -> int:
    if cache is not None and cache.tier() >= 1:
        count = 0
        for rel_path, fm in cache.all_frontmatters():
            parts = Path(rel_path).parts
            if not parts or parts[0] != top_level:
                continue
            value = fm.get(field_name)
            if value not in ("", [], None, 0, False):
                count += 1
        return count
    count = 0
    for rel_path in iter_note_paths(vault):
        if not rel_path.parts or rel_path.parts[0] != top_level:
            continue
        note = read_note_file(vault / rel_path, vault_root=vault)
        value = note.frontmatter.get(field_name)
        if value not in ("", [], None, 0, False):
            count += 1
    return count


def _cleaning_snapshot(vault: Path, *, cache: VaultScanCache | None = None) -> dict[str, int]:
    if cache is None:
        try:
            cache = VaultScanCache.build_or_load(vault, tier=2)
        except Exception:
            cache = None
    orphan_metrics = _orphan_metrics(vault, cache=cache)
    return {
        "orphaned_wikilinks": orphan_metrics["orphaned_wikilinks"],
        "repairable_orphaned_wikilinks": orphan_metrics["repairable_orphaned_wikilinks"],
        "sample_omitted_target_orphans": orphan_metrics["sample_omitted_target_orphans"],
        "email_messages_with_thread": _count_cards_with_nonempty_field(vault, "Email", "thread", cache=cache),
        "email_messages_with_people": _count_cards_with_nonempty_field(vault, "Email", "people", cache=cache),
        "email_messages_with_calendar_events": _count_cards_with_nonempty_field(
            vault, "Email", "calendar_events", cache=cache
        ),
        "email_messages_with_attachments": _count_cards_with_nonempty_field(vault, "Email", "attachments", cache=cache),
        "email_threads_with_messages": _count_cards_with_nonempty_field(vault, "EmailThreads", "messages", cache=cache),
        "email_threads_with_people": _count_cards_with_nonempty_field(vault, "EmailThreads", "people", cache=cache),
        "email_threads_with_calendar_events": _count_cards_with_nonempty_field(
            vault, "EmailThreads", "calendar_events", cache=cache
        ),
        "imessage_messages_with_thread": _count_cards_with_nonempty_field(vault, "IMessage", "thread", cache=cache),
        "imessage_messages_with_people": _count_cards_with_nonempty_field(vault, "IMessage", "people", cache=cache),
        "imessage_threads_with_messages": _count_cards_with_nonempty_field(
            vault, "IMessageThreads", "messages", cache=cache
        ),
        "imessage_threads_with_people": _count_cards_with_nonempty_field(vault, "IMessageThreads", "people", cache=cache),
        "calendar_events_with_source_messages": _count_cards_with_nonempty_field(
            vault, "Calendar", "source_messages", cache=cache
        ),
        "calendar_events_with_source_threads": _count_cards_with_nonempty_field(
            vault, "Calendar", "source_threads", cache=cache
        ),
        "calendar_events_with_people": _count_cards_with_nonempty_field(vault, "Calendar", "people", cache=cache),
        "photos_with_people": _count_cards_with_nonempty_field(vault, "Photos", "people", cache=cache),
    }


def _normalized_reference_targets(frontmatter: dict[str, Any], rel_path: str) -> set[str]:
    targets = {Path(rel_path).stem}
    summary = str(frontmatter.get("summary", "") or "").strip()
    if summary:
        targets.add(summary)
    if str(frontmatter.get("type", "") or "") == "person":
        for alias in frontmatter.get("aliases", []) or []:
            alias_text = str(alias).strip()
            if alias_text:
                targets.add(alias_text)
        for email in frontmatter.get("emails", []) or []:
            email_text = str(email).strip()
            if email_text:
                targets.add(email_text)
    return {item.replace(" ", "-").lower() for item in targets if item}


def _is_known_reference(slug: str, *, known: set[str], known_uids: set[str], normalized_known: set[str]) -> bool:
    if slug in known or slug in known_uids:
        return True
    return slug.replace(" ", "-").lower() in normalized_known


def _orphan_metrics_from_frontmatters(
    rel_by_uid: dict[str, Path],
    frontmatter_by_uid: dict[str, dict[str, Any]],
) -> dict[str, int]:
    """Compute orphan counts purely from in-memory frontmatter dicts (no disk I/O).

    Body wikilinks are not checked — only frontmatter relationship fields.
    This is the fast path used when the slicer already has all frontmatters loaded.
    """
    known: set[str] = set()
    known_uids: set[str] = set(rel_by_uid.keys())
    normalized_known: set[str] = set()
    for uid, rel in rel_by_uid.items():
        stem = rel.stem.strip()
        if stem:
            known.add(stem)
            normalized_known.add(stem.replace(" ", "-").lower())
    for uid, fm in frontmatter_by_uid.items():
        if uid not in rel_by_uid:
            continue
        rel_path = rel_by_uid[uid].as_posix()
        normalized_known |= _normalized_reference_targets(fm, rel_path)

    total = 0
    for uid, fm in frontmatter_by_uid.items():
        if uid not in rel_by_uid:
            continue
        for value in fm.values():
            if isinstance(value, str) and value.startswith("[[") and value.endswith("]]"):
                slug = value[2:-2].split("|", 1)[0].strip()
                if slug and not _is_known_reference(
                    slug, known=known, known_uids=known_uids, normalized_known=normalized_known
                ):
                    total += 1
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str) and item.startswith("[[") and item.endswith("]]"):
                        slug = item[2:-2].split("|", 1)[0].strip()
                        if slug and not _is_known_reference(
                            slug, known=known, known_uids=known_uids, normalized_known=normalized_known
                        ):
                            total += 1
    return {
        "orphaned_wikilinks": total,
        "repairable_orphaned_wikilinks": 0,
        "sample_omitted_target_orphans": 0,
    }


def _orphan_metrics(vault: Path, *, cache: VaultScanCache | None = None) -> dict[str, int]:
    manifest = _load_benchmark_manifest(vault)
    source_vault_text = str(manifest.get("source_vault", "") or "").strip()
    source_known: set[str] = set()
    if source_vault_text:
        source_vault = Path(source_vault_text)
        if source_vault.exists():
            # Walk-only (stems); do not open vault scan cache here — it would persist under
            # source_vault/_meta/ and surprise callers (e.g. slice-seed --no-cache tests).
            source_known = {path.stem for path in iter_note_paths(source_vault)}

    if cache is not None and cache.tier() >= 2:
        known = cache.all_stems()
        known_uids = set(cache.uid_to_rel_path().keys())
        normalized_known = {item.replace(" ", "-").lower() for item in known}
        for rel_path, frontmatter in cache.all_frontmatters():
            normalized_known |= _normalized_reference_targets(frontmatter, rel_path)
        total = 0
        repairable = 0
        sample_omitted = 0
        for rel_path, wikilinks in cache.all_wikilinks():
            fm = cache.frontmatter_for_rel_path(rel_path)
            for slug in wikilinks:
                if _is_known_reference(slug, known=known, known_uids=known_uids, normalized_known=normalized_known):
                    continue
                total += 1
                if source_known and slug in source_known:
                    sample_omitted += 1
            for value in fm.values():
                if isinstance(value, str) and value.startswith("[[") and value.endswith("]]"):
                    slug = value[2:-2].split("|", 1)[0].strip()
                    if slug and not _is_known_reference(
                        slug, known=known, known_uids=known_uids, normalized_known=normalized_known
                    ):
                        total += 1
                        if source_known and slug in source_known:
                            sample_omitted += 1
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, str) and item.startswith("[[") and item.endswith("]]"):
                            slug = item[2:-2].split("|", 1)[0].strip()
                            if slug and not _is_known_reference(
                                slug, known=known, known_uids=known_uids, normalized_known=normalized_known
                            ):
                                total += 1
                                if source_known and slug in source_known:
                                    sample_omitted += 1
        return {
            "orphaned_wikilinks": total,
            "repairable_orphaned_wikilinks": repairable,
            "sample_omitted_target_orphans": sample_omitted,
        }

    known = {path.stem for path in iter_note_paths(vault)}
    known_uids: set[str] = set()
    normalized_known = {item.replace(" ", "-").lower() for item in known}
    for rel_path in iter_note_paths(vault):
        try:
            note = read_note_file(vault / rel_path, vault_root=vault)
        except FileNotFoundError:
            continue
        u = str(note.frontmatter.get("uid", "") or "").strip()
        if u:
            known_uids.add(u)
        normalized_known |= _normalized_reference_targets(dict(note.frontmatter), rel_path.as_posix())
    total = 0
    repairable = 0
    sample_omitted = 0
    for rel_path in iter_note_paths(vault):
        note = read_note_file(vault / rel_path, vault_root=vault)
        for slug in extract_wikilinks(note.body):
            if _is_known_reference(slug, known=known, known_uids=known_uids, normalized_known=normalized_known):
                continue
            total += 1
            if source_known and slug in source_known:
                sample_omitted += 1
        for value in note.frontmatter.values():
            if isinstance(value, str) and value.startswith("[[") and value.endswith("]]"):
                slug = value[2:-2].split("|", 1)[0].strip()
                if slug and not _is_known_reference(
                    slug, known=known, known_uids=known_uids, normalized_known=normalized_known
                ):
                    total += 1
                    if source_known and slug in source_known:
                        sample_omitted += 1
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str) and item.startswith("[[") and item.endswith("]]"):
                        slug = item[2:-2].split("|", 1)[0].strip()
                        if slug and not _is_known_reference(
                            slug, known=known, known_uids=known_uids, normalized_known=normalized_known
                        ):
                            total += 1
                            if source_known and slug in source_known:
                                sample_omitted += 1
    return {
        "orphaned_wikilinks": total,
        "repairable_orphaned_wikilinks": repairable,
        "sample_omitted_target_orphans": sample_omitted,
    }


def _cleaning_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    keys = set(before) | set(after)
    return {key: int(after.get(key, 0)) - int(before.get(key, 0)) for key in sorted(keys)}


def _repair_opportunities(index: PostgresArchiveIndex) -> list[dict[str, Any]]:
    with index._connect() as conn:
        rows = conn.execute(
            f"""
            SELECT lc.module_name, lc.proposed_link_type, ld.decision, COUNT(*) AS count
            FROM {index.schema}.link_candidates lc
            JOIN {index.schema}.link_decisions ld ON ld.candidate_id = lc.candidate_id
            GROUP BY lc.module_name, lc.proposed_link_type, ld.decision
            ORDER BY lc.module_name ASC, lc.proposed_link_type ASC, ld.decision ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def benchmark_seed_links(
    *,
    vault: str | Path,
    schema: str,
    profile: str = "local-laptop",
    workers: int | None = None,
    batch_size: int | None = None,
    commit_interval: int | None = None,
    progress_every: int | None = None,
    executor_kind: str | None = None,
    include_llm: bool = False,
    apply_promotions: bool = False,
    modules: list[str] | None = None,
    rebuild_first: bool = True,
) -> dict[str, Any]:
    dsn = get_index_dsn()
    if not dsn:
        raise RuntimeError("PPA_INDEX_DSN is required")

    config = resolve_benchmark_profile(
        profile,
        workers=workers,
        batch_size=batch_size,
        commit_interval=commit_interval,
        progress_every=progress_every,
        executor_kind=executor_kind,
    )
    index = PostgresArchiveIndex(Path(vault), dsn=dsn)
    index.schema = schema
    cleaning_before = _cleaning_snapshot(Path(vault))
    rebuild_result: dict[str, Any] | None = None
    if rebuild_first:
        rebuild_started = time.time()
        index.bootstrap()
        rebuilt = index.rebuild_with_metrics(
            workers=config["workers"],
            batch_size=config["batch_size"],
            commit_interval=config["commit_interval"],
            progress_every=config["progress_every"],
            executor_kind=config["executor_kind"],
        )
        rebuild_result = {
            "elapsed_seconds": round(time.time() - rebuild_started, 6),
            "counts": rebuilt.counts,
            "metrics": rebuilt.metrics,
        }
    link_started = time.time()
    link_result = run_seed_link_backfill(
        index,
        max_workers=config["workers"],
        include_llm=include_llm,
        apply_promotions=apply_promotions,
        modules=modules,
    )
    link_elapsed = round(time.time() - link_started, 6)
    candidate_rate = round(float(link_result["candidates"]) / max(link_elapsed, 0.001), 3)
    job_rate = round(float(link_result["jobs_completed"]) / max(link_elapsed, 0.001), 3)
    cleaning_after = _cleaning_snapshot(Path(vault))
    gate = compute_link_quality_gate(index)
    repair_opportunities = _repair_opportunities(index)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "vault": str(vault),
        "schema": schema,
        "profile": profile,
        "config": config,
        "include_llm": include_llm,
        "apply_promotions": apply_promotions,
        "rebuild_first": rebuild_first,
        "rebuild": rebuild_result,
        "link_result": link_result,
        "link_metrics": {
            "elapsed_seconds": link_elapsed,
            "candidates_per_second": candidate_rate,
            "jobs_per_second": job_rate,
        },
        "cleaning_proof": {
            "before": cleaning_before,
            "after": cleaning_after,
            "delta": _cleaning_delta(cleaning_before, cleaning_after),
            "repair_opportunities": repair_opportunities,
            "sample_manifest": _load_benchmark_manifest(Path(vault)),
        },
        "quality_gate": gate,
        "max_rss": int(usage.ru_maxrss),
        "pid": os.getpid(),
    }


def _analyze_scaling(results: list[dict[str, Any]]) -> dict[str, Any]:
    flags: list[str] = []
    if len(results) >= 2:
        t0 = float(results[0].get("metrics", {}).get("total_seconds", 0) or 0)
        t1 = float(results[-1].get("metrics", {}).get("total_seconds", 0) or 0)
        if t0 > 0 and t1 / t0 > 5:
            flags.append("SUPERLINEAR_WARNING")
    return {"flags": flags}


def benchmark_multi_size(
    source_vault: Path,
    sizes: list[float],
    schema_prefix: str,
    profile: str = "local-laptop",
) -> dict[str, Any]:
    """Run rebuild at multiple slice sizes; include max_rss and scaling hints."""
    results: list[dict[str, Any]] = []
    for idx, pct in enumerate(sizes):
        out = Path(tempfile.mkdtemp(prefix="ppa-bench-"))
        try:
            build_benchmark_sample(
                source_vault=source_vault,
                output_vault=out,
                sample_percent=pct,
                max_notes=50_000,
            )
            sch = f"{schema_prefix}_{idx}"
            br = benchmark_rebuild(vault=out, schema=sch, profile=profile)
            results.append({"slice_percent": pct, **br})
        finally:
            shutil.rmtree(out, ignore_errors=True)
    prev_path = Path(os.environ.get("PPA_BENCHMARK_PREVIOUS_JSON", "") or "")
    regression_flags: list[str] = []
    if prev_path.is_file():
        try:
            prev = json.loads(prev_path.read_text(encoding="utf-8"))
            psec = float(prev.get("metrics", {}).get("total_seconds", 0) or 0)
            for r in results:
                csec = float(r.get("metrics", {}).get("total_seconds", 0) or 0)
                if psec > 0 and csec > psec * 1.15:
                    regression_flags.append(f"slow_vs_previous:{csec}>{psec}")
        except (OSError, json.JSONDecodeError):
            pass
    return {
        "results": results,
        "scaling_analysis": _analyze_scaling(results),
        "regression_flags": regression_flags,
    }


def benchmark_worker_sweep(
    vault: Path,
    schema: str,
    worker_counts: list[int],
    profile: str = "local-laptop",
) -> dict[str, Any]:
    out: list[dict[str, Any]] = []
    for w in worker_counts:
        out.append(
            benchmark_rebuild(
                vault=vault,
                schema=f"{schema}_w{w}",
                profile=profile,
                workers=w,
            )
        )
    return {"results": out}
