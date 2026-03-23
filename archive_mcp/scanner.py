"""Vault scanning, canonical row building, and manifest diffing."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hfa.schema import BaseCard, validate_card_permissive
from hfa.vault import (iter_note_paths, read_note_file,
                       read_note_frontmatter_file)

from .index_config import HASH_SUFFIX_RE, SCAN_MANIFEST_VERSION
from .projections.registry import projection_for_card_type


@dataclass(slots=True)
class CanonicalRow:
    rel_path: str
    frontmatter: dict[str, Any]
    card: BaseCard


@dataclass(slots=True)
class NoteManifestRow:
    rel_path: str
    card_uid: str
    slug: str
    content_hash: str
    frontmatter_hash: str
    file_size: int
    mtime_ns: int
    card_type: str
    typed_projection: str
    people_json: str
    orgs_json: str
    scan_version: int
    chunk_schema_version: int
    projection_registry_version: int
    index_schema_version: int


def _normalize_slug(value: str) -> str:
    return value.replace(" ", "-").lower().strip()


def _content_hash(frontmatter: dict[str, Any], body: str) -> str:
    sanitized_frontmatter = json.loads(json.dumps(frontmatter, sort_keys=True, default=str).replace("\\u0000", ""))
    payload = json.dumps(sanitized_frontmatter, sort_keys=True, default=str) + "\n" + body.replace("\x00", "")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _frontmatter_hash_stable(frontmatter: dict[str, Any]) -> str:
    sanitized = json.loads(json.dumps(frontmatter, sort_keys=True, default=str).replace("\\u0000", ""))
    payload = json.dumps(sanitized, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _people_orgs_json_for_card(card: BaseCard) -> tuple[str, str]:
    people = sorted(str(value) for value in getattr(card, "people", []) if value)
    orgs = sorted(str(value) for value in getattr(card, "orgs", []) if value)
    return json.dumps(people, sort_keys=True), json.dumps(orgs, sort_keys=True)


def _vault_paths_and_fingerprint(vault: Path, rel_paths: list[str]) -> tuple[dict[str, tuple[int, int]], str]:
    lines: list[str] = []
    stats: dict[str, tuple[int, int]] = {}
    for rel_path in sorted(rel_paths):
        target = vault / rel_path
        try:
            st = target.stat()
        except OSError:
            continue
        mtime_ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))
        size = int(st.st_size)
        stats[rel_path] = (mtime_ns, size)
        lines.append(f"{rel_path}\t{mtime_ns}\t{size}")
    fingerprint = hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()
    return stats, fingerprint


def _note_manifest_row_from_materialized(
    row: CanonicalRow,
    *,
    stats: dict[str, tuple[int, int]],
    content_hash: str,
    versions: tuple[int, int, int],
) -> NoteManifestRow:
    typed = projection_for_card_type(row.card.type)
    typed_name = typed.table_name if typed is not None else ""
    mtime_ns, file_size = stats.get(row.rel_path, (0, 0))
    people_json, orgs_json = _people_orgs_json_for_card(row.card)
    return NoteManifestRow(
        rel_path=row.rel_path,
        card_uid=str(row.card.uid),
        slug=Path(row.rel_path).stem,
        content_hash=content_hash,
        frontmatter_hash=_frontmatter_hash_stable(row.frontmatter),
        file_size=file_size,
        mtime_ns=mtime_ns,
        card_type=row.card.type,
        typed_projection=typed_name,
        people_json=people_json,
        orgs_json=orgs_json,
        scan_version=SCAN_MANIFEST_VERSION,
        chunk_schema_version=versions[1],
        projection_registry_version=versions[2],
        index_schema_version=versions[0],
    )


def _row_sort_key(rel_path: str) -> tuple[int, int, str]:
    stem = Path(rel_path).stem
    has_hash_suffix = 1 if HASH_SUFFIX_RE.search(stem) else 0
    return (has_hash_suffix, len(rel_path), rel_path)


def _register_slug(slug_map: dict[str, str], rel_path: str) -> None:
    slug = Path(rel_path).stem
    existing = slug_map.get(slug)
    if existing is None:
        slug_map[slug] = rel_path
        return
    if rel_path.startswith("People/") and not existing.startswith("People/"):
        slug_map[slug] = rel_path


def _canonical_row_from_rel_path(task: tuple[str, str]) -> CanonicalRow:
    vault_root, rel_path = task
    note = read_note_frontmatter_file(Path(vault_root) / rel_path, vault_root=vault_root)
    return CanonicalRow(
        rel_path=note.rel_path.as_posix(),
        frontmatter=note.frontmatter,
        card=validate_card_permissive(note.frontmatter),
    )


def _iter_canonical_rows(
    vault: Path,
    *,
    rel_paths: list[str],
    workers: int,
    executor_kind: str,
    reporter: Any | None,
) -> Iterable[CanonicalRow]:
    if workers <= 1 or executor_kind == "serial":
        for index, rel_path in enumerate(rel_paths, start=1):
            note = read_note_frontmatter_file(vault / rel_path, vault_root=vault)
            if reporter is not None:
                reporter.update(index)
            yield CanonicalRow(
                rel_path=note.rel_path.as_posix(),
                frontmatter=note.frontmatter,
                card=validate_card_permissive(note.frontmatter),
            )
        if reporter is not None:
            reporter.complete(len(rel_paths))
        return

    tasks = ((str(vault), rel_path) for rel_path in rel_paths)
    executor_cls = ProcessPoolExecutor if executor_kind == "process" else ThreadPoolExecutor
    map_kwargs: dict[str, Any] = {}
    if executor_kind == "process":
        map_kwargs["chunksize"] = max(1, min(256, (reporter.progress_every if reporter else 0) or 64))
    with executor_cls(max_workers=workers) as executor:
        for index, row in enumerate(executor.map(_canonical_row_from_rel_path, tasks, **map_kwargs), start=1):
            if reporter is not None:
                reporter.update(index)
            yield row
    if reporter is not None:
        reporter.complete(len(rel_paths))


def _classify_manifest_rebuild_delta(
    rows: list[CanonicalRow],
    *,
    manifest_by_path: dict[str, NoteManifestRow],
    file_stats: dict[str, tuple[int, int]],
    versions: tuple[int, int, int],
    duplicate_uid_count: int,
) -> tuple[str, set[str], set[str], dict[str, int]]:
    """Return rebuild_mode, materialize_uids, purge_uids, counters (new/changed/unchanged/deleted)."""
    if duplicate_uid_count > 0:
        return "full", set(), set(), {}
    current_paths = {row.rel_path for row in rows}
    manifest_paths = set(manifest_by_path.keys())
    deleted_paths = manifest_paths - current_paths
    purge_uids = {manifest_by_path[p].card_uid for p in deleted_paths if manifest_by_path[p].card_uid}
    materialize_uids: set[str] = set()
    counters = {"new": 0, "changed": 0, "unchanged": 0, "deleted": len(deleted_paths)}
    version_tuple = (versions[0], versions[1], versions[2])
    for row in rows:
        uid = str(row.card.uid)
        m = manifest_by_path.get(row.rel_path)
        if m is None:
            materialize_uids.add(uid)
            counters["new"] += 1
            continue
        if m.card_uid != uid:
            purge_uids.add(m.card_uid)
            materialize_uids.add(uid)
            counters["changed"] += 1
            continue
        st = file_stats.get(row.rel_path)
        if st is None or (st[0] != m.mtime_ns or st[1] != m.file_size):
            materialize_uids.add(uid)
            counters["changed"] += 1
            continue
        if version_tuple != (m.index_schema_version, m.chunk_schema_version, m.projection_registry_version):
            materialize_uids.add(uid)
            counters["changed"] += 1
            continue
        if _frontmatter_hash_stable(row.frontmatter) != m.frontmatter_hash:
            materialize_uids.add(uid)
            counters["changed"] += 1
            continue
        if Path(row.rel_path).stem != m.slug:
            materialize_uids.add(uid)
            counters["changed"] += 1
            continue
        people_json, orgs_json = _people_orgs_json_for_card(row.card)
        if people_json != m.people_json or orgs_json != m.orgs_json:
            materialize_uids.add(uid)
            counters["changed"] += 1
            continue
        counters["unchanged"] += 1
    if any(row.card.type == "person" and str(row.card.uid) in materialize_uids for row in rows):
        return "full", set(), set(), counters
    if not purge_uids and not materialize_uids:
        return "noop", set(), set(), counters
    if len(materialize_uids) >= len(rows) and not purge_uids:
        return "full", set(), set(), counters
    return "incremental", materialize_uids, purge_uids, counters


def _build_manifest_rows_from_canonical(
    rows: list[CanonicalRow],
    vault: Path,
    file_stats: dict[str, tuple[int, int]],
    versions: tuple[int, int, int],
) -> list[NoteManifestRow]:
    out: list[NoteManifestRow] = []
    for row in rows:
        body = read_note_file(vault / row.rel_path, vault_root=vault).body
        ch = _content_hash(row.frontmatter, body)
        out.append(
            _note_manifest_row_from_materialized(row, stats=file_stats, content_hash=ch, versions=versions)
        )
    return out


def _collect_canonical_rows(
    vault: Path,
    *,
    workers: int = 1,
    executor_kind: str = "thread",
    progress_every: int = 0,
) -> tuple[list[CanonicalRow], dict[str, str], int, list[tuple[Any, ...]], str, dict[str, tuple[int, int]]]:
    from .loader import _log_rebuild_step, _RebuildProgressReporter
    _log_rebuild_step(1, 6, "discover canonical note paths", f"vault={vault}")
    rel_paths = [rel_path.as_posix() for rel_path in iter_note_paths(vault)]
    file_stats, vault_fingerprint = _vault_paths_and_fingerprint(vault, rel_paths)
    _log_rebuild_step(1, 6, "discover canonical note paths complete", f"notes={len(rel_paths)}")
    scan_reporter = _RebuildProgressReporter(
        step_number=2,
        total_steps=6,
        stage="scan",
        total_items=len(rel_paths),
        progress_every=progress_every,
        started_at=time.time(),
    )
    rows_by_uid: dict[str, CanonicalRow] = {}
    anonymous_rows: list[CanonicalRow] = []
    slug_map: dict[str, str] = {}
    duplicate_uid_count = 0
    duplicate_uid_groups: dict[str, list[CanonicalRow]] = {}
    for row in _iter_canonical_rows(
        vault,
        rel_paths=rel_paths,
        workers=workers,
        executor_kind=executor_kind,
        reporter=scan_reporter,
    ):
        uid = str(row.card.uid).strip()
        if uid:
            existing = rows_by_uid.get(uid)
            if existing is None:
                rows_by_uid[uid] = row
            else:
                duplicate_uid_count += 1
                group = duplicate_uid_groups.setdefault(uid, [existing])
                group.append(row)
                preferred = min(group, key=lambda item: _row_sort_key(item.rel_path))
                rows_by_uid[uid] = preferred
        else:
            anonymous_rows.append(row)
        _register_slug(slug_map, row.rel_path)
    rows = list(rows_by_uid.values()) + anonymous_rows
    rows.sort(key=lambda item: item.rel_path)
    duplicate_uid_rows: list[tuple[Any, ...]] = []
    for uid, group in duplicate_uid_groups.items():
        preferred = min(group, key=lambda item: _row_sort_key(item.rel_path))
        duplicate_group_size = len(group)
        for row in sorted(group, key=lambda item: _row_sort_key(item.rel_path)):
            if row.rel_path == preferred.rel_path:
                continue
            duplicate_uid_rows.append(
                (
                    uid,
                    preferred.rel_path,
                    preferred.card.type,
                    str(preferred.frontmatter.get("source_id", "") or ""),
                    preferred.card.summary,
                    row.rel_path,
                    row.card.type,
                    str(row.frontmatter.get("source_id", "") or ""),
                    row.card.summary,
                    duplicate_group_size,
                )
            )
    return rows, slug_map, duplicate_uid_count, duplicate_uid_rows, vault_fingerprint, file_stats
