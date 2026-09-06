"""P02-B publication: immutable base+delta generations with a live-key map.

Incremental publish writes only dirty UIDs, tombstones, and new vectors.
Readers resolve the parent chain. Compaction is an explicit full rebuild.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archive_cli.index_config import (
    get_publication_delta_ratio,
    get_publication_max_chain_depth,
    get_serving_candidate_budget,
    get_serving_nlist,
    get_serving_nprobe,
    get_serving_train_iters,
    get_serving_train_memory_mb,
    get_serving_train_sample,
    get_serving_train_seed,
    get_vector_dimension,
)
from archive_engine.contracts import EmbeddingSpec
from archive_engine.errors import IncompatibleContractError

logger = logging.getLogger("ppa.publication")

LAYOUT_VERSION = 1
LAYOUT_FILE = "layout.json"

_PIN_LOCK = threading.Lock()
_PINS: dict[tuple[str, str], int] = {}


@dataclass(frozen=True)
class PublicationReceipt:
    """What P03 later consumes as ``publish(...) -> PublicationReceipt``."""

    generation_id: str
    mode: str
    parent_generation: str
    base_generation: str
    snapshot_id: str
    source_watermark: int
    cards: int
    chunks: int
    embeddings: int
    tombstone_uids: tuple[str, ...]
    replaced_uids: tuple[str, ...]
    compacted: bool
    ok: bool = True
    validation_summary: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "generation_id": self.generation_id,
            "mode": self.mode,
            "parent_generation": self.parent_generation,
            "base_generation": self.base_generation,
            "snapshot_id": self.snapshot_id,
            "source_watermark": self.source_watermark,
            "cards": self.cards,
            "chunks": self.chunks,
            "embeddings": self.embeddings,
            "tombstone_uids": list(self.tombstone_uids),
            "replaced_uids": list(self.replaced_uids),
            "compacted": self.compacted,
            "ok": self.ok,
            "validation_summary": self.validation_summary,
        }


@dataclass(frozen=True)
class ServingSnapshot:
    """One repeatable warehouse/file snapshot for a generation."""

    snapshot_id: str
    source_watermark: int
    cards: tuple[Mapping[str, Any], ...]
    chunks: tuple[Mapping[str, Any], ...]
    edges: tuple[Mapping[str, Any], ...]
    embeddings: tuple[tuple[str, tuple[float, ...]], ...]
    embedding_spec: EmbeddingSpec
    deleted_uids: tuple[str, ...] = ()
    dirty_uids: tuple[str, ...] = ()


@dataclass(frozen=True)
class LiveUniverse:
    live_uids: tuple[str, ...]
    live_chunk_keys: tuple[str, ...]
    edges: tuple[tuple[Any, ...], ...]
    evidence: tuple[tuple[Any, ...], ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            "live_uids": list(self.live_uids),
            "live_chunk_keys": list(self.live_chunk_keys),
            "edges": [list(row) for row in self.edges],
            "evidence": [list(row) for row in self.evidence],
        }


@dataclass(frozen=True)
class UniverseDiff:
    mismatches: tuple[str, ...]
    left_only: tuple[str, ...] = ()
    right_only: tuple[str, ...] = ()

    @property
    def unexplained(self) -> int:
        return len(self.mismatches)


def pin_generation(index_root: Path | str, generation_id: str) -> None:
    gid = str(generation_id or "").strip()
    if not gid:
        return
    key = (str(Path(index_root).resolve()), gid)
    with _PIN_LOCK:
        _PINS[key] = _PINS.get(key, 0) + 1


def unpin_generation(index_root: Path | str, generation_id: str) -> None:
    gid = str(generation_id or "").strip()
    if not gid:
        return
    key = (str(Path(index_root).resolve()), gid)
    with _PIN_LOCK:
        current = _PINS.get(key, 0)
        if current <= 1:
            _PINS.pop(key, None)
        else:
            _PINS[key] = current - 1


def pinned_generations(index_root: Path | str) -> set[str]:
    root = str(Path(index_root).resolve())
    with _PIN_LOCK:
        return {gid for (path, gid), count in _PINS.items() if path == root and count > 0}


def clear_publication_pins(index_root: Path | str | None = None) -> None:
    with _PIN_LOCK:
        if index_root is None:
            _PINS.clear()
            return
        root = str(Path(index_root).resolve())
        for key in [k for k in _PINS if k[0] == root]:
            _PINS.pop(key, None)


def should_compact(
    *,
    chain_depth: int,
    delta_vectors: int = 0,
    live_vectors: int = 0,
    max_depth: int | None = None,
    delta_ratio: float | None = None,
) -> bool:
    depth_cap = max_depth if max_depth is not None else get_publication_max_chain_depth()
    ratio = delta_ratio if delta_ratio is not None else get_publication_delta_ratio()
    if chain_depth > depth_cap:
        return True
    if live_vectors > 0 and (delta_vectors / live_vectors) > ratio:
        return True
    return False


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Stream JSONL. Never ``read_text().splitlines()`` at vault scale."""

    if not path.exists():
        return
    with path.open(encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(dict(row), ensure_ascii=False) + "\n")
            count += 1
    return count


def write_embeddings(keys_path: Path, bin_path: Path, items: Sequence[tuple[str, Sequence[float]]]) -> int:
    import array

    count = 0
    with bin_path.open("wb") as vf, keys_path.open("w", encoding="utf-8") as kf:
        for key, nums in items:
            text = str(key).strip()
            if not text:
                continue
            vf.write(array.array("f", [float(x) for x in nums]).tobytes())
            kf.write(text + "\n")
            count += 1
    return count


def read_layout(generation_dir: Path) -> dict[str, Any]:
    path = generation_dir / LAYOUT_FILE
    if not path.exists():
        return {
            "layout_version": LAYOUT_VERSION,
            "mode": "full",
            "parent_generation": "",
            "base_generation": "",
            "snapshot_id": "",
            "source_watermark": 0,
            "tombstone_uids": [],
            "tombstone_chunk_keys": [],
            "replaced_uids": [],
            "embedding_spec": None,
        }
    return json.loads(path.read_text(encoding="utf-8"))


def write_layout(generation_dir: Path, layout: Mapping[str, Any]) -> None:
    payload = {
        "layout_version": LAYOUT_VERSION,
        "mode": str(layout.get("mode") or "full"),
        "parent_generation": str(layout.get("parent_generation") or ""),
        "base_generation": str(layout.get("base_generation") or ""),
        "snapshot_id": str(layout.get("snapshot_id") or ""),
        "source_watermark": int(layout.get("source_watermark") or 0),
        "tombstone_uids": list(layout.get("tombstone_uids") or []),
        "tombstone_chunk_keys": list(layout.get("tombstone_chunk_keys") or []),
        "replaced_uids": list(layout.get("replaced_uids") or []),
        "embedding_spec": layout.get("embedding_spec"),
    }
    (generation_dir / LAYOUT_FILE).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def walk_generation_chain(index_root: Path, generation_id: str) -> list[Path]:
    """Oldest → newest. Fail closed on cycles or a missing parent."""

    newest_first: list[Path] = []
    seen: set[str] = set()
    gid = str(generation_id or "").strip()
    for _ in range(64):
        if not gid:
            break
        if gid in seen:
            raise IncompatibleContractError(f"serving_index_chain_cycle generation={gid}")
        seen.add(gid)
        dest = Path(index_root) / "generations" / gid
        if not dest.is_dir():
            raise IncompatibleContractError(f"serving_index_parent_missing generation={gid}")
        newest_first.append(dest)
        parent = str(read_layout(dest).get("parent_generation") or "").strip()
        if not parent:
            newest_first.reverse()
            return newest_first
        gid = parent
    raise IncompatibleContractError("serving_index_chain_unbounded")


def referenced_generations(index_root: Path, active_gid: str) -> set[str]:
    keep: set[str] = set()
    if active_gid:
        try:
            for dest in walk_generation_chain(index_root, active_gid):
                keep.add(dest.name)
        except IncompatibleContractError:
            keep.add(active_gid)
    keep |= pinned_generations(index_root)
    extra: set[str] = set()
    for pinned in list(keep):
        try:
            extra.update(dest.name for dest in walk_generation_chain(index_root, pinned))
        except IncompatibleContractError:
            extra.add(pinned)
    return keep | extra


def _edge_tuple(row: Mapping[str, Any]) -> tuple[Any, ...]:
    confidence = row.get("confidence", row.get("trust"))
    evidence = tuple(str(item) for item in (row.get("evidence_uids") or []) if str(item))
    return (
        str(row.get("source_uid") or ""),
        str(row.get("target_uid") or ""),
        str(row.get("edge_type") or ""),
        str(row.get("field_name") or ""),
        str(row.get("method") or ""),
        None if confidence is None else float(confidence),
        evidence,
    )


def _evidence_tuple(chunk: Mapping[str, Any]) -> tuple[Any, ...]:
    raw = chunk.get("evidence") if isinstance(chunk.get("evidence"), Mapping) else {}
    evidence = raw if isinstance(raw, Mapping) else {}
    lineage = evidence.get("lineage_complete")
    span_unavailable = evidence.get("span_unavailable")
    return (
        str(chunk.get("chunk_key") or ""),
        str(chunk.get("card_uid") or evidence.get("card_uid") or ""),
        str(evidence.get("chunk_schema_version") or ""),
        bool(lineage) if lineage is not None else False,
        bool(span_unavailable) if span_unavailable is not None else True,
        str(evidence.get("algorithm_version") or ""),
        str(evidence.get("evidence_kind") or ""),
        tuple(str(item) for item in (evidence.get("source_revisions") or []) if str(item)),
    )


def normalize_universe(resolved: Mapping[str, Any]) -> LiveUniverse:
    uids = tuple(sorted(str(uid) for uid in (resolved.get("live_uids") or []) if str(uid)))
    keys = tuple(sorted(str(key) for key in (resolved.get("live_chunk_keys") or []) if str(key)))
    edges = tuple(sorted(_edge_tuple(row) for row in (resolved.get("edges") or []) if isinstance(row, Mapping)))
    chunks = resolved.get("live_chunks") or []
    evidence = tuple(sorted(_evidence_tuple(row) for row in chunks if isinstance(row, Mapping)))
    return LiveUniverse(live_uids=uids, live_chunk_keys=keys, edges=edges, evidence=evidence)


def diff_universes(left: LiveUniverse, right: LiveUniverse) -> UniverseDiff:
    mismatches: list[str] = []
    if left.live_uids != right.live_uids:
        mismatches.append("live_uids")
    if left.live_chunk_keys != right.live_chunk_keys:
        mismatches.append("live_chunk_keys")
    if left.edges != right.edges:
        mismatches.append("edges")
    if left.evidence != right.evidence:
        mismatches.append("evidence")
    left_only = tuple(sorted(set(left.live_uids) - set(right.live_uids)))
    right_only = tuple(sorted(set(right.live_uids) - set(left.live_uids)))
    return UniverseDiff(mismatches=tuple(mismatches), left_only=left_only, right_only=right_only)


def resolve_live_universe(index_root: Path) -> LiveUniverse:
    import archive_crate

    raw = archive_crate.serving_index_resolve_layout(str(index_root))
    return normalize_universe(dict(raw or {}))


def _spec_space(spec: EmbeddingSpec | Mapping[str, Any] | None) -> tuple[Any, ...] | None:
    if spec is None:
        return None
    payload = spec.to_payload() if isinstance(spec, EmbeddingSpec) else dict(spec)
    dim = int(payload.get("dimension") or 0)
    if dim <= 0:
        return None
    return (
        str(payload.get("provider_namespace") or ""),
        str(payload.get("model") or ""),
        str(payload.get("model_revision") or ""),
        str(payload.get("metric") or ""),
        str(payload.get("normalization") or ""),
        dim,
    )


def _parent_spec(index_root: Path, parent_gid: str) -> EmbeddingSpec | None:
    if not parent_gid:
        return None
    layout = read_layout(Path(index_root) / "generations" / parent_gid)
    raw = layout.get("embedding_spec")
    if not isinstance(raw, Mapping):
        return None
    try:
        return EmbeddingSpec.from_payload(raw)
    except Exception:
        return None


def _count_key_lines(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count


def chain_vector_counts(index_root: Path, generation_id: str) -> tuple[int, int]:
    """(live_key_lines_across_chain, newest_delta_key_lines)."""

    chain = walk_generation_chain(index_root, generation_id)
    live = 0
    for dest in chain:
        live += _count_key_lines(dest / "embedding_keys.txt")
    newest = _count_key_lines(chain[-1] / "embedding_keys.txt") if chain else 0
    return live, newest


def ancestor_chunk_keys_for_uids(index_root: Path, generation_id: str, uids: Iterable[str]) -> set[str]:
    wanted = {str(uid).strip() for uid in uids if str(uid).strip()}
    keys: set[str] = set()
    if not wanted or not generation_id:
        return keys
    try:
        chain = walk_generation_chain(index_root, generation_id)
    except IncompatibleContractError:
        dest = Path(index_root) / "generations" / generation_id
        chain = [dest] if dest.is_dir() else []
    live: dict[str, str] = {}
    for dest in chain:
        layout = read_layout(dest)
        for uid in layout.get("tombstone_uids") or []:
            text = str(uid).strip()
            if not text:
                continue
            for key, owner in list(live.items()):
                if owner == text:
                    live.pop(key, None)
        for uid in layout.get("replaced_uids") or []:
            text = str(uid).strip()
            if not text:
                continue
            for key, owner in list(live.items()):
                if owner == text:
                    live.pop(key, None)
        for key in layout.get("tombstone_chunk_keys") or []:
            live.pop(str(key).strip(), None)
        for row in iter_jsonl(dest / "chunks.jsonl"):
            uid = str(row.get("card_uid") or "").strip()
            key = str(row.get("chunk_key") or "").strip()
            if uid and key:
                live[key] = uid
    for key, uid in live.items():
        if uid in wanted:
            keys.add(key)
    return keys


def _train_config(spec: EmbeddingSpec) -> dict[str, Any]:
    return {
        "nlist": get_serving_nlist(),
        "nprobe": get_serving_nprobe(),
        "train_sample": get_serving_train_sample(),
        "train_iters": get_serving_train_iters(),
        "seed": get_serving_train_seed(),
        "candidate_budget": get_serving_candidate_budget(),
        "memory_mb": get_serving_train_memory_mb(),
        "embedding_spec": spec.to_payload(),
    }


def publish_snapshot(
    index_root: Path,
    snapshot: ServingSnapshot,
    *,
    generation_id: str | None = None,
    parent_generation: str | None = None,
    mode: str = "full",
    force_compact: bool = False,
    crate: Any | None = None,
) -> PublicationReceipt:
    """Write one immutable generation. Delta writes only this snapshot's rows."""

    root = Path(index_root)
    gid = str(generation_id or int(time.time() * 1000))
    dest = root / "generations" / gid
    dest.mkdir(parents=True, exist_ok=True)
    parent = str(parent_generation or "").strip()
    chosen = "compact" if force_compact else str(mode or "full")
    if chosen == "delta" and not parent:
        chosen = "full"

    parent_spec = _parent_spec(root, parent) if parent and chosen == "delta" else None
    if parent_spec is not None:
        if _spec_space(parent_spec) != _spec_space(snapshot.embedding_spec):
            raise IncompatibleContractError("incompatible EmbeddingSpec for this serving generation")

    if chosen == "delta" and parent and not force_compact:
        try:
            chain_depth = len(walk_generation_chain(root, parent)) + 1
            live_vectors, _ = chain_vector_counts(root, parent)
        except IncompatibleContractError:
            chain_depth = 2
            live_vectors = 0
        if should_compact(chain_depth=chain_depth, delta_vectors=len(snapshot.embeddings), live_vectors=live_vectors):
            if snapshot.dirty_uids:
                logger.info(
                    "serving_index_publish compact deferred; delta snapshot is not a full universe parent=%s",
                    parent,
                )
            else:
                logger.info("serving_index_publish mode=compact reason=chain_or_ratio parent=%s", parent)
                chosen = "compact"
                parent = ""

    if chosen == "compact":
        parent = ""

    dirty = {str(uid).strip() for uid in snapshot.dirty_uids if str(uid).strip()}
    present = {str(row.get("card_uid") or "").strip() for row in snapshot.cards if str(row.get("card_uid") or "").strip()}
    deleted = tuple(sorted({str(uid).strip() for uid in snapshot.deleted_uids if str(uid).strip()} | (dirty - present)))
    replaced = tuple(sorted(uid for uid in (dirty | present) if uid and uid not in deleted)) if chosen == "delta" else ()
    if chosen != "delta":
        deleted = ()
        replaced = ()

    tombstone_chunk_keys: list[str] = []
    if chosen == "delta" and parent:
        old_keys = ancestor_chunk_keys_for_uids(root, parent, set(deleted) | set(replaced))
        new_keys = {str(row.get("chunk_key") or "").strip() for row in snapshot.chunks if str(row.get("chunk_key") or "").strip()}
        tombstone_chunk_keys = sorted(old_keys - new_keys)

    write_jsonl(dest / "cards.jsonl", snapshot.cards)
    write_jsonl(dest / "chunks.jsonl", snapshot.chunks)
    write_jsonl(dest / "edges.jsonl", snapshot.edges)
    embed_count = write_embeddings(dest / "embedding_keys.txt", dest / "embeddings.bin", snapshot.embeddings)
    base = parent
    if parent:
        try:
            chain = walk_generation_chain(root, parent)
            base = chain[0].name if chain else parent
        except IncompatibleContractError:
            base = parent
    if chosen in {"full", "compact"}:
        base = gid
        parent = ""

    write_layout(
        dest,
        {
            "mode": chosen,
            "parent_generation": parent,
            "base_generation": base,
            "snapshot_id": snapshot.snapshot_id,
            "source_watermark": snapshot.source_watermark,
            "tombstone_uids": list(deleted),
            "tombstone_chunk_keys": tombstone_chunk_keys,
            "replaced_uids": list(replaced),
            "embedding_spec": snapshot.embedding_spec.to_payload(),
        },
    )

    native = crate
    if native is None:
        import archive_crate as native
    native.serving_index_build(
        str(dest),
        str(dest / "cards.jsonl"),
        str(dest / "chunks.jsonl"),
        str(dest / "embedding_keys.txt"),
        str(dest / "embeddings.bin"),
        snapshot.embedding_spec.dimension or get_vector_dimension(),
        str(dest / "edges.jsonl"),
        json.dumps(_train_config(snapshot.embedding_spec)),
    )
    native.serving_index_publish(str(root), gid)
    logger.info(
        "serving_index_published mode=%s generation=%s parent=%s cards=%s chunks=%s embeddings=%s tombstones=%s",
        chosen,
        gid,
        parent,
        len(snapshot.cards),
        len(snapshot.chunks),
        embed_count,
        len(deleted),
    )
    return PublicationReceipt(
        generation_id=gid,
        mode=chosen,
        parent_generation=parent,
        base_generation=base,
        snapshot_id=snapshot.snapshot_id,
        source_watermark=snapshot.source_watermark,
        cards=len(snapshot.cards),
        chunks=len(snapshot.chunks),
        embeddings=embed_count,
        tombstone_uids=deleted,
        replaced_uids=replaced,
        compacted=chosen == "compact",
        validation_summary=f"layout={chosen} snapshot={snapshot.snapshot_id}",
    )
