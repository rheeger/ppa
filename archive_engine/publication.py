"""P02 publication: immutable generations, lease, validate, then ACTIVE.

Incremental publish writes only dirty UIDs, tombstones, and new vectors.
Readers resolve the parent chain. One interprocess publisher at a time.
COMPLETE+fsync happens before ACTIVE. Captured journal ack happens after.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import shutil
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from archive_cli.index_config import (
    get_publication_delta_ratio,
    get_publication_disk_budget_mb,
    get_publication_lease_stale_seconds,
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
from archive_engine.contracts import ChangeBatch, EmbeddingSpec
from archive_engine.errors import IncompatibleContractError, IncompatibleStateError, PublisherBusyError

logger = logging.getLogger("ppa.publication")

LAYOUT_VERSION = 1
LAYOUT_FILE = "layout.json"
COMPLETE_FILE = "COMPLETE"
LEASE_LOCK_NAME = "PUBLISHER.lock"
LEASE_FILE_NAME = "PUBLISHER.lease"
PINS_DIR_NAME = "pins"
PUBLICATION_PHASES = ("lease", "write", "build", "validate", "complete", "promote", "ack")

_PIN_LOCK = threading.Lock()
_PINS: dict[tuple[str, str], int] = {}


class PublicationFault(RuntimeError):
    """Test-injected crash at a named publication boundary."""

    def __init__(self, point: str):
        self.point = point
        super().__init__(f"publication_fault:{point}")


@dataclass(frozen=True)
class PublicationFaultHook:
    fail_at: str | None = None
    on_phase: Callable[[str], None] | None = None


def _maybe_fault(hook: PublicationFaultHook | None, phase: str) -> None:
    if hook is None:
        return
    if hook.on_phase is not None:
        hook.on_phase(phase)
    if hook.fail_at and hook.fail_at == phase:
        raise PublicationFault(phase)


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
    acked_watermark: int = 0
    lease_pid: int = 0
    unresolved_gaps: tuple[int, ...] = ()
    error: str = ""
    eligible_checkpoint: int = 0

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
            "acked_watermark": self.acked_watermark,
            "lease_pid": self.lease_pid,
            "unresolved_gaps": list(self.unresolved_gaps),
            "error": self.error,
            "eligible_checkpoint": self.eligible_checkpoint,
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
    embedding_keys_path: str = ""
    embeddings_bin_path: str = ""
    embedding_count: int = 0


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


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _pin_path(index_root: Path, generation_id: str, pid: int | None = None) -> Path:
    owner = pid if pid is not None else os.getpid()
    return Path(index_root) / PINS_DIR_NAME / f"{owner}-{generation_id}.json"


def pin_generation(index_root: Path | str, generation_id: str) -> None:
    gid = str(generation_id or "").strip()
    if not gid:
        return
    root = Path(index_root).resolve()
    key = (str(root), gid)
    with _PIN_LOCK:
        _PINS[key] = _PINS.get(key, 0) + 1
        path = _pin_path(root, gid)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"pid": os.getpid(), "generation_id": gid, "ts": time.time()}) + "\n",
            encoding="utf-8",
        )


def unpin_generation(index_root: Path | str, generation_id: str) -> None:
    gid = str(generation_id or "").strip()
    if not gid:
        return
    root = Path(index_root).resolve()
    key = (str(root), gid)
    with _PIN_LOCK:
        current = _PINS.get(key, 0)
        if current <= 1:
            _PINS.pop(key, None)
            path = _pin_path(root, gid)
            if path.exists():
                path.unlink()
        else:
            _PINS[key] = current - 1


def pinned_generations(index_root: Path | str) -> set[str]:
    root = Path(index_root).resolve()
    live: set[str] = set()
    with _PIN_LOCK:
        live.update(gid for (path, gid), count in _PINS.items() if path == str(root) and count > 0)
    pins = root / PINS_DIR_NAME
    if pins.is_dir():
        for child in pins.iterdir():
            if not child.is_file():
                continue
            try:
                payload = json.loads(child.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            pid = int(payload.get("pid") or 0)
            gid = str(payload.get("generation_id") or "").strip()
            if gid and _pid_alive(pid):
                live.add(gid)
            elif gid and not _pid_alive(pid):
                try:
                    child.unlink()
                except OSError:
                    pass
    return live


def clear_publication_pins(index_root: Path | str | None = None) -> None:
    pid = os.getpid()
    with _PIN_LOCK:
        if index_root is None:
            roots = {Path(path) for path, _gid in _PINS}
            _PINS.clear()
        else:
            root = Path(index_root).resolve()
            for key in [k for k in _PINS if k[0] == str(root)]:
                _PINS.pop(key, None)
            roots = {root}
    for root in roots:
        pins = root / PINS_DIR_NAME
        if not pins.is_dir():
            continue
        for child in pins.iterdir():
            if child.name.startswith(f"{pid}-"):
                try:
                    child.unlink()
                except OSError:
                    pass


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


def _install_snapshot_embeddings(dest: Path, snapshot: ServingSnapshot) -> int:
    """Move a streamed warehouse export into the generation, or write in-memory vectors."""

    from archive_engine.adapters.serving_export import install_streamed_embeddings

    return install_streamed_embeddings(dest, snapshot)


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


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_complete(generation_dir: Path, payload: Mapping[str, Any]) -> None:
    dest = Path(generation_dir)
    tmp = dest / "COMPLETE.tmp"
    tmp.write_text(json.dumps(dict(payload), indent=2) + "\n", encoding="utf-8")
    _fsync_file(tmp)
    tmp.replace(dest / COMPLETE_FILE)
    _fsync_file(dest / COMPLETE_FILE)
    _fsync_dir(dest)


def read_active_generation(index_root: Path) -> str:
    path = Path(index_root) / "ACTIVE"
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def estimate_snapshot_bytes(snapshot: ServingSnapshot) -> int:
    if snapshot.embeddings:
        vectors = sum((len(vec) * 4) + 64 for _key, vec in snapshot.embeddings)
    else:
        dim = int(snapshot.embedding_spec.dimension or 0)
        count = int(snapshot.embedding_count or 0)
        if snapshot.embeddings_bin_path:
            try:
                vectors = Path(snapshot.embeddings_bin_path).stat().st_size + (count * 64)
            except OSError:
                vectors = count * ((dim * 4) + 64)
        else:
            vectors = count * ((dim * 4) + 64)
    json_bytes = sum(len(json.dumps(dict(row))) + 1 for row in (*snapshot.cards, *snapshot.chunks, *snapshot.edges))
    return vectors + json_bytes + 1_048_576


def check_publication_budget(
    index_root: Path,
    estimated_bytes: int,
    *,
    budget_mb: int | None = None,
) -> None:
    cap = get_publication_disk_budget_mb() if budget_mb is None else int(budget_mb)
    if cap <= 0 or estimated_bytes > cap * 1024 * 1024:
        raise IncompatibleStateError("publication_disk_budget")
    Path(index_root).mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(index_root).free
    if estimated_bytes > int(free * 0.9):
        raise IncompatibleStateError("publication_disk_budget")


def validate_generation(
    generation_dir: Path,
    spec: EmbeddingSpec | None = None,
    *,
    require_native_open: bool = False,
) -> dict[str, Any]:
    dest = Path(generation_dir)
    errors: list[str] = []
    for name in ("manifest.json", "cards.jsonl", "chunks.jsonl", "edges.jsonl", "embedding_keys.txt"):
        if not (dest / name).exists():
            errors.append(f"missing:{name}")
    if errors:
        raise IncompatibleStateError("publication_validation_failed: " + ",".join(errors))
    raw = (dest / "manifest.json").read_text(encoding="utf-8")
    if not raw.strip():
        raise IncompatibleStateError("publication_validation_failed: truncated:manifest.json")
    try:
        manifest = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise IncompatibleStateError(f"publication_validation_failed: manifest:{exc}") from exc
    if int(manifest.get("serving_index_format_version") or 0) != 2:
        errors.append("format")
    card_count = 0
    live_uids: set[str] = set()
    for row in iter_jsonl(dest / "cards.jsonl"):
        card_count += 1
        uid = str(row.get("card_uid") or "").strip()
        if uid:
            live_uids.add(uid)
    if int(manifest.get("card_count") or 0) != card_count:
        errors.append("card_count")
    chunk_count = 0
    chunk_keys: set[str] = set()
    for row in iter_jsonl(dest / "chunks.jsonl"):
        chunk_count += 1
        key = str(row.get("chunk_key") or "").strip()
        if key:
            chunk_keys.add(key)
    key_count = 0
    orphan_count = 0
    with (dest / "embedding_keys.txt").open(encoding="utf-8") as fh:
        for line in fh:
            key = line.strip()
            if not key:
                continue
            key_count += 1
            if key not in chunk_keys:
                orphan_count += 1
                if orphan_count <= 8:
                    errors.append(f"orphan_embedding:{key}")
    if orphan_count > 8:
        errors.append(f"orphan_embedding_count:{orphan_count}")
    bin_path = dest / "embeddings.bin"
    bin_len = bin_path.stat().st_size if bin_path.exists() else 0
    spec_payload = manifest.get("embedding_spec") if isinstance(manifest.get("embedding_spec"), Mapping) else {}
    dim = int((spec_payload or {}).get("dimension") or (spec.dimension if spec else 0) or 0)
    if key_count and dim and bin_len != key_count * dim * 4:
        errors.append("embeddings_truncated")
    if key_count and not (dest / "ivf_meta.json").exists():
        errors.append("missing:ivf_meta.json")
    if spec is not None and spec_payload:
        if _spec_space(spec) != _spec_space(spec_payload):
            errors.append("embedding_spec")
    layout = read_layout(dest)
    for uid in layout.get("tombstone_uids") or []:
        if str(uid) in live_uids:
            errors.append(f"tombstone_live:{uid}")
    if errors:
        raise IncompatibleStateError("publication_validation_failed: " + ",".join(errors))
    native_open = "skipped"
    canaries: dict[str, str] = {}
    if require_native_open and card_count == 0 and key_count == 0:
        native_open = "empty"
        canaries = {"exact": "empty", "query": "empty", "graph": "empty"}
    elif require_native_open:
        try:
            import archive_crate

            opener = getattr(archive_crate, "serving_index_open_generation", None)
            if opener is None:
                raise IncompatibleStateError("publication_validation_failed: native_open:unavailable")
            if dest.parent.name == "generations":
                handle = opener(str(dest.parent.parent), dest.name)
            else:
                handle = opener(str(dest.parent), dest.name)
            native_open = "opened"
            search_fn = getattr(archive_crate, "serving_index_search", None)
            query_fn = getattr(archive_crate, "serving_index_query", None)
            graph_fn = getattr(archive_crate, "serving_index_graph", None)
            if callable(search_fn):
                search_fn(handle, {"query": "canary", "limit": 1})
                canaries["exact"] = "ok"
            if callable(query_fn):
                listed = query_fn(handle, {"limit": 1})
                canaries["query"] = "ok"
                start = ""
                if isinstance(listed, list) and listed:
                    start = str(listed[0].get("rel_path") or listed[0].get("card_uid") or "")
                if callable(graph_fn) and start:
                    graph_fn(handle, start, 1, {})
                    canaries["graph"] = "ok"
                elif callable(graph_fn):
                    canaries["graph"] = "empty"
            close = getattr(handle, "close", None)
            if callable(close):
                close()
        except IncompatibleStateError:
            raise
        except Exception as exc:
            raise IncompatibleStateError(f"publication_validation_failed: native_open:{exc}") from exc
    return {
        "ok": True,
        "cards": card_count,
        "chunks": chunk_count,
        "embeddings": key_count,
        "format": 2,
        "native_open": native_open,
        "canaries": canaries,
    }


class PublisherLease:
    """Interprocess exclusive publisher ownership with abandoned-owner recovery."""

    def __init__(self, index_root: Path, *, stale_seconds: int | None = None):
        self.index_root = Path(index_root)
        self.lock_path = self.index_root / LEASE_LOCK_NAME
        self.lease_path = self.index_root / LEASE_FILE_NAME
        self.stale_seconds = stale_seconds if stale_seconds is not None else get_publication_lease_stale_seconds()
        self.fd: int | None = None
        self.token = f"{os.getpid()}-{time.time_ns()}"
        self.pid = os.getpid()

    def _read_lease(self) -> dict[str, Any]:
        if not self.lease_path.exists():
            return {}
        try:
            return json.loads(self.lease_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _lease_abandoned(self) -> bool:
        payload = self._read_lease()
        pid = int(payload.get("pid") or 0)
        heartbeat = float(payload.get("heartbeat_at") or 0)
        if pid and not _pid_alive(pid):
            return True
        if heartbeat and (time.time() - heartbeat) > self.stale_seconds and not _pid_alive(pid):
            return True
        return False

    def acquire(self) -> "PublisherLease":
        self.index_root.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            if self._lease_abandoned():
                fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    os.close(fd)
                    raise PublisherBusyError("publication_lease_held")
            else:
                raise PublisherBusyError("publication_lease_held")
        self.fd = fd
        payload = {
            "pid": self.pid,
            "token": self.token,
            "heartbeat_at": time.time(),
            "started_at": time.time(),
        }
        self.lease_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
        _fsync_file(self.lease_path)
        return self

    def release(self) -> None:
        if self.fd is None:
            return
        try:
            if self.lease_path.exists():
                current = self._read_lease()
                if current.get("token") == self.token:
                    self.lease_path.unlink()
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
        finally:
            self.fd = None

    def __enter__(self) -> "PublisherLease":
        return self.acquire()

    def __exit__(self, *_exc: object) -> None:
        self.release()


def acknowledge_captured(vault: Path | None, batch: ChangeBatch | None) -> int:
    """Ack only the captured publication batch. Later sequences stay pending."""

    if vault is None or batch is None:
        return 0
    from archive_engine.changes import CONSUMER_PUBLICATION, acknowledge_batch
    from archive_vault.change_journal import ChangeJournal

    allowed = {record.sequence for record in batch.records}
    if batch.high_watermark > 0 and not allowed:
        return 0
    with ChangeJournal(vault) as journal:
        if batch.consumer_name != CONSUMER_PUBLICATION:
            raise IncompatibleStateError("captured batch is not a publication consumer")
        cursor = acknowledge_batch(
            journal,
            batch,
            acked_sequences=sorted(allowed),
        )
        return int(cursor.high_watermark)


def recover_publication(
    index_root: Path,
    *,
    generation_id: str,
    vault: Path | None = None,
    captured_batch: ChangeBatch | None = None,
    crate: Any | None = None,
) -> dict[str, Any]:
    """Replay a crash after COMPLETE or ACTIVE. Never acks beyond the captured batch."""

    dest = Path(index_root) / "generations" / generation_id
    active = read_active_generation(index_root)
    complete = dest.is_dir() and (dest / COMPLETE_FILE).exists()
    if not complete:
        return {"ok": True, "promoted": False, "acked": False, "active": active, "reason": "incomplete"}
    try:
        validate_generation(dest, require_native_open=True)
    except IncompatibleStateError as exc:
        return {"ok": False, "promoted": False, "acked": False, "active": active, "reason": str(exc)}
    native = crate
    if native is None:
        import archive_crate as native
    if active != generation_id:
        native.serving_index_publish(str(index_root), generation_id)
        active = generation_id
    if captured_batch is None and vault is not None:
        from archive_engine.contracts import ChangeBatch
        from archive_engine.journaled_state import PUBLICATION_CAPTURE_REL, load_json_state

        payload = load_json_state(vault, PUBLICATION_CAPTURE_REL)
        if payload.get("consumer_name"):
            captured_batch = ChangeBatch.from_payload(payload)
    acked = acknowledge_captured(vault, captured_batch)
    return {"ok": True, "promoted": True, "acked": bool(captured_batch), "active": active, "acked_watermark": acked}


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
    vault: Path | None = None,
    captured_batch: ChangeBatch | None = None,
    fault: PublicationFaultHook | None = None,
    disk_budget_mb: int | None = None,
    acquire_lease: bool = True,
) -> PublicationReceipt:
    """Write one immutable generation, validate, COMPLETE+fsync, then swap ACTIVE."""

    root = Path(index_root)
    gid = str(generation_id or int(time.time() * 1000))
    dest = root / "generations" / gid
    lease: PublisherLease | None = None
    if acquire_lease:
        lease = PublisherLease(root)
        lease.acquire()
    try:
        _maybe_fault(fault, "lease")
        if vault is not None:
            from archive_engine.journaled_state import SCAN_REJECTIONS_REL, load_json_state

            rejection_payload = load_json_state(vault, SCAN_REJECTIONS_REL)
            if rejection_payload.get("rejections"):
                raise IncompatibleStateError("publication_blocked_unresolved_scan_rejections")
        check_publication_budget(root, estimate_snapshot_bytes(snapshot), budget_mb=disk_budget_mb)
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
            if should_compact(
                chain_depth=chain_depth, delta_vectors=len(snapshot.embeddings), live_vectors=live_vectors
            ):
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
        present = {
            str(row.get("card_uid") or "").strip() for row in snapshot.cards if str(row.get("card_uid") or "").strip()
        }
        deleted = tuple(
            sorted({str(uid).strip() for uid in snapshot.deleted_uids if str(uid).strip()} | (dirty - present))
        )
        replaced = (
            tuple(sorted(uid for uid in (dirty | present) if uid and uid not in deleted)) if chosen == "delta" else ()
        )
        if chosen != "delta":
            deleted = ()
            replaced = ()

        tombstone_chunk_keys: list[str] = []
        if chosen == "delta" and parent:
            old_keys = ancestor_chunk_keys_for_uids(root, parent, set(deleted) | set(replaced))
            new_keys = {
                str(row.get("chunk_key") or "").strip()
                for row in snapshot.chunks
                if str(row.get("chunk_key") or "").strip()
            }
            tombstone_chunk_keys = sorted(old_keys - new_keys)

        write_jsonl(dest / "cards.jsonl", snapshot.cards)
        write_jsonl(dest / "chunks.jsonl", snapshot.chunks)
        write_jsonl(dest / "edges.jsonl", snapshot.edges)
        embed_count = _install_snapshot_embeddings(dest, snapshot)
        _maybe_fault(fault, "write")
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
        logger.info(
            "serving_index_build start generation=%s embeddings=%s cards=%s chunks=%s",
            gid,
            embed_count,
            len(snapshot.cards),
            len(snapshot.chunks),
        )
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
        logger.info("serving_index_build done generation=%s", gid)
        _maybe_fault(fault, "build")
        check_publication_budget(root, estimate_snapshot_bytes(snapshot), budget_mb=disk_budget_mb)
        report = validate_generation(dest, snapshot.embedding_spec, require_native_open=True)
        _maybe_fault(fault, "validate")
        write_complete(
            dest,
            {
                "generation_id": gid,
                "snapshot_id": snapshot.snapshot_id,
                "source_watermark": snapshot.source_watermark,
                "checks": report,
            },
        )
        _maybe_fault(fault, "complete")
        native.serving_index_publish(str(root), gid)
        _maybe_fault(fault, "promote")
        acked = acknowledge_captured(vault, captured_batch)
        _maybe_fault(fault, "ack")
        logger.info(
            "serving_index_published mode=%s generation=%s parent=%s cards=%s chunks=%s embeddings=%s tombstones=%s acked=%s",
            chosen,
            gid,
            parent,
            len(snapshot.cards),
            len(snapshot.chunks),
            embed_count,
            len(deleted),
            acked,
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
            validation_summary=f"layout={chosen} snapshot={snapshot.snapshot_id} validated=1",
            acked_watermark=acked,
            lease_pid=lease.pid if lease else os.getpid(),
        )
    finally:
        if lease is not None:
            lease.release()


@dataclass(frozen=True)
class PublishContext:
    """P03 maintain context. Store and snapshot are optional; vault is required."""

    vault: Path
    store: Any | None = None
    index_root: Path | None = None
    snapshot: ServingSnapshot | None = None
    generation_id: str | None = None
    parent_generation: str | None = None
    mode: str = "incremental"
    force_compact: bool = False
    dirty_uids: tuple[str, ...] = ()
    disk_budget_mb: int | None = None
    crate: Any | None = None


def _coerce_checkpoint(eligible_checkpoint: Any) -> int:
    if eligible_checkpoint is None:
        return 0
    if isinstance(eligible_checkpoint, bool):
        raise IncompatibleStateError("eligible_checkpoint must be an int or checkpoint binding")
    if isinstance(eligible_checkpoint, int):
        return int(eligible_checkpoint)
    if isinstance(eligible_checkpoint, Mapping):
        checkpoint = eligible_checkpoint.get("checkpoint")
        if isinstance(checkpoint, Mapping):
            value = checkpoint.get("value")
            if isinstance(value, Mapping):
                return int(value.get("high_watermark") or 0)
            return int(checkpoint.get("high_watermark") or 0)
        return int(eligible_checkpoint.get("high_watermark") or eligible_checkpoint.get("source_watermark") or 0)
    return int(getattr(eligible_checkpoint, "high_watermark", 0) or 0)


def _coerce_context(context: Any) -> PublishContext:
    if isinstance(context, PublishContext):
        return context
    if not isinstance(context, Mapping):
        raise IncompatibleStateError("publish context must be a mapping or PublishContext")
    vault = context.get("vault")
    if vault is None:
        raise IncompatibleStateError("publish context.vault is required")
    dirty = context.get("dirty_uids") or ()
    snapshot = context.get("snapshot")
    return PublishContext(
        vault=Path(vault),
        store=context.get("store"),
        index_root=Path(context["index_root"]) if context.get("index_root") else None,
        snapshot=snapshot if isinstance(snapshot, ServingSnapshot) else None,
        generation_id=context.get("generation_id"),
        parent_generation=context.get("parent_generation"),
        mode=str(context.get("mode") or "incremental"),
        force_compact=bool(context.get("force_compact")),
        dirty_uids=tuple(str(uid) for uid in dirty if str(uid).strip()),
        disk_budget_mb=context.get("disk_budget_mb"),
        crate=context.get("crate"),
    )


def _publication_cursor(vault: Path) -> tuple[ChangeBatch | None, tuple[int, ...]]:
    from archive_engine.changes import CONSUMER_PUBLICATION, consume_batch
    from archive_vault.change_journal import ChangeJournal

    try:
        with ChangeJournal(vault) as journal:
            batch = consume_batch(journal, CONSUMER_PUBLICATION, limit=10_000)
            gaps = tuple(int(item) for item in journal.consumer_cursor(CONSUMER_PUBLICATION).gaps)
            return batch, gaps
    except Exception:
        logger.debug("publication consume skipped", exc_info=True)
        return None, ()


def publish(eligible_checkpoint: Any, context: Any) -> PublicationReceipt:
    """P03 publisher port: ``publish(eligible_checkpoint, context) -> PublicationReceipt``."""

    ctx = _coerce_context(context)
    watermark = _coerce_checkpoint(eligible_checkpoint)
    captured, gaps = _publication_cursor(ctx.vault)
    try:
        if ctx.store is not None:
            from archive_cli.serving_index import publish_serving_index

            raw = publish_serving_index(
                ctx.store,
                dest_generation=ctx.generation_id,
                dirty_uids=list(ctx.dirty_uids) or None,
            )
            payload = dict(raw.get("report") or raw or {})
            if not raw.get("ok", True):
                return PublicationReceipt(
                    generation_id=str(payload.get("generation_id") or raw.get("generation") or ""),
                    mode=str(payload.get("mode") or ctx.mode),
                    parent_generation=str(payload.get("parent_generation") or ""),
                    base_generation=str(payload.get("base_generation") or ""),
                    snapshot_id=str(payload.get("snapshot_id") or ""),
                    source_watermark=int(payload.get("source_watermark") or watermark),
                    cards=int(payload.get("cards") or 0),
                    chunks=int(payload.get("chunks") or 0),
                    embeddings=int(payload.get("embeddings") or 0),
                    tombstone_uids=tuple(payload.get("tombstone_uids") or ()),
                    replaced_uids=tuple(payload.get("replaced_uids") or ()),
                    compacted=bool(payload.get("compacted")),
                    ok=False,
                    validation_summary=str(payload.get("validation_summary") or ""),
                    acked_watermark=int(payload.get("acked_watermark") or 0),
                    unresolved_gaps=gaps,
                    error=str(raw.get("error") or "publish_failed"),
                    eligible_checkpoint=watermark,
                )
            return PublicationReceipt(
                generation_id=str(payload.get("generation_id") or raw.get("generation") or ""),
                mode=str(payload.get("mode") or ctx.mode),
                parent_generation=str(payload.get("parent_generation") or ""),
                base_generation=str(payload.get("base_generation") or ""),
                snapshot_id=str(payload.get("snapshot_id") or ""),
                source_watermark=int(payload.get("source_watermark") or watermark),
                cards=int(payload.get("cards") or 0),
                chunks=int(payload.get("chunks") or 0),
                embeddings=int(payload.get("embeddings") or 0),
                tombstone_uids=tuple(payload.get("tombstone_uids") or ()),
                replaced_uids=tuple(payload.get("replaced_uids") or ()),
                compacted=bool(payload.get("compacted")),
                ok=True,
                validation_summary=str(payload.get("validation_summary") or ""),
                acked_watermark=int(payload.get("acked_watermark") or 0),
                unresolved_gaps=tuple(payload.get("unresolved_gaps") or gaps),
                eligible_checkpoint=watermark,
            )
        if ctx.snapshot is None:
            raise IncompatibleStateError("publish context requires store or snapshot")
        root = ctx.index_root
        if root is None:
            from archive_cli.index_config import get_serving_index_path

            root = get_serving_index_path(ctx.vault)
        chosen = ctx.mode
        if chosen == "incremental":
            chosen = "delta" if ctx.parent_generation else "full"
        receipt = publish_snapshot(
            root,
            ctx.snapshot,
            generation_id=ctx.generation_id,
            parent_generation=ctx.parent_generation,
            mode=chosen,
            force_compact=ctx.force_compact,
            crate=ctx.crate,
            vault=ctx.vault,
            captured_batch=captured,
            disk_budget_mb=ctx.disk_budget_mb,
        )
        _, after_gaps = _publication_cursor(ctx.vault)
        return replace(
            receipt,
            unresolved_gaps=after_gaps,
            eligible_checkpoint=watermark,
            source_watermark=watermark or receipt.source_watermark,
        )
    except (IncompatibleStateError, IncompatibleContractError, PublisherBusyError) as exc:
        return PublicationReceipt(
            generation_id=str(ctx.generation_id or ""),
            mode=ctx.mode,
            parent_generation=str(ctx.parent_generation or ""),
            base_generation="",
            snapshot_id="",
            source_watermark=watermark,
            cards=0,
            chunks=0,
            embeddings=0,
            tombstone_uids=(),
            replaced_uids=(),
            compacted=False,
            ok=False,
            error=str(exc),
            unresolved_gaps=gaps,
            eligible_checkpoint=watermark,
        )
