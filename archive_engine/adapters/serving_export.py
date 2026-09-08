"""PG snapshot-bound serving export. Does not promote ACTIVE or acknowledge."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from archive_engine.contracts import EmbeddingSpec, ExportReceipt
from archive_engine.errors import IncompatibleStateError
from archive_vault.canon import CANON_SCHEMA_VERSION

logger = logging.getLogger("ppa.serving_export")


class ExportFailed(IncompatibleStateError):
    """Staged export is incomplete, invalid, or interrupted."""


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def validate_staged_embeddings(
    keys_path: Path,
    bin_path: Path,
    *,
    dimension: int,
    expected_count: int | None = None,
) -> tuple[tuple[str, ...], int]:
    """Fail closed unless both artifacts exist, pair, and match dimension."""

    if not keys_path.is_file() or not bin_path.is_file():
        raise ExportFailed("export_staged_missing")
    keys = tuple(line.strip() for line in keys_path.read_text(encoding="utf-8").splitlines() if line.strip())
    bin_len = bin_path.stat().st_size
    if dimension <= 0:
        raise ExportFailed("export_invalid_dimension")
    expected_bytes = len(keys) * dimension * 4
    if bin_len != expected_bytes:
        raise ExportFailed("export_incomplete_or_truncated")
    if expected_count is not None and expected_count != len(keys):
        raise ExportFailed("export_count_mismatch")
    return keys, bin_len


def build_export_receipt(
    *,
    snapshot_id: str,
    checkpoint: int,
    keys_path: Path | None,
    bin_path: Path | None,
    exported_keys: Sequence[str],
    rejected_keys: Sequence[str],
    spec: EmbeddingSpec | None,
    complete: bool,
    captured_mutation_ids: Sequence[str] = (),
) -> ExportReceipt:
    hashes: list[tuple[str, str]] = []
    sizes: list[tuple[str, int]] = []
    if keys_path is not None and keys_path.is_file():
        hashes.append((keys_path.name, hash_file(keys_path)))
        sizes.append((keys_path.name, keys_path.stat().st_size))
    if bin_path is not None and bin_path.is_file():
        hashes.append((bin_path.name, hash_file(bin_path)))
        sizes.append((bin_path.name, bin_path.stat().st_size))
    return ExportReceipt(
        snapshot_id=snapshot_id,
        checkpoint=int(checkpoint or 0),
        exported_keys=tuple(exported_keys),
        rejected_keys=tuple(rejected_keys),
        artifact_hashes=tuple(hashes),
        artifact_bytes=tuple(sizes),
        count=len(exported_keys),
        embedding_spec=spec,
        canon_version=CANON_SCHEMA_VERSION,
        schema_version="serving-export.1",
        complete=complete,
        captured_mutation_ids=tuple(captured_mutation_ids),
    )


def require_complete_receipt(receipt: ExportReceipt | None) -> ExportReceipt:
    if receipt is None or not receipt.complete:
        raise ExportFailed("export_receipt_incomplete")
    return receipt


def install_streamed_embeddings(dest: Path, snapshot: Any) -> int:
    """Move streamed artifacts into ``dest`` or write in-memory vectors.

    If streamed paths are advertised they must both exist and pair. Missing
    staged files never fall back to empty in-memory embeddings.
    """

    from archive_engine.publication import write_embeddings

    keys_dest = dest / "embedding_keys.txt"
    bin_dest = dest / "embeddings.bin"
    keys_src = str(getattr(snapshot, "embedding_keys_path", "") or "").strip()
    bin_src = str(getattr(snapshot, "embeddings_bin_path", "") or "").strip()
    spec = getattr(snapshot, "embedding_spec", None)
    dim = int(getattr(spec, "dimension", 0) or 0)
    if keys_src or bin_src:
        if not (keys_src and bin_src):
            raise ExportFailed("export_staged_unpaired")
        src_keys = Path(keys_src)
        src_bin = Path(bin_src)
        keys, _size = validate_staged_embeddings(
            src_keys,
            src_bin,
            dimension=dim or max(1, int(getattr(snapshot, "embedding_count", 0) or 0) and 1),
            expected_count=int(getattr(snapshot, "embedding_count", 0) or 0) or None,
        )
        dest.mkdir(parents=True, exist_ok=True)
        if src_keys.resolve() != keys_dest.resolve():
            keys_dest.write_bytes(src_keys.read_bytes())
        if src_bin.resolve() != bin_dest.resolve():
            bin_dest.write_bytes(src_bin.read_bytes())
        return len(keys)
    return write_embeddings(keys_dest, bin_dest, getattr(snapshot, "embeddings", ()) or ())
