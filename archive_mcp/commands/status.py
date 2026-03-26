"""Stats, validation, duplicates, and index/embedding status commands."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from hfa.provenance import validate_provenance
from hfa.schema import validate_card_strict
from hfa.vault import iter_notes, read_note

from ..index_store import BaseArchiveIndex
from ..store import DefaultArchiveStore


def stats(*, index: BaseArchiveIndex, logger: logging.Logger) -> dict[str, Any]:
    """Vault/index cardinality and breakdowns from ``index.stats()``."""
    logger.info("stats_start")
    total, by_type, by_source = index.stats()
    result = {"total": total, "by_type": by_type, "by_source": by_source}
    logger.info("stats_done total=%s", total)
    return result


def validate(*, vault: Path, logger: logging.Logger) -> dict[str, Any]:
    """Walk the vault and validate every card (canonical markdown), not via index.

    Validation checks canonical on-disk data integrity rather than derived index rows.
    """
    logger.info("validate_start vault=%s", vault)
    total = 0
    valid = 0
    errors: list[str] = []
    for rel_path, _ in iter_notes(vault):
        total += 1
        try:
            frontmatter, _, provenance = read_note(vault, str(rel_path))
            card = validate_card_strict(frontmatter)
            provenance_errors = validate_provenance(card.model_dump(mode="python"), provenance)
            if provenance_errors:
                raise ValueError("; ".join(provenance_errors))
            valid += 1
        except Exception as exc:
            errors.append(f"- {rel_path}: {exc}")
    logger.info("validate_done total=%s valid=%s error_count=%s", total, valid, len(errors))
    return {"total": total, "valid": valid, "errors": errors}


def duplicates(*, vault: Path, logger: logging.Logger) -> dict[str, Any]:
    """Read pending dedup candidates from ``_meta/dedup-candidates.json``."""
    logger.info("duplicates_start vault=%s", vault)
    path = vault / "_meta" / "dedup-candidates.json"
    if not path.exists():
        return {"status": "missing", "candidates": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"status": "parse_error", "candidates": []}
    if not isinstance(payload, list) or not payload:
        return {"status": "empty", "candidates": []}
    logger.info("duplicates_done count=%s", len(payload))
    return {"status": "ok", "candidates": payload}


def duplicate_uids(
    *,
    limit: int,
    index: BaseArchiveIndex,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Duplicate UID rows from the derived index."""
    logger.info("duplicate_uids_start limit=%s", limit)
    rows = index.duplicate_uid_rows(limit=limit)
    logger.info("duplicate_uids_done count=%s", len(rows))
    return {"rows": rows}


def index_status(*, store: DefaultArchiveStore, logger: logging.Logger) -> dict[str, Any]:
    """Derived index status dict (same as MCP index status / JSON tools)."""
    logger.info("index_status_start")
    status = store.status()
    logger.info("index_status_done keys=%s", len(status) if status else 0)
    return status if status else {}


def embedding_status(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    embedding_model: str = "",
    embedding_version: int = 0,
) -> dict[str, Any]:
    """Embedding coverage for a model/version."""
    logger.info(
        "embedding_status_start model=%r version=%s",
        embedding_model,
        embedding_version,
    )
    result = store.embedding_status(
        embedding_model=embedding_model.strip(),
        embedding_version=embedding_version,
    )
    logger.info("embedding_status_done")
    return result


def embedding_backlog(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    limit: int = 20,
    embedding_model: str = "",
    embedding_version: int = 0,
) -> dict[str, Any]:
    """Pending embedding chunks listing."""
    logger.info(
        "embedding_backlog_start limit=%s model=%r version=%s",
        limit,
        embedding_model,
        embedding_version,
    )
    result = store.embedding_backlog(
        limit=limit,
        embedding_model=embedding_model.strip(),
        embedding_version=embedding_version,
    )
    rows = result.get("rows") or []
    logger.info("embedding_backlog_done count=%s", len(rows))
    return result


def status_json(*, store: DefaultArchiveStore, logger: logging.Logger) -> dict[str, Any]:
    """Alias for full index/runtime status JSON (same payload as ``index_status``)."""
    return index_status(store=store, logger=logger)
