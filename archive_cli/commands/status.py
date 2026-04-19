"""Stats, validation, duplicates, and index/embedding status commands."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from archive_vault.provenance import validate_provenance
from archive_vault.schema import validate_card_strict
from archive_vault.vault import _tier2_cache_path, iter_notes, read_note

from ..index_store import BaseArchiveIndex
from ..ppa_engine import ppa_engine
from ..store import DefaultArchiveStore

EMBEDDING_COST_PER_MILLION_TOKENS = 0.02  # text-embedding-3-small (approximate; check pricing)
# Context header (~100 tokens) + chunk body (~300 tokens) ≈ 400 tokens when include_in_embeddings is on.
AVG_TOKENS_PER_CHUNK = 400


def stats(*, index: BaseArchiveIndex, logger: logging.Logger) -> dict[str, Any]:
    """Vault/index cardinality and breakdowns from ``index.stats()``."""
    logger.info("stats_start")
    total, by_type, by_source = index.stats()
    result = {"total": total, "by_type": by_type, "by_source": by_source}
    logger.info("stats_done total=%s", total)
    return result


def validate(*, vault: Path, logger: logging.Logger) -> dict[str, Any]:
    """Walk the vault and validate every card.

    When ``PPA_ENGINE=rust`` and the tier-2 vault scan cache exists at
    ``_meta/vault-scan-cache.sqlite3``, uses ``archive_crate.validate_vault_from_cache``
    — parallel Rust validation of uid/type/source/dates + full provenance coverage
    from cached ``provenance_json`` (same provenance rules as Python
    ``validate_provenance``). This is authoritative and fast (~seconds on large vaults).

    Falls back to per-note Python (``read_note`` + Pydantic + ``validate_provenance``)
    when the cache is missing or ``archive_crate`` is unavailable.
    """
    logger.info("validate_start vault=%s", vault)
    vault = Path(vault)
    cache = _tier2_cache_path(vault)
    if cache is not None and ppa_engine() == "rust":
        try:
            import archive_crate

            raw = archive_crate.validate_vault_from_cache(str(cache))
            err_list = list(raw.get("errors", []) or [])
            total = int(raw.get("total", 0))
            valid = int(raw.get("valid", 0))
            logger.info(
                "validate_done engine=rust total=%s valid=%s error_count=%s",
                total,
                valid,
                len(err_list),
            )
            return {
                "total": total,
                "valid": valid,
                "errors": err_list,
                "engine": "rust",
                "cache_path": str(cache),
            }
        except Exception as exc:
            logger.warning("Rust validate unavailable, falling back to Python: %s", exc)

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
    logger.info("validate_done engine=python total=%s valid=%s error_count=%s", total, valid, len(errors))
    return {"total": total, "valid": valid, "errors": errors, "engine": "python"}


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


def embedding_estimate(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    embedding_model: str = "",
    embedding_version: int = 0,
) -> dict[str, Any]:
    """Rough cost and duration estimate for embedding pending chunks."""
    from ..index_config import (get_default_embedding_model,
                                get_default_embedding_version,
                                get_embed_batch_size, get_embed_concurrency)

    logger.info(
        "embedding_estimate_start model=%r version=%s",
        embedding_model,
        embedding_version,
    )
    model = embedding_model.strip() or get_default_embedding_model()
    version = embedding_version or get_default_embedding_version()
    status = store.embedding_status(embedding_model=model, embedding_version=version)
    pending = int(status.get("pending_chunk_count", 0))
    batch_size = get_embed_batch_size()
    concurrency = get_embed_concurrency()

    total_tokens = pending * AVG_TOKENS_PER_CHUNK
    estimated_cost_usd = round(total_tokens / 1_000_000 * EMBEDDING_COST_PER_MILLION_TOKENS, 2)

    chunks_per_second = batch_size * concurrency * 5
    estimated_seconds = pending / chunks_per_second if chunks_per_second > 0 else 0.0

    result = {
        "pending_chunks": pending,
        "estimated_tokens": total_tokens,
        "estimated_cost_usd": estimated_cost_usd,
        "estimated_minutes": round(estimated_seconds / 60, 1),
        "batch_size": batch_size,
        "concurrency": concurrency,
        "embedding_model": model,
        "embedding_version": version,
    }
    logger.info("embedding_estimate_done pending=%s", pending)
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
