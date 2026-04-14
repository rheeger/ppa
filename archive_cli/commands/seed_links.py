"""Seed link queue, candidates, and quality gate commands.

All operations require ``PPA_SEED_LINKS_ENABLED``; otherwise they raise
:class:`~archive_cli.errors.SeedLinksDisabledError`.
"""

from __future__ import annotations

import logging
from typing import Any

from ..errors import InvalidInputError, SeedLinksDisabledError
from ..index_config import get_seed_links_enabled
from ..index_store import BaseArchiveIndex


def default_seed_link_imports() -> dict[str, Any]:
    """Default lazy import map for seed link operations."""
    from ..seed_links import (
        compute_link_quality_gate,
        get_link_candidate_details,
        get_seed_scope_rows,
        get_surface_policy_rows,
        list_link_candidates,
        review_link_candidate,
        run_incremental_link_refresh,
        run_seed_link_backfill,
        run_seed_link_enqueue,
        run_seed_link_promotion_workers,
        run_seed_link_report,
        run_seed_link_workers,
    )

    return {
        "compute_link_quality_gate": compute_link_quality_gate,
        "get_link_candidate_details": get_link_candidate_details,
        "get_seed_scope_rows": get_seed_scope_rows,
        "get_surface_policy_rows": get_surface_policy_rows,
        "list_link_candidates": list_link_candidates,
        "review_link_candidate": review_link_candidate,
        "run_incremental_link_refresh": run_incremental_link_refresh,
        "run_seed_link_backfill": run_seed_link_backfill,
        "run_seed_link_enqueue": run_seed_link_enqueue,
        "run_seed_link_promotion_workers": run_seed_link_promotion_workers,
        "run_seed_link_report": run_seed_link_report,
        "run_seed_link_workers": run_seed_link_workers,
    }


def _sl() -> dict[str, Any]:
    """Resolve seed link callables (monkeypatch ``default_seed_link_imports`` in tests)."""
    return default_seed_link_imports()


def _require_seed_links(logger: logging.Logger) -> None:
    if not get_seed_links_enabled():
        logger.warning("seed_links_gated")
        raise SeedLinksDisabledError("Seed links are not enabled. Set PPA_SEED_LINKS_ENABLED=1 to enable.")


def seed_link_surface(*, logger: logging.Logger) -> dict[str, Any]:
    """Scope rows and surface policy rows."""
    _require_seed_links(logger)
    sl = _sl()
    return {
        "scope": sl["get_seed_scope_rows"](),
        "policies": sl["get_surface_policy_rows"](),
    }


def seed_link_enqueue(
    *,
    index: BaseArchiveIndex,
    logger: logging.Logger,
    modules: str = "",
    source_uids: str = "",
    job_type: str = "seed_backfill",
    reset_existing: bool = False,
) -> dict[str, Any]:
    _require_seed_links(logger)
    sl = _sl()
    selected_modules = [item.strip() for item in modules.split(",") if item.strip()]
    selected_uids = {item.strip() for item in source_uids.split(",") if item.strip()}
    return sl["run_seed_link_enqueue"](
        index,
        modules=selected_modules or None,
        source_uids=selected_uids or None,
        job_type=job_type.strip() or "seed_backfill",
        reset_existing=bool(reset_existing),
    )


def seed_link_backfill(
    *,
    index: BaseArchiveIndex,
    logger: logging.Logger,
    limit: int = 0,
    modules: str = "",
    workers: int = 0,
    include_llm: bool = True,
    apply_promotions: bool = True,
) -> dict[str, Any]:
    _require_seed_links(logger)
    sl = _sl()
    selected_modules = [item.strip() for item in modules.split(",") if item.strip()]
    return sl["run_seed_link_backfill"](
        index,
        modules=selected_modules or None,
        limit=limit,
        max_workers=workers,
        include_llm=bool(include_llm),
        apply_promotions=bool(apply_promotions),
    )


def seed_link_refresh(
    *,
    index: BaseArchiveIndex,
    logger: logging.Logger,
    source_uids: str,
    modules: str = "",
    workers: int = 0,
    include_llm: bool = True,
    apply_promotions: bool = True,
) -> dict[str, Any]:
    _require_seed_links(logger)
    sl = _sl()
    selected_uids = [item.strip() for item in source_uids.split(",") if item.strip()]
    if not selected_uids:
        raise InvalidInputError("source_uids is required")
    selected_modules = [item.strip() for item in modules.split(",") if item.strip()]
    return sl["run_incremental_link_refresh"](
        index,
        source_uids=selected_uids,
        modules=selected_modules or None,
        max_workers=workers,
        include_llm=bool(include_llm),
        apply_promotions=bool(apply_promotions),
    )


def seed_link_worker(
    *,
    index: BaseArchiveIndex,
    logger: logging.Logger,
    limit: int = 0,
    modules: str = "",
    workers: int = 0,
    include_llm: bool = True,
) -> dict[str, Any]:
    _require_seed_links(logger)
    sl = _sl()
    selected_modules = [item.strip() for item in modules.split(",") if item.strip()]
    return sl["run_seed_link_workers"](
        index,
        modules=selected_modules or None,
        limit=limit,
        max_workers=workers,
        include_llm=bool(include_llm),
    )


def seed_link_promote(
    *,
    index: BaseArchiveIndex,
    logger: logging.Logger,
    limit: int = 0,
    workers: int = 1,
) -> dict[str, Any]:
    _require_seed_links(logger)
    sl = _sl()
    return sl["run_seed_link_promotion_workers"](index, limit=limit, max_workers=max(1, workers))


def seed_link_report(
    *,
    index: BaseArchiveIndex,
    logger: logging.Logger,
    rebuild_if_dirty: bool = True,
) -> dict[str, Any]:
    _require_seed_links(logger)
    sl = _sl()
    return sl["run_seed_link_report"](index, rebuild_if_dirty=bool(rebuild_if_dirty))


def link_candidates(
    *,
    index: BaseArchiveIndex,
    logger: logging.Logger,
    status: str = "",
    module_name: str = "",
    min_confidence: float = 0.0,
    limit: int = 20,
) -> dict[str, Any]:
    _require_seed_links(logger)
    sl = _sl()
    rows = sl["list_link_candidates"](
        index,
        status=status.strip(),
        module_name=module_name.strip(),
        min_confidence=min_confidence,
        limit=limit,
    )
    return {"rows": rows}


def link_candidate(
    candidate_id: int,
    *,
    index: BaseArchiveIndex,
    logger: logging.Logger,
) -> dict[str, Any] | None:
    _require_seed_links(logger)
    sl = _sl()
    return sl["get_link_candidate_details"](index, candidate_id)


def review_link_candidate(
    *,
    index: BaseArchiveIndex,
    logger: logging.Logger,
    candidate_id: int,
    reviewer: str,
    action: str,
    notes: str = "",
) -> dict[str, Any]:
    _require_seed_links(logger)
    sl = _sl()
    return sl["review_link_candidate"](index, candidate_id=candidate_id, reviewer=reviewer, action=action, notes=notes)


def link_quality_gate(*, index: BaseArchiveIndex, logger: logging.Logger) -> dict[str, Any]:
    _require_seed_links(logger)
    sl = _sl()
    return sl["compute_link_quality_gate"](index)
