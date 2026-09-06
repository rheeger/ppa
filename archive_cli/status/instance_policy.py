"""Current-instance readiness policy (P09-D).

The historical v2.5 leftover ``local_seed_living_corpus`` is a bound record on
the original local seed instance only. New instances evaluate fail-closed and
never inherit that exception. A config manifest is never freshness. Formal
``ready: true`` is not a product claim. Analytics CLI/MCP stay pending until
P10/P04-D. ``production_proven`` stays false — a smoke test is not a soak.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

HISTORICAL_EXCEPTION_ID = "local_seed_living_corpus"
HISTORICAL_SEED_PATH = "/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127"
HISTORICAL_SEED_MARKERS = (
    "hf-archives-seed",
    "/Users/rheeger/Archive/seed/",
    "/Archive/seed/",
)
POLICY_ID = "current-instance-v1"
ANALYTICS_STATUS = "pending"


def is_historical_local_seed_instance(vault_path: str | Path) -> bool:
    """True only when the vault path is the original local seed instance."""

    text = str(Path(vault_path).expanduser()) if str(vault_path).strip() else ""
    if not text:
        return False
    return any(marker in text for marker in HISTORICAL_SEED_MARKERS)


def historical_exception_record() -> dict[str, Any]:
    """Preserved v2.5 leftover record. Does not transfer to new instances."""

    return {
        "id": HISTORICAL_EXCEPTION_ID,
        "bound_to": "original local seed instance only",
        "seed_path": HISTORICAL_SEED_PATH,
        "meaning": (
            "accepted leftover ready:false from missing validation_gates / "
            "corpus_cleanup review rows on the original local seed"
        ),
        "does_not_transfer": True,
        "does_not_set_ready": True,
    }


def current_instance_policy(
    *,
    vault_path: str | Path = "",
    archive_instance: str = "",
    archive_id: str = "",
) -> dict[str, Any]:
    """Explicit policy for this instance. New archives never inherit the seed waiver."""

    historical = is_historical_local_seed_instance(vault_path)
    return {
        "policy_id": POLICY_ID,
        "archive_instance": archive_instance,
        "archive_id": archive_id,
        "vault_path": str(vault_path or ""),
        "historical_exception": historical_exception_record(),
        "historical_exception_applies": historical,
        "inherits_local_seed_exception": False,
        "accepted_local_exception": HISTORICAL_EXCEPTION_ID if historical else None,
        "production_proven": False,
        "fresh": False,
        "fresh_from_manifest": False,
        "freshness_reason": "a config manifest is not a freshness signal",
        "analytics_cli": ANALYTICS_STATUS,
        "analytics_mcp": ANALYTICS_STATUS,
        "analytics_owner": "P10",
        "analytics_closes_on": "P04-D",
    }


def capability_status(*, available: bool, reason: str = "", pending: bool = False) -> dict[str, Any]:
    """Honest capability cell: available, pending, or unavailable."""

    if pending:
        status = "pending"
    elif available:
        status = "available"
    else:
        status = "unavailable"
    return {"status": status, "reason": reason}
