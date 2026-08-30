"""Status payload helpers for processor DAG (read without execution)."""

from __future__ import annotations

from typing import Any

from .constants import SECTION_E_COMPLETION_STATE
from .declarations import iter_processor_declarations, validate_all_declarations
from .state_store import ProcessorStateStore


def status_payload(
    state_store: ProcessorStateStore,
    *,
    archive_instance: str,
    engine_mode: str,
) -> dict[str, Any]:
    """Build machine-readable processor status without running processors."""

    validation_errors = validate_all_declarations()
    declarations = [d.to_dict() for d in iter_processor_declarations()]
    stored = {s.processor_key: s for s in state_store.list_state()}
    processors: list[dict[str, Any]] = []
    totals = {"pending": 0, "stale": 0, "failed": 0}

    for decl in iter_processor_declarations():
        state = stored.get(decl.processor_key)
        entry = {
            "processor_key": decl.processor_key,
            "processor_version": decl.processor_version,
            "enabled": decl.enabled,
            "active_only": decl.active_only,
            "llm_dependent": decl.llm_dependent,
            "depends_on": list(decl.depends_on),
            "last_success_at": state.last_success_at if state else None,
            "last_attempt_at": state.last_attempt_at if state else None,
            "last_error": state.last_error if state else "",
            "pending_count": state.pending_count if state else 0,
            "stale_count": state.stale_count if state else 0,
            "failed_count": state.failed_count if state else 0,
            "last_run_id": state.last_run_id if state else "",
        }
        totals["pending"] += entry["pending_count"]
        totals["stale"] += entry["stale_count"]
        totals["failed"] += entry["failed_count"]
        processors.append(entry)

    return {
        "completion_state": SECTION_E_COMPLETION_STATE,
        "archive_instance": archive_instance,
        "engine_mode": engine_mode,
        "declaration_validation_errors": validation_errors,
        "declarations": declarations,
        "processors": processors,
        "totals": totals,
    }
