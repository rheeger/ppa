"""Source updater contract (PPA v2.5 Section D)."""

from .batch import (
    SourceUpdaterBatchSummary,
    SourceUpdaterRunReport,
    batch_summary_from_skip_details,
    commit_cursor_after_persisted,
)
from .constants import (
    SECTION_D_COMPLETION_STATE,
    SECTION_D_EXECUTION_STATE,
    STALENESS_BLOCKED,
    STALENESS_FAILED,
    STALENESS_FRESH,
    STALENESS_NEVER_SYNCED,
    STALENESS_STALE,
)
from .declarations import (
    SourceUpdaterDeclaration,
    declaration_for_adapter_source_id,
    expand_declarations,
    iter_declaration_templates,
    validate_declaration,
    validate_all_declarations,
)
from .runner import run_source_updater, run_source_updaters
from .state_store import SourceUpdaterStateStore

__all__ = [
    "SECTION_D_COMPLETION_STATE",
    "SECTION_D_EXECUTION_STATE",
    "STALENESS_BLOCKED",
    "STALENESS_FAILED",
    "STALENESS_FRESH",
    "STALENESS_NEVER_SYNCED",
    "STALENESS_STALE",
    "SourceUpdaterBatchSummary",
    "SourceUpdaterDeclaration",
    "SourceUpdaterRunReport",
    "SourceUpdaterStateStore",
    "batch_summary_from_skip_details",
    "commit_cursor_after_persisted",
    "declaration_for_adapter_source_id",
    "expand_declarations",
    "iter_declaration_templates",
    "run_source_updater",
    "run_source_updaters",
    "validate_declaration",
    "validate_all_declarations",
]
