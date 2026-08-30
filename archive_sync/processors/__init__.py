"""Processor DAG contract (PPA v2.5 Section E)."""

from .batch import ProcessorPlanItem, ProcessorPlanSummary, ProcessorRunReport
from .constants import (
    BROAD_LLM_PROCESSOR_KEYS,
    EXPENSIVE_PROCESSOR_KEYS,
    PROCESSOR_EMAIL_PROMOTION_POLICY,
    PROCESSOR_EMAIL_THREAD_ENRICHMENT,
    PROCESSOR_EMAIL_TYPED_EXTRACTION,
    PROCESSOR_EMBEDDING,
    PROCESSOR_ENTITY_RESOLUTION,
    PROCESSOR_LINKERS,
    PROCESSOR_MATERIALIZATION,
    SECTION_E_COMPLETION_STATE,
    SECTION_E_EXECUTION_STATE,
    STALE_CORPUS_STATE,
    STALE_DIRTY_INPUT,
    STALE_FAILED_OUTPUT,
    STALE_INPUT_HASH,
    STALE_MISSING_OUTPUT,
    STALE_PROCESSOR_VERSION,
    STALE_UPSTREAM,
)
from .declarations import (
    ProcessorDeclaration,
    declaration_for_key,
    iter_processor_declarations,
    topological_order,
    validate_all_declarations,
    validate_declaration,
)
from .dirty_io import dirty_uids_from_source_reports, load_dirty_inputs, load_dirty_uids
from .input_hash import compute_input_hash, format_output_identity
from .plan import build_processor_plan, processors_for_dirty_input
from .runner import ProcessorExecutionResult, run_processors
from .staleness import ProcessorInputSnapshot, StalenessEvaluation, evaluate_staleness
from .state_store import ProcessorInputStateRecord, ProcessorStateRecord, ProcessorStateStore
from .status import status_payload

__all__ = [
    "BROAD_LLM_PROCESSOR_KEYS",
    "EXPENSIVE_PROCESSOR_KEYS",
    "PROCESSOR_EMAIL_PROMOTION_POLICY",
    "PROCESSOR_EMAIL_THREAD_ENRICHMENT",
    "PROCESSOR_EMAIL_TYPED_EXTRACTION",
    "PROCESSOR_EMBEDDING",
    "PROCESSOR_ENTITY_RESOLUTION",
    "PROCESSOR_LINKERS",
    "PROCESSOR_MATERIALIZATION",
    "SECTION_E_COMPLETION_STATE",
    "SECTION_E_EXECUTION_STATE",
    "STALE_CORPUS_STATE",
    "STALE_DIRTY_INPUT",
    "STALE_FAILED_OUTPUT",
    "STALE_INPUT_HASH",
    "STALE_MISSING_OUTPUT",
    "STALE_PROCESSOR_VERSION",
    "STALE_UPSTREAM",
    "ProcessorDeclaration",
    "ProcessorExecutionResult",
    "ProcessorInputSnapshot",
    "ProcessorInputStateRecord",
    "ProcessorPlanItem",
    "ProcessorPlanSummary",
    "ProcessorRunReport",
    "ProcessorStateRecord",
    "ProcessorStateStore",
    "StalenessEvaluation",
    "build_processor_plan",
    "compute_input_hash",
    "declaration_for_key",
    "dirty_uids_from_source_reports",
    "evaluate_staleness",
    "format_output_identity",
    "iter_processor_declarations",
    "load_dirty_inputs",
    "load_dirty_uids",
    "processors_for_dirty_input",
    "run_processors",
    "status_payload",
    "topological_order",
    "validate_all_declarations",
    "validate_declaration",
]
