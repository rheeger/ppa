"""Constants for the processor DAG contract (Section E)."""

from __future__ import annotations

SECTION_E_COMPLETION_STATE = "processor_dag_contract_complete"
SECTION_E_EXECUTION_STATE = "processor_dag_execution_complete"

PROCESSOR_LOG_ROOT = "processors"

# Per-input run status (processor_runs detail / input evaluation)
INPUT_STATUS_PENDING = "pending"
INPUT_STATUS_RUNNING = "running"
INPUT_STATUS_COMPLETE = "complete"
INPUT_STATUS_SKIPPED = "skipped"
INPUT_STATUS_FAILED = "failed"
INPUT_STATUS_STALE = "stale"
INPUT_STATUS_VALID_NO_OUTPUT = "valid_no_output"
INPUT_STATUS_BLOCKED_DEPENDENCY = "blocked_dependency"
INPUT_STATUS_BLOCKED_PROVIDER = "blocked_provider"
INPUT_STATUS_RETRYABLE_FAILURE = "retryable_failure"
INPUT_STATUS_PERMANENT_FAILURE = "permanent_failure"
INPUT_STATUS_SUPERSEDED = "superseded"

INPUT_STATUSES = frozenset(
    {
        INPUT_STATUS_PENDING,
        INPUT_STATUS_RUNNING,
        INPUT_STATUS_COMPLETE,
        INPUT_STATUS_SKIPPED,
        INPUT_STATUS_FAILED,
        INPUT_STATUS_STALE,
        INPUT_STATUS_VALID_NO_OUTPUT,
        INPUT_STATUS_BLOCKED_DEPENDENCY,
        INPUT_STATUS_BLOCKED_PROVIDER,
        INPUT_STATUS_RETRYABLE_FAILURE,
        INPUT_STATUS_PERMANENT_FAILURE,
        INPUT_STATUS_SUPERSEDED,
    }
)

# Scheduler state machine (persisted on receipts). Public CLI strings map through
# ``legacy_input_status`` / ``output_receipt_status``.
RECEIPT_STATUS_PENDING = INPUT_STATUS_PENDING
RECEIPT_STATUS_RUNNING = INPUT_STATUS_RUNNING
RECEIPT_STATUS_COMPLETE = INPUT_STATUS_COMPLETE
RECEIPT_STATUS_VALID_NO_OUTPUT = INPUT_STATUS_VALID_NO_OUTPUT
RECEIPT_STATUS_BLOCKED_DEPENDENCY = INPUT_STATUS_BLOCKED_DEPENDENCY
RECEIPT_STATUS_BLOCKED_PROVIDER = INPUT_STATUS_BLOCKED_PROVIDER
RECEIPT_STATUS_RETRYABLE_FAILURE = INPUT_STATUS_RETRYABLE_FAILURE
RECEIPT_STATUS_PERMANENT_FAILURE = INPUT_STATUS_PERMANENT_FAILURE
RECEIPT_STATUS_SUPERSEDED = INPUT_STATUS_SUPERSEDED

RECEIPT_STATUSES = frozenset(
    {
        RECEIPT_STATUS_PENDING,
        RECEIPT_STATUS_RUNNING,
        RECEIPT_STATUS_COMPLETE,
        RECEIPT_STATUS_VALID_NO_OUTPUT,
        RECEIPT_STATUS_BLOCKED_DEPENDENCY,
        RECEIPT_STATUS_BLOCKED_PROVIDER,
        RECEIPT_STATUS_RETRYABLE_FAILURE,
        RECEIPT_STATUS_PERMANENT_FAILURE,
        RECEIPT_STATUS_SUPERSEDED,
    }
)

SUCCESS_RECEIPT_STATUSES = frozenset({RECEIPT_STATUS_COMPLETE, RECEIPT_STATUS_VALID_NO_OUTPUT})
FAILED_RECEIPT_STATUSES = frozenset(
    {
        RECEIPT_STATUS_RETRYABLE_FAILURE,
        RECEIPT_STATUS_PERMANENT_FAILURE,
        INPUT_STATUS_FAILED,
    }
)
BLOCKING_RECEIPT_STATUSES = frozenset(
    {
        RECEIPT_STATUS_BLOCKED_DEPENDENCY,
        RECEIPT_STATUS_BLOCKED_PROVIDER,
        RECEIPT_STATUS_RETRYABLE_FAILURE,
        RECEIPT_STATUS_PERMANENT_FAILURE,
        INPUT_STATUS_FAILED,
    }
)

DEP_REQUIRED = "required"
DEP_OPTIONAL = "optional"
DEP_CONDITIONAL = "conditional"
DEPENDENCY_KINDS = frozenset({DEP_REQUIRED, DEP_OPTIONAL, DEP_CONDITIONAL})

LEASE_SECONDS_DEFAULT = 300
LEGACY_UNKNOWN = "legacy_unknown"

SKIP_BLOCKED_DEPENDENCY = "blocked_dependency"
SKIP_BLOCKED_PROVIDER = "blocked_provider"
SKIP_SUPERSEDED = "superseded"
SKIP_VALID_NO_OUTPUT = "valid_no_output"

# Aggregate run status
RUN_STATUS_SUCCESS = "success"
RUN_STATUS_PARTIAL = "partial"
RUN_STATUS_FAILED = "failed"
RUN_STATUS_BLOCKED = "blocked"
RUN_STATUS_SKIPPED = "skipped"

RUN_STATUSES = frozenset(
    {RUN_STATUS_SUCCESS, RUN_STATUS_PARTIAL, RUN_STATUS_FAILED, RUN_STATUS_BLOCKED, RUN_STATUS_SKIPPED}
)

# Staleness reasons
STALE_INPUT_HASH = "input_hash_changed"
STALE_DIRTY_INPUT = "source_updater_dirty"
STALE_UPSTREAM = "upstream_output_changed"
STALE_PROCESSOR_VERSION = "processor_version_changed"
STALE_CORPUS_STATE = "corpus_state_changed"
STALE_MISSING_OUTPUT = "missing_output"
STALE_FAILED_OUTPUT = "failed_output"

STALE_REASONS = frozenset(
    {
        STALE_INPUT_HASH,
        STALE_DIRTY_INPUT,
        STALE_UPSTREAM,
        STALE_PROCESSOR_VERSION,
        STALE_CORPUS_STATE,
        STALE_MISSING_OUTPUT,
        STALE_FAILED_OUTPUT,
    }
)

# Skip reasons
SKIP_SUPPRESSED = "suppressed_input"
SKIP_QUARANTINE = "quarantine_input"
SKIP_ACTIVE_ONLY = "active_only_processor"
SKIP_UPSTREAM = "upstream_not_complete"
SKIP_PROVIDER = "llm_provider_unavailable"
SKIP_NOT_APPLICABLE = "input_filter_mismatch"

SKIP_REASONS = frozenset(
    {
        SKIP_SUPPRESSED,
        SKIP_QUARANTINE,
        SKIP_ACTIVE_ONLY,
        SKIP_UPSTREAM,
        SKIP_PROVIDER,
        SKIP_NOT_APPLICABLE,
        SKIP_BLOCKED_DEPENDENCY,
        SKIP_BLOCKED_PROVIDER,
        SKIP_SUPERSEDED,
        SKIP_VALID_NO_OUTPUT,
    }
)


def output_receipt_status(scheduler_status: str) -> str:
    """Map scheduler state-machine status onto ``OutputReceipt.status``."""

    if scheduler_status in SUCCESS_RECEIPT_STATUSES:
        return "completed"
    if scheduler_status in {RECEIPT_STATUS_PENDING, RECEIPT_STATUS_RUNNING}:
        return "pending"
    if scheduler_status == RECEIPT_STATUS_BLOCKED_DEPENDENCY:
        return "dependency_unmet"
    if scheduler_status in {RECEIPT_STATUS_BLOCKED_PROVIDER, INPUT_STATUS_SKIPPED}:
        return "blocked" if scheduler_status == RECEIPT_STATUS_BLOCKED_PROVIDER else "skipped"
    if scheduler_status == RECEIPT_STATUS_SUPERSEDED:
        return "skipped"
    if scheduler_status in FAILED_RECEIPT_STATUSES:
        return "failed"
    return "pending"


def legacy_input_status(scheduler_status: str) -> str:
    """Map scheduler status onto the pre-P03 public input-state strings."""

    if scheduler_status == RECEIPT_STATUS_VALID_NO_OUTPUT:
        return INPUT_STATUS_COMPLETE
    if scheduler_status in FAILED_RECEIPT_STATUSES:
        return INPUT_STATUS_FAILED
    if scheduler_status in {
        RECEIPT_STATUS_BLOCKED_DEPENDENCY,
        RECEIPT_STATUS_BLOCKED_PROVIDER,
        RECEIPT_STATUS_SUPERSEDED,
    }:
        return INPUT_STATUS_SKIPPED
    if scheduler_status in INPUT_STATUSES:
        return scheduler_status
    return INPUT_STATUS_PENDING

CORPUS_ACTIVE = "active"
CORPUS_SUPPRESSED = "suppressed"
CORPUS_QUARANTINE = "quarantine"

CORPUS_STATES = frozenset({CORPUS_ACTIVE, CORPUS_SUPPRESSED, CORPUS_QUARANTINE})

# Processor keys
PROCESSOR_EMAIL_PROMOTION_POLICY = "email_promotion_policy"
PROCESSOR_EMAIL_TYPED_EXTRACTION = "email_typed_extraction"
PROCESSOR_EMAIL_THREAD_ENRICHMENT = "email_thread_enrichment"
PROCESSOR_MATERIALIZATION = "materialization"
PROCESSOR_EMBEDDING = "embedding"
PROCESSOR_LINKERS = "linkers"
PROCESSOR_ENTITY_RESOLUTION = "entity_resolution"

PROCESSOR_KEYS = frozenset(
    {
        PROCESSOR_EMAIL_PROMOTION_POLICY,
        PROCESSOR_EMAIL_TYPED_EXTRACTION,
        PROCESSOR_EMAIL_THREAD_ENRICHMENT,
        PROCESSOR_MATERIALIZATION,
        PROCESSOR_EMBEDDING,
        PROCESSOR_LINKERS,
        PROCESSOR_ENTITY_RESOLUTION,
    }
)

# Version constants (explicit bumps invalidate prior outputs)
EMAIL_PROMOTION_PROCESSOR_VERSION = "email-promotion-v1"
EMAIL_TYPED_EXTRACTION_VERSION = "email-typed-extraction-v1"
EMAIL_THREAD_ENRICHMENT_VERSION = "email-thread-enrichment-v1"
MATERIALIZATION_VERSION = "materialization-v1"
EMBEDDING_PROCESSOR_VERSION = "embedding-v1"
LINKERS_PROCESSOR_VERSION = "linkers-v1"
ENTITY_RESOLUTION_VERSION = "entity-resolution-v1"

ROLLBACK_SUPERSEDE = "supersede_by_output_identity"
ROLLBACK_BY_RUN_ID = "rollback_by_run_id"
ROLLBACK_MARK_INACTIVE = "mark_inactive_by_chunk_key"

EXPENSIVE_PROCESSOR_KEYS = frozenset({PROCESSOR_EMBEDDING, PROCESSOR_LINKERS})
BROAD_LLM_PROCESSOR_KEYS = frozenset(
    {PROCESSOR_EMAIL_THREAD_ENRICHMENT, PROCESSOR_EMAIL_TYPED_EXTRACTION, PROCESSOR_ENTITY_RESOLUTION}
)

OUTPUT_KIND_EMAIL_CORPUS_DECISIONS = "email_corpus_decisions"
OUTPUT_KIND_DERIVED_CARDS = "derived_cards"
OUTPUT_KIND_SUMMARIES = "summaries"
OUTPUT_KIND_ENTITIES = "entities"
OUTPUT_KIND_MATCHES = "matches"
OUTPUT_KIND_CARDS = "cards"
OUTPUT_KIND_CHUNKS = "chunks"
OUTPUT_KIND_PROJECTIONS = "projections"
OUTPUT_KIND_EMBEDDINGS = "embeddings"
OUTPUT_KIND_GRAPH_EDGES = "graph_edges"
OUTPUT_KIND_LINK_DECISIONS = "link_decisions"
OUTPUT_KIND_PERSON_LINKS = "person_links"
OUTPUT_KIND_PLACE_LINKS = "place_links"
OUTPUT_KIND_ORG_LINKS = "org_links"
