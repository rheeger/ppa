"""Constants for the source updater contract (Section D)."""

from __future__ import annotations

SECTION_D_COMPLETION_STATE = "source_updater_contract_complete"
SECTION_D_EXECUTION_STATE = "source_updater_execution_complete"

SOURCE_UPDATER_LOG_ROOT = "source-updaters"

# Live (non-export) adapters the source-updater runner can construct and ingest.
EXECUTABLE_ADAPTER_SOURCE_IDS: frozenset[str] = frozenset(
    {
        "gmail-messages",
        "calendar-events",
        "imessage",
        "otter-transcripts",
        "file-libraries",
        "photos",
        "beeper",
        "contacts",
        "github-history",
        "gmail-correspondents",
    }
)

# Manual-export adapters: declared or known, but refused as not executable.
EXPORT_ADAPTER_SOURCE_IDS: frozenset[str] = frozenset(
    {
        "copilot-finance",
        "linkedin",
        "notion-people",
        "notion-staff",
        "apple-health",
        "health",
        "medical-records",
        "seed-people",
    }
)

# contacts:google is live; apple/vcf stay export-only.
CONTACTS_EXPORT_SCOPES: frozenset[str] = frozenset({"apple", "vcf"})

# Declared but not required for v3 freshness on this campaign (manual / parked).
PARKED_ADAPTER_SOURCE_IDS: frozenset[str] = frozenset({"photos", "apple-health", "health"})

STALENESS_FRESH = "fresh"
STALENESS_STALE = "stale"
STALENESS_FAILED = "failed"
STALENESS_BLOCKED = "blocked"
STALENESS_NEVER_SYNCED = "never_synced"

STALENESS_STATES = frozenset(
    {
        STALENESS_FRESH,
        STALENESS_STALE,
        STALENESS_FAILED,
        STALENESS_BLOCKED,
        STALENESS_NEVER_SYNCED,
    }
)

RUN_STATUS_SUCCESS = "success"
RUN_STATUS_PARTIAL = "partial"
RUN_STATUS_FAILED = "failed"
RUN_STATUS_BLOCKED = "blocked"

RUN_STATUSES = frozenset({RUN_STATUS_SUCCESS, RUN_STATUS_PARTIAL, RUN_STATUS_FAILED, RUN_STATUS_BLOCKED})

DEFAULT_ACTIVE_ALL = "all_active"
DEFAULT_ACTIVE_PROMOTION_GATED = "promotion_gated"
DEFAULT_ACTIVE_METADATA_GATED = "metadata_gated"

DEFAULT_ACTIVE_POLICIES = frozenset({DEFAULT_ACTIVE_ALL, DEFAULT_ACTIVE_PROMOTION_GATED, DEFAULT_ACTIVE_METADATA_GATED})

CURSOR_HISTORY_ID = "history_id"
CURSOR_SYNC_TOKEN = "sync_token"
CURSOR_PAGE_TOKEN = "page_token"
CURSOR_ROWID = "rowid"
CURSOR_MODIFIED_AT = "modified_at"
CURSOR_HASH = "hash"
CURSOR_ETAG = "etag"

SOURCE_TYPE_GMAIL = "gmail"
SOURCE_TYPE_CALENDAR = "calendar"
SOURCE_TYPE_IMESSAGE = "imessage"
SOURCE_TYPE_OTTER = "otter"
SOURCE_TYPE_DOCUMENTS = "documents"
SOURCE_TYPE_PHOTOS = "photos"
SOURCE_TYPE_BEEPER = "beeper"
SOURCE_TYPE_CONTACTS = "contacts"
SOURCE_TYPE_GITHUB = "github"
SOURCE_TYPE_HEALTH = "health"

# Freshness: last success within this many days => fresh (else stale if success exists).
FRESHNESS_WINDOW_DAYS = 7

ADAPTER_VERSION_DEFAULT = "v2"
GMAIL_POLICY_VERSION = "email-promotion-v1"
