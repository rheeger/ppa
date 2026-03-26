"""Pydantic schemas for HFA cards."""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
PARTIAL_DATE_RE = re.compile(r"^(?:\d{4}-\d{2}|\d{2}-\d{2}|\d{4}-\d{2}-\d{2})$")
LINKEDIN_URL_RE = re.compile(r"(?:https?://)?(?:[\w]+\.)?linkedin\.com/in/([^/?#]+)")
GITHUB_URL_RE = re.compile(r"(?:https?://)?(?:www\.)?github\.com/([^/?#]+)")
TWITTER_URL_RE = re.compile(r"(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/([^/?#]+)")
INSTAGRAM_URL_RE = re.compile(r"(?:https?://)?(?:www\.)?instagram\.com/([^/?#]+)")
TELEGRAM_URL_RE = re.compile(r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/([^/?#]+)")


class ProvenanceMethod(str, Enum):
    deterministic = "deterministic"
    llm = "llm"


DETERMINISTIC_ONLY = frozenset(
    {
        "uid",
        "type",
        "source",
        "source_id",
        "created",
        "updated",
        "emails",
        "phones",
        "birthday",
        "first_name",
        "last_name",
        "aliases",
        "company",
        "companies",
        "title",
        "titles",
        "linkedin",
        "linkedin_url",
        "linkedin_connected_on",
        "twitter",
        "github",
        "instagram",
        "telegram",
        "discord",
        "pronouns",
        "reports_to",
        "websites",
        "emails_seen_count",
        "amount",
        "currency",
        "gmail_thread_id",
        "gmail_message_id",
        "gmail_history_id",
        "imessage_chat_id",
        "imessage_message_id",
        "beeper_room_id",
        "beeper_event_id",
        "attachment_id",
        "account_email",
        "account_id",
        "thread",
        "message",
        "service",
        "protocol",
        "bridge_name",
        "thread_type",
        "thread_title",
        "thread_description",
        "participant_ids",
        "participant_names",
        "participant_identifiers",
        "counterpart_ids",
        "counterpart_names",
        "counterpart_identifiers",
        "message_type",
        "sender_id",
        "sender_name",
        "sender_identifier",
        "sender_person",
        "chat_identifier",
        "display_name",
        "participant_handles",
        "sender_handle",
        "is_from_me",
        "attachment_count",
        "is_group",
        "edited_at",
        "deleted_at",
        "associated_message_guid",
        "associated_message_type",
        "associated_message_emoji",
        "expressive_send_style_id",
        "balloon_bundle_id",
        "transfer_name",
        "uti",
        "original_path",
        "exported_path",
        "calendar_id",
        "event_id",
        "event_etag",
        "ical_uid",
        "direction",
        "from_name",
        "from_email",
        "to_emails",
        "cc_emails",
        "bcc_emails",
        "reply_to_emails",
        "participant_emails",
        "participants",
        "label_ids",
        "sent_at",
        "start_at",
        "end_at",
        "timezone",
        "subject",
        "snippet",
        "message_id_header",
        "in_reply_to",
        "references",
        "has_attachments",
        "attachments",
        "calendar_events",
        "messages",
        "message_count",
        "first_message_at",
        "last_message_at",
        "filename",
        "mime_type",
        "size_bytes",
        "content_id",
        "is_inline",
        "invite_ical_uid",
        "invite_event_id_hint",
        "invite_method",
        "invite_title",
        "invite_start_at",
        "invite_end_at",
        "invite_ical_uids",
        "invite_event_id_hints",
        "thread_body_sha",
        "message_body_sha",
        "attachment_metadata_sha",
        "linked_message_event_id",
        "reply_to_event_id",
        "reaction_key",
        "status",
        "organizer_email",
        "organizer_name",
        "attendee_emails",
        "conference_url",
        "source_messages",
        "source_threads",
        "meeting_transcripts",
        "all_day",
        "event_body_sha",
        "otter_meeting_id",
        "otter_conversation_id",
        "meeting_url",
        "transcript_url",
        "recording_url",
        "speaker_names",
        "speaker_emails",
        "participant_names",
        "participant_emails",
        "host_name",
        "host_email",
        "language",
        "duration_seconds",
        "event_id_hint",
        "transcript_body_sha",
        "otter_updated_at",
        "photos_asset_id",
        "photos_source_label",
        "media_type",
        "captured_at",
        "modified_at",
        "keywords",
        "labels",
        "person_labels",
        "albums",
        "album_paths",
        "folders",
        "favorite",
        "hidden",
        "has_adjustments",
        "live_photo",
        "burst",
        "screenshot",
        "slow_mo",
        "time_lapse",
        "width",
        "height",
        "duration_seconds",
        "place_name",
        "place_city",
        "place_state",
        "place_country",
        "latitude",
        "longitude",
        "edited_path",
        "metadata_sha",
        "original_filename",
        "is_missing",
        "attachment_type",
        "src_url",
        "cached_path",
        "duration_ms",
        "is_voice_note",
        "is_gif",
        "is_sticker",
        "library_root",
        "relative_path",
        "extension",
        "content_sha",
        "file_created_at",
        "file_modified_at",
        "date_start",
        "date_end",
        "document_type",
        "document_date",
        "authors",
        "counterparties",
        "emails",
        "phones",
        "websites",
        "location",
        "sheet_names",
        "page_count",
        "text_source",
        "extracted_text_sha",
        "extraction_status",
        "quality_flags",
        "source_system",
        "source_format",
        "record_type",
        "record_subtype",
        "occurred_at",
        "recorded_at",
        "provider_name",
        "facility_name",
        "encounter_source_id",
        "code_system",
        "code",
        "code_display",
        "value_text",
        "value_numeric",
        "unit",
        "raw_source_ref",
        "details_json",
        "vaccine_name",
        "cvx_code",
        "manufacturer",
        "brand_name",
        "lot_number",
        "expiration_date",
        "administered_at",
        "performer_name",
        "github_repo_id",
        "github_thread_id",
        "github_message_id",
        "github_node_id",
        "name_with_owner",
        "owner_login",
        "owner_type",
        "html_url",
        "api_url",
        "ssh_url",
        "default_branch",
        "homepage_url",
        "visibility",
        "is_private",
        "is_fork",
        "is_archived",
        "parent_name_with_owner",
        "primary_language",
        "languages",
        "topics",
        "license_name",
        "created_at",
        "pushed_at",
        "commit_sha",
        "repository_name_with_owner",
        "repository",
        "parent_shas",
        "authored_at",
        "committed_at",
        "message_headline",
        "additions",
        "deletions",
        "changed_files",
        "author_login",
        "author_name",
        "author_email",
        "committer_login",
        "committer_name",
        "committer_email",
        "associated_pr_numbers",
        "associated_pr_urls",
        "thread_type",
        "number",
        "state",
        "is_draft",
        "merged_at",
        "closed_at",
        "labels",
        "assignees",
        "milestone",
        "base_ref",
        "head_ref",
        "participant_logins",
        "actor_login",
        "actor_name",
        "actor_email",
        "sent_at",
        "message_type",
        "review_state",
        "review_commit_sha",
        "in_reply_to_message_id",
        "path",
        "position",
        "original_position",
        "original_commit_sha",
        "diff_hunk",
    }
)

LLM_ELIGIBLE = frozenset({"description", "relationship_type", "tags", "summary", "thread_summary"})


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _clean_string(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def _normalize_handle(value: str) -> str:
    cleaned = _clean_string(value).removeprefix("@").strip("/")
    return cleaned.lower()


def _normalize_linkedin_fields(handle: str, url: str) -> tuple[str, str]:
    handle_value = _normalize_handle(handle)
    url_value = _clean_string(url)
    match = LINKEDIN_URL_RE.search(url_value or handle_value)
    if match:
        handle_value = _normalize_handle(match.group(1))
    if handle_value and not url_value:
        url_value = f"https://www.linkedin.com/in/{handle_value}"
    return handle_value, url_value


def _normalize_profile_handle(value: str, *, provider: str) -> str:
    raw = _clean_string(value)
    if not raw:
        return ""
    if provider == "github":
        match = GITHUB_URL_RE.search(raw)
        if match:
            return _normalize_handle(match.group(1))
    if provider == "twitter":
        match = TWITTER_URL_RE.search(raw)
        if match:
            return _normalize_handle(match.group(1))
    if provider == "instagram":
        match = INSTAGRAM_URL_RE.search(raw)
        if match:
            return _normalize_handle(match.group(1))
    if provider == "telegram":
        match = TELEGRAM_URL_RE.search(raw)
        if match:
            return _normalize_handle(match.group(1))
    return _normalize_handle(raw)


def _normalize_contact_handle(value: str) -> str:
    raw = _clean_string(value)
    if not raw:
        return ""
    if "@" in raw:
        return raw.lower()
    digits = re.sub(r"\D", "", raw)
    if digits:
        if raw.startswith("+") or (len(digits) == 11 and digits.startswith("1")):
            return f"+{digits}"
        if len(digits) == 10:
            return f"+1{digits}"
        return digits
    return raw.lower()


class BaseCard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    uid: str
    type: str
    source: list[str]
    source_id: str
    created: str
    updated: str
    summary: str = ""
    tags: list[str] = Field(default_factory=list)
    people: list[str] = Field(default_factory=list)
    orgs: list[str] = Field(default_factory=list)

    @field_validator("created", "updated")
    @classmethod
    def validate_date_format(cls, value: str) -> str:
        if not DATE_RE.fullmatch(value):
            raise ValueError("must use YYYY-MM-DD format")
        return value

    @field_validator("uid")
    @classmethod
    def validate_uid_prefix(cls, value: str) -> str:
        if not value.startswith("hfa-"):
            raise ValueError("uid must start with 'hfa-'")
        return value

    @field_validator("source")
    @classmethod
    def validate_source_nonempty(cls, value: list[str]) -> list[str]:
        cleaned = _dedupe_preserve_order([item.strip() for item in value if item and item.strip()])
        if not cleaned:
            raise ValueError("at least one source is required")
        return cleaned

    @field_validator("tags", "people", "orgs")
    @classmethod
    def dedupe_string_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([item.strip() for item in value if item and item.strip()])


class PersonCard(BaseCard):
    type: Literal["person"] = "person"
    first_name: str = ""
    last_name: str = ""
    aliases: list[str] = Field(default_factory=list)
    emails: list[str] = Field(default_factory=list)
    phones: list[str] = Field(default_factory=list)
    birthday: str = ""
    company: str = ""
    companies: list[str] = Field(default_factory=list)
    title: str = ""
    titles: list[str] = Field(default_factory=list)
    linkedin: str = ""
    linkedin_url: str = ""
    linkedin_connected_on: str = ""
    twitter: str = ""
    github: str = ""
    instagram: str = ""
    telegram: str = ""
    discord: str = ""
    pronouns: str = ""
    reports_to: str = ""
    websites: list[str] = Field(default_factory=list)
    description: str = ""
    relationship_type: str = ""
    emails_seen_count: int = 0

    @field_validator("emails")
    @classmethod
    def lowercase_emails(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([item.lower().strip() for item in value if item and item.strip()])

    @field_validator("phones")
    @classmethod
    def dedupe_phones(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([item.strip() for item in value if item and item.strip()])

    @field_validator("aliases", "companies", "titles", "websites")
    @classmethod
    def dedupe_metadata_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @field_validator(
        "first_name",
        "last_name",
        "company",
        "title",
        "description",
        "relationship_type",
        "discord",
        "pronouns",
        "reports_to",
    )
    @classmethod
    def clean_scalar_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("birthday")
    @classmethod
    def validate_birthday(cls, value: str) -> str:
        if value and not PARTIAL_DATE_RE.fullmatch(value):
            raise ValueError("birthday must use YYYY-MM-DD or partial MM-DD / YYYY-MM format")
        return value

    @field_validator("linkedin_connected_on")
    @classmethod
    def validate_linkedin_connected_on(cls, value: str) -> str:
        cleaned = _clean_string(value)
        if cleaned and not DATE_RE.fullmatch(cleaned):
            raise ValueError("linkedin_connected_on must use YYYY-MM-DD format")
        return cleaned

    @field_validator("linkedin_url")
    @classmethod
    def clean_linkedin_url(cls, value: str) -> str:
        return _clean_string(value)

    @model_validator(mode="after")
    def summary_fallback(self) -> PersonCard:
        self.first_name = _clean_string(self.first_name)
        self.last_name = _clean_string(self.last_name)
        self.linkedin, self.linkedin_url = _normalize_linkedin_fields(self.linkedin, self.linkedin_url)
        self.github = _normalize_profile_handle(self.github, provider="github")
        self.twitter = _normalize_profile_handle(self.twitter, provider="twitter")
        self.instagram = _normalize_profile_handle(self.instagram, provider="instagram")
        self.telegram = _normalize_profile_handle(self.telegram, provider="telegram")
        if self.companies and not self.company:
            self.company = self.companies[0]
        if self.titles and not self.title:
            self.title = self.titles[0]
        if not self.summary:
            if self.first_name or self.last_name:
                self.summary = " ".join(part for part in [self.first_name, self.last_name] if part)
            else:
                self.summary = self.emails[0] if self.emails else "unknown"
        self.summary = _clean_string(self.summary)
        full_name = " ".join(part for part in [self.first_name, self.last_name] if part)
        alias_candidates = [alias for alias in self.aliases if _clean_string(alias)]
        if full_name and full_name != self.summary:
            alias_candidates.append(full_name)
        normalized_summary = self.summary.lower()
        self.aliases = _dedupe_preserve_order(
            [alias for alias in alias_candidates if alias.lower() != normalized_summary]
        )
        return self


class FinanceCard(BaseCard):
    type: Literal["finance"] = "finance"
    amount: float
    currency: str = "USD"
    counterparty: str = ""
    category: str = ""
    parent_category: str = ""
    account: str = ""
    account_mask: str = ""
    transaction_status: str = ""
    transaction_type: str = ""
    excluded: bool = False
    provider_tags: list[str] = Field(default_factory=list)
    note: str = ""
    recurring_label: str = ""


class MedicalRecordCard(BaseCard):
    type: Literal["medical_record"] = "medical_record"
    source_system: str = ""
    source_format: str = ""
    record_type: str = ""
    record_subtype: str = ""
    status: str = ""
    occurred_at: str = ""
    recorded_at: str = ""
    provider_name: str = ""
    facility_name: str = ""
    encounter_source_id: str = ""
    code_system: str = ""
    code: str = ""
    code_display: str = ""
    value_text: str = ""
    value_numeric: float = 0.0
    unit: str = ""
    raw_source_ref: str = ""
    details_json: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "source_system",
        "source_format",
        "record_type",
        "record_subtype",
        "status",
        "occurred_at",
        "recorded_at",
        "provider_name",
        "facility_name",
        "encounter_source_id",
        "code_system",
        "code",
        "code_display",
        "value_text",
        "unit",
        "raw_source_ref",
    )
    @classmethod
    def clean_medical_record_strings(cls, value: str) -> str:
        return _clean_string(value)

    @model_validator(mode="after")
    def medical_record_summary_fallback(self) -> MedicalRecordCard:
        self.source_system = _clean_string(self.source_system).lower()
        self.source_format = _clean_string(self.source_format).lower()
        self.record_type = _clean_string(self.record_type).lower()
        self.record_subtype = _clean_string(self.record_subtype)
        self.status = _clean_string(self.status).lower()
        if not self.summary:
            self.summary = (
                self.code_display or self.value_text or self.record_subtype or self.record_type or self.source_id
            )
        self.summary = _clean_string(self.summary)
        return self


class VaccinationCard(BaseCard):
    type: Literal["vaccination"] = "vaccination"
    source_system: str = ""
    source_format: str = ""
    occurred_at: str = ""
    vaccine_name: str = ""
    cvx_code: str = ""
    status: str = ""
    manufacturer: str = ""
    brand_name: str = ""
    lot_number: str = ""
    expiration_date: str = ""
    administered_at: str = ""
    performer_name: str = ""
    location: str = ""
    raw_source_ref: str = ""
    details_json: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "source_system",
        "source_format",
        "occurred_at",
        "vaccine_name",
        "cvx_code",
        "status",
        "manufacturer",
        "brand_name",
        "lot_number",
        "expiration_date",
        "administered_at",
        "performer_name",
        "location",
        "raw_source_ref",
    )
    @classmethod
    def clean_vaccination_strings(cls, value: str) -> str:
        return _clean_string(value)

    @model_validator(mode="after")
    def vaccination_summary_fallback(self) -> VaccinationCard:
        self.source_system = _clean_string(self.source_system).lower()
        self.source_format = _clean_string(self.source_format).lower()
        self.status = _clean_string(self.status).lower()
        if not self.summary:
            self.summary = self.vaccine_name or self.brand_name or self.occurred_at or self.source_id
        self.summary = _clean_string(self.summary)
        return self


class EmailThreadCard(BaseCard):
    type: Literal["email_thread"] = "email_thread"
    gmail_thread_id: str
    gmail_history_id: str = ""
    account_email: str = ""
    subject: str = ""
    participants: list[str] = Field(default_factory=list)
    label_ids: list[str] = Field(default_factory=list)
    messages: list[str] = Field(default_factory=list)
    calendar_events: list[str] = Field(default_factory=list)
    first_message_at: str = ""
    last_message_at: str = ""
    message_count: int = 0
    has_attachments: bool = False
    invite_ical_uids: list[str] = Field(default_factory=list)
    invite_event_id_hints: list[str] = Field(default_factory=list)
    thread_summary: str = ""
    thread_body_sha: str = ""

    @field_validator("account_email", "participants")
    @classmethod
    def lowercase_email_fields(cls, value: str | list[str]) -> str | list[str]:
        if isinstance(value, str):
            return value.lower().strip()
        return _dedupe_preserve_order([item.lower().strip() for item in value if item and item.strip()])

    @field_validator(
        "subject",
        "gmail_history_id",
        "first_message_at",
        "last_message_at",
        "thread_summary",
        "thread_body_sha",
    )
    @classmethod
    def clean_thread_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator(
        "label_ids",
        "messages",
        "calendar_events",
        "invite_ical_uids",
        "invite_event_id_hints",
    )
    @classmethod
    def dedupe_thread_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def thread_summary_fallback(self) -> EmailThreadCard:
        if not self.summary:
            self.summary = self.subject or self.gmail_thread_id
        self.summary = _clean_string(self.summary)
        if self.messages:
            self.message_count = max(self.message_count, len(self.messages))
        return self


class EmailMessageCard(BaseCard):
    type: Literal["email_message"] = "email_message"
    gmail_message_id: str
    gmail_thread_id: str
    account_email: str = ""
    thread: str = ""
    direction: str = ""
    from_name: str = ""
    from_email: str = ""
    to_emails: list[str] = Field(default_factory=list)
    cc_emails: list[str] = Field(default_factory=list)
    bcc_emails: list[str] = Field(default_factory=list)
    reply_to_emails: list[str] = Field(default_factory=list)
    participant_emails: list[str] = Field(default_factory=list)
    sent_at: str = ""
    subject: str = ""
    snippet: str = ""
    label_ids: list[str] = Field(default_factory=list)
    message_id_header: str = ""
    in_reply_to: str = ""
    references: list[str] = Field(default_factory=list)
    has_attachments: bool = False
    attachments: list[str] = Field(default_factory=list)
    calendar_events: list[str] = Field(default_factory=list)
    invite_ical_uid: str = ""
    invite_event_id_hint: str = ""
    invite_method: str = ""
    invite_title: str = ""
    invite_start_at: str = ""
    invite_end_at: str = ""
    message_body_sha: str = ""

    @field_validator(
        "account_email",
        "from_email",
        "to_emails",
        "cc_emails",
        "bcc_emails",
        "reply_to_emails",
        "participant_emails",
    )
    @classmethod
    def lowercase_message_emails(cls, value: str | list[str]) -> str | list[str]:
        if isinstance(value, str):
            return value.lower().strip()
        return _dedupe_preserve_order([item.lower().strip() for item in value if item and item.strip()])

    @field_validator(
        "direction",
        "from_name",
        "sent_at",
        "subject",
        "snippet",
        "message_id_header",
        "in_reply_to",
        "invite_ical_uid",
        "invite_event_id_hint",
        "invite_method",
        "invite_title",
        "invite_start_at",
        "invite_end_at",
        "message_body_sha",
    )
    @classmethod
    def clean_message_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("label_ids", "references", "attachments", "calendar_events")
    @classmethod
    def dedupe_message_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def message_summary_fallback(self) -> EmailMessageCard:
        if not self.summary:
            self.summary = self.subject or self.snippet or self.gmail_message_id
        self.summary = _clean_string(self.summary)
        return self


class EmailAttachmentCard(BaseCard):
    type: Literal["email_attachment"] = "email_attachment"
    gmail_message_id: str
    gmail_thread_id: str
    attachment_id: str
    account_email: str = ""
    message: str = ""
    thread: str = ""
    filename: str = ""
    mime_type: str = ""
    size_bytes: int = 0
    content_id: str = ""
    is_inline: bool = False
    attachment_metadata_sha: str = ""

    @field_validator("account_email")
    @classmethod
    def lowercase_attachment_account(cls, value: str) -> str:
        return value.lower().strip()

    @field_validator("filename", "mime_type", "content_id", "attachment_metadata_sha")
    @classmethod
    def clean_attachment_strings(cls, value: str) -> str:
        return _clean_string(value)

    @model_validator(mode="after")
    def attachment_summary_fallback(self) -> EmailAttachmentCard:
        if not self.summary:
            self.summary = self.filename or self.attachment_id
        self.summary = _clean_string(self.summary)
        return self


class IMessageThreadCard(BaseCard):
    type: Literal["imessage_thread"] = "imessage_thread"
    imessage_chat_id: str
    service: str = ""
    chat_identifier: str = ""
    display_name: str = ""
    participant_handles: list[str] = Field(default_factory=list)
    messages: list[str] = Field(default_factory=list)
    attachments: list[str] = Field(default_factory=list)
    first_message_at: str = ""
    last_message_at: str = ""
    message_count: int = 0
    attachment_count: int = 0
    is_group: bool = False
    has_attachments: bool = False
    thread_summary: str = ""
    thread_body_sha: str = ""

    @field_validator(
        "service",
        "chat_identifier",
        "display_name",
        "first_message_at",
        "last_message_at",
        "thread_summary",
        "thread_body_sha",
    )
    @classmethod
    def clean_imessage_thread_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("participant_handles")
    @classmethod
    def normalize_imessage_participant_handles(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order(
            [_normalize_contact_handle(item) for item in value if item and _normalize_contact_handle(item)]
        )

    @field_validator("messages", "attachments")
    @classmethod
    def dedupe_imessage_thread_links(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def thread_summary_fallback(self) -> IMessageThreadCard:
        if not self.summary:
            self.summary = (
                self.display_name
                or self.chat_identifier
                or (", ".join(self.participant_handles[:3]) if self.participant_handles else self.imessage_chat_id)
            )
        self.summary = _clean_string(self.summary)
        if self.messages:
            self.message_count = max(self.message_count, len(self.messages))
        if self.attachments:
            self.attachment_count = max(self.attachment_count, len(self.attachments))
        self.has_attachments = self.has_attachments or bool(self.attachments) or self.attachment_count > 0
        return self


class IMessageMessageCard(BaseCard):
    type: Literal["imessage_message"] = "imessage_message"
    imessage_message_id: str
    imessage_chat_id: str = ""
    thread: str = ""
    service: str = ""
    sender_handle: str = ""
    participant_handles: list[str] = Field(default_factory=list)
    is_from_me: bool = False
    sent_at: str = ""
    edited_at: str = ""
    deleted_at: str = ""
    subject: str = ""
    associated_message_guid: str = ""
    associated_message_type: str = ""
    associated_message_emoji: str = ""
    expressive_send_style_id: str = ""
    balloon_bundle_id: str = ""
    has_attachments: bool = False
    attachments: list[str] = Field(default_factory=list)

    @field_validator(
        "thread",
        "service",
        "sender_handle",
        "sent_at",
        "edited_at",
        "deleted_at",
        "subject",
        "associated_message_guid",
        "associated_message_type",
        "associated_message_emoji",
        "expressive_send_style_id",
        "balloon_bundle_id",
    )
    @classmethod
    def clean_imessage_message_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("participant_handles")
    @classmethod
    def normalize_imessage_message_handles(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order(
            [_normalize_contact_handle(item) for item in value if item and _normalize_contact_handle(item)]
        )

    @field_validator("attachments")
    @classmethod
    def dedupe_imessage_message_attachments(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def message_summary_fallback(self) -> IMessageMessageCard:
        self.sender_handle = _normalize_contact_handle(self.sender_handle)
        if not self.summary:
            self.summary = self.subject or self.sender_handle or self.imessage_message_id
        self.summary = _clean_string(self.summary)
        self.has_attachments = self.has_attachments or bool(self.attachments)
        return self


class IMessageAttachmentCard(BaseCard):
    type: Literal["imessage_attachment"] = "imessage_attachment"
    imessage_message_id: str
    imessage_chat_id: str = ""
    attachment_id: str
    message: str = ""
    thread: str = ""
    filename: str = ""
    transfer_name: str = ""
    mime_type: str = ""
    uti: str = ""
    size_bytes: int = 0
    original_path: str = ""
    exported_path: str = ""

    @field_validator(
        "message", "thread", "filename", "transfer_name", "mime_type", "uti", "original_path", "exported_path"
    )
    @classmethod
    def clean_imessage_attachment_strings(cls, value: str) -> str:
        return _clean_string(value)

    @model_validator(mode="after")
    def attachment_summary_fallback(self) -> IMessageAttachmentCard:
        if not self.summary:
            self.summary = self.filename or self.transfer_name or self.attachment_id
        self.summary = _clean_string(self.summary)
        return self


class BeeperThreadCard(BaseCard):
    type: Literal["beeper_thread"] = "beeper_thread"
    beeper_room_id: str
    account_id: str = ""
    protocol: str = ""
    bridge_name: str = ""
    thread_type: str = ""
    thread_title: str = ""
    thread_description: str = ""
    participant_ids: list[str] = Field(default_factory=list)
    participant_names: list[str] = Field(default_factory=list)
    participant_identifiers: list[str] = Field(default_factory=list)
    counterpart_ids: list[str] = Field(default_factory=list)
    counterpart_names: list[str] = Field(default_factory=list)
    counterpart_identifiers: list[str] = Field(default_factory=list)
    messages: list[str] = Field(default_factory=list)
    attachments: list[str] = Field(default_factory=list)
    first_message_at: str = ""
    last_message_at: str = ""
    message_count: int = 0
    attachment_count: int = 0
    is_group: bool = False
    has_attachments: bool = False
    thread_summary: str = ""
    thread_body_sha: str = ""

    @field_validator(
        "beeper_room_id",
        "account_id",
        "protocol",
        "bridge_name",
        "thread_type",
        "thread_title",
        "thread_description",
        "first_message_at",
        "last_message_at",
        "thread_summary",
        "thread_body_sha",
    )
    @classmethod
    def clean_beeper_thread_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator(
        "participant_ids",
        "participant_names",
        "participant_identifiers",
        "counterpart_ids",
        "counterpart_names",
        "counterpart_identifiers",
        "messages",
        "attachments",
    )
    @classmethod
    def dedupe_beeper_thread_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def thread_summary_fallback(self) -> BeeperThreadCard:
        self.thread_type = _clean_string(self.thread_type).lower()
        if not self.summary:
            self.summary = (
                self.thread_title
                or ", ".join(self.counterpart_names[:3])
                or ", ".join(self.counterpart_identifiers[:3])
                or ", ".join(self.participant_names[:3])
                or self.thread_description
                or self.beeper_room_id
            )
        self.summary = _clean_string(self.summary)
        if self.messages:
            self.message_count = max(self.message_count, len(self.messages))
        if self.attachments:
            self.attachment_count = max(self.attachment_count, len(self.attachments))
        self.is_group = self.is_group or self.thread_type == "group"
        self.has_attachments = self.has_attachments or bool(self.attachments) or self.attachment_count > 0
        return self


class BeeperMessageCard(BaseCard):
    type: Literal["beeper_message"] = "beeper_message"
    beeper_event_id: str
    beeper_room_id: str = ""
    account_id: str = ""
    protocol: str = ""
    bridge_name: str = ""
    thread: str = ""
    message_type: str = ""
    sender_id: str = ""
    sender_name: str = ""
    sender_identifier: str = ""
    sender_person: str = ""
    is_from_me: bool = False
    sent_at: str = ""
    edited_at: str = ""
    deleted_at: str = ""
    linked_message_event_id: str = ""
    reply_to_event_id: str = ""
    reaction_key: str = ""
    has_attachments: bool = False
    attachments: list[str] = Field(default_factory=list)
    message_body_sha: str = ""

    @field_validator(
        "beeper_event_id",
        "beeper_room_id",
        "account_id",
        "protocol",
        "bridge_name",
        "thread",
        "message_type",
        "sender_id",
        "sender_name",
        "sender_identifier",
        "sender_person",
        "sent_at",
        "edited_at",
        "deleted_at",
        "linked_message_event_id",
        "reply_to_event_id",
        "reaction_key",
        "message_body_sha",
    )
    @classmethod
    def clean_beeper_message_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("attachments")
    @classmethod
    def dedupe_beeper_message_attachments(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def message_summary_fallback(self) -> BeeperMessageCard:
        self.message_type = _clean_string(self.message_type).upper()
        if not self.summary:
            self.summary = self.sender_name or self.sender_identifier or self.sender_id or self.beeper_event_id
        self.summary = _clean_string(self.summary)
        self.has_attachments = self.has_attachments or bool(self.attachments)
        return self


class BeeperAttachmentCard(BaseCard):
    type: Literal["beeper_attachment"] = "beeper_attachment"
    beeper_event_id: str
    beeper_room_id: str = ""
    attachment_id: str
    account_id: str = ""
    protocol: str = ""
    message: str = ""
    thread: str = ""
    attachment_type: str = ""
    filename: str = ""
    mime_type: str = ""
    size_bytes: int = 0
    src_url: str = ""
    cached_path: str = ""
    width: int = 0
    height: int = 0
    duration_ms: int = 0
    is_voice_note: bool = False
    is_gif: bool = False
    is_sticker: bool = False
    attachment_metadata_sha: str = ""

    @field_validator(
        "beeper_event_id",
        "beeper_room_id",
        "attachment_id",
        "account_id",
        "protocol",
        "message",
        "thread",
        "attachment_type",
        "filename",
        "mime_type",
        "src_url",
        "cached_path",
        "attachment_metadata_sha",
    )
    @classmethod
    def clean_beeper_attachment_strings(cls, value: str) -> str:
        return _clean_string(value)

    @model_validator(mode="after")
    def attachment_summary_fallback(self) -> BeeperAttachmentCard:
        self.attachment_type = _clean_string(self.attachment_type).lower()
        if not self.summary:
            self.summary = self.filename or self.attachment_id
        self.summary = _clean_string(self.summary)
        return self


class CalendarEventCard(BaseCard):
    type: Literal["calendar_event"] = "calendar_event"
    account_email: str = ""
    calendar_id: str
    event_id: str
    event_etag: str = ""
    ical_uid: str = ""
    status: str = ""
    title: str = ""
    description: str = ""
    location: str = ""
    start_at: str = ""
    end_at: str = ""
    timezone: str = ""
    organizer_email: str = ""
    organizer_name: str = ""
    attendee_emails: list[str] = Field(default_factory=list)
    recurrence: list[str] = Field(default_factory=list)
    conference_url: str = ""
    source_messages: list[str] = Field(default_factory=list)
    source_threads: list[str] = Field(default_factory=list)
    meeting_transcripts: list[str] = Field(default_factory=list)
    all_day: bool = False
    event_body_sha: str = ""

    @field_validator("account_email", "organizer_email", "attendee_emails")
    @classmethod
    def lowercase_event_emails(cls, value: str | list[str]) -> str | list[str]:
        if isinstance(value, str):
            return value.lower().strip()
        return _dedupe_preserve_order([item.lower().strip() for item in value if item and item.strip()])

    @field_validator(
        "ical_uid",
        "event_etag",
        "status",
        "title",
        "description",
        "location",
        "start_at",
        "end_at",
        "timezone",
        "organizer_name",
        "conference_url",
        "event_body_sha",
    )
    @classmethod
    def clean_event_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("recurrence", "source_messages", "source_threads", "meeting_transcripts")
    @classmethod
    def dedupe_event_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def event_summary_fallback(self) -> CalendarEventCard:
        if not self.summary:
            self.summary = self.title or self.event_id
        self.summary = _clean_string(self.summary)
        return self


class MediaAssetCard(BaseCard):
    type: Literal["media_asset"] = "media_asset"
    photos_asset_id: str
    photos_source_label: str = ""
    media_type: str = ""
    filename: str = ""
    original_filename: str = ""
    mime_type: str = ""
    original_path: str = ""
    edited_path: str = ""
    size_bytes: int = 0
    width: int = 0
    height: int = 0
    duration_seconds: float = 0.0
    captured_at: str = ""
    modified_at: str = ""
    title: str = ""
    description: str = ""
    keywords: list[str] = Field(default_factory=list)
    labels: list[str] = Field(default_factory=list)
    person_labels: list[str] = Field(default_factory=list)
    albums: list[str] = Field(default_factory=list)
    album_paths: list[str] = Field(default_factory=list)
    folders: list[str] = Field(default_factory=list)
    favorite: bool = False
    hidden: bool = False
    has_adjustments: bool = False
    live_photo: bool = False
    burst: bool = False
    screenshot: bool = False
    slow_mo: bool = False
    time_lapse: bool = False
    is_missing: bool = False
    place_name: str = ""
    place_city: str = ""
    place_state: str = ""
    place_country: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    metadata_sha: str = ""

    @field_validator(
        "photos_source_label",
        "media_type",
        "filename",
        "original_filename",
        "mime_type",
        "original_path",
        "edited_path",
        "captured_at",
        "modified_at",
        "title",
        "description",
        "place_name",
        "place_city",
        "place_state",
        "place_country",
        "metadata_sha",
    )
    @classmethod
    def clean_media_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("labels")
    @classmethod
    def normalize_media_labels(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item).lower() for item in value if item and _clean_string(item)])

    @field_validator("keywords", "person_labels", "albums", "album_paths", "folders")
    @classmethod
    def dedupe_media_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def media_summary_fallback(self) -> MediaAssetCard:
        self.media_type = _clean_string(self.media_type).lower()
        if not self.summary:
            self.summary = self.title or self.original_filename or self.filename or self.photos_asset_id
        self.summary = _clean_string(self.summary)
        return self


class DocumentCard(BaseCard):
    type: Literal["document"] = "document"
    library_root: str = ""
    relative_path: str = ""
    filename: str = ""
    extension: str = ""
    mime_type: str = ""
    size_bytes: int = 0
    content_sha: str = ""
    metadata_sha: str = ""
    file_created_at: str = ""
    file_modified_at: str = ""
    date_start: str = ""
    date_end: str = ""
    document_type: str = ""
    document_date: str = ""
    title: str = ""
    description: str = ""
    authors: list[str] = Field(default_factory=list)
    counterparties: list[str] = Field(default_factory=list)
    emails: list[str] = Field(default_factory=list)
    phones: list[str] = Field(default_factory=list)
    websites: list[str] = Field(default_factory=list)
    location: str = ""
    sheet_names: list[str] = Field(default_factory=list)
    page_count: int = 0
    text_source: str = ""
    extracted_text_sha: str = ""
    extraction_status: str = ""
    quality_flags: list[str] = Field(default_factory=list)

    @field_validator(
        "library_root",
        "relative_path",
        "filename",
        "extension",
        "mime_type",
        "content_sha",
        "metadata_sha",
        "file_created_at",
        "file_modified_at",
        "date_start",
        "date_end",
        "document_type",
        "document_date",
        "title",
        "description",
        "location",
        "text_source",
        "extracted_text_sha",
        "extraction_status",
    )
    @classmethod
    def clean_document_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("emails")
    @classmethod
    def lowercase_document_emails(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([item.lower().strip() for item in value if item and item.strip()])

    @field_validator("phones")
    @classmethod
    def dedupe_document_phones(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([item.strip() for item in value if item and item.strip()])

    @field_validator("authors", "counterparties", "websites", "sheet_names", "quality_flags")
    @classmethod
    def dedupe_document_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def document_summary_fallback(self) -> DocumentCard:
        self.extension = _clean_string(self.extension).lower().lstrip(".")
        self.document_type = _clean_string(self.document_type).lower()
        self.text_source = _clean_string(self.text_source).lower()
        self.extraction_status = _clean_string(self.extraction_status).lower()
        if not self.summary:
            self.summary = self.title or self.filename or self.relative_path or self.source_id
        self.summary = _clean_string(self.summary)
        return self


class MeetingTranscriptCard(BaseCard):
    type: Literal["meeting_transcript"] = "meeting_transcript"
    otter_meeting_id: str
    otter_conversation_id: str = ""
    account_email: str = ""
    title: str = ""
    meeting_url: str = ""
    transcript_url: str = ""
    recording_url: str = ""
    conference_url: str = ""
    language: str = ""
    status: str = ""
    start_at: str = ""
    end_at: str = ""
    duration_seconds: int = 0
    speaker_names: list[str] = Field(default_factory=list)
    speaker_emails: list[str] = Field(default_factory=list)
    participant_names: list[str] = Field(default_factory=list)
    participant_emails: list[str] = Field(default_factory=list)
    host_name: str = ""
    host_email: str = ""
    calendar_events: list[str] = Field(default_factory=list)
    event_id_hint: str = ""
    ical_uid: str = ""
    otter_updated_at: str = ""
    transcript_body_sha: str = ""

    @field_validator("account_email", "speaker_emails", "participant_emails", "host_email")
    @classmethod
    def lowercase_transcript_emails(cls, value: str | list[str]) -> str | list[str]:
        if isinstance(value, str):
            return value.lower().strip()
        return _dedupe_preserve_order([item.lower().strip() for item in value if item and item.strip()])

    @field_validator(
        "otter_conversation_id",
        "title",
        "meeting_url",
        "transcript_url",
        "recording_url",
        "conference_url",
        "language",
        "status",
        "start_at",
        "end_at",
        "host_name",
        "event_id_hint",
        "otter_updated_at",
        "transcript_body_sha",
    )
    @classmethod
    def clean_transcript_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("speaker_names", "participant_names", "calendar_events")
    @classmethod
    def dedupe_transcript_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def transcript_summary_fallback(self) -> MeetingTranscriptCard:
        self.status = _clean_string(self.status).lower()
        self.language = _clean_string(self.language).lower()
        if not self.summary:
            self.summary = self.title or self.otter_meeting_id
        self.summary = _clean_string(self.summary)
        return self


class GitRepositoryCard(BaseCard):
    type: Literal["git_repository"] = "git_repository"
    github_repo_id: str
    github_node_id: str = ""
    name_with_owner: str = ""
    owner_login: str = ""
    owner_type: str = ""
    html_url: str = ""
    api_url: str = ""
    ssh_url: str = ""
    default_branch: str = ""
    homepage_url: str = ""
    description: str = ""
    visibility: str = ""
    is_private: bool = False
    is_fork: bool = False
    is_archived: bool = False
    parent_name_with_owner: str = ""
    primary_language: str = ""
    languages: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    license_name: str = ""
    created_at: str = ""
    pushed_at: str = ""

    @field_validator(
        "github_repo_id",
        "github_node_id",
        "name_with_owner",
        "owner_login",
        "owner_type",
        "html_url",
        "api_url",
        "ssh_url",
        "default_branch",
        "homepage_url",
        "description",
        "visibility",
        "parent_name_with_owner",
        "primary_language",
        "license_name",
        "created_at",
        "pushed_at",
    )
    @classmethod
    def clean_git_repository_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("languages", "topics")
    @classmethod
    def dedupe_git_repository_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def git_repository_summary_fallback(self) -> GitRepositoryCard:
        self.owner_login = _normalize_handle(self.owner_login)
        self.owner_type = _clean_string(self.owner_type)
        self.visibility = _clean_string(self.visibility).lower()
        self.primary_language = _clean_string(self.primary_language)
        self.languages = _dedupe_preserve_order([_clean_string(item) for item in self.languages if _clean_string(item)])
        self.topics = _dedupe_preserve_order(
            [_clean_string(item).lower() for item in self.topics if _clean_string(item)]
        )
        if not self.summary:
            self.summary = self.name_with_owner or self.source_id or self.github_repo_id
        self.summary = _clean_string(self.summary)
        return self


class GitCommitCard(BaseCard):
    type: Literal["git_commit"] = "git_commit"
    github_node_id: str = ""
    commit_sha: str
    repository_name_with_owner: str = ""
    repository: str = ""
    parent_shas: list[str] = Field(default_factory=list)
    html_url: str = ""
    api_url: str = ""
    authored_at: str = ""
    committed_at: str = ""
    message_headline: str = ""
    additions: int = 0
    deletions: int = 0
    changed_files: int = 0
    author_login: str = ""
    author_name: str = ""
    author_email: str = ""
    committer_login: str = ""
    committer_name: str = ""
    committer_email: str = ""
    associated_pr_numbers: list[str] = Field(default_factory=list)
    associated_pr_urls: list[str] = Field(default_factory=list)

    @field_validator(
        "github_node_id",
        "commit_sha",
        "repository_name_with_owner",
        "repository",
        "html_url",
        "api_url",
        "authored_at",
        "committed_at",
        "message_headline",
        "author_login",
        "author_name",
        "author_email",
        "committer_login",
        "committer_name",
        "committer_email",
    )
    @classmethod
    def clean_git_commit_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("parent_shas", "associated_pr_numbers", "associated_pr_urls")
    @classmethod
    def dedupe_git_commit_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def git_commit_summary_fallback(self) -> GitCommitCard:
        self.commit_sha = _clean_string(self.commit_sha).lower()
        self.author_login = _normalize_handle(self.author_login)
        self.committer_login = _normalize_handle(self.committer_login)
        self.author_email = _clean_string(self.author_email).lower()
        self.committer_email = _clean_string(self.committer_email).lower()
        if not self.summary:
            self.summary = self.message_headline or self.commit_sha[:12] or self.source_id
        self.summary = _clean_string(self.summary)
        return self


class GitThreadCard(BaseCard):
    type: Literal["git_thread"] = "git_thread"
    github_thread_id: str
    github_node_id: str = ""
    repository_name_with_owner: str = ""
    repository: str = ""
    thread_type: str = ""
    number: str = ""
    html_url: str = ""
    api_url: str = ""
    state: str = ""
    is_draft: bool = False
    merged_at: str = ""
    closed_at: str = ""
    title: str = ""
    labels: list[str] = Field(default_factory=list)
    assignees: list[str] = Field(default_factory=list)
    milestone: str = ""
    base_ref: str = ""
    head_ref: str = ""
    participant_logins: list[str] = Field(default_factory=list)
    messages: list[str] = Field(default_factory=list)
    first_message_at: str = ""
    last_message_at: str = ""
    message_count: int = 0

    @field_validator(
        "github_thread_id",
        "github_node_id",
        "repository_name_with_owner",
        "repository",
        "thread_type",
        "number",
        "html_url",
        "api_url",
        "state",
        "merged_at",
        "closed_at",
        "title",
        "milestone",
        "base_ref",
        "head_ref",
        "first_message_at",
        "last_message_at",
    )
    @classmethod
    def clean_git_thread_strings(cls, value: str) -> str:
        return _clean_string(value)

    @field_validator("labels", "assignees", "participant_logins", "messages")
    @classmethod
    def dedupe_git_thread_lists(cls, value: list[str]) -> list[str]:
        return _dedupe_preserve_order([_clean_string(item) for item in value if item and _clean_string(item)])

    @model_validator(mode="after")
    def git_thread_summary_fallback(self) -> GitThreadCard:
        self.thread_type = _clean_string(self.thread_type).lower()
        self.state = _clean_string(self.state).lower()
        self.assignees = _dedupe_preserve_order(
            [_normalize_handle(item) for item in self.assignees if _normalize_handle(item)]
        )
        self.participant_logins = _dedupe_preserve_order(
            [_normalize_handle(item) for item in self.participant_logins if _normalize_handle(item)]
        )
        self.labels = _dedupe_preserve_order(
            [_clean_string(item).lower() for item in self.labels if _clean_string(item)]
        )
        if self.messages:
            self.message_count = max(self.message_count, len(self.messages))
        if not self.summary:
            repo_ref = self.repository_name_with_owner or self.source_id
            number_ref = f"#{self.number}" if self.number else self.github_thread_id
            self.summary = self.title or " ".join(part for part in [repo_ref, number_ref] if part)
        self.summary = _clean_string(self.summary)
        return self


class GitMessageCard(BaseCard):
    type: Literal["git_message"] = "git_message"
    github_message_id: str
    github_node_id: str = ""
    repository_name_with_owner: str = ""
    repository: str = ""
    thread: str = ""
    message_type: str = ""
    html_url: str = ""
    api_url: str = ""
    actor_login: str = ""
    actor_name: str = ""
    actor_email: str = ""
    sent_at: str = ""
    updated_at: str = ""
    review_state: str = ""
    review_commit_sha: str = ""
    in_reply_to_message_id: str = ""
    path: str = ""
    position: str = ""
    original_position: str = ""
    original_commit_sha: str = ""
    diff_hunk: str = ""

    @field_validator(
        "github_message_id",
        "github_node_id",
        "repository_name_with_owner",
        "repository",
        "thread",
        "message_type",
        "html_url",
        "api_url",
        "actor_login",
        "actor_name",
        "actor_email",
        "sent_at",
        "updated_at",
        "review_state",
        "review_commit_sha",
        "in_reply_to_message_id",
        "path",
        "position",
        "original_position",
        "original_commit_sha",
        "diff_hunk",
    )
    @classmethod
    def clean_git_message_strings(cls, value: str) -> str:
        return _clean_string(value)

    @model_validator(mode="after")
    def git_message_summary_fallback(self) -> GitMessageCard:
        self.message_type = _clean_string(self.message_type).lower()
        self.review_state = _clean_string(self.review_state).upper()
        self.actor_login = _normalize_handle(self.actor_login)
        self.actor_email = _clean_string(self.actor_email).lower()
        self.review_commit_sha = _clean_string(self.review_commit_sha).lower()
        self.original_commit_sha = _clean_string(self.original_commit_sha).lower()
        if not self.summary:
            self.summary = self.actor_name or self.actor_login or self.message_type or self.github_message_id
        self.summary = _clean_string(self.summary)
        return self


CARD_TYPES: dict[str, type[BaseCard]] = {
    "person": PersonCard,
    "finance": FinanceCard,
    "medical_record": MedicalRecordCard,
    "vaccination": VaccinationCard,
    "email_thread": EmailThreadCard,
    "email_message": EmailMessageCard,
    "email_attachment": EmailAttachmentCard,
    "imessage_thread": IMessageThreadCard,
    "imessage_message": IMessageMessageCard,
    "imessage_attachment": IMessageAttachmentCard,
    "beeper_thread": BeeperThreadCard,
    "beeper_message": BeeperMessageCard,
    "beeper_attachment": BeeperAttachmentCard,
    "calendar_event": CalendarEventCard,
    "media_asset": MediaAssetCard,
    "document": DocumentCard,
    "meeting_transcript": MeetingTranscriptCard,
    "git_repository": GitRepositoryCard,
    "git_commit": GitCommitCard,
    "git_thread": GitThreadCard,
    "git_message": GitMessageCard,
}


def _model_for_type(card_type: str) -> type[BaseCard]:
    return CARD_TYPES.get(card_type, BaseCard)


def _known_fields(model: type[BaseCard]) -> set[str]:
    return set(model.model_fields)


def validate_card_strict(data: dict[str, Any]) -> BaseCard:
    """Validate a card for write paths, rejecting unknown fields."""

    model = _model_for_type(str(data.get("type", "")))
    return model.model_validate(data)


def validate_card_permissive(data: dict[str, Any]) -> BaseCard:
    """Validate a card for read paths, ignoring unknown fields."""

    model = _model_for_type(str(data.get("type", "")))
    filtered = {key: value for key, value in data.items() if key in _known_fields(model)}
    return model.model_validate(filtered)


def validate_card(data: dict[str, Any]) -> BaseCard:
    """Validate a card using the strict write-path rules."""

    return validate_card_strict(data)


def card_to_frontmatter(card: BaseCard) -> dict[str, Any]:
    """Convert a validated card to a frontmatter dict with empty defaults omitted."""

    dumped = card.model_dump(mode="python")
    rendered: dict[str, Any] = {}
    for field_name, field in type(card).model_fields.items():
        value = dumped[field_name]
        default: Any
        if field.is_required():
            rendered[field_name] = value
            continue
        default = field.default_factory() if field.default_factory is not None else field.default
        if value == default and default in ("", [], 0, {}):
            continue
        rendered[field_name] = value
    return rendered


def get_card_type_spec(card_type: str):
    """Return the explicit contract for a canonical card type."""

    from hfa.card_contracts import get_card_type_spec as _get_card_type_spec

    return _get_card_type_spec(card_type)


def iter_card_type_specs():
    """Return all canonical card contracts in registry order."""

    from hfa.card_contracts import iter_card_type_specs as _iter_card_type_specs

    return _iter_card_type_specs()
