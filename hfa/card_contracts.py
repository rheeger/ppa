"""Canonical card contract registry for the archive platform."""

from __future__ import annotations

from dataclasses import dataclass

from hfa.schema import CARD_TYPES, DETERMINISTIC_ONLY, LLM_ELIGIBLE, BaseCard


@dataclass(frozen=True)
class CardTypeSpec:
    card_type: str
    model_cls: type[BaseCard]
    rel_path_family: str
    deterministic_fields: tuple[str, ...]
    llm_eligible_fields: tuple[str, ...]
    external_id_fields: tuple[str, ...]
    relationship_fields: tuple[str, ...]
    chunk_profile: str
    edge_profile: str
    typed_projection: str


_CARD_TYPE_METADATA: dict[str, dict[str, object]] = {
    "person": {
        "rel_path_family": "People",
        "external_id_fields": (
            "source_id",
            "linkedin",
            "linkedin_url",
            "github",
            "twitter",
            "instagram",
            "telegram",
            "discord",
        ),
        "relationship_fields": ("reports_to", "people", "orgs"),
        "chunk_profile": "person",
        "edge_profile": "person",
        "typed_projection": "people",
    },
    "finance": {
        "rel_path_family": "Finance",
        "external_id_fields": ("source_id",),
        "relationship_fields": ("people", "orgs", "counterparties"),
        "chunk_profile": "default",
        "edge_profile": "default",
        "typed_projection": "finance_records",
    },
    "medical_record": {
        "rel_path_family": "Medical",
        "external_id_fields": ("source_id", "encounter_source_id"),
        "relationship_fields": ("people", "orgs"),
        "chunk_profile": "default",
        "edge_profile": "default",
        "typed_projection": "medical_records",
    },
    "vaccination": {
        "rel_path_family": "Vaccinations",
        "external_id_fields": ("source_id",),
        "relationship_fields": ("people", "orgs"),
        "chunk_profile": "default",
        "edge_profile": "default",
        "typed_projection": "vaccinations",
    },
    "email_thread": {
        "rel_path_family": "EmailThreads",
        "external_id_fields": ("source_id", "gmail_thread_id"),
        "relationship_fields": ("people", "orgs", "messages", "calendar_events"),
        "chunk_profile": "email_thread",
        "edge_profile": "email_thread",
        "typed_projection": "email_threads",
    },
    "email_message": {
        "rel_path_family": "Email",
        "external_id_fields": (
            "source_id",
            "gmail_message_id",
            "gmail_history_id",
            "message_id_header",
            "invite_ical_uid",
            "invite_event_id_hint",
        ),
        "relationship_fields": ("people", "orgs", "thread", "attachments", "calendar_events"),
        "chunk_profile": "email_message",
        "edge_profile": "email_message",
        "typed_projection": "email_messages",
    },
    "email_attachment": {
        "rel_path_family": "EmailAttachments",
        "external_id_fields": ("source_id", "attachment_id", "content_id"),
        "relationship_fields": ("people", "orgs", "message", "thread"),
        "chunk_profile": "default",
        "edge_profile": "default",
        "typed_projection": "email_attachments",
    },
    "imessage_thread": {
        "rel_path_family": "IMessageThreads",
        "external_id_fields": ("source_id", "imessage_chat_id"),
        "relationship_fields": ("people", "orgs", "messages"),
        "chunk_profile": "imessage_thread",
        "edge_profile": "imessage_thread",
        "typed_projection": "imessage_threads",
    },
    "imessage_message": {
        "rel_path_family": "IMessage",
        "external_id_fields": ("source_id", "imessage_message_id", "linked_message_event_id", "reply_to_event_id"),
        "relationship_fields": ("people", "orgs", "thread"),
        "chunk_profile": "default",
        "edge_profile": "imessage_message",
        "typed_projection": "imessage_messages",
    },
    "imessage_attachment": {
        "rel_path_family": "IMessageAttachments",
        "external_id_fields": ("source_id", "attachment_id"),
        "relationship_fields": ("people", "orgs", "message", "thread"),
        "chunk_profile": "default",
        "edge_profile": "default",
        "typed_projection": "imessage_attachments",
    },
    "beeper_thread": {
        "rel_path_family": "BeeperThreads",
        "external_id_fields": ("source_id", "beeper_room_id"),
        "relationship_fields": ("people", "orgs", "messages"),
        "chunk_profile": "default",
        "edge_profile": "default",
        "typed_projection": "beeper_threads",
    },
    "beeper_message": {
        "rel_path_family": "Beeper",
        "external_id_fields": ("source_id", "beeper_event_id", "linked_message_event_id", "reply_to_event_id"),
        "relationship_fields": ("people", "orgs", "thread"),
        "chunk_profile": "default",
        "edge_profile": "default",
        "typed_projection": "beeper_messages",
    },
    "beeper_attachment": {
        "rel_path_family": "BeeperAttachments",
        "external_id_fields": ("source_id", "attachment_id", "src_url"),
        "relationship_fields": ("people", "orgs", "message", "thread"),
        "chunk_profile": "default",
        "edge_profile": "default",
        "typed_projection": "beeper_attachments",
    },
    "calendar_event": {
        "rel_path_family": "Calendar",
        "external_id_fields": (
            "source_id",
            "calendar_id",
            "event_id",
            "event_etag",
            "ical_uid",
            "invite_ical_uid",
            "invite_event_id_hint",
            "event_id_hint",
        ),
        "relationship_fields": ("people", "orgs", "source_messages", "source_threads", "meeting_transcripts"),
        "chunk_profile": "calendar_event",
        "edge_profile": "calendar_event",
        "typed_projection": "calendar_events",
    },
    "media_asset": {
        "rel_path_family": "Photos",
        "external_id_fields": ("source_id", "photos_asset_id"),
        "relationship_fields": ("people", "orgs"),
        "chunk_profile": "default",
        "edge_profile": "default",
        "typed_projection": "media_assets",
    },
    "document": {
        "rel_path_family": "Documents",
        "external_id_fields": ("source_id", "content_sha", "extracted_text_sha"),
        "relationship_fields": ("people", "orgs", "authors", "counterparties"),
        "chunk_profile": "document",
        "edge_profile": "default",
        "typed_projection": "documents",
    },
    "meeting_transcript": {
        "rel_path_family": "MeetingTranscripts",
        "external_id_fields": ("source_id", "otter_meeting_id", "otter_conversation_id", "event_id_hint"),
        "relationship_fields": ("people", "orgs", "calendar_events"),
        "chunk_profile": "meeting_transcript",
        "edge_profile": "meeting_transcript",
        "typed_projection": "meeting_transcripts",
    },
    "git_repository": {
        "rel_path_family": "GitRepos",
        "external_id_fields": ("source_id", "repository_id", "repository_name_with_owner"),
        "relationship_fields": ("people", "orgs"),
        "chunk_profile": "git_repository",
        "edge_profile": "git_repository",
        "typed_projection": "git_repositories",
    },
    "git_commit": {
        "rel_path_family": "GitCommits",
        "external_id_fields": ("source_id", "commit_sha"),
        "relationship_fields": ("people", "orgs", "parent_shas", "repository"),
        "chunk_profile": "git_commit",
        "edge_profile": "git_commit",
        "typed_projection": "git_commits",
    },
    "git_thread": {
        "rel_path_family": "GitThreads",
        "external_id_fields": ("source_id", "github_thread_id", "number", "associated_pr_numbers"),
        "relationship_fields": ("people", "orgs", "messages", "repository"),
        "chunk_profile": "git_thread",
        "edge_profile": "git_thread",
        "typed_projection": "git_threads",
    },
    "git_message": {
        "rel_path_family": "GitMessages",
        "external_id_fields": ("source_id", "github_message_id", "review_commit_sha", "original_commit_sha"),
        "relationship_fields": ("people", "orgs", "thread"),
        "chunk_profile": "git_message",
        "edge_profile": "git_message",
        "typed_projection": "git_messages",
    },
}


def _ordered_subset(model_cls: type[BaseCard], fields: frozenset[str]) -> tuple[str, ...]:
    return tuple(field_name for field_name in model_cls.model_fields if field_name in fields)


def _build_specs() -> dict[str, CardTypeSpec]:
    specs: dict[str, CardTypeSpec] = {}
    for card_type, model_cls in CARD_TYPES.items():
        metadata = _CARD_TYPE_METADATA[card_type]
        specs[card_type] = CardTypeSpec(
            card_type=card_type,
            model_cls=model_cls,
            rel_path_family=str(metadata["rel_path_family"]),
            deterministic_fields=_ordered_subset(model_cls, DETERMINISTIC_ONLY),
            llm_eligible_fields=_ordered_subset(model_cls, LLM_ELIGIBLE),
            external_id_fields=tuple(str(item) for item in metadata["external_id_fields"]),
            relationship_fields=tuple(str(item) for item in metadata["relationship_fields"]),
            chunk_profile=str(metadata["chunk_profile"]),
            edge_profile=str(metadata["edge_profile"]),
            typed_projection=str(metadata["typed_projection"]),
        )
    return specs


CARD_TYPE_SPECS = _build_specs()


def get_card_type_spec(card_type: str) -> CardTypeSpec:
    return CARD_TYPE_SPECS[card_type]


def iter_card_type_specs() -> tuple[CardTypeSpec, ...]:
    return tuple(CARD_TYPE_SPECS[card_type] for card_type in CARD_TYPES)
