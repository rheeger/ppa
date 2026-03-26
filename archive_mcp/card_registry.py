"""Unified card-type registry for the derived index.

Each CardTypeRegistration declares the typed projection columns, edge rules,
chunk builder reference, and person-edge labelling for one card type.  Adding
a new card type is a two-file change:

  1. hfa/schema.py  -- Pydantic model + CARD_TYPES entry
  2. This file       -- CardTypeRegistration entry in CARD_TYPE_REGISTRATIONS
"""

from __future__ import annotations

from .contracts import CardTypeRegistration, DeclEdgeRule, ProjectionColumnSpec

# ---------------------------------------------------------------------------
# Column helpers (convenience constructors for ProjectionColumnSpec)
# ---------------------------------------------------------------------------


def _text(
    name: str,
    *,
    source_field: str | None = None,
    nullable: bool = True,
    indexed: bool = False,
    default: str = "",
    value_mode: str = "text",
) -> ProjectionColumnSpec:
    return ProjectionColumnSpec(
        name,
        "TEXT",
        nullable=nullable,
        indexed=indexed,
        source_field=source_field or name,
        value_mode=value_mode,
        default=default,
    )


def _json(
    name: str, *, source_field: str | None = None, nullable: bool = False, indexed: bool = False, default: str = "[]"
) -> ProjectionColumnSpec:
    return ProjectionColumnSpec(
        name,
        "JSONB",
        nullable=nullable,
        indexed=indexed,
        source_field=source_field or name,
        value_mode="json",
        default=default,
    )


def _bool(
    name: str, *, source_field: str | None = None, nullable: bool = False, indexed: bool = False, default: bool = False
) -> ProjectionColumnSpec:
    return ProjectionColumnSpec(
        name,
        "BOOLEAN",
        nullable=nullable,
        indexed=indexed,
        source_field=source_field or name,
        value_mode="bool",
        default=default,
    )


def _float(
    name: str, *, source_field: str | None = None, nullable: bool = False, indexed: bool = False, default: float = 0.0
) -> ProjectionColumnSpec:
    return ProjectionColumnSpec(
        name,
        "DOUBLE PRECISION",
        nullable=nullable,
        indexed=indexed,
        source_field=source_field or name,
        value_mode="float",
        default=default,
    )


def _int(
    name: str, *, source_field: str | None = None, nullable: bool = False, indexed: bool = False, default: int = 0
) -> ProjectionColumnSpec:
    return ProjectionColumnSpec(
        name,
        "INTEGER",
        nullable=nullable,
        indexed=indexed,
        source_field=source_field or name,
        value_mode="int",
        default=default,
    )


# ---------------------------------------------------------------------------
# Card-type registrations (one per card type, sorted by card_type)
# ---------------------------------------------------------------------------

CARD_TYPE_REGISTRATIONS: tuple[CardTypeRegistration, ...] = (
    # ── person ──────────────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="person",
        projection_table="people",
        projection_columns=(
            _text("first_name"),
            _text("last_name"),
            _json("aliases_json", source_field="aliases"),
            _json("emails_json", source_field="emails"),
            _json("phones_json", source_field="phones"),
            _text("company"),
            _json("companies_json", source_field="companies"),
            _text("title"),
            _json("titles_json", source_field="titles"),
            _text("linkedin"),
            _text("github"),
            _text("twitter"),
            _text("instagram"),
            _text("telegram"),
            _text("discord"),
            _text("reports_to"),
            _json("websites_json", source_field="websites"),
            _text("birthday"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(DeclEdgeRule("reports_to", "person_reports_to", "person", ("reports_to",), multi=False),),
        chunk_builder_name="person",
        chunk_types=("person_profile", "person_role", "person_context", "person_body"),
    ),
    # ── finance ─────────────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="finance",
        projection_table="finance_records",
        projection_columns=(
            _float("amount"),
            _text("currency"),
            _text("date_start"),
            _text("date_end"),
            _json("counterparties_json", source_field="counterparties"),
            _json("emails_json", source_field="emails"),
            _json("phones_json", source_field="phones"),
            _json("websites_json", source_field="websites"),
            _text("location"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(),
        chunk_builder_name=None,
        chunk_types=(),
    ),
    # ── medical_record ──────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="medical_record",
        projection_table="medical_records",
        projection_columns=(
            _text("person", source_field="people", value_mode="primary_person"),
            _text("source_system"),
            _text("source_format"),
            _text("record_type"),
            _text("record_subtype"),
            _text("status"),
            _text("occurred_at"),
            _text("recorded_at"),
            _text("provider_name"),
            _text("facility_name"),
            _text("encounter_source_id"),
            _text("code_system"),
            _text("code"),
            _text("code_display"),
            _text("value_text"),
            _float("value_numeric"),
            _text("unit"),
            _text("raw_source_ref"),
            _json("details_json", source_field="details_json", default="{}"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(),
        chunk_builder_name=None,
        chunk_types=(),
    ),
    # ── vaccination ─────────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="vaccination",
        projection_table="vaccinations",
        projection_columns=(
            _text("person", source_field="people", value_mode="primary_person"),
            _text("source_system"),
            _text("source_format"),
            _text("occurred_at"),
            _text("recorded_at"),
            _text("vaccine_name"),
            _text("cvx_code"),
            _text("manufacturer"),
            _text("brand_name"),
            _text("lot_number"),
            _text("dose_number"),
            _bool("series_complete"),
            _text("provider_name"),
            _text("facility_name"),
            _text("raw_source_ref"),
            _json("details_json", source_field="details_json", default="{}"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(),
        chunk_builder_name=None,
        chunk_types=(),
    ),
    # ── email_thread ────────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="email_thread",
        projection_table="email_threads",
        projection_columns=(
            _text("subject"),
            _json("participants_json", source_field="participants"),
            _json("participant_emails_json", source_field="participant_emails"),
            _int("message_count"),
            _text("first_message_at"),
            _text("last_message_at"),
            _json("calendar_events_json", source_field="calendar_events"),
            _json("messages_json", source_field="messages"),
            _text("thread_body_sha"),
        ),
        person_edge_type="thread_has_person",
        edge_rules=(
            DeclEdgeRule("messages", "thread_has_message", "card", ("messages",)),
            DeclEdgeRule("calendar_events", "thread_has_calendar_event", "card", ("calendar_events",)),
            DeclEdgeRule("participants", "thread_has_person", "person", ("account_email", "participants")),
        ),
        chunk_builder_name="email_thread",
        chunk_types=("thread_subject", "thread_context", "thread_summary", "thread_window", "thread_recent_window"),
    ),
    # ── email_message ───────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="email_message",
        projection_table="email_messages",
        projection_columns=(
            _text("thread"),
            _text("direction"),
            _text("from_name"),
            _text("from_email"),
            _json("to_emails_json", source_field="to_emails"),
            _json("cc_emails_json", source_field="cc_emails"),
            _json("bcc_emails_json", source_field="bcc_emails"),
            _json("reply_to_emails_json", source_field="reply_to_emails"),
            _json("participant_emails_json", source_field="participant_emails"),
            _text("sent_at"),
            _text("subject"),
            _text("snippet"),
            _text("message_id_header"),
            _text("in_reply_to"),
            _json("references_json", source_field="references"),
            _bool("has_attachments"),
            _json("attachments_json", source_field="attachments"),
            _json("calendar_events_json", source_field="calendar_events"),
            _text("message_body_sha"),
        ),
        person_edge_type="message_mentions_person",
        edge_rules=(
            DeclEdgeRule("attachments", "message_has_attachment", "card", ("attachments",)),
            DeclEdgeRule("calendar_events", "message_has_calendar_event", "card", ("calendar_events",)),
            DeclEdgeRule("thread", "message_in_thread", "card", ("thread",), multi=False),
            DeclEdgeRule(
                "participant_emails",
                "message_mentions_person",
                "person",
                ("from_email", "participant_emails", "to_emails", "cc_emails", "bcc_emails"),
            ),
        ),
        chunk_builder_name="email_message",
        chunk_types=("message_subject", "message_snippet", "message_context", "message_invite_context", "message_body"),
    ),
    # ── email_attachment ────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="email_attachment",
        projection_table="email_attachments",
        projection_columns=(
            _text("message"),
            _text("thread"),
            _text("filename"),
            _text("mime_type"),
            _int("size_bytes"),
            _text("content_id"),
            _bool("is_inline"),
            _text("attachment_id"),
            _text("attachment_metadata_sha"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(),
        chunk_builder_name=None,
        chunk_types=(),
    ),
    # ── imessage_thread ─────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="imessage_thread",
        projection_table="imessage_threads",
        projection_columns=(
            _text("service"),
            _text("protocol"),
            _text("thread_type"),
            _text("thread_title"),
            _text("thread_description"),
            _json("participant_ids_json", source_field="participant_ids"),
            _json("participant_names_json", source_field="participant_names"),
            _json("participant_identifiers_json", source_field="participant_identifiers"),
            _json("counterpart_ids_json", source_field="counterpart_ids"),
            _json("counterpart_names_json", source_field="counterpart_names"),
            _json("counterpart_identifiers_json", source_field="counterpart_identifiers"),
            _json("participant_handles_json", source_field="participant_handles"),
            _bool("is_group"),
        ),
        person_edge_type="thread_has_person",
        edge_rules=(
            DeclEdgeRule("messages", "thread_has_message", "card", ("messages",)),
            DeclEdgeRule("attachments", "thread_has_attachment", "card", ("attachments",)),
            DeclEdgeRule("participant_handles", "thread_has_person", "person", ("participant_handles",)),
        ),
        chunk_builder_name="imessage_thread",
        chunk_types=(
            "imessage_thread_context",
            "imessage_thread_summary",
            "imessage_thread_window",
            "imessage_thread_recent_window",
        ),
    ),
    # ── imessage_message ────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="imessage_message",
        projection_table="imessage_messages",
        projection_columns=(
            _text("thread"),
            _text("message_type"),
            _text("sender_id"),
            _text("sender_name"),
            _text("sender_identifier"),
            _text("sender_person"),
            _text("sender_handle"),
            _bool("is_from_me"),
            _text("edited_at"),
            _text("deleted_at"),
            _text("associated_message_guid"),
            _text("associated_message_type"),
            _text("associated_message_emoji"),
            _text("reply_to_event_id"),
            _text("reaction_key"),
            _text("linked_message_event_id"),
            _text("message_body_sha"),
        ),
        person_edge_type="message_mentions_person",
        edge_rules=(
            DeclEdgeRule("thread", "message_in_thread", "card", ("thread",), multi=False),
            DeclEdgeRule("attachments", "message_has_attachment", "card", ("attachments",)),
            DeclEdgeRule(
                "participant_handles", "message_mentions_person", "person", ("sender_handle", "participant_handles")
            ),
        ),
        chunk_builder_name=None,
        chunk_types=(),
    ),
    # ── imessage_attachment ─────────────────────────────────────────────
    CardTypeRegistration(
        card_type="imessage_attachment",
        projection_table="imessage_attachments",
        projection_columns=(
            _text("message"),
            _text("thread"),
            _text("transfer_name"),
            _text("uti"),
            _text("mime_type"),
            _text("original_path"),
            _text("exported_path"),
            _text("attachment_type"),
            _bool("is_missing"),
            _text("metadata_sha"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(),
        chunk_builder_name=None,
        chunk_types=(),
    ),
    # ── beeper_thread ───────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="beeper_thread",
        projection_table="beeper_threads",
        projection_columns=(
            _text("bridge_name"),
            _text("service"),
            _text("protocol"),
            _text("thread_type"),
            _text("thread_title"),
            _text("thread_description"),
            _json("participant_ids_json", source_field="participant_ids"),
            _json("participant_names_json", source_field="participant_names"),
            _json("participant_identifiers_json", source_field="participant_identifiers"),
            _json("counterpart_ids_json", source_field="counterpart_ids"),
            _json("counterpart_names_json", source_field="counterpart_names"),
            _json("counterpart_identifiers_json", source_field="counterpart_identifiers"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(),
        chunk_builder_name=None,
        chunk_types=(),
    ),
    # ── beeper_message ──────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="beeper_message",
        projection_table="beeper_messages",
        projection_columns=(
            _text("thread"),
            _text("message_type"),
            _text("sender_id"),
            _text("sender_name"),
            _text("sender_identifier"),
            _text("sender_person"),
            _bool("is_from_me"),
            _text("edited_at"),
            _text("deleted_at"),
            _text("reply_to_event_id"),
            _text("reaction_key"),
            _text("linked_message_event_id"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(),
        chunk_builder_name=None,
        chunk_types=(),
    ),
    # ── beeper_attachment ───────────────────────────────────────────────
    CardTypeRegistration(
        card_type="beeper_attachment",
        projection_table="beeper_attachments",
        projection_columns=(
            _text("message"),
            _text("thread"),
            _text("mime_type"),
            _int("size_bytes"),
            _text("attachment_type"),
            _text("src_url"),
            _text("cached_path"),
            _bool("is_voice_note"),
            _bool("is_gif"),
            _bool("is_sticker"),
            _text("metadata_sha"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(),
        chunk_builder_name=None,
        chunk_types=(),
    ),
    # ── calendar_event ──────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="calendar_event",
        projection_table="calendar_events",
        projection_columns=(
            _text("calendar_id"),
            _text("event_id"),
            _text("event_etag"),
            _text("ical_uid"),
            _text("status"),
            _text("organizer_email"),
            _text("organizer_name"),
            _json("attendee_emails_json", source_field="attendee_emails"),
            _text("start_at"),
            _text("end_at"),
            _text("timezone"),
            _bool("all_day"),
            _text("conference_url"),
            _json("source_messages_json", source_field="source_messages"),
            _json("source_threads_json", source_field="source_threads"),
            _json("meeting_transcripts_json", source_field="meeting_transcripts"),
            _text("event_body_sha"),
        ),
        person_edge_type="event_has_person",
        edge_rules=(
            DeclEdgeRule("source_messages", "event_has_message", "card", ("source_messages",)),
            DeclEdgeRule("source_threads", "event_has_thread", "card", ("source_threads",)),
            DeclEdgeRule("meeting_transcripts", "event_has_transcript", "card", ("meeting_transcripts",)),
            DeclEdgeRule("attendee_emails", "event_has_person", "person", ("organizer_email", "attendee_emails")),
        ),
        chunk_builder_name="calendar_event",
        chunk_types=("event_title_time", "event_participants", "event_description", "event_sources", "event_body"),
    ),
    # ── media_asset ─────────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="media_asset",
        projection_table="media_assets",
        projection_columns=(
            _text("photos_asset_id"),
            _text("photos_source_label"),
            _text("media_type"),
            _text("captured_at"),
            _text("modified_at"),
            _json("keywords_json", source_field="keywords"),
            _json("labels_json", source_field="labels"),
            _json("person_labels_json", source_field="person_labels"),
            _json("albums_json", source_field="albums"),
            _json("album_paths_json", source_field="album_paths"),
            _json("folders_json", source_field="folders"),
            _bool("favorite"),
            _bool("hidden"),
            _bool("has_adjustments"),
            _bool("live_photo"),
            _bool("burst"),
            _bool("screenshot"),
            _bool("slow_mo"),
            _bool("time_lapse"),
            _int("width"),
            _int("height"),
            _float("duration_seconds"),
            _text("place_name"),
            _text("place_city"),
            _text("place_state"),
            _text("place_country"),
            _float("latitude"),
            _float("longitude"),
            _text("edited_path"),
            _text("metadata_sha"),
            _text("original_filename"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(),
        chunk_builder_name=None,
        chunk_types=(),
    ),
    # ── document ────────────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="document",
        projection_table="documents",
        projection_columns=(
            _text("library_root"),
            _text("relative_path"),
            _text("extension"),
            _text("content_sha"),
            _text("file_created_at"),
            _text("file_modified_at"),
            _text("document_type"),
            _text("document_date"),
            _json("authors_json", source_field="authors"),
            _json("counterparties_json", source_field="counterparties"),
            _json("emails_json", source_field="emails"),
            _json("phones_json", source_field="phones"),
            _json("websites_json", source_field="websites"),
            _text("location"),
            _json("sheet_names_json", source_field="sheet_names"),
            _int("page_count"),
            _text("text_source"),
            _text("extracted_text_sha"),
            _text("extraction_status"),
            _json("quality_flags_json", source_field="quality_flags"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(),
        chunk_builder_name="document",
        chunk_types=(
            "document_title_meta",
            "document_entities",
            "document_extraction_meta",
            "document_description",
            "document_body",
        ),
    ),
    # ── meeting_transcript ──────────────────────────────────────────────
    CardTypeRegistration(
        card_type="meeting_transcript",
        projection_table="meeting_transcripts",
        projection_columns=(
            _text("otter_meeting_id"),
            _text("otter_conversation_id"),
            _text("meeting_url"),
            _text("transcript_url"),
            _text("recording_url"),
            _json("speaker_names_json", source_field="speaker_names"),
            _json("speaker_emails_json", source_field="speaker_emails"),
            _json("participant_names_json", source_field="participant_names"),
            _json("participant_emails_json", source_field="participant_emails"),
            _text("host_name"),
            _text("host_email"),
            _text("language"),
            _float("duration_seconds"),
            _text("event_id_hint"),
            _text("otter_updated_at"),
            _text("transcript_body_sha"),
        ),
        person_edge_type="mentions_person",
        edge_rules=(DeclEdgeRule("calendar_events", "transcript_has_calendar_event", "card", ("calendar_events",)),),
        chunk_builder_name="meeting_transcript",
        chunk_types=(
            "meeting_transcript_identity",
            "meeting_transcript_participants",
            "meeting_transcript_links",
            "meeting_transcript_body",
        ),
    ),
    # ── git_repository ──────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="git_repository",
        projection_table="git_repositories",
        projection_columns=(
            _text("repository_name_with_owner"),
            _text("repository_id"),
            _text("repository_url"),
            _text("default_branch"),
            _bool("is_private"),
            _bool("is_fork"),
            _json("topics_json", source_field="topics"),
            _json("languages_json", source_field="languages"),
            _int("stargazer_count"),
            _int("fork_count"),
            _int("open_issue_count"),
        ),
        person_edge_type="repo_owned_by_person",
        edge_rules=(),
        chunk_builder_name="git_repository",
        chunk_types=("git_repo_identity", "git_repo_topics", "git_repo_description"),
    ),
    # ── git_commit ──────────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="git_commit",
        projection_table="git_commits",
        projection_columns=(
            _text("repository_name_with_owner"),
            _text("repository"),
            _text("commit_sha"),
            _text("author_name"),
            _text("author_email"),
            _text("committed_at"),
            _json("parent_shas_json", source_field="parent_shas"),
            _json("branch_names_json", source_field="branch_names"),
            _json("pull_numbers_json", source_field="pull_numbers"),
        ),
        person_edge_type="commit_authored_by_person",
        edge_rules=(DeclEdgeRule("repository", "commit_in_repo", "card", ("repository",), multi=False),),
        chunk_builder_name="git_commit",
        chunk_types=("git_commit_headline", "git_commit_context", "git_commit_body"),
    ),
    # ── git_thread ──────────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="git_thread",
        projection_table="git_threads",
        projection_columns=(
            _text("repository_name_with_owner"),
            _text("repository"),
            _text("github_thread_id"),
            _text("thread_kind", source_field="thread_type"),
            _text("number"),
            _text("title"),
            _text("state"),
            _text("author_login"),
            _json("assignees_json", source_field="assignees"),
            _json("labels_json", source_field="labels"),
            _text("milestone"),
            _text("base_ref"),
            _text("head_ref"),
            _text("merged_at"),
            _text("closed_at"),
            _json("associated_pr_numbers_json", source_field="associated_pr_numbers"),
        ),
        person_edge_type="thread_has_person",
        edge_rules=(
            DeclEdgeRule("repository", "thread_in_repo", "card", ("repository",), multi=False),
            DeclEdgeRule("messages", "thread_has_message", "card", ("messages",)),
        ),
        chunk_builder_name="git_thread",
        chunk_types=(
            "git_thread_title_state",
            "git_thread_participants",
            "git_thread_branch_context",
            "git_thread_body",
        ),
    ),
    # ── git_message ─────────────────────────────────────────────────────
    CardTypeRegistration(
        card_type="git_message",
        projection_table="git_messages",
        projection_columns=(
            _text("repository_name_with_owner"),
            _text("thread"),
            _text("github_message_id"),
            _text("message_type"),
            _text("review_state"),
            _text("actor_login"),
            _text("path"),
            _text("position"),
            _text("review_commit_sha"),
            _text("diff_hunk"),
        ),
        person_edge_type="message_authored_by_person",
        edge_rules=(
            DeclEdgeRule("thread", "message_in_thread", "card", ("thread",), multi=False),
            DeclEdgeRule("repository", "message_in_repo", "card", ("repository",), multi=False),
        ),
        chunk_builder_name="git_message",
        chunk_types=("git_message_context", "git_message_review_context", "git_message_diff_hunk", "git_message_body"),
    ),
)


# ---------------------------------------------------------------------------
# Lookup indexes
# ---------------------------------------------------------------------------

REGISTRATION_BY_CARD_TYPE: dict[str, CardTypeRegistration] = {reg.card_type: reg for reg in CARD_TYPE_REGISTRATIONS}


def get_registration(card_type: str) -> CardTypeRegistration | None:
    return REGISTRATION_BY_CARD_TYPE.get(card_type)
