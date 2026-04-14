//! Per-card-type chunk builders — parity with `archive_cli/chunk_builders.py`.

use serde_json::{Map, Value};

use crate::chunk::accumulator::ChunkAccumulator;
use crate::chunk::fm::{coerce_string_list_json, fm_str_value, stat_str_json};
use crate::chunk::helpers::{
    colon_speaker_turns, format_labeled_block, markdown_heading_sections,
    meeting_transcript_focus_section, otter_pipe_turns, rolling_text_windows, split_paragraphs,
};

pub fn build_person_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
) {
    let summary = fm_str_value(fm, "summary");
    let profile_lines = [
        format_labeled_block(
            "name",
            &[
                summary,
                fm_str_value(fm, "first_name"),
                fm_str_value(fm, "last_name"),
            ],
        ),
        format_labeled_block("aliases", &coerce_string_list_json(fm.get("aliases"))),
        format_labeled_block("emails", &coerce_string_list_json(fm.get("emails"))),
        format_labeled_block("phones", &coerce_string_list_json(fm.get("phones"))),
        format_labeled_block(
            "handles",
            &[
                fm_str_value(fm, "linkedin"),
                fm_str_value(fm, "github"),
                fm_str_value(fm, "twitter"),
                fm_str_value(fm, "instagram"),
                fm_str_value(fm, "telegram"),
            ],
        ),
    ];
    acc.append_chunks(
        "person_profile",
        &profile_lines
            .iter()
            .filter(|l| !l.is_empty())
            .cloned()
            .collect::<Vec<_>>()
            .join("\n"),
        &["summary", "emails", "phones", "aliases"],
    );
    let mut company_values = vec![fm_str_value(fm, "company")];
    company_values.extend(coerce_string_list_json(fm.get("companies")));
    let mut title_values = vec![fm_str_value(fm, "title")];
    title_values.extend(coerce_string_list_json(fm.get("titles")));
    let role_lines = [
        format_labeled_block("company", &company_values),
        format_labeled_block("title", &title_values),
        format_labeled_block("reports_to", &[fm_str_value(fm, "reports_to")]),
        format_labeled_block("relationship", &[fm_str_value(fm, "relationship_type")]),
    ];
    acc.append_chunks(
        "person_role",
        &role_lines
            .iter()
            .filter(|l| !l.is_empty())
            .cloned()
            .collect::<Vec<_>>()
            .join("\n"),
        &[
            "company",
            "companies",
            "title",
            "titles",
            "reports_to",
            "relationship_type",
        ],
    );
    let context_lines = [
        format_labeled_block("description", &[fm_str_value(fm, "description")]),
        format_labeled_block("people", &coerce_string_list_json(fm.get("people"))),
        format_labeled_block("orgs", &coerce_string_list_json(fm.get("orgs"))),
        format_labeled_block("tags", &coerce_string_list_json(fm.get("tags"))),
    ];
    acc.append_chunks(
        "person_context",
        &context_lines
            .iter()
            .filter(|l| !l.is_empty())
            .cloned()
            .collect::<Vec<_>>()
            .join("\n"),
        &["description", "people", "orgs", "tags"],
    );
    if body.trim().len() > 0 {
        acc.append_chunks("person_body", body.trim(), &["body"]);
    }
}

pub fn build_email_thread_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
    limit: usize,
) {
    acc.append_chunks("thread_subject", &fm_str_value(fm, "subject"), &["subject"]);
    let mut participant_vals = vec![fm_str_value(fm, "account_email")];
    participant_vals.extend(coerce_string_list_json(fm.get("participants")));
    let thread_meta = [
        format_labeled_block("summary", &[fm_str_value(fm, "summary")]),
        format_labeled_block("participants", &participant_vals),
        format_labeled_block(
            "labels",
            &coerce_string_list_json(fm.get("label_ids")),
        ),
        format_labeled_block(
            "time",
            &[
                fm_str_value(fm, "first_message_at"),
                fm_str_value(fm, "last_message_at"),
            ],
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "thread_context",
        &thread_meta,
        &[
            "summary",
            "participants",
            "account_email",
            "label_ids",
            "first_message_at",
            "last_message_at",
        ],
    );
    acc.append_chunks(
        "thread_summary",
        &fm_str_value(fm, "thread_summary"),
        &["thread_summary"],
    );
    for window in rolling_text_windows(body, limit, 2) {
        acc.append_chunks("thread_window", &window, &["body", "messages"]);
    }
    if body.trim().len() > 0 {
        let paragraphs = split_paragraphs(body);
        if !paragraphs.is_empty() {
            let recent_slice = paragraphs[paragraphs.len().saturating_sub(2)..].join("\n\n");
            acc.append_chunks(
                "thread_recent_window",
                &recent_slice,
                &["body", "last_message_at"],
            );
        }
    }
}

pub fn build_email_message_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
) {
    acc.append_chunks("message_subject", &fm_str_value(fm, "subject"), &["subject"]);
    acc.append_chunks("message_snippet", &fm_str_value(fm, "snippet"), &["snippet"]);
    let envelope = [
        format_labeled_block("summary", &[fm_str_value(fm, "summary")]),
        format_labeled_block(
            "from",
            &[fm_str_value(fm, "from_name"), fm_str_value(fm, "from_email")],
        ),
        format_labeled_block("to", &coerce_string_list_json(fm.get("to_emails"))),
        format_labeled_block(
            "participants",
            &coerce_string_list_json(fm.get("participant_emails")),
        ),
        format_labeled_block(
            "thread",
            &[
                fm_str_value(fm, "thread"),
                fm_str_value(fm, "gmail_thread_id"),
            ],
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "message_context",
        &envelope,
        &[
            "summary",
            "from_name",
            "from_email",
            "to_emails",
            "participant_emails",
            "thread",
            "gmail_thread_id",
        ],
    );
    let invite_context = [
        format_labeled_block("invite_title", &[fm_str_value(fm, "invite_title")]),
        format_labeled_block(
            "invite_time",
            &[
                fm_str_value(fm, "invite_start_at"),
                fm_str_value(fm, "invite_end_at"),
            ],
        ),
        format_labeled_block(
            "calendar_events",
            &coerce_string_list_json(fm.get("calendar_events")),
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "message_invite_context",
        &invite_context,
        &[
            "invite_title",
            "invite_start_at",
            "invite_end_at",
            "calendar_events",
            "invite_ical_uid",
            "invite_event_id_hint",
        ],
    );
    if body.trim().len() > 0 {
        acc.append_chunks("message_body", body.trim(), &["body"]);
    }
}

pub fn build_imessage_thread_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
    limit: usize,
) {
    let meta = [
        format_labeled_block("summary", &[fm_str_value(fm, "summary")]),
        format_labeled_block(
            "display",
            &[
                fm_str_value(fm, "display_name"),
                fm_str_value(fm, "chat_identifier"),
            ],
        ),
        format_labeled_block(
            "participants",
            &coerce_string_list_json(fm.get("participant_handles")),
        ),
        format_labeled_block("service", &[fm_str_value(fm, "service")]),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "imessage_thread_context",
        &meta,
        &[
            "summary",
            "display_name",
            "chat_identifier",
            "participant_handles",
            "service",
        ],
    );
    acc.append_chunks(
        "imessage_thread_summary",
        &fm_str_value(fm, "thread_summary"),
        &["thread_summary"],
    );
    for window in rolling_text_windows(body, limit, 3) {
        acc.append_chunks("imessage_thread_window", &window, &["body", "messages"]);
    }
    if body.trim().len() > 0 {
        let paragraphs = split_paragraphs(body);
        if !paragraphs.is_empty() {
            let recent_slice = paragraphs[paragraphs.len().saturating_sub(3)..].join("\n\n");
            acc.append_chunks(
                "imessage_thread_recent_window",
                &recent_slice,
                &["body", "last_message_at"],
            );
        }
    }
}

pub fn build_calendar_event_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
) {
    let title_time = [
        format_labeled_block(
            "title",
            &[fm_str_value(fm, "title"), fm_str_value(fm, "summary")],
        ),
        format_labeled_block(
            "time",
            &[
                fm_str_value(fm, "start_at"),
                fm_str_value(fm, "end_at"),
                fm_str_value(fm, "timezone"),
            ],
        ),
        format_labeled_block(
            "location",
            &[
                fm_str_value(fm, "location"),
                fm_str_value(fm, "conference_url"),
            ],
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "event_title_time",
        &title_time,
        &[
            "title",
            "summary",
            "start_at",
            "end_at",
            "timezone",
            "location",
            "conference_url",
        ],
    );
    let participants = [
        format_labeled_block(
            "organizer",
            &[
                fm_str_value(fm, "organizer_name"),
                fm_str_value(fm, "organizer_email"),
            ],
        ),
        format_labeled_block(
            "attendees",
            &coerce_string_list_json(fm.get("attendee_emails")),
        ),
        format_labeled_block("people", &coerce_string_list_json(fm.get("people"))),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "event_participants",
        &participants,
        &[
            "organizer_name",
            "organizer_email",
            "attendee_emails",
            "people",
        ],
    );
    acc.append_chunks(
        "event_description",
        &fm_str_value(fm, "description"),
        &["description"],
    );
    let source_context = [
        format_labeled_block(
            "source_messages",
            &coerce_string_list_json(fm.get("source_messages")),
        ),
        format_labeled_block(
            "source_threads",
            &coerce_string_list_json(fm.get("source_threads")),
        ),
        format_labeled_block(
            "meeting_transcripts",
            &coerce_string_list_json(fm.get("meeting_transcripts")),
        ),
        format_labeled_block(
            "status",
            &[fm_str_value(fm, "status"), fm_str_value(fm, "ical_uid")],
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "event_sources",
        &source_context,
        &[
            "source_messages",
            "source_threads",
            "meeting_transcripts",
            "status",
            "ical_uid",
        ],
    );
    if body.trim().len() > 0 {
        acc.append_chunks("event_body", body.trim(), &["body"]);
    }
}

pub fn build_meeting_transcript_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
    limit: usize,
) {
    let identity = [
        format_labeled_block(
            "title",
            &[fm_str_value(fm, "title"), fm_str_value(fm, "summary")],
        ),
        format_labeled_block(
            "otter_ids",
            &[
                fm_str_value(fm, "otter_meeting_id"),
                fm_str_value(fm, "otter_conversation_id"),
            ],
        ),
        format_labeled_block(
            "status",
            &[fm_str_value(fm, "status"), fm_str_value(fm, "language")],
        ),
        format_labeled_block(
            "time",
            &[fm_str_value(fm, "start_at"), fm_str_value(fm, "end_at")],
        ),
        format_labeled_block(
            "urls",
            &[
                fm_str_value(fm, "meeting_url"),
                fm_str_value(fm, "transcript_url"),
            ],
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "meeting_transcript_identity",
        &identity,
        &[
            "title",
            "summary",
            "otter_meeting_id",
            "otter_conversation_id",
            "status",
            "language",
            "start_at",
            "end_at",
            "meeting_url",
            "transcript_url",
        ],
    );
    let participants = [
        format_labeled_block(
            "speaker_names",
            &coerce_string_list_json(fm.get("speaker_names")),
        ),
        format_labeled_block(
            "speaker_emails",
            &coerce_string_list_json(fm.get("speaker_emails")),
        ),
        format_labeled_block(
            "participant_names",
            &coerce_string_list_json(fm.get("participant_names")),
        ),
        format_labeled_block(
            "participant_emails",
            &coerce_string_list_json(fm.get("participant_emails")),
        ),
        format_labeled_block("people", &coerce_string_list_json(fm.get("people"))),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "meeting_transcript_participants",
        &participants,
        &[
            "speaker_names",
            "speaker_emails",
            "participant_names",
            "participant_emails",
            "people",
        ],
    );
    let links = [
        format_labeled_block(
            "calendar_events",
            &coerce_string_list_json(fm.get("calendar_events")),
        ),
        format_labeled_block(
            "event_hints",
            &[
                fm_str_value(fm, "event_id_hint"),
                fm_str_value(fm, "ical_uid"),
            ],
        ),
        format_labeled_block("conference_url", &[fm_str_value(fm, "conference_url")]),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "meeting_transcript_links",
        &links,
        &[
            "calendar_events",
            "event_id_hint",
            "ical_uid",
            "conference_url",
        ],
    );
    if body.trim().len() > 0 {
        let sections = markdown_heading_sections(body);
        if sections.len() >= 2 {
            for section in &sections {
                acc.append_chunks("meeting_transcript_section", section, &["body"]);
            }
            let focus = meeting_transcript_focus_section(&sections);
            let mut turns = otter_pipe_turns(&focus);
            if turns.is_empty() {
                turns = colon_speaker_turns(&focus);
            }
            for turn in turns {
                acc.append_chunks("meeting_transcript_turn", &turn, &["body"]);
            }
        } else {
            let paragraphs = split_paragraphs(body);
            if !paragraphs.is_empty() {
                let end = paragraphs.len().min(2);
                let summary_slice = paragraphs[..end].join("\n\n");
                acc.append_chunks("meeting_transcript_summary", &summary_slice, &["body"]);
            }
            for window in rolling_text_windows(body, limit, 3) {
                acc.append_chunks("meeting_transcript_window", &window, &["body"]);
            }
            acc.append_chunks("meeting_transcript_body", body.trim(), &["body"]);
        }
    }
}

pub fn build_document_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
) {
    let title_meta = [
        format_labeled_block(
            "title",
            &[fm_str_value(fm, "title"), fm_str_value(fm, "summary")],
        ),
        format_labeled_block(
            "type",
            &[
                fm_str_value(fm, "document_type"),
                fm_str_value(fm, "extension"),
            ],
        ),
        format_labeled_block(
            "date",
            &[
                fm_str_value(fm, "document_date"),
                fm_str_value(fm, "date_start"),
                fm_str_value(fm, "date_end"),
                fm_str_value(fm, "file_created_at"),
                fm_str_value(fm, "file_modified_at"),
            ],
        ),
        format_labeled_block("location", &[fm_str_value(fm, "location")]),
        format_labeled_block(
            "path",
            &[
                fm_str_value(fm, "library_root"),
                fm_str_value(fm, "relative_path"),
            ],
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "document_title_meta",
        &title_meta,
        &[
            "title",
            "summary",
            "document_type",
            "extension",
            "document_date",
            "date_start",
            "date_end",
            "file_created_at",
            "file_modified_at",
            "location",
            "library_root",
            "relative_path",
        ],
    );
    let participants = [
        format_labeled_block("authors", &coerce_string_list_json(fm.get("authors"))),
        format_labeled_block(
            "counterparties",
            &coerce_string_list_json(fm.get("counterparties")),
        ),
        format_labeled_block("emails", &coerce_string_list_json(fm.get("emails"))),
        format_labeled_block("phones", &coerce_string_list_json(fm.get("phones"))),
        format_labeled_block(
            "websites",
            &coerce_string_list_json(fm.get("websites")),
        ),
        format_labeled_block("people", &coerce_string_list_json(fm.get("people"))),
        format_labeled_block("orgs", &coerce_string_list_json(fm.get("orgs"))),
        format_labeled_block(
            "sheets",
            &coerce_string_list_json(fm.get("sheet_names")),
        ),
        format_labeled_block("tags", &coerce_string_list_json(fm.get("tags"))),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "document_entities",
        &participants,
        &[
            "authors",
            "counterparties",
            "emails",
            "phones",
            "websites",
            "people",
            "orgs",
            "sheet_names",
            "tags",
        ],
    );
    let extraction_meta = [
        format_labeled_block("status", &[fm_str_value(fm, "extraction_status")]),
        format_labeled_block(
            "quality_flags",
            &coerce_string_list_json(fm.get("quality_flags")),
        ),
        format_labeled_block("page_count", &[fm_str_value(fm, "page_count")]),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "document_extraction_meta",
        &extraction_meta,
        &["extraction_status", "quality_flags", "page_count"],
    );
    acc.append_chunks(
        "document_description",
        &fm_str_value(fm, "description"),
        &["description"],
    );
    if body.trim().len() > 0 {
        let sections = markdown_heading_sections(body);
        if sections.len() >= 2 {
            for section in &sections {
                acc.append_chunks("document_section", section, &["body"]);
            }
        } else {
            acc.append_chunks("document_body", body.trim(), &["body"]);
        }
    }
}

pub fn build_git_repository_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
) {
    let identity = [
        format_labeled_block(
            "repo",
            &[
                fm_str_value(fm, "name_with_owner"),
                fm_str_value(fm, "summary"),
            ],
        ),
        format_labeled_block(
            "owner",
            &[
                fm_str_value(fm, "owner_login"),
                fm_str_value(fm, "owner_type"),
            ],
        ),
        format_labeled_block("visibility", &[fm_str_value(fm, "visibility")]),
        format_labeled_block("default_branch", &[fm_str_value(fm, "default_branch")]),
        format_labeled_block("parent", &[fm_str_value(fm, "parent_name_with_owner")]),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "git_repo_identity",
        &identity,
        &[
            "name_with_owner",
            "summary",
            "owner_login",
            "owner_type",
            "visibility",
            "default_branch",
            "parent_name_with_owner",
        ],
    );
    let topics = [
        format_labeled_block("primary_language", &[fm_str_value(fm, "primary_language")]),
        format_labeled_block(
            "languages",
            &coerce_string_list_json(fm.get("languages")),
        ),
        format_labeled_block("topics", &coerce_string_list_json(fm.get("topics"))),
        format_labeled_block("license", &[fm_str_value(fm, "license_name")]),
        format_labeled_block("orgs", &coerce_string_list_json(fm.get("orgs"))),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "git_repo_topics",
        &topics,
        &[
            "primary_language",
            "languages",
            "topics",
            "license_name",
            "orgs",
        ],
    );
    acc.append_chunks(
        "git_repo_description",
        &fm_str_value(fm, "description"),
        &["description"],
    );
    if body.trim().len() > 0 {
        acc.append_chunks("git_repo_body", body.trim(), &["body"]);
    }
}

pub fn build_git_commit_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
) {
    let headline = {
        let mh = fm_str_value(fm, "message_headline");
        if mh.is_empty() {
            fm_str_value(fm, "summary")
        } else {
            mh
        }
    };
    acc.append_chunks(
        "git_commit_headline",
        &headline,
        &["message_headline", "summary"],
    );
    let context = [
        format_labeled_block("repo", &[fm_str_value(fm, "repository_name_with_owner")]),
        format_labeled_block("sha", &[fm_str_value(fm, "commit_sha")]),
        format_labeled_block(
            "author",
            &[
                fm_str_value(fm, "author_name"),
                fm_str_value(fm, "author_login"),
                fm_str_value(fm, "author_email"),
            ],
        ),
        format_labeled_block(
            "committer",
            &[
                fm_str_value(fm, "committer_name"),
                fm_str_value(fm, "committer_login"),
                fm_str_value(fm, "committer_email"),
            ],
        ),
        format_labeled_block(
            "time",
            &[
                fm_str_value(fm, "authored_at"),
                fm_str_value(fm, "committed_at"),
            ],
        ),
        format_labeled_block(
            "stats",
            &[
                format!("additions={}", stat_str_json(fm, "additions")),
                format!("deletions={}", stat_str_json(fm, "deletions")),
                format!("changed_files={}", stat_str_json(fm, "changed_files")),
            ],
        ),
        format_labeled_block(
            "associated_prs",
            &coerce_string_list_json(fm.get("associated_pr_numbers")),
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "git_commit_context",
        &context,
        &[
            "repository_name_with_owner",
            "commit_sha",
            "author_name",
            "author_login",
            "author_email",
            "committer_name",
            "committer_login",
            "committer_email",
            "authored_at",
            "committed_at",
            "additions",
            "deletions",
            "changed_files",
            "associated_pr_numbers",
        ],
    );
    if body.trim().len() > 0 {
        acc.append_chunks("git_commit_body", body.trim(), &["body"]);
    }
}

pub fn build_git_thread_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
) {
    let title = [
        format_labeled_block(
            "title",
            &[fm_str_value(fm, "title"), fm_str_value(fm, "summary")],
        ),
        format_labeled_block("repo", &[fm_str_value(fm, "repository_name_with_owner")]),
        format_labeled_block(
            "thread",
            &[fm_str_value(fm, "thread_type"), fm_str_value(fm, "number")],
        ),
        format_labeled_block(
            "state",
            &[
                fm_str_value(fm, "state"),
                fm_str_value(fm, "merged_at"),
                fm_str_value(fm, "closed_at"),
            ],
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "git_thread_title_state",
        &title,
        &[
            "title",
            "summary",
            "repository_name_with_owner",
            "thread_type",
            "number",
            "state",
            "merged_at",
            "closed_at",
        ],
    );
    let participants = [
        format_labeled_block(
            "participants",
            &coerce_string_list_json(fm.get("participant_logins")),
        ),
        format_labeled_block(
            "assignees",
            &coerce_string_list_json(fm.get("assignees")),
        ),
        format_labeled_block("labels", &coerce_string_list_json(fm.get("labels"))),
        format_labeled_block("people", &coerce_string_list_json(fm.get("people"))),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "git_thread_participants",
        &participants,
        &["participant_logins", "assignees", "labels", "people"],
    );
    let branch_context = [
        format_labeled_block("base_ref", &[fm_str_value(fm, "base_ref")]),
        format_labeled_block("head_ref", &[fm_str_value(fm, "head_ref")]),
        format_labeled_block("message_count", &[fm_str_value(fm, "message_count")]),
        format_labeled_block(
            "messages",
            &coerce_string_list_json(fm.get("messages")),
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "git_thread_branch_context",
        &branch_context,
        &["base_ref", "head_ref", "message_count", "messages"],
    );
    if body.trim().len() > 0 {
        acc.append_chunks("git_thread_body", body.trim(), &["body"]);
    }
}

pub fn build_git_message_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
) {
    let context = [
        format_labeled_block("summary", &[fm_str_value(fm, "summary")]),
        format_labeled_block("repo", &[fm_str_value(fm, "repository_name_with_owner")]),
        format_labeled_block("thread", &[fm_str_value(fm, "thread")]),
        format_labeled_block(
            "type",
            &[
                fm_str_value(fm, "message_type"),
                fm_str_value(fm, "review_state"),
            ],
        ),
        format_labeled_block(
            "actor",
            &[
                fm_str_value(fm, "actor_name"),
                fm_str_value(fm, "actor_login"),
                fm_str_value(fm, "actor_email"),
            ],
        ),
        format_labeled_block(
            "time",
            &[fm_str_value(fm, "sent_at"), fm_str_value(fm, "updated_at")],
        ),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "git_message_context",
        &context,
        &[
            "summary",
            "repository_name_with_owner",
            "thread",
            "message_type",
            "review_state",
            "actor_name",
            "actor_login",
            "actor_email",
            "sent_at",
            "updated_at",
        ],
    );
    let review_context = [
        format_labeled_block("path", &[fm_str_value(fm, "path")]),
        format_labeled_block(
            "position",
            &[
                fm_str_value(fm, "position"),
                fm_str_value(fm, "original_position"),
            ],
        ),
        format_labeled_block(
            "commits",
            &[
                fm_str_value(fm, "review_commit_sha"),
                fm_str_value(fm, "original_commit_sha"),
            ],
        ),
        format_labeled_block("reply_to", &[fm_str_value(fm, "in_reply_to_message_id")]),
    ]
    .into_iter()
    .filter(|l| !l.is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    acc.append_chunks(
        "git_message_review_context",
        &review_context,
        &[
            "path",
            "position",
            "original_position",
            "review_commit_sha",
            "original_commit_sha",
            "in_reply_to_message_id",
        ],
    );
    acc.append_chunks(
        "git_message_diff_hunk",
        &fm_str_value(fm, "diff_hunk"),
        &["diff_hunk"],
    );
    if body.trim().len() > 0 {
        acc.append_chunks("git_message_body", body.trim(), &["body"]);
    }
}

/// `CHUNKABLE_TEXT_FIELDS` + body — same as `archive_cli.features`.
pub fn build_default_chunks(
    fm: &Map<String, Value>,
    body: &str,
    acc: &mut ChunkAccumulator,
) {
    const FIELDS: &[&str] = &[
        "summary",
        "subject",
        "snippet",
        "description",
        "thread_summary",
        "title",
    ];
    for field_name in FIELDS {
        if let Some(Value::String(s)) = fm.get(*field_name) {
            let text = s.trim();
            if !text.is_empty() {
                acc.append_chunks(field_name, text, &[*field_name]);
            }
        }
    }
    if body.trim().len() > 0 {
        acc.append_chunks("body", body.trim(), &["body"]);
    }
}
