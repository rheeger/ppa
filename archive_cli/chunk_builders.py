"""Type-aware chunk builders for each card type."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from archive_vault.schema import BaseCard, validate_card_permissive

from .card_registry import REGISTRATION_BY_CARD_TYPE
from .features import CHUNKABLE_TEXT_FIELDS
from .index_config import CHUNK_SCHEMA_VERSION, get_chunk_char_limit


def _clean_text(value: str) -> str:
    sanitized = str(value).replace("\x00", "")
    return re.sub(r"\s+", " ", sanitized.strip())


def _iter_string_values(value: Any):
    if isinstance(value, str):
        cleaned = value.replace("\x00", "").strip()
        if cleaned:
            yield cleaned
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_string_values(item)
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from _iter_string_values(item)


def _coerce_string_list(value: Any) -> list[str]:
    return list(_iter_string_values(value))


def _token_count(content: str) -> int:
    return max(len(content.split()), 1) if content.strip() else 0


def _chunk_hash(chunk_type: str, content: str, source_fields: list[str]) -> str:
    payload = json.dumps(
        {
            "chunk_schema_version": CHUNK_SCHEMA_VERSION,
            "chunk_type": chunk_type,
            "source_fields": source_fields,
            "content": content,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _split_text_chunks(text: str, *, limit: int) -> list[str]:
    cleaned = text.replace("\x00", "").strip()
    if not cleaned:
        return []
    if len(cleaned) <= limit:
        return [cleaned]

    paragraphs = [part.strip() for part in cleaned.split("\n\n") if part.strip()]
    if not paragraphs:
        paragraphs = [cleaned]

    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
        remainder = paragraph
        while len(remainder) > limit:
            window = remainder[:limit]
            split_at = window.rfind(" ")
            if split_at <= 0:
                split_at = limit
            chunk = remainder[:split_at].strip()
            if chunk:
                chunks.append(chunk)
            remainder = remainder[split_at:].strip()
        current = remainder
    if current:
        chunks.append(current)
    return chunks


def _split_paragraphs(text: str) -> list[str]:
    sanitized = text.replace("\x00", "")
    return [part.strip() for part in sanitized.split("\n\n") if part.strip()]


def _rolling_text_windows(text: str, *, limit: int, window_size: int = 2) -> list[str]:
    paragraphs = _split_paragraphs(text)
    if not paragraphs:
        return []
    if len(paragraphs) == 1:
        return _split_text_chunks(paragraphs[0], limit=limit)
    windows: list[str] = []
    for start in range(len(paragraphs)):
        piece = "\n\n".join(paragraphs[start : start + window_size]).strip()
        if not piece:
            continue
        windows.extend(_split_text_chunks(piece, limit=limit))
    deduped: list[str] = []
    seen: set[str] = set()
    for window in windows:
        if window in seen:
            continue
        seen.add(window)
        deduped.append(window)
    return deduped


def _markdown_heading_sections(text: str) -> list[str]:
    """Split markdown body on ATX headings (# … ######). Empty if fewer than two headings."""
    cleaned = text.replace("\x00", "").strip()
    if not cleaned:
        return []
    pattern = re.compile(r"(?m)^(#{1,6}\s+[^\n]+)\s*$")
    matches = list(pattern.finditer(cleaned))
    if len(matches) < 2:
        return []
    sections: list[str] = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(cleaned)
        piece = cleaned[start:end].strip()
        if piece:
            sections.append(piece)
    return sections


def _otter_pipe_turns(text: str) -> list[str]:
    """One line per `Speaker | text` (Otter-style export)."""
    out: list[str] = []
    for raw in text.replace("\x00", "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "|" not in line:
            continue
        left, _, right = line.partition("|")
        if left.strip() and right.strip():
            out.append(f"{left.strip()}: {right.strip()}")
    return out


def _colon_speaker_turns(text: str) -> list[str]:
    """Group lines into blocks starting with `Name: first-token` at a line boundary."""
    lines = text.replace("\x00", "").splitlines()
    if not lines:
        return []
    blocks: list[str] = []
    current: list[str] = []
    start_pat = re.compile(r"^[^:\n]{1,100}: \S")
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if current:
                current.append(line)
            continue
        if start_pat.match(stripped) and current and any(c.strip() for c in current):
            blocks.append("\n".join(current).strip())
            current = [line]
        else:
            current.append(line)
    if current and any(c.strip() for c in current):
        blocks.append("\n".join(current).strip())
    return [b for b in blocks if b.strip()]


def _meeting_transcript_focus_section(sections: list[str]) -> str:
    for sec in sections:
        head = sec.strip().split("\n", 1)[0].strip().lower()
        if head.startswith("#") and "transcript" in head:
            return sec
    return sections[-1] if sections else ""


def _format_labeled_block(title: str, values: list[str]) -> str:
    cleaned = [_clean_text(value) for value in values if _clean_text(value)]
    if not cleaned:
        return ""
    return f"{title}: " + "; ".join(cleaned)


def _build_person_chunks(card: BaseCard, frontmatter: dict[str, Any], body: str, append_chunks) -> None:
    profile_lines = [
        _format_labeled_block(
            "name", [card.summary, str(frontmatter.get("first_name", "")), str(frontmatter.get("last_name", ""))]
        ),
        _format_labeled_block("aliases", _coerce_string_list(frontmatter.get("aliases", []))),
        _format_labeled_block("emails", _coerce_string_list(frontmatter.get("emails", []))),
        _format_labeled_block("phones", _coerce_string_list(frontmatter.get("phones", []))),
        _format_labeled_block(
            "handles",
            [
                str(frontmatter.get("linkedin", "")),
                str(frontmatter.get("github", "")),
                str(frontmatter.get("twitter", "")),
                str(frontmatter.get("instagram", "")),
                str(frontmatter.get("telegram", "")),
            ],
        ),
    ]
    append_chunks(
        "person_profile", "\n".join(line for line in profile_lines if line), ["summary", "emails", "phones", "aliases"]
    )
    role_lines = [
        _format_labeled_block(
            "company", [str(frontmatter.get("company", ""))] + _coerce_string_list(frontmatter.get("companies", []))
        ),
        _format_labeled_block(
            "title", [str(frontmatter.get("title", ""))] + _coerce_string_list(frontmatter.get("titles", []))
        ),
        _format_labeled_block("reports_to", [str(frontmatter.get("reports_to", ""))]),
        _format_labeled_block("relationship", [str(frontmatter.get("relationship_type", ""))]),
    ]
    append_chunks(
        "person_role",
        "\n".join(line for line in role_lines if line),
        ["company", "companies", "title", "titles", "reports_to", "relationship_type"],
    )
    context_lines = [
        _format_labeled_block("description", [str(frontmatter.get("description", ""))]),
        _format_labeled_block("people", _coerce_string_list(frontmatter.get("people", []))),
        _format_labeled_block("orgs", _coerce_string_list(frontmatter.get("orgs", []))),
        _format_labeled_block("tags", _coerce_string_list(frontmatter.get("tags", []))),
    ]
    append_chunks(
        "person_context", "\n".join(line for line in context_lines if line), ["description", "people", "orgs", "tags"]
    )
    if body.strip():
        append_chunks("person_body", body.strip(), ["body"])


def _build_email_thread_chunks(frontmatter: dict[str, Any], body: str, append_chunks, *, limit: int) -> None:
    append_chunks("thread_subject", str(frontmatter.get("subject", "")), ["subject"])
    thread_meta = "\n".join(
        line
        for line in [
            _format_labeled_block("summary", [str(frontmatter.get("summary", ""))]),
            _format_labeled_block(
                "participants",
                [str(frontmatter.get("account_email", ""))] + _coerce_string_list(frontmatter.get("participants", [])),
            ),
            _format_labeled_block("labels", _coerce_string_list(frontmatter.get("label_ids", []))),
            _format_labeled_block(
                "time", [str(frontmatter.get("first_message_at", "")), str(frontmatter.get("last_message_at", ""))]
            ),
        ]
        if line
    )
    append_chunks(
        "thread_context",
        thread_meta,
        ["summary", "participants", "account_email", "label_ids", "first_message_at", "last_message_at"],
    )
    append_chunks("thread_summary", str(frontmatter.get("thread_summary", "")), ["thread_summary"])
    for window in _rolling_text_windows(body, limit=limit, window_size=2):
        append_chunks("thread_window", window, ["body", "messages"])
    if body.strip():
        paragraphs = _split_paragraphs(body)
        if paragraphs:
            recent_slice = "\n\n".join(paragraphs[-2:])
            append_chunks("thread_recent_window", recent_slice, ["body", "last_message_at"])


def _build_email_message_chunks(frontmatter: dict[str, Any], body: str, append_chunks) -> None:
    append_chunks("message_subject", str(frontmatter.get("subject", "")), ["subject"])
    append_chunks("message_snippet", str(frontmatter.get("snippet", "")), ["snippet"])
    envelope = "\n".join(
        line
        for line in [
            _format_labeled_block("summary", [str(frontmatter.get("summary", ""))]),
            _format_labeled_block(
                "from", [str(frontmatter.get("from_name", "")), str(frontmatter.get("from_email", ""))]
            ),
            _format_labeled_block("to", _coerce_string_list(frontmatter.get("to_emails", []))),
            _format_labeled_block("participants", _coerce_string_list(frontmatter.get("participant_emails", []))),
            _format_labeled_block(
                "thread", [str(frontmatter.get("thread", "")), str(frontmatter.get("gmail_thread_id", ""))]
            ),
        ]
        if line
    )
    append_chunks(
        "message_context",
        envelope,
        ["summary", "from_name", "from_email", "to_emails", "participant_emails", "thread", "gmail_thread_id"],
    )
    invite_context = "\n".join(
        line
        for line in [
            _format_labeled_block("invite_title", [str(frontmatter.get("invite_title", ""))]),
            _format_labeled_block(
                "invite_time", [str(frontmatter.get("invite_start_at", "")), str(frontmatter.get("invite_end_at", ""))]
            ),
            _format_labeled_block("calendar_events", _coerce_string_list(frontmatter.get("calendar_events", []))),
        ]
        if line
    )
    append_chunks(
        "message_invite_context",
        invite_context,
        [
            "invite_title",
            "invite_start_at",
            "invite_end_at",
            "calendar_events",
            "invite_ical_uid",
            "invite_event_id_hint",
        ],
    )
    if body.strip():
        append_chunks("message_body", body.strip(), ["body"])


def _build_imessage_thread_chunks(frontmatter: dict[str, Any], body: str, append_chunks, *, limit: int) -> None:
    meta = "\n".join(
        line
        for line in [
            _format_labeled_block("summary", [str(frontmatter.get("summary", ""))]),
            _format_labeled_block(
                "display", [str(frontmatter.get("display_name", "")), str(frontmatter.get("chat_identifier", ""))]
            ),
            _format_labeled_block("participants", _coerce_string_list(frontmatter.get("participant_handles", []))),
            _format_labeled_block("service", [str(frontmatter.get("service", ""))]),
        ]
        if line
    )
    append_chunks(
        "imessage_thread_context",
        meta,
        ["summary", "display_name", "chat_identifier", "participant_handles", "service"],
    )
    append_chunks("imessage_thread_summary", str(frontmatter.get("thread_summary", "")), ["thread_summary"])
    for window in _rolling_text_windows(body, limit=limit, window_size=3):
        append_chunks("imessage_thread_window", window, ["body", "messages"])
    if body.strip():
        paragraphs = _split_paragraphs(body)
        if paragraphs:
            recent_slice = "\n\n".join(paragraphs[-3:])
            append_chunks("imessage_thread_recent_window", recent_slice, ["body", "last_message_at"])


def _build_calendar_event_chunks(frontmatter: dict[str, Any], body: str, append_chunks) -> None:
    title_time = "\n".join(
        line
        for line in [
            _format_labeled_block("title", [str(frontmatter.get("title", "")), str(frontmatter.get("summary", ""))]),
            _format_labeled_block(
                "time",
                [
                    str(frontmatter.get("start_at", "")),
                    str(frontmatter.get("end_at", "")),
                    str(frontmatter.get("timezone", "")),
                ],
            ),
            _format_labeled_block(
                "location", [str(frontmatter.get("location", "")), str(frontmatter.get("conference_url", ""))]
            ),
        ]
        if line
    )
    append_chunks(
        "event_title_time",
        title_time,
        ["title", "summary", "start_at", "end_at", "timezone", "location", "conference_url"],
    )
    participants = "\n".join(
        line
        for line in [
            _format_labeled_block(
                "organizer", [str(frontmatter.get("organizer_name", "")), str(frontmatter.get("organizer_email", ""))]
            ),
            _format_labeled_block("attendees", _coerce_string_list(frontmatter.get("attendee_emails", []))),
            _format_labeled_block("people", _coerce_string_list(frontmatter.get("people", []))),
        ]
        if line
    )
    append_chunks(
        "event_participants", participants, ["organizer_name", "organizer_email", "attendee_emails", "people"]
    )
    append_chunks("event_description", str(frontmatter.get("description", "")), ["description"])
    source_context = "\n".join(
        line
        for line in [
            _format_labeled_block("source_messages", _coerce_string_list(frontmatter.get("source_messages", []))),
            _format_labeled_block("source_threads", _coerce_string_list(frontmatter.get("source_threads", []))),
            _format_labeled_block(
                "meeting_transcripts", _coerce_string_list(frontmatter.get("meeting_transcripts", []))
            ),
            _format_labeled_block("status", [str(frontmatter.get("status", "")), str(frontmatter.get("ical_uid", ""))]),
        ]
        if line
    )
    append_chunks(
        "event_sources",
        source_context,
        ["source_messages", "source_threads", "meeting_transcripts", "status", "ical_uid"],
    )
    if body.strip():
        append_chunks("event_body", body.strip(), ["body"])


def _build_meeting_transcript_chunks(frontmatter: dict[str, Any], body: str, append_chunks, *, limit: int) -> None:
    identity = "\n".join(
        line
        for line in [
            _format_labeled_block("title", [str(frontmatter.get("title", "")), str(frontmatter.get("summary", ""))]),
            _format_labeled_block(
                "otter_ids",
                [str(frontmatter.get("otter_meeting_id", "")), str(frontmatter.get("otter_conversation_id", ""))],
            ),
            _format_labeled_block("status", [str(frontmatter.get("status", "")), str(frontmatter.get("language", ""))]),
            _format_labeled_block("time", [str(frontmatter.get("start_at", "")), str(frontmatter.get("end_at", ""))]),
            _format_labeled_block(
                "urls", [str(frontmatter.get("meeting_url", "")), str(frontmatter.get("transcript_url", ""))]
            ),
        ]
        if line
    )
    append_chunks(
        "meeting_transcript_identity",
        identity,
        [
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
    )
    participants = "\n".join(
        line
        for line in [
            _format_labeled_block("speaker_names", _coerce_string_list(frontmatter.get("speaker_names", []))),
            _format_labeled_block("speaker_emails", _coerce_string_list(frontmatter.get("speaker_emails", []))),
            _format_labeled_block("participant_names", _coerce_string_list(frontmatter.get("participant_names", []))),
            _format_labeled_block("participant_emails", _coerce_string_list(frontmatter.get("participant_emails", []))),
            _format_labeled_block("people", _coerce_string_list(frontmatter.get("people", []))),
        ]
        if line
    )
    append_chunks(
        "meeting_transcript_participants",
        participants,
        ["speaker_names", "speaker_emails", "participant_names", "participant_emails", "people"],
    )
    links = "\n".join(
        line
        for line in [
            _format_labeled_block("calendar_events", _coerce_string_list(frontmatter.get("calendar_events", []))),
            _format_labeled_block(
                "event_hints", [str(frontmatter.get("event_id_hint", "")), str(frontmatter.get("ical_uid", ""))]
            ),
            _format_labeled_block("conference_url", [str(frontmatter.get("conference_url", ""))]),
        ]
        if line
    )
    append_chunks(
        "meeting_transcript_links",
        links,
        ["calendar_events", "event_id_hint", "ical_uid", "conference_url"],
    )
    if body.strip():
        sections = _markdown_heading_sections(body)
        if len(sections) >= 2:
            for section in sections:
                append_chunks("meeting_transcript_section", section, ["body"])
            focus = _meeting_transcript_focus_section(sections)
            turns = _otter_pipe_turns(focus)
            if not turns:
                turns = _colon_speaker_turns(focus)
            for turn in turns:
                append_chunks("meeting_transcript_turn", turn, ["body"])
        else:
            paragraphs = _split_paragraphs(body)
            if paragraphs:
                append_chunks("meeting_transcript_summary", "\n\n".join(paragraphs[:2]), ["body"])
            for window in _rolling_text_windows(body, limit=limit, window_size=3):
                append_chunks("meeting_transcript_window", window, ["body"])
            append_chunks("meeting_transcript_body", body.strip(), ["body"])


def _build_document_chunks(frontmatter: dict[str, Any], body: str, append_chunks) -> None:
    title_meta = "\n".join(
        line
        for line in [
            _format_labeled_block("title", [str(frontmatter.get("title", "")), str(frontmatter.get("summary", ""))]),
            _format_labeled_block(
                "type", [str(frontmatter.get("document_type", "")), str(frontmatter.get("extension", ""))]
            ),
            _format_labeled_block(
                "date",
                [
                    str(frontmatter.get("document_date", "")),
                    str(frontmatter.get("date_start", "")),
                    str(frontmatter.get("date_end", "")),
                    str(frontmatter.get("file_created_at", "")),
                    str(frontmatter.get("file_modified_at", "")),
                ],
            ),
            _format_labeled_block("location", [str(frontmatter.get("location", ""))]),
            _format_labeled_block(
                "path", [str(frontmatter.get("library_root", "")), str(frontmatter.get("relative_path", ""))]
            ),
        ]
        if line
    )
    append_chunks(
        "document_title_meta",
        title_meta,
        [
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
    )
    participants = "\n".join(
        line
        for line in [
            _format_labeled_block("authors", _coerce_string_list(frontmatter.get("authors", []))),
            _format_labeled_block("counterparties", _coerce_string_list(frontmatter.get("counterparties", []))),
            _format_labeled_block("emails", _coerce_string_list(frontmatter.get("emails", []))),
            _format_labeled_block("phones", _coerce_string_list(frontmatter.get("phones", []))),
            _format_labeled_block("websites", _coerce_string_list(frontmatter.get("websites", []))),
            _format_labeled_block("people", _coerce_string_list(frontmatter.get("people", []))),
            _format_labeled_block("orgs", _coerce_string_list(frontmatter.get("orgs", []))),
            _format_labeled_block("sheets", _coerce_string_list(frontmatter.get("sheet_names", []))),
            _format_labeled_block("tags", _coerce_string_list(frontmatter.get("tags", []))),
        ]
        if line
    )
    append_chunks(
        "document_entities",
        participants,
        ["authors", "counterparties", "emails", "phones", "websites", "people", "orgs", "sheet_names", "tags"],
    )
    extraction_meta = "\n".join(
        line
        for line in [
            _format_labeled_block("status", [str(frontmatter.get("extraction_status", ""))]),
            _format_labeled_block("quality_flags", _coerce_string_list(frontmatter.get("quality_flags", []))),
            _format_labeled_block("page_count", [str(frontmatter.get("page_count", ""))]),
        ]
        if line
    )
    append_chunks("document_extraction_meta", extraction_meta, ["extraction_status", "quality_flags", "page_count"])
    append_chunks("document_description", str(frontmatter.get("description", "")), ["description"])
    if body.strip():
        sections = _markdown_heading_sections(body)
        if len(sections) >= 2:
            for section in sections:
                append_chunks("document_section", section, ["body"])
        else:
            append_chunks("document_body", body.strip(), ["body"])


def _build_git_repository_chunks(frontmatter: dict[str, Any], body: str, append_chunks) -> None:
    identity = "\n".join(
        line
        for line in [
            _format_labeled_block(
                "repo", [str(frontmatter.get("name_with_owner", "")), str(frontmatter.get("summary", ""))]
            ),
            _format_labeled_block(
                "owner", [str(frontmatter.get("owner_login", "")), str(frontmatter.get("owner_type", ""))]
            ),
            _format_labeled_block("visibility", [str(frontmatter.get("visibility", ""))]),
            _format_labeled_block("default_branch", [str(frontmatter.get("default_branch", ""))]),
            _format_labeled_block("parent", [str(frontmatter.get("parent_name_with_owner", ""))]),
        ]
        if line
    )
    append_chunks(
        "git_repo_identity",
        identity,
        [
            "name_with_owner",
            "summary",
            "owner_login",
            "owner_type",
            "visibility",
            "default_branch",
            "parent_name_with_owner",
        ],
    )
    topics = "\n".join(
        line
        for line in [
            _format_labeled_block("primary_language", [str(frontmatter.get("primary_language", ""))]),
            _format_labeled_block("languages", _coerce_string_list(frontmatter.get("languages", []))),
            _format_labeled_block("topics", _coerce_string_list(frontmatter.get("topics", []))),
            _format_labeled_block("license", [str(frontmatter.get("license_name", ""))]),
            _format_labeled_block("orgs", _coerce_string_list(frontmatter.get("orgs", []))),
        ]
        if line
    )
    append_chunks(
        "git_repo_topics",
        topics,
        ["primary_language", "languages", "topics", "license_name", "orgs"],
    )
    append_chunks("git_repo_description", str(frontmatter.get("description", "")), ["description"])
    if body.strip():
        append_chunks("git_repo_body", body.strip(), ["body"])


def _build_git_commit_chunks(frontmatter: dict[str, Any], body: str, append_chunks) -> None:
    append_chunks(
        "git_commit_headline",
        str(frontmatter.get("message_headline", "")) or str(frontmatter.get("summary", "")),
        ["message_headline", "summary"],
    )
    context = "\n".join(
        line
        for line in [
            _format_labeled_block("repo", [str(frontmatter.get("repository_name_with_owner", ""))]),
            _format_labeled_block("sha", [str(frontmatter.get("commit_sha", ""))]),
            _format_labeled_block(
                "author",
                [
                    str(frontmatter.get("author_name", "")),
                    str(frontmatter.get("author_login", "")),
                    str(frontmatter.get("author_email", "")),
                ],
            ),
            _format_labeled_block(
                "committer",
                [
                    str(frontmatter.get("committer_name", "")),
                    str(frontmatter.get("committer_login", "")),
                    str(frontmatter.get("committer_email", "")),
                ],
            ),
            _format_labeled_block(
                "time", [str(frontmatter.get("authored_at", "")), str(frontmatter.get("committed_at", ""))]
            ),
            _format_labeled_block(
                "stats",
                [
                    f"additions={frontmatter.get('additions', 0)}",
                    f"deletions={frontmatter.get('deletions', 0)}",
                    f"changed_files={frontmatter.get('changed_files', 0)}",
                ],
            ),
            _format_labeled_block("associated_prs", _coerce_string_list(frontmatter.get("associated_pr_numbers", []))),
        ]
        if line
    )
    append_chunks(
        "git_commit_context",
        context,
        [
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
    )
    if body.strip():
        append_chunks("git_commit_body", body.strip(), ["body"])


def _build_git_thread_chunks(frontmatter: dict[str, Any], body: str, append_chunks) -> None:
    title = "\n".join(
        line
        for line in [
            _format_labeled_block("title", [str(frontmatter.get("title", "")), str(frontmatter.get("summary", ""))]),
            _format_labeled_block("repo", [str(frontmatter.get("repository_name_with_owner", ""))]),
            _format_labeled_block(
                "thread", [str(frontmatter.get("thread_type", "")), str(frontmatter.get("number", ""))]
            ),
            _format_labeled_block(
                "state",
                [
                    str(frontmatter.get("state", "")),
                    str(frontmatter.get("merged_at", "")),
                    str(frontmatter.get("closed_at", "")),
                ],
            ),
        ]
        if line
    )
    append_chunks(
        "git_thread_title_state",
        title,
        ["title", "summary", "repository_name_with_owner", "thread_type", "number", "state", "merged_at", "closed_at"],
    )
    participants = "\n".join(
        line
        for line in [
            _format_labeled_block("participants", _coerce_string_list(frontmatter.get("participant_logins", []))),
            _format_labeled_block("assignees", _coerce_string_list(frontmatter.get("assignees", []))),
            _format_labeled_block("labels", _coerce_string_list(frontmatter.get("labels", []))),
            _format_labeled_block("people", _coerce_string_list(frontmatter.get("people", []))),
        ]
        if line
    )
    append_chunks(
        "git_thread_participants",
        participants,
        ["participant_logins", "assignees", "labels", "people"],
    )
    branch_context = "\n".join(
        line
        for line in [
            _format_labeled_block("base_ref", [str(frontmatter.get("base_ref", ""))]),
            _format_labeled_block("head_ref", [str(frontmatter.get("head_ref", ""))]),
            _format_labeled_block("message_count", [str(frontmatter.get("message_count", ""))]),
            _format_labeled_block("messages", _coerce_string_list(frontmatter.get("messages", []))),
        ]
        if line
    )
    append_chunks(
        "git_thread_branch_context",
        branch_context,
        ["base_ref", "head_ref", "message_count", "messages"],
    )
    if body.strip():
        append_chunks("git_thread_body", body.strip(), ["body"])


def _build_git_message_chunks(frontmatter: dict[str, Any], body: str, append_chunks) -> None:
    context = "\n".join(
        line
        for line in [
            _format_labeled_block("summary", [str(frontmatter.get("summary", ""))]),
            _format_labeled_block("repo", [str(frontmatter.get("repository_name_with_owner", ""))]),
            _format_labeled_block("thread", [str(frontmatter.get("thread", ""))]),
            _format_labeled_block(
                "type", [str(frontmatter.get("message_type", "")), str(frontmatter.get("review_state", ""))]
            ),
            _format_labeled_block(
                "actor",
                [
                    str(frontmatter.get("actor_name", "")),
                    str(frontmatter.get("actor_login", "")),
                    str(frontmatter.get("actor_email", "")),
                ],
            ),
            _format_labeled_block(
                "time", [str(frontmatter.get("sent_at", "")), str(frontmatter.get("updated_at", ""))]
            ),
        ]
        if line
    )
    append_chunks(
        "git_message_context",
        context,
        [
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
    )
    review_context = "\n".join(
        line
        for line in [
            _format_labeled_block("path", [str(frontmatter.get("path", ""))]),
            _format_labeled_block(
                "position", [str(frontmatter.get("position", "")), str(frontmatter.get("original_position", ""))]
            ),
            _format_labeled_block(
                "commits",
                [str(frontmatter.get("review_commit_sha", "")), str(frontmatter.get("original_commit_sha", ""))],
            ),
            _format_labeled_block("reply_to", [str(frontmatter.get("in_reply_to_message_id", ""))]),
        ]
        if line
    )
    append_chunks(
        "git_message_review_context",
        review_context,
        ["path", "position", "original_position", "review_commit_sha", "original_commit_sha", "in_reply_to_message_id"],
    )
    append_chunks("git_message_diff_hunk", str(frontmatter.get("diff_hunk", "")), ["diff_hunk"])
    if body.strip():
        append_chunks("git_message_body", body.strip(), ["body"])


_CHUNK_BUILDER_FUNCTIONS: dict[str, object] = {
    "person": lambda card, fm, body, ac, limit: _build_person_chunks(card, fm, body, ac),
    "email_thread": lambda card, fm, body, ac, limit: _build_email_thread_chunks(fm, body, ac, limit=limit),
    "email_message": lambda card, fm, body, ac, limit: _build_email_message_chunks(fm, body, ac),
    "imessage_thread": lambda card, fm, body, ac, limit: _build_imessage_thread_chunks(fm, body, ac, limit=limit),
    "calendar_event": lambda card, fm, body, ac, limit: _build_calendar_event_chunks(fm, body, ac),
    "meeting_transcript": lambda card, fm, body, ac, limit: _build_meeting_transcript_chunks(fm, body, ac, limit=limit),
    "document": lambda card, fm, body, ac, limit: _build_document_chunks(fm, body, ac),
    "git_repository": lambda card, fm, body, ac, limit: _build_git_repository_chunks(fm, body, ac),
    "git_commit": lambda card, fm, body, ac, limit: _build_git_commit_chunks(fm, body, ac),
    "git_thread": lambda card, fm, body, ac, limit: _build_git_thread_chunks(fm, body, ac),
    "git_message": lambda card, fm, body, ac, limit: _build_git_message_chunks(fm, body, ac),
}


def _build_chunks(frontmatter: dict[str, Any], body: str) -> list[dict[str, Any]]:
    limit = get_chunk_char_limit()
    card = validate_card_permissive(frontmatter)
    chunks: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    chunk_type_counts: dict[str, int] = {}

    def append_chunks(chunk_type: str, content: str, source_fields: list[str]) -> None:
        start_index = chunk_type_counts.get(chunk_type, 0)
        for offset, piece in enumerate(_split_text_chunks(content, limit=limit)):
            key = (chunk_type, piece)
            if key in seen:
                continue
            seen.add(key)
            index = start_index + offset
            chunks.append(
                {
                    "chunk_type": chunk_type,
                    "chunk_index": index,
                    "source_fields": source_fields,
                    "content": piece,
                    "content_hash": _chunk_hash(chunk_type, piece, source_fields),
                    "token_count": _token_count(piece),
                }
            )
        chunk_type_counts[chunk_type] = start_index + len(_split_text_chunks(content, limit=limit))

    registration = REGISTRATION_BY_CARD_TYPE.get(card.type)
    builder_name = registration.chunk_builder_name if registration else None
    builder_fn = _CHUNK_BUILDER_FUNCTIONS.get(builder_name) if builder_name else None

    if builder_fn is not None:
        builder_fn(card, frontmatter, body, append_chunks, limit)
    else:
        for field_name in CHUNKABLE_TEXT_FIELDS:
            value = frontmatter.get(field_name, "")
            if isinstance(value, str) and value.strip():
                append_chunks(field_name, value.strip(), [field_name])
        if body.strip():
            append_chunks("body", body.strip(), ["body"])
    return chunks
