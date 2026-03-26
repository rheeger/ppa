"""Live Postgres retrieval quality tests for ppa."""

from __future__ import annotations

import json
import math
import socket
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from psycopg import connect

from archive_mcp.benchmark import (
    BENCHMARK_PROFILES,
    benchmark_rebuild,
    benchmark_seed_links,
    build_benchmark_sample,
    resolve_benchmark_profile,
)
from archive_mcp.chunking import render_chunks_for_card
from archive_mcp.index_store import PostgresArchiveIndex
from archive_mcp.server import (
    archive_embed_pending,
    archive_graph,
    archive_hybrid_search,
    archive_rebuild_indexes,
    archive_search,
    archive_vector_search,
)
from hfa.provenance import ProvenanceEntry
from hfa.schema import CalendarEventCard, EmailMessageCard, EmailThreadCard, MeetingTranscriptCard, PersonCard
from hfa.vault import write_card

PGVECTOR_IMAGE = "pgvector/pgvector:pg14"


class SemanticFixtureProvider:
    """Simple semantic fixture provider with stable topic dimensions."""

    name = "fixture-semantic"

    def __init__(self, *, model: str, dimension: int = 8):
        self.model = model
        self.dimension = dimension

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_text(text) for text in texts]

    def _embed_text(self, text: str) -> list[float]:
        lowered = text.lower()
        topics = [
            ("jane", "smith", "donor", "endaoment", "philanthropy", "support", "operations"),
            ("board", "dinner", "thread", "coordination", "conversation"),
            ("message", "reply", "followup", "email", "invite"),
            ("calendar", "meeting", "tomorrow", "event", "schedule"),
            ("arnold", "friedman"),
            ("travel", "flight"),
            ("mary", "acme"),
            ("attachment", "pdf"),
        ]
        vector = [0.0] * self.dimension
        for idx, keywords in enumerate(topics):
            for keyword in keywords:
                if keyword in lowered:
                    vector[idx] += 1.0
        if not any(vector):
            vector[0] = 0.1
        norm = math.sqrt(sum(value * value for value in vector))
        return [value / norm for value in vector]


class SlowSemanticFixtureProvider(SemanticFixtureProvider):
    def __init__(self, *, model: str, dimension: int = 8, delay_seconds: float = 0.02):
        super().__init__(model=model, dimension=dimension)
        self.delay_seconds = delay_seconds

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        time.sleep(self.delay_seconds)
        return super().embed_texts(texts)


def _docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], check=True, capture_output=True, text=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False
    return True


def _pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_postgres(dsn: str, *, timeout_seconds: float = 45.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with connect(dsn) as conn:
                conn.execute("SELECT 1")
            return
        except Exception:
            time.sleep(1.0)
    raise RuntimeError(f"Timed out waiting for Postgres at {dsn}")


def _common_provenance(source: str, *fields: str) -> dict[str, ProvenanceEntry]:
    return {field: ProvenanceEntry(source, "2026-03-10", "deterministic") for field in fields}


def _seed_live_vault(vault: Path) -> None:
    jane = PersonCard(
        uid="hfa-person-jane11111111",
        type="person",
        source=["contacts.apple", "linkedin"],
        source_id="jane@example.com",
        created="2026-03-08",
        updated="2026-03-10",
        summary="Jane Smith",
        first_name="Jane",
        last_name="Smith",
        emails=["jane@example.com"],
        company="Endaoment",
        title="Donor Operations Lead",
        description="Leads donor support, philanthropic partnerships, and Endaoment donor operations.",
        tags=["donor", "endaoment"],
    )
    arnold = PersonCard(
        uid="hfa-person-arnold222222",
        type="person",
        source=["contacts.apple"],
        source_id="arnold@example.com",
        created="2026-03-08",
        updated="2026-03-10",
        summary="Arnold Friedman",
        first_name="Arnold",
        last_name="Friedman",
        emails=["arnold@example.com"],
        company="Endaoment",
        title="Board Member",
    )
    mary = PersonCard(
        uid="hfa-person-mary33333333",
        type="person",
        source=["notion"],
        source_id="mary@example.com",
        created="2026-03-08",
        updated="2026-03-10",
        summary="Mary Jones",
        first_name="Mary",
        last_name="Jones",
        emails=["mary@example.com"],
        company="Acme",
        title="Sales Lead",
        description="Unrelated contact for control coverage.",
    )
    thread = EmailThreadCard(
        uid="hfa-email-thread-aaaa1111",
        type="email_thread",
        source=["gmail"],
        source_id="gmail-thread-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Board dinner with Jane",
        gmail_thread_id="gmail-thread-1",
        account_email="robbie@example.com",
        subject="Board dinner with Jane Smith",
        participants=["jane@example.com", "arnold@example.com"],
        messages=["board-dinner-message-1", "board-dinner-message-2"],
        calendar_events=["board-dinner-event"],
        first_message_at="2026-03-10T09:00:00Z",
        last_message_at="2026-03-10T11:00:00Z",
        message_count=2,
        thread_summary="Email thread coordinating the Endaoment board dinner with Jane Smith tomorrow.",
        people=["Jane Smith", "Arnold Friedman"],
    )
    message_one = EmailMessageCard(
        uid="hfa-email-message-bbbb1111",
        type="email_message",
        source=["gmail"],
        source_id="gmail-message-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Board dinner invite",
        gmail_message_id="gmail-message-1",
        gmail_thread_id="gmail-thread-1",
        account_email="robbie@example.com",
        thread="board-dinner-thread",
        from_name="Robbie Heeger",
        from_email="robbie@example.com",
        to_emails=["jane@example.com"],
        participant_emails=["jane@example.com", "arnold@example.com"],
        sent_at="2026-03-10T09:00:00Z",
        subject="Board dinner invite",
        snippet="Board dinner with Jane tomorrow after the Endaoment meeting.",
        calendar_events=["board-dinner-event"],
        invite_title="Endaoment board dinner",
        invite_event_id_hint="board-dinner-event",
        invite_start_at="2026-03-11T18:00:00Z",
        invite_end_at="2026-03-11T20:00:00Z",
        people=["Jane Smith", "Arnold Friedman"],
    )
    message_two = EmailMessageCard(
        uid="hfa-email-message-bbbb2222",
        type="email_message",
        source=["gmail"],
        source_id="gmail-message-2",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Dinner follow-up",
        gmail_message_id="gmail-message-2",
        gmail_thread_id="gmail-thread-1",
        account_email="robbie@example.com",
        thread="board-dinner-thread",
        from_name="Jane Smith",
        from_email="jane@example.com",
        to_emails=["robbie@example.com"],
        participant_emails=["jane@example.com", "arnold@example.com"],
        sent_at="2026-03-10T11:00:00Z",
        subject="Dinner follow-up",
        snippet="Looking forward to tomorrow's board dinner and donor conversation.",
        calendar_events=["board-dinner-event"],
        people=["Jane Smith", "Arnold Friedman"],
    )
    event = CalendarEventCard(
        uid="hfa-calendar-event-cccc111",
        type="calendar_event",
        source=["google.calendar"],
        source_id="calendar-event-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Endaoment board dinner",
        account_email="robbie@example.com",
        calendar_id="primary",
        event_id="board-dinner-event",
        ical_uid="board-dinner-ical",
        title="Endaoment board dinner",
        description="Dinner meeting with Jane Smith and Arnold Friedman about donors and philanthropy.",
        location="Mission District",
        start_at="2026-03-11T18:00:00Z",
        end_at="2026-03-11T20:00:00Z",
        timezone="America/Los_Angeles",
        organizer_email="robbie@example.com",
        organizer_name="Robbie Heeger",
        attendee_emails=["jane@example.com", "arnold@example.com"],
        source_messages=["board-dinner-message-1"],
        source_threads=["board-dinner-thread"],
        meeting_transcripts=["board-dinner-transcript"],
        people=["Jane Smith", "Arnold Friedman"],
    )
    transcript = MeetingTranscriptCard(
        uid="hfa-meeting-transcript-dddd1",
        type="meeting_transcript",
        source=["otter.meeting"],
        source_id="meeting-1",
        created="2026-03-11",
        updated="2026-03-11",
        summary="Endaoment board dinner transcript",
        otter_meeting_id="meeting-1",
        title="Endaoment board dinner",
        transcript_url="https://otter.ai/u/meeting-1/transcript",
        conference_url="https://meet.google.com/example",
        start_at="2026-03-11T18:00:00Z",
        end_at="2026-03-11T20:00:00Z",
        participant_emails=["jane@example.com", "arnold@example.com"],
        calendar_events=["board-dinner-event"],
        event_id_hint="board-dinner-event",
        people=["Jane Smith", "Arnold Friedman"],
    )

    write_card(
        vault,
        "People/jane-smith.md",
        jane,
        body="Jane leads donor support and philanthropic partnerships at Endaoment.",
        provenance=_common_provenance(
            "contacts.apple",
            "summary",
            "first_name",
            "last_name",
            "emails",
            "company",
            "title",
            "description",
            "tags",
        ),
    )
    write_card(
        vault,
        "People/arnold-friedman.md",
        arnold,
        body="Arnold joins the Endaoment board dinner with Jane.",
        provenance=_common_provenance(
            "contacts.apple", "summary", "first_name", "last_name", "emails", "company", "title"
        ),
    )
    write_card(
        vault,
        "People/mary-jones.md",
        mary,
        body="Mary is unrelated to the dinner thread.",
        provenance=_common_provenance(
            "notion", "summary", "first_name", "last_name", "emails", "company", "title", "description"
        ),
    )
    write_card(
        vault,
        "Email/board-dinner-thread.md",
        thread,
        body=(
            "Robbie: Can we lock dinner with Jane after the Endaoment meeting?\n\n"
            "Arnold: Yes, let's keep the board dinner focused on donors and philanthropy.\n\n"
            "Jane: Tomorrow works well for me."
        ),
        provenance=_common_provenance(
            "gmail",
            "summary",
            "gmail_thread_id",
            "account_email",
            "subject",
            "participants",
            "messages",
            "calendar_events",
            "thread_summary",
            "first_message_at",
            "last_message_at",
            "message_count",
        ),
    )
    write_card(
        vault,
        "Email/board-dinner-message-1.md",
        message_one,
        body="Board dinner invite for tomorrow after the Endaoment donor meeting with Jane.",
        provenance=_common_provenance(
            "gmail",
            "summary",
            "gmail_message_id",
            "gmail_thread_id",
            "account_email",
            "thread",
            "from_name",
            "from_email",
            "to_emails",
            "participant_emails",
            "sent_at",
            "subject",
            "snippet",
            "calendar_events",
            "invite_title",
            "invite_event_id_hint",
            "invite_start_at",
            "invite_end_at",
        ),
    )
    write_card(
        vault,
        "Email/board-dinner-message-2.md",
        message_two,
        body="Following up on tomorrow's dinner and donor conversation with Arnold.",
        provenance=_common_provenance(
            "gmail",
            "summary",
            "gmail_message_id",
            "gmail_thread_id",
            "account_email",
            "thread",
            "from_name",
            "from_email",
            "to_emails",
            "participant_emails",
            "sent_at",
            "subject",
            "snippet",
            "calendar_events",
        ),
    )
    write_card(
        vault,
        "Calendar/board-dinner-event.md",
        event,
        body="Calendar invite linked from the dinner email thread.",
        provenance=_common_provenance(
            "google.calendar",
            "summary",
            "account_email",
            "calendar_id",
            "event_id",
            "ical_uid",
            "title",
            "description",
            "location",
            "start_at",
            "end_at",
            "timezone",
            "organizer_email",
            "organizer_name",
            "attendee_emails",
            "source_messages",
            "source_threads",
            "meeting_transcripts",
        ),
    )
    write_card(
        vault,
        "MeetingTranscripts/board-dinner-transcript.md",
        transcript,
        body=(
            "## Summary\n\nCovered the Endaoment board dinner agenda.\n\n"
            "## Transcript\n\nRobbie Heeger | Let's talk through tomorrow's donor dinner."
        ),
        provenance=_common_provenance(
            "otter.meeting",
            "summary",
            "otter_meeting_id",
            "title",
            "transcript_url",
            "conference_url",
            "start_at",
            "end_at",
            "participant_emails",
            "calendar_events",
            "event_id_hint",
        ),
    )


@pytest.fixture(scope="session")
def pgvector_dsn() -> str:
    if not _docker_available():
        pytest.skip("Docker is required for live Postgres retrieval tests")
    container_name = f"ppa-test-{uuid.uuid4().hex[:10]}"
    port = _pick_port()
    dsn = f"postgresql://archive:archive@127.0.0.1:{port}/archive"
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            container_name,
            "-e",
            "POSTGRES_USER=archive",
            "-e",
            "POSTGRES_PASSWORD=archive",
            "-e",
            "POSTGRES_DB=archive",
            "-p",
            f"127.0.0.1:{port}:5432",
            PGVECTOR_IMAGE,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        _wait_for_postgres(dsn)
        yield dsn
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], check=False, capture_output=True, text=True)


@pytest.fixture
def live_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> tuple[Path, PostgresArchiveIndex, SemanticFixtureProvider]:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Email").mkdir()
    (vault / "Calendar").mkdir()
    (vault / "MeetingTranscripts").mkdir()
    (vault / "_templates").mkdir()
    (vault / ".obsidian").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    (meta / "dedup-candidates.json").write_text(json.dumps([]), encoding="utf-8")
    _seed_live_vault(vault)

    schema_name = f"archive_test_{uuid.uuid4().hex[:10]}"
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema_name)
    monkeypatch.setenv("PPA_VECTOR_DIMENSION", "8")

    index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    index.schema = schema_name
    provider = SemanticFixtureProvider(model="fixture-semantic-v1", dimension=8)

    def _fixture_embedding_provider(model: str = "") -> SemanticFixtureProvider:
        return provider

    monkeypatch.setattr("archive_mcp.store.get_embedding_provider", _fixture_embedding_provider)
    monkeypatch.setattr("archive_mcp.commands._resolve.get_embedding_provider", _fixture_embedding_provider)
    return vault, index, provider


def test_type_aware_chunk_builder_emits_stable_chunk_types():
    person_card = {
        "uid": "hfa-person-test0000001",
        "type": "person",
        "source": ["contacts.apple"],
        "source_id": "jane@example.com",
        "created": "2026-03-10",
        "updated": "2026-03-10",
        "summary": "Jane Smith",
        "emails": ["jane@example.com"],
        "company": "Endaoment",
        "title": "Donor Operations Lead",
        "description": "Leads donor support.",
    }
    thread_card = {
        "uid": "hfa-email-thread-test1",
        "type": "email_thread",
        "source": ["gmail"],
        "source_id": "thread-1",
        "created": "2026-03-10",
        "updated": "2026-03-10",
        "summary": "Board dinner with Jane",
        "gmail_thread_id": "thread-1",
        "subject": "Board dinner with Jane Smith",
        "participants": ["jane@example.com"],
        "thread_summary": "Dinner coordination thread",
    }
    event_card = {
        "uid": "hfa-calendar-event-test1",
        "type": "calendar_event",
        "source": ["google.calendar"],
        "source_id": "event-1",
        "created": "2026-03-10",
        "updated": "2026-03-10",
        "summary": "Endaoment dinner",
        "calendar_id": "primary",
        "event_id": "event-1",
        "title": "Endaoment dinner",
        "description": "Meeting with Jane",
        "organizer_email": "robbie@example.com",
        "attendee_emails": ["jane@example.com"],
        "source_threads": ["board-dinner-thread"],
        "meeting_transcripts": ["board-dinner-transcript"],
    }
    transcript_card = {
        "uid": "hfa-meeting-transcript-test1",
        "type": "meeting_transcript",
        "source": ["otter.meeting"],
        "source_id": "meeting-1",
        "created": "2026-03-11",
        "updated": "2026-03-11",
        "summary": "Endaoment dinner transcript",
        "otter_meeting_id": "meeting-1",
        "title": "Endaoment dinner",
        "status": "completed",
        "conference_url": "https://meet.google.com/example",
        "start_at": "2026-03-11T18:00:00Z",
        "end_at": "2026-03-11T20:00:00Z",
        "speaker_names": ["Robbie Heeger"],
        "participant_emails": ["jane@example.com"],
        "calendar_events": ["board-dinner-event"],
        "event_id_hint": "board-dinner-event",
    }
    document_card = {
        "uid": "hfa-document-test1",
        "type": "document",
        "source": ["file.library", "file.library.documents"],
        "source_id": "documents:Work/Endaoment/endaoment-overview.pdf",
        "created": "2026-03-10",
        "updated": "2026-03-10",
        "summary": "Endaoment Overview",
        "title": "Endaoment Overview",
        "document_type": "pdf",
        "extension": "pdf",
        "library_root": "documents",
        "relative_path": "Work/Endaoment/endaoment-overview.pdf",
        "document_date": "2026-03-10",
        "date_start": "2026-03-10",
        "date_end": "2026-03-11",
        "authors": ["Robbie Heeger"],
        "counterparties": ["Endaoment"],
        "emails": ["robbie@example.com"],
        "websites": ["https://endaoment.org"],
        "location": "New York",
        "sheet_names": ["Summary"],
        "page_count": 12,
        "extraction_status": "content_extracted",
        "quality_flags": ["title_from_filename"],
        "people": ["[[robbie-heeger]]"],
        "orgs": ["Endaoment"],
        "description": "Overview deck for donors.",
    }
    git_repo_card = {
        "uid": "hfa-git-repository-test1",
        "type": "git_repository",
        "source": ["github.repo"],
        "source_id": "rheeger/hey-arnold-hfa",
        "created": "2026-03-10",
        "updated": "2026-03-10",
        "summary": "rheeger/hey-arnold-hfa",
        "github_repo_id": "123",
        "name_with_owner": "rheeger/hey-arnold-hfa",
        "owner_login": "rheeger",
        "owner_type": "User",
        "visibility": "private",
        "default_branch": "main",
        "primary_language": "Python",
        "languages": ["Python", "TypeScript"],
        "topics": ["archive", "mcp"],
        "description": "Archive tooling.",
    }
    git_commit_card = {
        "uid": "hfa-git-commit-test1",
        "type": "git_commit",
        "source": ["github.commit"],
        "source_id": "rheeger/hey-arnold-hfa:deadbeef",
        "created": "2026-03-10",
        "updated": "2026-03-10",
        "summary": "Add GitHub archive ingest",
        "commit_sha": "deadbeef",
        "repository_name_with_owner": "rheeger/hey-arnold-hfa",
        "author_login": "rheeger",
        "author_email": "robbie@endaoment.org",
        "message_headline": "Add GitHub archive ingest",
        "additions": 10,
        "deletions": 2,
        "changed_files": 3,
    }
    git_thread_card = {
        "uid": "hfa-git-thread-test1",
        "type": "git_thread",
        "source": ["github.thread", "github.thread.pull_request"],
        "source_id": "rheeger/hey-arnold-hfa:pull_request:12",
        "created": "2026-03-10",
        "updated": "2026-03-10",
        "summary": "Add GitHub archive ingest",
        "github_thread_id": "456",
        "repository_name_with_owner": "rheeger/hey-arnold-hfa",
        "thread_type": "pull_request",
        "number": "12",
        "title": "Add GitHub archive ingest",
        "state": "open",
        "participant_logins": ["rheeger"],
        "messages": ["[[hfa-git-message-test1]]"],
        "base_ref": "main",
        "head_ref": "feature/github-ingest",
    }
    git_message_card = {
        "uid": "hfa-git-message-test1",
        "type": "git_message",
        "source": ["github.message", "github.message.review_comment"],
        "source_id": "rheeger/hey-arnold-hfa:review_comment:789",
        "created": "2026-03-10",
        "updated": "2026-03-10",
        "summary": "Can we preserve the GitHub ids here?",
        "github_message_id": "789",
        "repository_name_with_owner": "rheeger/hey-arnold-hfa",
        "thread": "[[hfa-git-thread-test1]]",
        "message_type": "review_comment",
        "review_state": "COMMENTED",
        "actor_login": "rheeger",
        "path": "archive_mcp/index_store.py",
        "position": "12",
        "review_commit_sha": "deadbeef",
        "diff_hunk": "@@ -1,2 +1,2 @@",
    }

    person_chunks = {chunk["chunk_type"] for chunk in render_chunks_for_card(person_card, "Jane supports donors.")}
    thread_chunks = {
        chunk["chunk_type"]
        for chunk in render_chunks_for_card(
            thread_card, "Robbie: dinner?\n\nJane: yes tomorrow.\n\nArnold: works for me."
        )
    }
    event_chunks = {chunk["chunk_type"] for chunk in render_chunks_for_card(event_card, "Invite linked from email.")}
    transcript_chunks = {
        chunk["chunk_type"]
        for chunk in render_chunks_for_card(
            transcript_card,
            "## Summary\n\nCovered dinner plans.\n\n## Transcript\n\nRobbie Heeger | Let's review tomorrow's agenda.",
        )
    }
    document_chunks = {
        chunk["chunk_type"]
        for chunk in render_chunks_for_card(document_card, "Endaoment helps donors give complex assets.")
    }
    document_section_chunks = {
        chunk["chunk_type"]
        for chunk in render_chunks_for_card(
            document_card,
            "# Executive summary\n\nOne paragraph.\n\n## Details\n\nMore about Endaoment and donors.",
        )
    }
    git_repo_chunks = {chunk["chunk_type"] for chunk in render_chunks_for_card(git_repo_card, "")}
    git_commit_chunks = {
        chunk["chunk_type"]
        for chunk in render_chunks_for_card(git_commit_card, "Build the first GitHub archive ingest path.")
    }
    git_thread_chunks = {chunk["chunk_type"] for chunk in render_chunks_for_card(git_thread_card, "")}
    git_message_chunks = {
        chunk["chunk_type"]
        for chunk in render_chunks_for_card(git_message_card, "Can we preserve the GitHub ids here?")
    }

    assert {"person_profile", "person_role", "person_context", "person_body"} <= person_chunks
    assert {
        "thread_subject",
        "thread_context",
        "thread_summary",
        "thread_window",
        "thread_recent_window",
    } <= thread_chunks
    assert {
        "event_title_time",
        "event_participants",
        "event_description",
        "event_sources",
        "event_body",
    } <= event_chunks
    assert {
        "meeting_transcript_identity",
        "meeting_transcript_participants",
        "meeting_transcript_links",
        "meeting_transcript_section",
        "meeting_transcript_turn",
    } <= transcript_chunks
    assert {
        "document_title_meta",
        "document_entities",
        "document_extraction_meta",
        "document_description",
        "document_body",
    } <= document_chunks
    assert {"document_section"} <= document_section_chunks
    assert {"git_repo_identity", "git_repo_topics", "git_repo_description"} <= git_repo_chunks
    assert {"git_commit_headline", "git_commit_context", "git_commit_body"} <= git_commit_chunks
    assert {"git_thread_title_state", "git_thread_participants", "git_thread_branch_context"} <= git_thread_chunks
    assert {
        "git_message_context",
        "git_message_review_context",
        "git_message_diff_hunk",
        "git_message_body",
    } <= git_message_chunks


def test_live_postgres_rebuild_graph_and_lexical_search(live_archive):
    vault, index, _provider = live_archive
    rebuilt = archive_rebuild_indexes()
    assert "cards: 8" in rebuilt
    assert index.status()["chunk_schema_version"] == "4"

    lexical = archive_search("Jane Smith", limit=3).splitlines()
    assert lexical[0].startswith("- People/jane-smith.md")

    graph = archive_graph("Email/board-dinner-thread.md")
    assert "Email/board-dinner-message-1.md" in graph
    assert "Calendar/board-dinner-event.md" in graph

    transcript_graph = archive_graph("MeetingTranscripts/board-dinner-transcript.md")
    assert "Calendar/board-dinner-event.md" in transcript_graph

    event_graph = archive_graph("Calendar/board-dinner-event.md")
    assert "MeetingTranscripts/board-dinner-transcript.md" in event_graph


def test_live_postgres_lexical_candidates_fts_and_exact(live_archive):
    """Exercise _lexical_candidates SQL directly against real Postgres.

    Catches schema interpolation bugs (un-interpolated f-strings) that
    only surface at query execution time.
    """
    _vault, index, _provider = live_archive
    index.rebuild()

    # FTS path: "Jane Smith" should match via search_document
    fts_rows = index._lexical_candidates(query="Jane Smith", limit=5)
    assert fts_rows, "FTS branch returned no rows for seeded person card"
    jane_row = next((r for r in fts_rows if "jane-smith" in str(r["rel_path"])), None)
    assert jane_row is not None, "Jane Smith card not in FTS results"
    assert float(jane_row["lexical_score"]) > 0

    # Exact path: slug match
    exact_rows = index._lexical_candidates(query="jane-smith", limit=5)
    assert exact_rows, "Exact branch returned no rows for slug match"
    exact_jane = next((r for r in exact_rows if "jane-smith" in str(r["rel_path"])), None)
    assert exact_jane is not None
    assert int(exact_jane["slug_exact"]) == 1 or int(exact_jane["person_exact"]) == 1


def test_live_postgres_vector_search_groups_to_card_level_and_supports_filters(live_archive):
    _vault, index, provider = live_archive
    index.rebuild()
    embed_result = archive_embed_pending(limit=50, embedding_model=provider.model, embedding_version=1)
    assert "- embedded:" in embed_result
    assert "- failed: 0" in embed_result

    vector_result = archive_vector_search(
        "donor support at Endaoment",
        limit=3,
        embedding_model=provider.model,
        embedding_version=1,
    )
    vector_lines = vector_result.splitlines()
    assert vector_lines[1].startswith("- People/jane-smith.md")
    assert "matched_by=vector" in vector_result
    assert "chunk=person_" in vector_result

    filtered = archive_vector_search(
        "calendar dinner tomorrow",
        limit=3,
        embedding_model=provider.model,
        embedding_version=1,
        type_filter="calendar_event",
    )
    assert "Calendar/board-dinner-event.md" in filtered
    assert "Email/board-dinner-thread.md" not in filtered


def test_live_postgres_embed_pending_supports_concurrent_claims(live_archive, monkeypatch: pytest.MonkeyPatch):
    _vault, index, _provider = live_archive
    index.rebuild()
    slow_provider = SlowSemanticFixtureProvider(model="fixture-semantic-v1", dimension=8)
    monkeypatch.setenv("PPA_EMBED_BATCH_SIZE", "1")
    monkeypatch.setenv("PPA_EMBED_WRITE_BATCH_SIZE", "1")
    monkeypatch.setenv("PPA_EMBED_CONCURRENCY", "1")
    monkeypatch.setenv("PPA_EMBED_PROGRESS_EVERY", "0")

    total_chunks = int(index.embedding_status(embedding_model=slow_provider.model, embedding_version=1)["chunk_count"])
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                index.embed_pending,
                provider=slow_provider,
                embedding_model=slow_provider.model,
                embedding_version=1,
                limit=total_chunks,
            )
            for _ in range(2)
        ]
        results = [future.result() for future in futures]

    embedded_total = sum(int(result["embedded"]) for result in results)
    final_status = index.embedding_status(embedding_model=slow_provider.model, embedding_version=1)
    assert embedded_total == total_chunks
    assert int(final_status["embedded_chunk_count"]) == total_chunks
    assert int(final_status["pending_chunk_count"]) == 0


def test_live_postgres_hybrid_search_prefers_exact_anchor_and_boosts_graph_neighbors(live_archive):
    _vault, index, provider = live_archive
    index.rebuild()
    index.embed_pending(provider=provider, embedding_model=provider.model, embedding_version=1, limit=50)

    hybrid_rows = index.hybrid_search(
        query="jane@example.com",
        query_vector=provider.embed_texts(["jane@example.com"])[0],
        embedding_model=provider.model,
        embedding_version=1,
        limit=5,
    )
    assert hybrid_rows[0]["rel_path"] == "People/jane-smith.md"
    assert hybrid_rows[0]["exact_match"] is True
    assert any(row["graph_hops"] == "1" for row in hybrid_rows[1:])

    hybrid_result = archive_hybrid_search(
        "calendar dinner with Jane tomorrow",
        limit=5,
        embedding_model=provider.model,
        embedding_version=1,
        start_date="2026-03-11",
        end_date="2026-03-11",
    )
    assert "Calendar/board-dinner-event.md" in hybrid_result
    assert "matched_by=" in hybrid_result
    assert "provenance_bias=" in hybrid_result


def test_benchmark_sample_builder_preserves_notes_and_manifest(tmp_path: Path, live_archive):
    vault, _index, _provider = live_archive
    output_vault = tmp_path / "sample-vault"

    manifest = build_benchmark_sample(
        source_vault=vault,
        output_vault=output_vault,
        per_group_limit=1,
        max_notes=4,
        neighborhood_hops=1,
    )

    assert manifest["selected_note_count"] >= 1
    assert (output_vault / "_meta" / "benchmark-sample.json").exists()
    copied_notes = sorted(path for path in output_vault.rglob("*.md"))
    assert copied_notes


def test_benchmark_sample_builder_supports_percent_limit(tmp_path: Path, live_archive):
    vault, _index, _provider = live_archive
    output_vault = tmp_path / "sample-vault-percent"

    manifest = build_benchmark_sample(
        source_vault=vault,
        output_vault=output_vault,
        sample_percent=10,
        max_notes=9999,
        per_group_limit=10,
        neighborhood_hops=0,
    )

    assert manifest["sample_percent"] == 10
    assert manifest["percent_note_limit"] >= 1
    assert manifest["selected_note_count"] <= manifest["percent_note_limit"]


def test_benchmark_sample_builder_rejects_percent_above_ten(tmp_path: Path):
    source_vault = tmp_path / "source"
    source_vault.mkdir()
    with pytest.raises(ValueError, match="<= 10"):
        build_benchmark_sample(source_vault=source_vault, output_vault=tmp_path / "out", sample_percent=11)


def test_benchmark_sample_builder_dedupes_duplicate_uids(tmp_path: Path):
    source_vault = tmp_path / "source-dup"
    (source_vault / "People").mkdir(parents=True)
    (source_vault / "_meta").mkdir()
    (source_vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (source_vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")

    person_a = PersonCard(
        uid="hfa-person-duplicate-1",
        type="person",
        source=["contacts.apple"],
        source_id="dup-a",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Duplicate Person A",
        emails=["dup@example.com"],
    )
    person_b = PersonCard(
        uid="hfa-person-duplicate-1",
        type="person",
        source=["contacts.apple"],
        source_id="dup-b",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Duplicate Person B",
        emails=["dup@example.com"],
    )
    write_card(
        source_vault,
        "People/duplicate-a.md",
        person_a,
        provenance=_common_provenance("contacts.apple", "summary", "emails"),
    )
    write_card(
        source_vault,
        "People/duplicate-b.md",
        person_b,
        provenance=_common_provenance("contacts.apple", "summary", "emails"),
    )

    output_vault = tmp_path / "sample-deduped"
    manifest = build_benchmark_sample(
        source_vault=source_vault,
        output_vault=output_vault,
        per_group_limit=10,
        max_notes=10,
        neighborhood_hops=0,
    )

    copied_notes = sorted(path for path in output_vault.rglob("*.md"))
    assert manifest["duplicate_uid_collision_count"] == 1
    assert len(copied_notes) == 1


def test_benchmark_rebuild_returns_metrics(live_archive):
    vault, index, _provider = live_archive

    result = benchmark_rebuild(
        vault=vault,
        schema=f"{index.schema}_bench",
        workers=1,
        batch_size=2,
        commit_interval=2,
        progress_every=0,
        executor_kind="serial",
    )

    assert result["counts"]["cards"] == 8
    assert result["counts"]["edges"] >= 2
    assert result["metrics"]["scan_seconds"] >= 0
    assert result["metrics"]["load_seconds"] >= 0


def test_benchmark_seed_links_returns_metrics(live_archive, monkeypatch: pytest.MonkeyPatch):
    vault, index, _provider = live_archive

    monkeypatch.setattr(
        "archive_mcp.benchmark.run_seed_link_backfill",
        lambda idx, **kwargs: {
            "workers": kwargs.get("max_workers", 1),
            "jobs_enqueued": 4,
            "jobs_completed": 4,
            "jobs_failed": 0,
            "candidates": 12,
            "needs_review": 3,
            "auto_promoted": 2,
            "canonical_safe": 1,
            "derived_promotions_applied": 0,
            "canonical_applied": 0,
            "llm_judged": 0,
            "promotion_blocked": 0,
            "orphaned_links_before": 2,
            "orphaned_links_after": 1,
            "job_type": "seed_backfill",
        },
    )
    monkeypatch.setattr(
        "archive_mcp.benchmark.compute_link_quality_gate",
        lambda idx: {"passes": True, "scan_coverage": 1.0},
    )
    monkeypatch.setattr(
        "archive_mcp.benchmark._cleaning_snapshot",
        lambda vault: {"email_messages_with_thread": 1, "orphaned_wikilinks": 2},
    )
    monkeypatch.setattr(
        "archive_mcp.benchmark._repair_opportunities",
        lambda index: [
            {
                "module_name": "communicationLinker",
                "proposed_link_type": "message_in_thread",
                "decision": "canonical_safe",
                "count": 2,
            }
        ],
    )

    result = benchmark_seed_links(
        vault=vault,
        schema=f"{index.schema}_seed_bench",
        workers=1,
        batch_size=2,
        commit_interval=2,
        progress_every=0,
        executor_kind="serial",
        rebuild_first=False,
    )

    assert result["link_result"]["candidates"] == 12
    assert result["link_metrics"]["elapsed_seconds"] >= 0
    assert result["link_metrics"]["jobs_per_second"] >= 0
    assert result["cleaning_proof"]["delta"]["email_messages_with_thread"] == 0
    assert result["cleaning_proof"]["repair_opportunities"][0]["count"] == 2
    assert result["quality_gate"]["passes"] is True


def test_resolve_benchmark_profile_supports_local_and_vm_profiles():
    local = resolve_benchmark_profile("local-laptop")
    vm = resolve_benchmark_profile("vm-large")

    assert set(BENCHMARK_PROFILES) == {"local-laptop", "vm-large"}
    assert local["profile"] == "local-laptop"
    assert vm["profile"] == "vm-large"
    assert vm["workers"] >= local["workers"]
