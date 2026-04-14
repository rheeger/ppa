import pytest
from pydantic import ValidationError

from archive_vault.schema import (
    BeeperAttachmentCard,
    BeeperMessageCard,
    BeeperThreadCard,
    DocumentCard,
    EmailMessageCard,
    EmailThreadCard,
    GitCommitCard,
    GitMessageCard,
    GitRepositoryCard,
    GitThreadCard,
    IMessageAttachmentCard,
    IMessageMessageCard,
    IMessageThreadCard,
    MediaAssetCard,
    MedicalRecordCard,
    MeetingTranscriptCard,
    PersonCard,
    VaccinationCard,
    card_to_frontmatter,
    validate_card_permissive,
    validate_card_strict,
)


def test_valid_person_card(sample_person_card):
    assert sample_person_card.type == "person"
    assert sample_person_card.emails == ["jane@example.com"]


def test_validate_card_strict_rejects_extra_field():
    with pytest.raises(ValidationError):
        validate_card_strict(
            {
                "uid": "hfa-person-abc123def456",
                "type": "person",
                "source": ["contacts.apple"],
                "source_id": "jane@example.com",
                "created": "2026-03-06",
                "updated": "2026-03-06",
                "summary": "Jane",
                "favorite_color": "blue",
            }
        )


def test_validate_card_permissive_ignores_extra_field():
    card = validate_card_permissive(
        {
            "uid": "hfa-person-abc123def456",
            "type": "person",
            "source": ["contacts.apple"],
            "source_id": "jane@example.com",
            "created": "2026-03-06",
            "updated": "2026-03-06",
            "summary": "",
            "emails": ["JANE@example.com", "jane@example.com"],
            "future_field": "allowed on read",
        }
    )
    assert isinstance(card, PersonCard)
    assert card.emails == ["jane@example.com"]
    assert card.summary == "jane@example.com"


def test_card_to_frontmatter_omits_empty_defaults():
    card = PersonCard(
        uid="hfa-person-abc123def456",
        type="person",
        source=["contacts.apple"],
        source_id="jane@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
    )
    frontmatter = card_to_frontmatter(card)
    assert "description" not in frontmatter
    assert "tags" not in frontmatter
    assert frontmatter["summary"] == "Jane Smith"


def test_person_card_normalizes_social_and_profile_fields():
    card = PersonCard(
        uid="hfa-person-abc123def456",
        type="person",
        source=["linkedin"],
        source_id="janesmith",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
        linkedin="https://www.linkedin.com/in/JaneSmith/",
        github="https://github.com/JaneSmith",
        twitter="@JaneSmith",
        companies=["Endaoment", "Endaoment Labs"],
        titles=["VP Partnerships"],
        linkedin_connected_on="2024-01-01",
    )
    assert card.linkedin == "janesmith"
    assert card.linkedin_url == "https://www.linkedin.com/in/janesmith"
    assert card.github == "janesmith"
    assert card.twitter == "janesmith"
    assert card.company == "Endaoment"
    assert card.title == "VP Partnerships"


def test_email_message_card_normalizes_addresses_and_summary():
    card = EmailMessageCard(
        uid="hfa-email-message-abc123def456",
        type="email_message",
        source=["gmail.message"],
        source_id="message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="",
        gmail_message_id="message-1",
        gmail_thread_id="thread-1",
        account_email="ME@EXAMPLE.COM",
        from_email="ALICE@EXAMPLE.COM",
        to_emails=["Bob@Example.com", "bob@example.com"],
        participant_emails=["ALICE@EXAMPLE.COM", "bob@example.com"],
        subject=" Hello there ",
        snippet=" Hi ",
    )
    assert card.account_email == "me@example.com"
    assert card.from_email == "alice@example.com"
    assert card.to_emails == ["bob@example.com"]
    assert card.summary == "Hello there"


def test_email_thread_card_sets_message_count_from_messages():
    card = EmailThreadCard(
        uid="hfa-email-thread-abc123def456",
        type="email_thread",
        source=["gmail.thread"],
        source_id="thread-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="",
        gmail_thread_id="thread-1",
        participants=["ALICE@EXAMPLE.COM", "alice@example.com"],
        messages=["[[hfa-email-message-1]]", "[[hfa-email-message-2]]"],
        subject=" Project thread ",
    )
    assert card.participants == ["alice@example.com"]
    assert card.summary == "Project thread"
    assert card.message_count == 2


def test_imessage_thread_card_normalizes_handles_and_counts():
    card = IMessageThreadCard(
        uid="hfa-imessage-thread-abc123def456",
        type="imessage_thread",
        source=["imessage.thread"],
        source_id="chat-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="",
        imessage_chat_id="chat-1",
        display_name=" Family Chat ",
        participant_handles=["(650) 555-1111", "+1 650 555 1111", "MOM@example.com"],
        messages=["[[hfa-imessage-message-1]]", "[[hfa-imessage-message-2]]"],
        attachments=["[[hfa-imessage-attachment-1]]"],
    )
    assert card.participant_handles == ["+16505551111", "mom@example.com"]
    assert card.summary == "Family Chat"
    assert card.message_count == 2
    assert card.attachment_count == 1
    assert card.has_attachments is True


def test_imessage_message_card_uses_sender_or_subject_summary():
    card = IMessageMessageCard(
        uid="hfa-imessage-message-abc123def456",
        type="imessage_message",
        source=["imessage.message"],
        source_id="message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="",
        imessage_message_id="message-1",
        sender_handle="(650) 555-1111",
        participant_handles=["(650) 555-1111", "Mom@example.com"],
        attachments=["[[hfa-imessage-attachment-1]]"],
    )
    assert card.sender_handle == "+16505551111"
    assert card.participant_handles == ["+16505551111", "mom@example.com"]
    assert card.summary == "+16505551111"
    assert card.has_attachments is True


def test_imessage_attachment_card_prefers_filename_for_summary():
    card = IMessageAttachmentCard(
        uid="hfa-imessage-attachment-abc123def456",
        type="imessage_attachment",
        source=["imessage.attachment"],
        source_id="message-1:a1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="",
        imessage_message_id="message-1",
        attachment_id="a1",
        filename="IMG_1001.JPG",
        transfer_name="IMG_1001.JPG",
    )
    assert card.summary == "IMG_1001.JPG"


def test_beeper_thread_card_prefers_counterpart_summary_and_counts():
    card = BeeperThreadCard(
        uid="hfa-beeper-thread-abc123def456",
        type="beeper_thread",
        source=["beeper.thread"],
        source_id="room-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="",
        beeper_room_id="room-1",
        thread_type="single",
        counterpart_names=[" PedroYan ", "PedroYan"],
        counterpart_identifiers=["username:pedroyan", "username:pedroyan"],
        messages=["[[hfa-beeper-message-1]]", "[[hfa-beeper-message-2]]"],
        attachments=["[[hfa-beeper-attachment-1]]"],
    )
    assert card.summary == "PedroYan"
    assert card.counterpart_identifiers == ["username:pedroyan"]
    assert card.message_count == 2
    assert card.attachment_count == 1
    assert card.has_attachments is True


def test_beeper_message_and_attachment_cards_normalize_summary_fields():
    message = BeeperMessageCard(
        uid="hfa-beeper-message-abc123def456",
        type="beeper_message",
        source=["beeper.message"],
        source_id="event-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="",
        beeper_event_id="event-1",
        sender_name=" PedroYan ",
        message_type=" text ",
        sender_person=" [[Pedro Yan]] ",
        attachments=["[[hfa-beeper-attachment-1]]"],
    )
    attachment = BeeperAttachmentCard(
        uid="hfa-beeper-attachment-abc123def456",
        type="beeper_attachment",
        source=["beeper.attachment"],
        source_id="event-1:att-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="",
        beeper_event_id="event-1",
        attachment_id="att-1",
        attachment_type=" IMG ",
        filename=" photo.jpg ",
    )
    assert message.summary == "PedroYan"
    assert message.message_type == "TEXT"
    assert message.sender_person == "[[Pedro Yan]]"
    assert message.has_attachments is True
    assert attachment.summary == "photo.jpg"
    assert attachment.attachment_type == "img"


def test_media_asset_card_normalizes_private_metadata_and_summary():
    card = MediaAssetCard(
        uid="hfa-media-asset-abc123def456",
        type="media_asset",
        source=["photos.asset", "photos.private.person", "photos.private.label"],
        source_id="apple-photos:asset-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="",
        photos_asset_id="asset-1",
        photos_source_label=" apple-photos ",
        filename=" IMG_1001.JPG ",
        original_filename=" IMG_0001.HEIC ",
        labels=["Beach", "beach", "Sunset"],
        person_labels=[" Alice Example ", "Alice Example"],
        albums=[" Summer Trip ", "Summer Trip"],
        album_paths=[" Family/Trips/Summer Trip ", "Family/Trips/Summer Trip"],
    )
    assert card.summary == "IMG_0001.HEIC"
    assert card.photos_source_label == "apple-photos"
    assert card.labels == ["beach", "sunset"]
    assert card.person_labels == ["Alice Example"]
    assert card.albums == ["Summer Trip"]
    assert card.album_paths == ["Family/Trips/Summer Trip"]


def test_document_card_normalizes_fields_and_summary():
    card = DocumentCard(
        uid="hfa-document-abc123def456",
        type="document",
        source=["file.library"],
        source_id="documents:Work/Endaoment/endaoment-overview.pdf",
        created="2026-03-10",
        updated="2026-03-10",
        summary="",
        library_root=" documents ",
        relative_path=" Work/Endaoment/endaoment-overview.pdf ",
        filename=" Endaoment Overview.pdf ",
        extension=".PDF",
        mime_type=" application/pdf ",
        document_type=" Board Deck ",
        title=" Endaoment Overview ",
        authors=[" Robbie Heeger ", "Robbie Heeger"],
        counterparties=[" Endaoment ", "Endaoment"],
        emails=["ROBBIE@ENDAOMENT.ORG", "robbie@endaoment.org"],
        websites=[" https://endaoment.org ", "https://endaoment.org"],
        location=" New York ",
        text_source=" PDF ",
        extraction_status=" CONTENT_EXTRACTED ",
        quality_flags=[" title_from_filename ", "title_from_filename"],
    )
    assert card.summary == "Endaoment Overview"
    assert card.library_root == "documents"
    assert card.extension == "pdf"
    assert card.document_type == "board deck"
    assert card.authors == ["Robbie Heeger"]
    assert card.counterparties == ["Endaoment"]
    assert card.emails == ["robbie@endaoment.org"]
    assert card.websites == ["https://endaoment.org"]
    assert card.location == "New York"
    assert card.text_source == "pdf"
    assert card.extraction_status == "content_extracted"
    assert card.quality_flags == ["title_from_filename"]


def test_medical_record_card_normalizes_fields_and_summary():
    card = MedicalRecordCard(
        uid="hfa-medical-record-abc123def456",
        type="medical_record",
        source=["onemedical.fhir"],
        source_id="onemedical:Observation:obs-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="",
        source_system=" OneMedical ",
        source_format=" FHIR_JSON ",
        record_type=" Observation ",
        record_subtype=" Vital Signs ",
        status=" FINAL ",
        code_display=" Blood Pressure ",
        details_json={"source_hashes": {"fhir_json": "abc"}},
    )
    assert card.summary == "Blood Pressure"
    assert card.source_system == "onemedical"
    assert card.source_format == "fhir_json"
    assert card.record_type == "observation"
    assert card.record_subtype == "Vital Signs"
    assert card.status == "final"


def test_vaccination_card_normalizes_fields_and_summary():
    card = VaccinationCard(
        uid="hfa-vaccination-abc123def456",
        type="vaccination",
        source=["onemedical.fhir"],
        source_id="onemedical:Immunization:imm-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="",
        source_system=" OneMedical ",
        source_format=" VACCINE_PDF ",
        status=" COMPLETED ",
        vaccine_name=" Influenza ",
        brand_name=" Flublok ",
        lot_number=" LOT123 ",
        details_json={"source_hashes": {"vaccine_pdf": "abc"}},
    )
    assert card.summary == "Influenza"
    assert card.source_system == "onemedical"
    assert card.source_format == "vaccine_pdf"
    assert card.status == "completed"
    assert card.brand_name == "Flublok"


def test_git_repository_card_normalizes_fields_and_summary():
    card = GitRepositoryCard(
        uid="hfa-git-repository-abc123def456",
        type="git_repository",
        source=["github.repo"],
        source_id="rheeger/hey-arnold-hfa",
        created="2026-03-10",
        updated="2026-03-10",
        summary="",
        github_repo_id="12345",
        github_node_id="R_kgDOExample",
        name_with_owner="rheeger/hey-arnold-hfa",
        owner_login="@rheeger",
        owner_type=" User ",
        visibility=" PRIVATE ",
        topics=["Archive", "archive", "MCP"],
        languages=["Python", "Python", "TypeScript"],
    )
    assert card.summary == "rheeger/hey-arnold-hfa"
    assert card.owner_login == "rheeger"
    assert card.visibility == "private"
    assert card.topics == ["archive", "mcp"]
    assert card.languages == ["Python", "TypeScript"]


def test_git_commit_card_normalizes_people_and_summary():
    card = GitCommitCard(
        uid="hfa-git-commit-abc123def456",
        type="git_commit",
        source=["github.commit"],
        source_id="rheeger/hey-arnold-hfa:deadbeef",
        created="2026-03-10",
        updated="2026-03-10",
        summary="",
        commit_sha="DEADBEEF",
        repository_name_with_owner="rheeger/hey-arnold-hfa",
        author_login="@rheeger",
        author_email="ROBBIE@ENDAOMENT.ORG",
        committer_login="RHeeger",
        message_headline=" Add github ingestion ",
        parent_shas=["ABC123", "abc123"],
    )
    assert card.summary == "Add github ingestion"
    assert card.commit_sha == "deadbeef"
    assert card.author_login == "rheeger"
    assert card.author_email == "robbie@endaoment.org"
    assert card.committer_login == "rheeger"
    assert card.parent_shas == ["ABC123", "abc123"]


def test_git_thread_card_normalizes_lists_and_counts():
    card = GitThreadCard(
        uid="hfa-git-thread-abc123def456",
        type="git_thread",
        source=["github.thread", "github.thread.pull_request"],
        source_id="rheeger/hey-arnold-hfa:pull_request:12",
        created="2026-03-10",
        updated="2026-03-10",
        summary="",
        github_thread_id="999",
        repository_name_with_owner="rheeger/hey-arnold-hfa",
        thread_type=" Pull_Request ",
        number="12",
        title=" Add github archive ingest ",
        state=" OPEN ",
        labels=["Archive", "archive"],
        assignees=["@rheeger", "rheeger"],
        participant_logins=["OctoCat", "octocat"],
        messages=["[[hfa-git-message-1]]", "[[hfa-git-message-2]]"],
    )
    assert card.summary == "Add github archive ingest"
    assert card.thread_type == "pull_request"
    assert card.state == "open"
    assert card.labels == ["archive"]
    assert card.assignees == ["rheeger"]
    assert card.participant_logins == ["octocat"]
    assert card.message_count == 2


def test_git_message_card_normalizes_review_fields_and_summary():
    card = GitMessageCard(
        uid="hfa-git-message-abc123def456",
        type="git_message",
        source=["github.message", "github.message.review_comment"],
        source_id="rheeger/hey-arnold-hfa:review_comment:55",
        created="2026-03-10",
        updated="2026-03-10",
        summary="",
        github_message_id="55",
        repository_name_with_owner="rheeger/hey-arnold-hfa",
        thread="[[hfa-git-thread-abc123def456]]",
        message_type=" Review_Comment ",
        actor_login="@rheeger",
        actor_email="ROBBIE@ENDAOMENT.ORG",
        review_state=" approved ",
        review_commit_sha="DEADBEEF",
        original_commit_sha="BEEFCAFE",
    )
    assert card.summary == "rheeger"
    assert card.message_type == "review_comment"
    assert card.actor_login == "rheeger"
    assert card.actor_email == "robbie@endaoment.org"
    assert card.review_state == "APPROVED"
    assert card.review_commit_sha == "deadbeef"
    assert card.original_commit_sha == "beefcafe"


def test_calendar_event_card_dedupes_meeting_transcripts():
    card = validate_card_strict(
        {
            "uid": "hfa-calendar-event-abc123def456",
            "type": "calendar_event",
            "source": ["google.calendar"],
            "source_id": "primary:event-1",
            "created": "2026-03-10",
            "updated": "2026-03-10",
            "summary": "",
            "calendar_id": "primary",
            "event_id": "event-1",
            "meeting_transcripts": ["[[hfa-meeting-transcript-1]]", " [[hfa-meeting-transcript-1]] "],
            "title": "Board sync",
        }
    )
    assert card.meeting_transcripts == ["[[hfa-meeting-transcript-1]]"]


def test_meeting_transcript_card_normalizes_fields_and_summary():
    card = MeetingTranscriptCard(
        uid="hfa-meeting-transcript-abc123def456",
        type="meeting_transcript",
        source=["otter.meeting"],
        source_id="meeting-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="",
        otter_meeting_id="meeting-1",
        otter_conversation_id=" convo-1 ",
        account_email="ROBBIE@EXAMPLE.COM",
        title=" Weekly Product Sync ",
        meeting_url=" https://otter.ai/u/meeting-1 ",
        transcript_url=" https://otter.ai/u/meeting-1/transcript ",
        conference_url=" https://meet.google.com/abc-defg-hij?authuser=0 ",
        language=" EN ",
        status=" COMPLETED ",
        start_at="2026-03-10T15:00:00Z",
        end_at="2026-03-10T16:00:00Z",
        speaker_names=[" Robbie Heeger ", "Robbie Heeger"],
        speaker_emails=["ROBBIE@EXAMPLE.COM", "robbie@example.com"],
        participant_names=[" Alice Example ", "Alice Example"],
        participant_emails=["ALICE@example.com", "alice@example.com"],
        host_name=" Robbie Heeger ",
        host_email="ROBBIE@EXAMPLE.COM",
        calendar_events=["[[board-sync]]", " [[board-sync]] "],
        event_id_hint=" event-1 ",
        transcript_body_sha=" abc123 ",
    )
    assert card.summary == "Weekly Product Sync"
    assert card.account_email == "robbie@example.com"
    assert card.language == "en"
    assert card.status == "completed"
    assert card.speaker_names == ["Robbie Heeger"]
    assert card.speaker_emails == ["robbie@example.com"]
    assert card.participant_names == ["Alice Example"]
    assert card.participant_emails == ["alice@example.com"]
    assert card.host_name == "Robbie Heeger"
    assert card.host_email == "robbie@example.com"
    assert card.calendar_events == ["[[board-sync]]"]
    assert card.event_id_hint == "event-1"
    assert card.transcript_body_sha == "abc123"
