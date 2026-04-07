#!/usr/bin/env python3
"""Archive sync CLI for HFA imports."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_repo_dotenv() -> None:
    dotenv_path = os.path.join(_REPO_ROOT, ".env")
    if not os.path.exists(dotenv_path):
        return
    try:
        with open(dotenv_path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export ") :].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key or key in os.environ:
                    continue
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                    value = value[1:-1]
                os.environ[key] = value
    except OSError:
        return


from .adapters.apple_health import AppleHealthAdapter
from .adapters.base import IngestResult
from .adapters.beeper import BeeperAdapter
from .adapters.calendar_events import CalendarEventsAdapter
from .adapters.contacts import ContactsAdapter
from .adapters.copilot_finance import CopilotFinanceAdapter
from .adapters.file_libraries import FileLibrariesAdapter
from .adapters.github_history import GitHubHistoryAdapter
from .adapters.gmail_correspondents import GmailCorrespondentsAdapter
from .adapters.gmail_messages import GmailMessagesAdapter
from .adapters.imessage import IMessageAdapter
from .adapters.linkedin import LinkedInAdapter
from .adapters.medical_records import MedicalRecordsAdapter
from .adapters.notion_people import NotionPeopleAdapter, NotionStaffAdapter
from .adapters.otter_transcripts import OtterTranscriptsAdapter
from .adapters.photos import PhotosAdapter
from .adapters.seed_people import SeedPeopleAdapter


def get_vault_path() -> str:
    return os.environ.get("PPA_PATH", os.path.join(os.path.expanduser("~"), "Archive", "vault"))


def get_default_otter_stage_dir() -> str:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(os.path.expanduser("~"), "Archive", "raw-data", "otter", run_id)


def _print_result(label: str, result: IngestResult) -> None:
    print(
        f"{label}: created={result.created} merged={result.merged} conflicted={result.conflicted} "
        f"skipped={result.skipped} errors={len(result.errors)}"
    )
    if result.skip_details:
        skip_summary = ", ".join(f"{key}={value}" for key, value in sorted(result.skip_details.items()))
        print(f"  skip_details: {skip_summary}")
    for error in result.errors[:20]:
        print(f"  error: {error}")
    if len(result.errors) > 20:
        print(f"  ... and {len(result.errors) - 20} more")


def _run(label: str, adapter, *, vault: str, dry_run: bool = False, **kwargs) -> IngestResult:
    result = adapter.ingest(vault, dry_run=dry_run, **kwargs)
    _print_result(label, result)
    return result


def cmd_contacts(args):
    sources = [item.strip() for item in args.sources.split(",") if item.strip()] if args.sources else None
    _run(
        "contacts",
        ContactsAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        sources=sources,
    )


def cmd_linkedin(args):
    _run(
        "linkedin",
        LinkedInAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        csv_path=args.csv_path,
        workers=args.workers,
        chunk_size=args.chunk_size,
        verbose=args.verbose,
        progress_every=args.progress_every,
    )


def cmd_notion_people(args):
    _run(
        "notion-people",
        NotionPeopleAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        csv_path=args.csv_path,
    )


def cmd_notion_staff(args):
    _run(
        "notion-staff",
        NotionStaffAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        csv_path=args.csv_path,
    )


def cmd_copilot(args):
    _run(
        "copilot-finance",
        CopilotFinanceAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        csv_path=args.csv_path,
    )


def cmd_contacts_import(args):
    totals = IngestResult()
    steps = [
        ("contacts.apple", ContactsAdapter(), {"sources": ["apple", "vcf"]}),
        ("contacts.google", ContactsAdapter(), {"sources": ["google"]}),
        (
            "linkedin",
            LinkedInAdapter(),
            {
                "csv_path": args.linkedin_csv,
                "workers": args.linkedin_workers,
                "chunk_size": args.linkedin_chunk_size,
            },
        ),
        ("notion-people", NotionPeopleAdapter(), {"csv_path": args.notion_csv}),
        ("notion-staff", NotionStaffAdapter(), {"csv_path": args.notion_staff_csv}),
    ]
    for label, adapter, kwargs in steps:
        result = _run(label, adapter, vault=args.vault, dry_run=args.dry_run, **kwargs)
        totals.created += result.created
        totals.merged += result.merged
        totals.conflicted += result.conflicted
        totals.skipped += result.skipped
        totals.errors.extend(result.errors)
    _print_result("contacts-import", totals)


def cmd_seed_people(args):
    _run(
        "seed-people",
        SeedPeopleAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        source_dir=args.source_dir,
    )


def cmd_gmail_correspondents(args):
    _run(
        "gmail-correspondents",
        GmailCorrespondentsAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        account_email=args.account_email,
        query=args.query,
        max_messages=args.max_messages,
    )


def cmd_gmail_messages(args):
    _run(
        "gmail-messages",
        GmailMessagesAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        account_email=args.account_email,
        query=args.query,
        max_threads=args.max_threads,
        max_messages=args.max_messages,
        max_attachments=args.max_attachments,
        page_size=args.page_size,
        workers=args.workers,
        quick_update=args.quick_update,
    )


def cmd_calendar_events(args):
    _run(
        "calendar-events",
        CalendarEventsAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        account_email=args.account_email,
        calendar_id=args.calendar_id,
        query=args.query,
        time_min=args.time_min,
        time_max=args.time_max,
        max_events=args.max_events,
        quick_update=args.quick_update,
    )


def cmd_otter_transcripts(args):
    _run(
        "otter-transcripts",
        OtterTranscriptsAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        account_email=args.account_email,
        max_meetings=args.max_meetings,
        page_size=args.page_size,
        workers=args.workers,
        updated_after=args.updated_after,
        start_after=args.start_after,
        end_before=args.end_before,
        quick_update=args.quick_update,
    )


def cmd_otter_transcripts_stage(args):
    adapter = OtterTranscriptsAdapter()
    stage_dir = args.stage_dir or get_default_otter_stage_dir()
    manifest = adapter.stage_transcripts(
        args.vault,
        stage_dir,
        account_email=args.account_email,
        max_meetings=args.max_meetings,
        page_size=args.page_size,
        workers=args.workers,
        updated_after=args.updated_after,
        start_after=args.start_after,
        end_before=args.end_before,
        quick_update=args.quick_update,
        progress_every=args.progress_every,
        verbose=args.verbose,
    )
    counts = manifest.get("counts", {})
    print(
        "otter-transcripts-stage: "
        f"meetings={counts.get('meetings', 0)} skipped={counts.get('skipped_unchanged_meetings', 0)} "
        f"failed={counts.get('failed_hydrations', 0)} elapsed_s={manifest.get('elapsed_seconds', 0)}"
    )
    print(f"  manifest: {stage_dir}")


def cmd_otter_transcripts_import_stage(args):
    _run(
        "otter-transcripts-import-stage",
        OtterTranscriptsAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        stage_dir=args.stage_dir,
        batch_size=args.batch_size,
        max_items=args.max_items,
        verbose=args.verbose,
        progress_every=args.progress_every,
    )


def cmd_otter_transcripts_relink(args):
    adapter = OtterTranscriptsAdapter()
    stats = adapter.relink_existing(
        args.vault,
        progress_every=args.progress_every,
        verbose=args.verbose,
    )
    print(
        "otter-transcripts-relink: "
        f"rows={stats['rows_scanned']} matches={stats['matches_found']} "
        f"transcripts_updated={stats['transcripts_updated']} "
        f"calendar_events_updated={stats['calendar_events_updated']} "
        f"unmatched={stats['unmatched_rows']}"
    )


def cmd_imessage(args):
    _run(
        "imessage",
        IMessageAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        snapshot_dir=args.snapshot_dir,
        source_label=args.source_label,
        max_messages=args.max_messages,
        workers=args.workers,
    )


def cmd_photos(args):
    _run(
        "photos",
        PhotosAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        library_path=args.library_path,
        source_label=args.source_label,
        max_assets=args.max_assets,
        quick_update=args.quick_update,
        include_private_people=not args.no_private_people,
        include_private_labels=not args.no_private_labels,
    )


def cmd_file_libraries(args):
    _run(
        "file-libraries",
        FileLibrariesAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        roots=args.roots,
        max_files=args.max_files,
        batch_size=args.batch_size,
        quick_update=args.quick_update,
    )


def cmd_file_libraries_stage(args):
    adapter = FileLibrariesAdapter()
    manifest = adapter.stage_documents(
        args.vault,
        args.stage_dir,
        roots=args.roots,
        max_files=args.max_files,
        quick_update=args.quick_update,
        workers=args.workers,
        progress_every=args.progress_every,
        verbose=args.verbose,
    )
    print(
        "file-libraries-stage: "
        f"candidates={manifest['total_candidates']} emitted={manifest['emitted_documents']} "
        f"elapsed_s={manifest['elapsed_seconds']}"
    )
    if manifest.get("inventory_skip_details"):
        inventory_summary = ", ".join(
            f"{key}={value}" for key, value in sorted(manifest["inventory_skip_details"].items())
        )
        print(f"  inventory_skip_details: {inventory_summary}")
    if manifest.get("analysis_skip_details"):
        analysis_summary = ", ".join(
            f"{key}={value}" for key, value in sorted(manifest["analysis_skip_details"].items())
        )
        print(f"  analysis_skip_details: {analysis_summary}")
    print(f"  manifest: {args.stage_dir}")


def cmd_file_libraries_import_stage(args):
    _run(
        "file-libraries-import-stage",
        FileLibrariesAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        stage_dir=args.stage_dir,
        max_files=args.max_files,
        batch_size=args.batch_size,
        verbose=args.verbose,
        progress_every=args.progress_every,
    )


def cmd_github_history_stage(args):
    adapter = GitHubHistoryAdapter()
    manifest = adapter.stage_history(
        args.vault,
        args.stage_dir,
        max_repos=args.max_repos,
        max_commits_per_repo=args.max_commits_per_repo,
        max_threads_per_repo=args.max_threads_per_repo,
        max_messages_per_thread=args.max_messages_per_thread,
        workers=args.workers,
        progress_every=args.progress_every,
        verbose=args.verbose,
    )
    counts = manifest.get("counts", {})
    print(
        "github-history-stage: "
        f"repos={counts.get('repos', 0)} commits={counts.get('commits', 0)} "
        f"threads={counts.get('threads', 0)} messages={counts.get('messages', 0)} "
        f"elapsed_s={manifest.get('elapsed_seconds', 0)}"
    )
    if manifest.get("failures"):
        print(f"  failures: {len(manifest['failures'])}")
    print(f"  manifest: {args.stage_dir}")


def cmd_github_history_import_stage(args):
    _run(
        "github-history-import-stage",
        GitHubHistoryAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        stage_dir=args.stage_dir,
        batch_size=args.batch_size,
        max_items=args.max_items,
        verbose=args.verbose,
        progress_every=args.progress_every,
    )


def cmd_medical_records(args):
    _run(
        "medical-records",
        MedicalRecordsAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        fhir_json_path=args.fhir_json_path,
        ccd_xml_path=args.ccd_xml_path,
        ccd_dir_path=args.ccd_dir_path,
        ehi_tables_dir_path=args.ehi_tables_dir_path,
        vaccine_pdf_path=args.vaccine_pdf_path,
        person_wikilink=args.person_wikilink,
        epic_pat_id=args.epic_pat_id,
        ehi_include_order_results=not args.ehi_no_order_results,
        ehi_include_adt=not args.ehi_no_adt,
        verbose=args.verbose,
        progress_every=args.progress_every,
    )


def cmd_apple_health(args):
    _run(
        "apple-health",
        AppleHealthAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        export_xml_path=args.export_xml_path,
        person_wikilink=args.person_wikilink,
        verbose=args.verbose,
        progress_every=args.progress_every,
    )


def cmd_extract_emails(args):
    import logging

    from archive_mcp.log import configure_logging
    from archive_sync.extractors.registry import build_default_registry
    from archive_sync.extractors.runner import ExtractionRunner

    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    log = logging.getLogger("ppa.archive_sync.extract")
    registry = build_default_registry()
    staging = str(getattr(args, "staging_dir", "") or "").strip() or None
    raw_sender = str(getattr(args, "sender", "") or "").strip()
    sender_f = raw_sender.lower() if raw_sender else None
    vp = float(getattr(args, "limit_vault_percent", 0.0) or 0.0)
    runner = ExtractionRunner(
        vault_path=args.vault,
        registry=registry,
        staging_dir=staging,
        workers=int(getattr(args, "workers", 4)),
        batch_size=int(getattr(args, "batch_size", 500)),
        dry_run=bool(args.dry_run),
        sender_filter=sender_f,
        limit=(int(args.limit) if int(getattr(args, "limit", 0) or 0) > 0 else None),
        progress_every=int(getattr(args, "progress_every", 5000) or 5000),
        vault_percent=(vp if vp > 0 else None),
    )
    metrics = runner.run()
    log.info("extract-emails finished: %s", metrics.to_dict())
    print(json.dumps(metrics.to_dict(), indent=2))
    if bool(getattr(args, "full_report", False)) and staging:
        from archive_mcp.commands.staging import emit_full_staging_report

        emit_full_staging_report(staging)


def cmd_resolve_entities(args):
    from archive_mcp.log import configure_logging
    from archive_sync.extractors.entity_resolution import run_entity_resolution

    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    out = run_entity_resolution(
        args.vault,
        entity_filter=str(getattr(args, "entity_type", "all")),
        dry_run=bool(args.dry_run),
        report_dir=str(getattr(args, "report_dir", "") or "").strip(),
    )
    print(json.dumps(out, indent=2))


def cmd_promote_staging(args):
    import dataclasses
    import json

    from archive_mcp.log import configure_logging
    from archive_sync.extractors.promoter import promote_staging

    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    res = promote_staging(
        vault_path=args.vault,
        staging_dir=str(args.staging_dir),
        dry_run=bool(getattr(args, "dry_run", False)),
    )
    print(json.dumps(dataclasses.asdict(res), indent=2, default=str))


def cmd_beeper(args):
    account_ids = [item.strip() for item in args.account_ids.split(",") if item.strip()] if args.account_ids else None
    thread_types = (
        [item.strip() for item in args.thread_types.split(",") if item.strip()] if args.thread_types else None
    )
    _run(
        "beeper",
        BeeperAdapter(),
        vault=args.vault,
        dry_run=args.dry_run,
        db_path=args.db_path,
        media_root=args.media_root,
        thread_types=thread_types,
        account_ids=account_ids,
        max_threads=args.max_threads,
        batch_size=args.batch_size,
        workers=args.workers,
        verbose=args.verbose,
        progress_every=args.progress_every,
    )


def main():
    _load_repo_dotenv()
    parser = argparse.ArgumentParser(description="Archive sync for family archives")
    parser.add_argument("--vault", default=get_vault_path(), help="Vault path")
    sub = parser.add_subparsers(dest="command", required=True)

    p_contacts = sub.add_parser("contacts")
    p_contacts.add_argument("--sources", default="apple,vcf,google", help="comma list: apple,vcf,google")
    p_contacts.add_argument("--dry-run", action="store_true")
    p_contacts.set_defaults(func=cmd_contacts)

    p_linkedin = sub.add_parser("linkedin")
    p_linkedin.add_argument(
        "--csv-path",
        default=None,
        help="Connections CSV path or LinkedIn export directory",
    )
    p_linkedin.add_argument("--workers", type=int, default=None, help="Parallel LinkedIn match workers")
    p_linkedin.add_argument("--chunk-size", type=int, default=None, help="People resolution chunk size")
    p_linkedin.add_argument("--verbose", action="store_true", help="Print ingest phase and chunk progress")
    p_linkedin.add_argument(
        "--progress-every",
        type=int,
        default=None,
        help="Progress log interval for preload loops",
    )
    p_linkedin.add_argument("--dry-run", action="store_true")
    p_linkedin.set_defaults(func=cmd_linkedin)

    p_notion = sub.add_parser("notion-people")
    p_notion.add_argument("--csv-path", default=None)
    p_notion.add_argument("--dry-run", action="store_true")
    p_notion.set_defaults(func=cmd_notion_people)

    p_notion_staff = sub.add_parser("notion-staff")
    p_notion_staff.add_argument("--csv-path", default=None)
    p_notion_staff.add_argument("--dry-run", action="store_true")
    p_notion_staff.set_defaults(func=cmd_notion_staff)

    p_copilot = sub.add_parser("copilot-finance")
    p_copilot.add_argument("--csv-path", default=None)
    p_copilot.add_argument("--dry-run", action="store_true")
    p_copilot.set_defaults(func=cmd_copilot)

    p_import = sub.add_parser("contacts-import")
    p_import.add_argument("--linkedin-csv", default=None)
    p_import.add_argument("--linkedin-workers", type=int, default=None)
    p_import.add_argument("--linkedin-chunk-size", type=int, default=None)
    p_import.add_argument("--notion-csv", default=None)
    p_import.add_argument("--notion-staff-csv", default=None)
    p_import.add_argument("--dry-run", action="store_true")
    p_import.set_defaults(func=cmd_contacts_import)

    p_seed = sub.add_parser("seed-people")
    p_seed.add_argument(
        "--source-dir",
        default=None,
        help="Directory of canonical local People markdown notes",
    )
    p_seed.add_argument("--dry-run", action="store_true")
    p_seed.set_defaults(func=cmd_seed_people)

    p_gmailc = sub.add_parser("gmail-correspondents")
    p_gmailc.add_argument("--account-email", default="rheeger@gmail.com")
    p_gmailc.add_argument("--query", default=None, help="Optional Gmail search query")
    p_gmailc.add_argument("--max-messages", type=int, default=None, help="Process cap for one run")
    p_gmailc.add_argument("--dry-run", action="store_true")
    p_gmailc.set_defaults(func=cmd_gmail_correspondents)

    p_gmailm = sub.add_parser("gmail-messages")
    p_gmailm.add_argument("--account-email", default="rheeger@gmail.com")
    p_gmailm.add_argument("--query", default=None, help="Optional Gmail search query")
    p_gmailm.add_argument("--max-threads", type=int, default=100, help="Thread card cap for one run")
    p_gmailm.add_argument("--max-messages", type=int, default=100, help="Message card cap for one run")
    p_gmailm.add_argument(
        "--max-attachments",
        type=int,
        default=100,
        help="Attachment card cap for one run",
    )
    p_gmailm.add_argument("--page-size", type=int, default=25, help="Gmail threads page size")
    p_gmailm.add_argument("--workers", type=int, default=32, help="Concurrent Gmail thread fetch workers")
    p_gmailm.add_argument(
        "--quick-update",
        action="store_true",
        help="Skip unchanged Gmail threads and records using cached hashes",
    )
    p_gmailm.add_argument("--dry-run", action="store_true")
    p_gmailm.set_defaults(func=cmd_gmail_messages)

    p_calendar = sub.add_parser("calendar-events")
    p_calendar.add_argument("--account-email", default="rheeger@gmail.com")
    p_calendar.add_argument("--calendar-id", default="primary")
    p_calendar.add_argument("--query", default=None)
    p_calendar.add_argument("--time-min", default=None, help="Optional ISO datetime lower bound")
    p_calendar.add_argument("--time-max", default=None, help="Optional ISO datetime upper bound")
    p_calendar.add_argument("--max-events", type=int, default=100, help="Event card cap for one run")
    p_calendar.add_argument(
        "--quick-update",
        action="store_true",
        help="Skip unchanged calendar events using cached hashes",
    )
    p_calendar.add_argument("--dry-run", action="store_true")
    p_calendar.set_defaults(func=cmd_calendar_events)

    p_otter = sub.add_parser("otter-transcripts")
    p_otter.add_argument(
        "--account-email",
        default="",
        help="Optional account email label for Otter transcript cards",
    )
    p_otter.add_argument("--max-meetings", type=int, default=100, help="Transcript card cap for one run")
    p_otter.add_argument("--page-size", type=int, default=25, help="Otter meetings page size")
    p_otter.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Concurrent Otter meeting hydration workers",
    )
    p_otter.add_argument("--updated-after", default=None, help="Optional API updated-after lower bound")
    p_otter.add_argument("--start-after", default=None, help="Optional meeting start lower bound")
    p_otter.add_argument("--end-before", default=None, help="Optional meeting start upper bound")
    p_otter.add_argument(
        "--quick-update",
        action="store_true",
        help="Skip unchanged meetings using cached Otter update markers",
    )
    p_otter.add_argument("--dry-run", action="store_true")
    p_otter.set_defaults(func=cmd_otter_transcripts)

    p_otter_stage = sub.add_parser("otter-transcripts-stage")
    p_otter_stage.add_argument(
        "--stage-dir",
        default=None,
        help="Directory for staged Otter transcript harvest output",
    )
    p_otter_stage.add_argument(
        "--account-email",
        default="",
        help="Optional account email label for Otter transcript cards",
    )
    p_otter_stage.add_argument("--max-meetings", type=int, default=100, help="Transcript stage cap for one run")
    p_otter_stage.add_argument("--page-size", type=int, default=25, help="Otter meetings page size")
    p_otter_stage.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Concurrent Otter meeting hydration workers",
    )
    p_otter_stage.add_argument("--updated-after", default=None, help="Optional API updated-after lower bound")
    p_otter_stage.add_argument("--start-after", default=None, help="Optional meeting start lower bound")
    p_otter_stage.add_argument("--end-before", default=None, help="Optional meeting start upper bound")
    p_otter_stage.add_argument(
        "--quick-update",
        action="store_true",
        help="Skip unchanged meetings using cached Otter update markers",
    )
    p_otter_stage.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Progress log interval during staged extraction",
    )
    p_otter_stage.add_argument("--verbose", action="store_true", help="Print staged extraction progress")
    p_otter_stage.set_defaults(func=cmd_otter_transcripts_stage)

    p_otter_import = sub.add_parser("otter-transcripts-import-stage")
    p_otter_import.add_argument(
        "--stage-dir",
        required=True,
        help="Directory containing staged Otter transcript harvest output",
    )
    p_otter_import.add_argument("--batch-size", type=int, default=100, help="Records to commit per ingest batch")
    p_otter_import.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Optional cap for staged records to import",
    )
    p_otter_import.add_argument(
        "--progress-every",
        type=int,
        default=None,
        help="Progress log interval for staged import",
    )
    p_otter_import.add_argument("--verbose", action="store_true", help="Print staged import progress")
    p_otter_import.add_argument("--dry-run", action="store_true")
    p_otter_import.set_defaults(func=cmd_otter_transcripts_import_stage)

    p_otter_relink = sub.add_parser("otter-transcripts-relink")
    p_otter_relink.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Progress log interval for transcript relinking",
    )
    p_otter_relink.add_argument("--verbose", action="store_true", help="Print transcript relink progress")
    p_otter_relink.set_defaults(func=cmd_otter_transcripts_relink)

    p_imessage = sub.add_parser("imessage")
    p_imessage.add_argument(
        "--snapshot-dir",
        required=True,
        help="Path to an exported Apple Messages snapshot bundle",
    )
    p_imessage.add_argument(
        "--source-label",
        default="local-messages",
        help="Stable source label for sync-state",
    )
    p_imessage.add_argument("--max-messages", type=int, default=100, help="Message cap for one run")
    p_imessage.add_argument("--workers", type=int, default=4, help="Concurrent row-processing workers")
    p_imessage.add_argument("--dry-run", action="store_true")
    p_imessage.set_defaults(func=cmd_imessage)

    p_photos = sub.add_parser("photos")
    p_photos.add_argument("--library-path", default=None, help="Optional path to a Photos library bundle")
    p_photos.add_argument(
        "--source-label",
        default="apple-photos",
        help="Stable source label for sync-state and UIDs",
    )
    p_photos.add_argument("--max-assets", type=int, default=None, help="Asset card cap for one run")
    p_photos.add_argument(
        "--quick-update",
        action="store_true",
        help="Skip unchanged assets using cached metadata hashes",
    )
    p_photos.add_argument(
        "--no-private-people",
        action="store_true",
        help="Disable private people label extraction",
    )
    p_photos.add_argument(
        "--no-private-labels",
        action="store_true",
        help="Disable private ML label extraction",
    )
    p_photos.add_argument("--dry-run", action="store_true")
    p_photos.set_defaults(func=cmd_photos)

    p_files = sub.add_parser("file-libraries")
    p_files.add_argument(
        "--roots",
        default="documents,gdrive.personal,gdrive.endaoment,gdrive.gtt,downloads",
        help="comma list of root labels or absolute paths",
    )
    p_files.add_argument("--max-files", type=int, default=None, help="Document card cap for one run")
    p_files.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Documents to commit per ingest batch",
    )
    p_files.add_argument(
        "--quick-update",
        action="store_true",
        help="Skip unchanged document files using cached metadata hashes",
    )
    p_files.add_argument("--dry-run", action="store_true")
    p_files.set_defaults(func=cmd_file_libraries)

    p_files_stage = sub.add_parser("file-libraries-stage")
    p_files_stage.add_argument(
        "--roots",
        default="documents,gdrive.personal,gdrive.endaoment,gdrive.gtt,downloads",
        help="comma list of root labels or absolute paths",
    )
    p_files_stage.add_argument("--stage-dir", required=True, help="Directory for staged analysis output")
    p_files_stage.add_argument("--max-files", type=int, default=None, help="Candidate cap for one stage run")
    p_files_stage.add_argument(
        "--quick-update",
        action="store_true",
        help="Skip unchanged documents during staged analysis",
    )
    p_files_stage.add_argument("--workers", type=int, default=None, help="Parallel analysis workers")
    p_files_stage.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Progress log interval during staged analysis",
    )
    p_files_stage.add_argument("--verbose", action="store_true", help="Print staged analysis progress and ETA")
    p_files_stage.set_defaults(func=cmd_file_libraries_stage)

    p_files_import = sub.add_parser("file-libraries-import-stage")
    p_files_import.add_argument("--stage-dir", required=True, help="Directory containing staged analysis output")
    p_files_import.add_argument("--max-files", type=int, default=None, help="Document cap for one import run")
    p_files_import.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Documents to commit per ingest batch",
    )
    p_files_import.add_argument(
        "--progress-every",
        type=int,
        default=None,
        help="Progress log interval for staged import",
    )
    p_files_import.add_argument("--verbose", action="store_true", help="Print staged import progress")
    p_files_import.add_argument("--dry-run", action="store_true")
    p_files_import.set_defaults(func=cmd_file_libraries_import_stage)

    p_github_stage = sub.add_parser("github-history-stage")
    p_github_stage.add_argument(
        "--stage-dir",
        required=True,
        help="Directory for staged GitHub extraction output",
    )
    p_github_stage.add_argument("--max-repos", type=int, default=None, help="Repository cap for one stage run")
    p_github_stage.add_argument(
        "--max-commits-per-repo",
        type=int,
        default=None,
        help="Commit cap per repository",
    )
    p_github_stage.add_argument(
        "--max-threads-per-repo",
        type=int,
        default=None,
        help="Issue/PR thread cap per repository",
    )
    p_github_stage.add_argument(
        "--max-messages-per-thread",
        type=int,
        default=None,
        help="Comment/review cap per thread",
    )
    p_github_stage.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Parallel GitHub repo extraction workers",
    )
    p_github_stage.add_argument(
        "--progress-every",
        type=int,
        default=10,
        help="Progress log interval during staged extraction",
    )
    p_github_stage.add_argument("--verbose", action="store_true", help="Print staged extraction progress")
    p_github_stage.set_defaults(func=cmd_github_history_stage)

    p_github_import = sub.add_parser("github-history-import-stage")
    p_github_import.add_argument(
        "--stage-dir",
        required=True,
        help="Directory containing staged GitHub extraction output",
    )
    p_github_import.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Records to commit per ingest batch",
    )
    p_github_import.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Optional cap for staged records to import",
    )
    p_github_import.add_argument(
        "--progress-every",
        type=int,
        default=None,
        help="Progress log interval for staged import",
    )
    p_github_import.add_argument("--verbose", action="store_true", help="Print staged import progress")
    p_github_import.add_argument("--dry-run", action="store_true")
    p_github_import.set_defaults(func=cmd_github_history_import_stage)

    p_medical = sub.add_parser("medical-records")
    p_medical.add_argument("--fhir-json-path", default=None, help="Path to FHIR export JSON")
    p_medical.add_argument("--ccd-xml-path", default=None, help="Path to One Medical CCD XML export")
    p_medical.add_argument(
        "--ccd-dir-path",
        default=None,
        help="Path to a directory of CCD XML exports, including Epic exports",
    )
    p_medical.add_argument(
        "--ehi-tables-dir-path",
        default=None,
        help="Path to an Epic EHI tables directory of TSV exports",
    )
    p_medical.add_argument(
        "--epic-pat-id",
        dest="epic_pat_id",
        default=None,
        help="PAT_ID anchor when PATIENT.tsv lists more than one patient",
    )
    p_medical.add_argument(
        "--ehi-no-order-results",
        action="store_true",
        help="Skip ORDER_RESULTS -> observation cards (large exports)",
    )
    p_medical.add_argument(
        "--ehi-no-adt",
        action="store_true",
        help="Skip CLARITY_ADT timeline cards",
    )
    p_medical.add_argument(
        "--vaccine-pdf-path",
        default=None,
        help="Path to supplemental vaccine record PDF",
    )
    p_medical.add_argument(
        "--person-wikilink",
        default=None,
        help="Explicit person wikilink or slug for imported records; required for CCD-only imports",
    )
    p_medical.add_argument("--verbose", action="store_true", help="Print import step and progress logging")
    p_medical.add_argument(
        "--progress-every",
        type=int,
        default=None,
        help="Progress log interval for large structured imports",
    )
    p_medical.add_argument("--dry-run", action="store_true")
    p_medical.set_defaults(func=cmd_medical_records)

    p_apple_health = sub.add_parser("apple-health")
    p_apple_health.add_argument("--export-xml-path", required=True, help="Path to Apple Health export XML")
    p_apple_health.add_argument(
        "--person-wikilink",
        required=True,
        help="Explicit person wikilink or slug for imported records",
    )
    p_apple_health.add_argument("--verbose", action="store_true", help="Print import step and progress logging")
    p_apple_health.add_argument(
        "--progress-every",
        type=int,
        default=None,
        help="Progress log interval for large Apple Health imports",
    )
    p_apple_health.add_argument("--dry-run", action="store_true")
    p_apple_health.set_defaults(func=cmd_apple_health)

    p_extract = sub.add_parser("extract-emails", help="Extract derived cards from email_message bodies")
    p_extract.add_argument("--sender", default="", help="Filter to single extractor id (e.g. doordash)")
    p_extract.add_argument("--dry-run", action="store_true")
    p_extract.add_argument("--limit", type=int, default=0, help="Max matched emails (0 = no cap)")
    p_extract.add_argument("--staging-dir", default="", help="Write to staging directory instead of vault")
    p_extract.add_argument("--workers", type=int, default=4)
    p_extract.add_argument("--batch-size", type=int, default=500)
    p_extract.add_argument(
        "--full-report",
        action="store_true",
        help="After extraction, log staging summary to stderr (requires --staging-dir)",
    )
    p_extract.add_argument(
        "--limit-vault-percent",
        type=float,
        default=0.0,
        help="Deterministic sample: process ~N%% of email_message cards (0 = full vault)",
    )
    p_extract.add_argument("--progress-every", type=int, default=5000, help="Progress log interval")
    p_extract.add_argument("--verbose", action="store_true")
    p_extract.set_defaults(func=cmd_extract_emails)

    p_resolve_ent = sub.add_parser("resolve-entities", help="Place/Org/Person resolution from derived cards")
    p_resolve_ent.add_argument("--dry-run", action="store_true")
    p_resolve_ent.add_argument(
        "--type",
        dest="entity_type",
        choices=["place", "org", "person", "all"],
        default="all",
    )
    p_resolve_ent.add_argument(
        "--report-dir",
        default="",
        help="Write entity-resolution-report.json and entity-resolution-spot-check.md here",
    )
    p_resolve_ent.add_argument("--verbose", action="store_true")
    p_resolve_ent.set_defaults(func=cmd_resolve_entities)

    p_promote = sub.add_parser("promote-staging", help="Promote staged derived cards into the vault")
    p_promote.add_argument("--staging-dir", required=True)
    p_promote.add_argument("--dry-run", action="store_true")
    p_promote.add_argument("--verbose", action="store_true")
    p_promote.set_defaults(func=cmd_promote_staging)

    p_beeper = sub.add_parser("beeper")
    p_beeper.add_argument("--db-path", default=None, help="Optional path to BeeperTexts index.db")
    p_beeper.add_argument(
        "--media-root",
        default=None,
        help="Optional path to the BeeperTexts media cache root",
    )
    p_beeper.add_argument(
        "--thread-types",
        default="single",
        help="comma list of Beeper thread types, e.g. single,group",
    )
    p_beeper.add_argument(
        "--account-ids",
        default=None,
        help="optional comma list of Beeper account ids to include",
    )
    p_beeper.add_argument("--max-threads", type=int, default=None, help="Thread cap for one run")
    p_beeper.add_argument("--batch-size", type=int, default=10, help="Threads to commit per ingest batch")
    p_beeper.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Concurrent Beeper thread hydration workers",
    )
    p_beeper.add_argument("--verbose", action="store_true", help="Print Beeper fetch and ingest progress")
    p_beeper.add_argument(
        "--progress-every",
        type=int,
        default=None,
        help="Progress log interval for Beeper thread counts",
    )
    p_beeper.add_argument("--dry-run", action="store_true")
    p_beeper.set_defaults(func=cmd_beeper)

    args = parser.parse_args()
    os.environ["PPA_PATH"] = args.vault
    args.func(args)


if __name__ == "__main__":
    main()
