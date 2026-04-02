"""Shared ingest pipeline for HFA archive adapters."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from archive_mcp.vault_cache import VaultScanCache
from hfa.config import PPAConfig, load_config
from hfa.identity import IdentityCache
from hfa.identity_resolver import (
    PersonIndex,
    ResolveResult,
    load_nicknames,
    log_conflict,
    merge_into_existing,
    resolve_person,
    resolve_person_snapshot,
)
from hfa.provenance import PROVENANCE_EXEMPT_FIELDS, ProvenanceEntry, merge_provenance
from hfa.schema import BaseCard, PersonCard, card_to_frontmatter, validate_card_permissive, validate_card_strict
from hfa.slugger import normalize_for_slug, unique_slug
from hfa.sync_state import load_sync_state, update_cursor
from hfa.vault import read_note, write_card


def deterministic_provenance(
    card: BaseCard,
    source: str,
    *,
    field_sources: dict[str, str] | None = None,
) -> dict[str, ProvenanceEntry]:
    """Create deterministic provenance entries for all non-empty card fields."""

    field_sources = field_sources or {}
    today = date.today().isoformat()
    frontmatter = card_to_frontmatter(card)
    provenance: dict[str, ProvenanceEntry] = {}
    for field_name, value in frontmatter.items():
        if field_name in PROVENANCE_EXEMPT_FIELDS or value in ("", [], None, 0):
            continue
        provenance[field_name] = ProvenanceEntry(
            source=field_sources.get(field_name, source),
            date=today,
            method="deterministic",
        )
    return provenance


@dataclass
class IngestResult:
    created: int = 0
    merged: int = 0
    conflicted: int = 0
    skipped: int = 0
    skip_details: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass
class FetchedBatch:
    items: list[dict[str, Any]]
    cursor_patch: dict[str, Any] = field(default_factory=dict)
    sequence: int = 0
    skipped_count: int = 0
    skip_details: dict[str, int] = field(default_factory=dict)


@dataclass
class PreparedIngestItem:
    raw_item: dict[str, Any]
    item_index: int
    card: BaseCard
    provenance: dict[str, ProvenanceEntry]
    body: str


class BaseAdapter(ABC):
    source_id: str = "unknown"
    preload_existing_uid_index: bool = True
    enable_person_resolution: bool = True
    parallel_person_matching: bool = False
    parallel_person_match_default_workers: int = 1
    parallel_person_match_default_chunk_size: int = 128

    def should_enable_person_resolution(self, **kwargs) -> bool:
        return bool(self.enable_person_resolution)

    @abstractmethod
    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config: PPAConfig | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch raw source items without touching the vault."""

    @abstractmethod
    def to_card(self, item: dict[str, Any]) -> tuple[BaseCard, dict[str, ProvenanceEntry], str]:
        """Convert one raw item into a validated card, provenance, and body."""

    def fetch_batches(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config: PPAConfig | None = None,
        **kwargs,
    ) -> Iterable[FetchedBatch]:
        """Yield fetched items in committed batches.

        Adapters can override this to stream upstream work incrementally while the
        shared ingest path remains the sole owner of merge/write/checkpoint logic.
        The default behavior preserves the original fetch-all contract.
        """

        yield FetchedBatch(items=self.fetch(vault_path, cursor, config=config, **kwargs), sequence=0)

    def get_cursor_key(self, **kwargs) -> str:
        return self.source_id

    def adapter_spec(self):
        """Return the explicit contract for this adapter."""

        from adapter_contracts import get_adapter_spec

        return get_adapter_spec(self.source_id)

    def cursor_checkpoint(
        self,
        item: dict[str, Any],
        *,
        card: BaseCard | None = None,
        index: int = -1,
        processed_successfully: int = 0,
        result: IngestResult | None = None,
        **kwargs,
    ) -> dict[str, Any] | None:
        if not isinstance(item, dict):
            return None
        checkpoint = item.get("_cursor")
        return checkpoint if isinstance(checkpoint, dict) else None

    def finalize_cursor(self, cursor: dict[str, Any], **kwargs) -> dict[str, Any] | None:
        return None

    def person_match_workers(self, **kwargs) -> int:
        if not self.parallel_person_matching:
            return 1
        raw_value = kwargs.get("workers") or os.environ.get("HFA_PERSON_MATCH_WORKERS")
        if raw_value in (None, ""):
            return max(1, min(self.parallel_person_match_default_workers, os.cpu_count() or 1))
        try:
            return max(1, int(raw_value))
        except (TypeError, ValueError):
            return max(1, min(self.parallel_person_match_default_workers, os.cpu_count() or 1))

    def person_match_chunk_size(self, **kwargs) -> int:
        raw_value = kwargs.get("chunk_size") or os.environ.get("HFA_PERSON_MATCH_CHUNK_SIZE")
        if raw_value in (None, ""):
            return max(1, int(self.parallel_person_match_default_chunk_size))
        try:
            return max(1, int(raw_value))
        except (TypeError, ValueError):
            return max(1, int(self.parallel_person_match_default_chunk_size))

    def _item_log_label(self, card: BaseCard) -> str:
        summary = getattr(card, "summary", "") or card.source_id or card.uid
        text = " ".join(str(summary).split())
        if len(text) > 80:
            text = text[:77].rstrip() + "..."
        return f"uid={card.uid} source_id={card.source_id} summary={text}"

    def ingest_verbose(self, **kwargs) -> bool:
        raw_value = kwargs.get("verbose")
        if raw_value is None:
            raw_value = os.environ.get("HFA_IMPORT_VERBOSE")
        if isinstance(raw_value, bool):
            return raw_value
        return str(raw_value).strip().lower() in {"1", "true", "yes", "on"}

    def ingest_progress_every(self, **kwargs) -> int:
        raw_value = kwargs.get("progress_every")
        if raw_value in (None, ""):
            raw_value = os.environ.get("HFA_IMPORT_PROGRESS_EVERY")
        if raw_value in (None, ""):
            return 1000
        try:
            return max(1, int(raw_value))
        except (TypeError, ValueError):
            return 1000

    def _person_identifiers(self, card: PersonCard) -> dict[str, Any]:
        return {
            "summary": card.summary,
            "name": card.summary,
            "aliases": card.aliases,
            "first_name": card.first_name,
            "last_name": card.last_name,
            "emails": card.emails,
            "phones": card.phones,
            "company": card.company,
            "companies": card.companies,
            "title": card.title,
            "titles": card.titles,
            "github": card.github,
            "linkedin": card.linkedin,
            "linkedin_url": card.linkedin_url,
            "twitter": card.twitter,
            "instagram": card.instagram,
            "telegram": card.telegram,
            "discord": card.discord,
        }

    def _person_identity_aliases(self, card: PersonCard) -> dict[str, Any]:
        return {
            "name": [card.summary, *card.aliases],
            "emails": card.emails,
            "phones": card.phones,
            "github": card.github,
            "linkedin": card.linkedin,
            "twitter": card.twitter,
            "instagram": card.instagram,
            "telegram": card.telegram,
            "discord": card.discord,
        }

    def _person_rel_path(self, vault_path: str | Path, card: PersonCard) -> str:
        base_slug = normalize_for_slug(card.summary)
        slug = unique_slug(vault_path, base_slug, card.source_id)
        return f"People/{slug}.md"

    def _card_rel_path(self, vault_path: str | Path, card: BaseCard) -> str:
        if isinstance(card, PersonCard):
            return self._person_rel_path(vault_path, card)
        if card.type == "media_asset":
            month_source = getattr(card, "captured_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"Photos/{year_month}/{card.uid}.md"
        if card.type == "document":
            month_source = getattr(card, "document_date", "") or getattr(card, "file_modified_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"Documents/{year_month}/{card.uid}.md"
        if card.type == "medical_record":
            month_source = getattr(card, "occurred_at", "") or getattr(card, "recorded_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"Medical/{year_month}/{card.uid}.md"
        if card.type == "vaccination":
            year_source = getattr(card, "occurred_at", "") or card.created
            year = year_source[:4] if len(year_source) >= 4 else card.created[:4]
            return f"Vaccinations/{year}/{card.uid}.md"
        if card.type == "finance":
            year_month = card.created[:7]
            return f"Finance/{year_month}/{card.uid}.md"
        if card.type == "email_thread":
            month_source = getattr(card, "last_message_at", "") or getattr(card, "first_message_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"EmailThreads/{year_month}/{card.uid}.md"
        if card.type == "email_message":
            month_source = getattr(card, "sent_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"Email/{year_month}/{card.uid}.md"
        if card.type == "email_attachment":
            month_source = getattr(card, "created", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"EmailAttachments/{year_month}/{card.uid}.md"
        if card.type == "imessage_thread":
            month_source = getattr(card, "last_message_at", "") or getattr(card, "first_message_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"IMessageThreads/{year_month}/{card.uid}.md"
        if card.type == "imessage_message":
            month_source = getattr(card, "sent_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"IMessage/{year_month}/{card.uid}.md"
        if card.type == "imessage_attachment":
            month_source = getattr(card, "created", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"IMessageAttachments/{year_month}/{card.uid}.md"
        if card.type == "beeper_thread":
            month_source = getattr(card, "first_message_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"BeeperThreads/{year_month}/{card.uid}.md"
        if card.type == "beeper_message":
            month_source = getattr(card, "sent_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"Beeper/{year_month}/{card.uid}.md"
        if card.type == "beeper_attachment":
            month_source = getattr(card, "created", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"BeeperAttachments/{year_month}/{card.uid}.md"
        if card.type == "calendar_event":
            month_source = getattr(card, "start_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"Calendar/{year_month}/{card.uid}.md"
        if card.type == "meeting_transcript":
            month_source = getattr(card, "start_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"MeetingTranscripts/{year_month}/{card.uid}.md"
        if card.type == "git_repository":
            owner_login = normalize_for_slug(getattr(card, "owner_login", "") or "unknown-owner")
            return f"GitRepos/{owner_login}/{card.uid}.md"
        if card.type == "git_commit":
            month_source = getattr(card, "committed_at", "") or getattr(card, "authored_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"GitCommits/{year_month}/{card.uid}.md"
        if card.type == "git_thread":
            owner_login = normalize_for_slug(
                (getattr(card, "repository_name_with_owner", "") or card.source_id).split("/", 1)[0] or "unknown-owner"
            )
            repo_name = normalize_for_slug(
                (getattr(card, "repository_name_with_owner", "") or card.source_id).split("/", 1)[-1].split("#", 1)[0]
                or card.uid
            )
            return f"GitThreads/{owner_login}/{repo_name}/{card.uid}.md"
        if card.type == "git_message":
            month_source = getattr(card, "sent_at", "") or getattr(card, "updated_at", "") or card.created
            year_month = month_source[:7] if len(month_source) >= 7 else card.created[:7]
            return f"GitMessages/{year_month}/{card.uid}.md"
        return f"{card.type.title()}/{card.uid}.md"

    def _merge_generic_card(
        self,
        vault_path: str | Path,
        rel_path: Path,
        card: BaseCard,
        body: str,
        provenance: dict[str, ProvenanceEntry],
    ) -> None:
        frontmatter, existing_body, existing_provenance = read_note(vault_path, str(rel_path))
        existing_card = validate_card_permissive(frontmatter)
        merged_data = existing_card.model_dump(mode="python")
        incoming_data = card.model_dump(mode="python")
        changed = False

        for field_name, incoming_value in incoming_data.items():
            if field_name not in merged_data or field_name == "updated":
                continue
            existing_value = merged_data[field_name]
            if isinstance(existing_value, list) and isinstance(incoming_value, list):
                merged: list[Any] = []
                seen: set[Any] = set()
                for value in [*existing_value, *incoming_value]:
                    if value in seen:
                        continue
                    seen.add(value)
                    merged.append(value)
                if merged != existing_value:
                    merged_data[field_name] = merged
                    changed = True
            elif existing_value in ("", [], None, 0) and incoming_value not in ("", [], None, 0):
                merged_data[field_name] = incoming_value
                changed = True

        merged_body = body or existing_body
        if merged_body != existing_body:
            changed = True
        if changed:
            merged_data["updated"] = date.today().isoformat()

        merged_card = validate_card_strict(merged_data)
        merged_provenance = merge_provenance(existing_provenance, provenance)
        write_card(vault_path, str(rel_path), merged_card, body=merged_body, provenance=merged_provenance)

    def _replace_generic_card(
        self,
        vault_path: str | Path,
        rel_path: Path,
        card: BaseCard,
        body: str,
        provenance: dict[str, ProvenanceEntry],
    ) -> None:
        frontmatter, existing_body, existing_provenance = read_note(vault_path, str(rel_path))
        existing_card = validate_card_permissive(frontmatter)
        merged_data = existing_card.model_dump(mode="python")
        incoming_data = card.model_dump(mode="python")
        changed = False

        for field_name, incoming_value in incoming_data.items():
            if field_name in {"uid", "type", "source_id", "created"}:
                continue
            if field_name == "source":
                merged_source: list[Any] = []
                seen: set[Any] = set()
                existing_value = merged_data.get(field_name, [])
                for value in [*(existing_value if isinstance(existing_value, list) else []), *incoming_value]:
                    if value in seen:
                        continue
                    seen.add(value)
                    merged_source.append(value)
                if merged_source != merged_data.get(field_name):
                    merged_data[field_name] = merged_source
                    changed = True
                continue
            existing_entry = existing_provenance.get(field_name)
            if (
                incoming_value in ("", [], None, 0, False)
                and existing_entry is not None
                and existing_entry.method == "llm"
            ):
                continue
            if merged_data.get(field_name) != incoming_value:
                merged_data[field_name] = incoming_value
                changed = True

        merged_body = body or existing_body
        if merged_body != existing_body:
            changed = True
        if changed:
            merged_data["updated"] = date.today().isoformat()

        merged_card = validate_card_strict(merged_data)
        merged_provenance = merge_provenance(existing_provenance, provenance)
        write_card(vault_path, str(rel_path), merged_card, body=merged_body, provenance=merged_provenance)

    def merge_card(
        self,
        vault_path: str | Path,
        rel_path: Path,
        card: BaseCard,
        body: str,
        provenance: dict[str, ProvenanceEntry],
    ) -> None:
        self._merge_generic_card(vault_path, rel_path, card, body, provenance)

    def after_card_write(
        self,
        vault_path: str | Path,
        card: BaseCard,
        rel_path: Path,
        *,
        raw_item: dict[str, Any],
        action: str,
        **kwargs,
    ) -> None:
        """Optional post-write hook for adapter-specific enrichment side effects."""

        return None

    def ingest(self, vault_path: str, dry_run: bool = False, **kwargs) -> IngestResult:
        """Run the standard fetch -> to_card -> resolve -> write pipeline."""

        vault = Path(vault_path)
        result = IngestResult()
        verbose = self.ingest_verbose(**kwargs)
        progress_every = self.ingest_progress_every(**kwargs)

        def _log(message: str) -> None:
            if not verbose:
                return
            timestamp = datetime.now().isoformat(timespec="seconds")
            print(f"[{timestamp}] {self.source_id}: {message}", flush=True)

        def _run_logged(label: str, func):
            _log(f"{label} start")
            started_at = perf_counter()
            try:
                value = func()
            except Exception as exc:
                _log(f"{label} error: {exc} elapsed_s={perf_counter() - started_at:.2f}")
                raise
            _log(f"{label} done: elapsed_s={perf_counter() - started_at:.2f}")
            return value

        ingest_started_at = perf_counter()
        config = _run_logged("load config", lambda: load_config(vault))
        cursor_key = _run_logged("resolve cursor key", lambda: self.get_cursor_key(**kwargs))
        cursor = _run_logged("load sync state", lambda: load_sync_state(vault).get(cursor_key, {}))
        if not isinstance(cursor, dict):
            cursor = {}
        enable_person_resolution = bool(self.should_enable_person_resolution(**kwargs))
        identity_cache = (
            _run_logged("load identity cache", lambda: IdentityCache(vault)) if enable_person_resolution else None
        )
        _log("ingest start")
        preload_started_at = perf_counter()
        if enable_person_resolution:
            people_index = _run_logged(
                "load person index", lambda: PersonIndex(vault, log=_log, progress_every=progress_every)
            )
            person_uid_index = _run_logged(
                "build person uid index",
                lambda: {
                    str(data.get("uid")): wikilink
                    for wikilink, data in people_index.records.items()
                    if str(data.get("uid", "")).strip()
                },
            )
        else:
            people_index = None
            person_uid_index = {}
            _log("person resolution preload skipped")
        existing_uid_index: dict[str, Path] = {}
        if self.preload_existing_uid_index:
            _log("existing uid preload start")
            existing_uid_started_at = perf_counter()
            uid_cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=progress_every or 0)
            existing_uid_index = {uid: Path(rp) for uid, rp in uid_cache.uid_to_rel_path().items()}
            loaded_uid_count = len(existing_uid_index)
            _log(
                f"existing uid preload done: loaded={loaded_uid_count} elapsed_s={perf_counter() - existing_uid_started_at:.2f}"
            )
        else:
            _log("existing uid preload skipped")
        nicknames = _run_logged("load nicknames", lambda: load_nicknames(vault)) if enable_person_resolution else {}
        person_match_workers = (
            _run_logged("resolve person match workers", lambda: self.person_match_workers(**kwargs))
            if enable_person_resolution
            else 1
        )
        person_match_chunk_size = _run_logged(
            "resolve person match chunk size",
            lambda: self.person_match_chunk_size(**kwargs),
        )
        _log(
            "preload done: "
            f"people={len(people_index.records) if people_index is not None else 0} "
            f"identity_aliases={len(identity_cache.entries) if identity_cache is not None else 0} "
            f"existing_uids={len(existing_uid_index)} "
            f"workers={person_match_workers} "
            f"chunk_size={person_match_chunk_size} "
            f"elapsed_s={perf_counter() - preload_started_at:.2f}"
        )
        processed_successfully = 0
        seen_items = 0
        global_index = 0

        def _write_progress() -> None:
            if dry_run:
                return
            _log(
                f"write progress start: processed={processed_successfully} created={result.created} "
                f"merged={result.merged} conflicted={result.conflicted} errors={len(result.errors)}"
            )
            started_at = perf_counter()
            update_cursor(
                vault,
                cursor_key,
                {
                    **cursor,
                    "last_sync": datetime.now().isoformat(),
                    "seen": seen_items,
                    "processed": processed_successfully,
                    "last_processed_index": max(processed_successfully - 1, -1),
                    "created": result.created,
                    "merged": result.merged,
                    "conflicted": result.conflicted,
                    "skipped": result.skipped,
                    "skip_details": dict(sorted(result.skip_details.items())),
                    "errors": len(result.errors),
                },
            )
            _log(f"write progress done: elapsed_s={perf_counter() - started_at:.2f}")

        def _checkpoint(item: dict[str, Any], card: BaseCard | None, index: int) -> None:
            if dry_run:
                return
            checkpoint = self.cursor_checkpoint(
                item,
                card=card,
                index=index,
                processed_successfully=processed_successfully,
                result=result,
                **kwargs,
            )
            if checkpoint:
                cursor.update(checkpoint)
                _write_progress()

        def _refresh_person_indexes(wikilink: str, rel_path: Path, delta_people_index: PersonIndex) -> None:
            assert people_index is not None
            started_at = perf_counter()
            _log(f"refresh person indexes start: wikilink={wikilink} rel_path={rel_path}")
            frontmatter, _, _ = read_note(vault, str(rel_path))
            person_data = validate_card_permissive(frontmatter).model_dump(mode="python")
            people_index.upsert(wikilink, person_data)
            delta_people_index.upsert(wikilink, person_data)
            person_uid = str(person_data.get("uid", "")).strip()
            if person_uid:
                person_uid_index[person_uid] = wikilink
            _log(f"refresh person indexes done: wikilink={wikilink} elapsed_s={perf_counter() - started_at:.2f}")

        def _process_nonperson(prepared: PreparedIngestItem) -> str:
            nonlocal processed_successfully
            card = prepared.card
            existing_rel_path = existing_uid_index.get(card.uid)
            if existing_rel_path is None and not self.preload_existing_uid_index:
                candidate_rel_path = Path(self._card_rel_path(vault, card))
                if (vault / candidate_rel_path).exists():
                    existing_rel_path = candidate_rel_path
            if existing_rel_path is not None:
                if not dry_run:
                    self.merge_card(vault, existing_rel_path, card, prepared.body, prepared.provenance)
                    self.after_card_write(
                        vault,
                        card,
                        existing_rel_path,
                        raw_item=prepared.raw_item,
                        action="merge",
                        **kwargs,
                    )
                result.merged += 1
                processed_successfully += 1
                existing_uid_index[card.uid] = existing_rel_path
                _checkpoint(prepared.raw_item, card, prepared.item_index)
                return "merge-existing-uid"

            rel_path = self._card_rel_path(vault, card)
            if not dry_run:
                write_card(vault, rel_path, card, body=prepared.body, provenance=prepared.provenance)
                self.after_card_write(
                    vault,
                    card,
                    Path(rel_path),
                    raw_item=prepared.raw_item,
                    action="create",
                    **kwargs,
                )
            result.created += 1
            processed_successfully += 1
            existing_uid_index[card.uid] = Path(rel_path)
            _checkpoint(prepared.raw_item, card, prepared.item_index)
            return "create"

        def _commit_person(
            prepared: PreparedIngestItem,
            resolve_hint: ResolveResult | None,
            delta_people_index: PersonIndex,
        ) -> str:
            nonlocal processed_successfully
            card = prepared.card
            assert isinstance(card, PersonCard)
            assert identity_cache is not None
            assert people_index is not None
            wikilink = person_uid_index.get(card.uid)
            if wikilink is not None:
                if not dry_run:
                    merged_path = merge_into_existing(
                        vault, wikilink, card.model_dump(mode="python"), prepared.provenance, prepared.body
                    )
                    identity_cache.upsert(wikilink, self._person_identity_aliases(card))
                    if merged_path is not None:
                        _refresh_person_indexes(wikilink, Path(merged_path).relative_to(vault), delta_people_index)
                    person_uid_index[card.uid] = wikilink
                result.merged += 1
                processed_successfully += 1
                _checkpoint(prepared.raw_item, card, prepared.item_index)
                return "merge-existing-uid"

            resolve_result = resolve_hint
            if resolve_result is None:
                resolve_result = resolve_person(
                    vault,
                    self._person_identifiers(card),
                    cache=identity_cache,
                    people_index=people_index,
                    nicknames=nicknames,
                    config=config,
                )
            elif resolve_result.action == "create":
                delta_result = resolve_person(
                    vault,
                    self._person_identifiers(card),
                    cache=identity_cache,
                    people_index=delta_people_index if delta_people_index.records else None,
                    nicknames=nicknames,
                    config=config,
                )
                if delta_result.action != "create":
                    resolve_result = delta_result

            if resolve_result.action == "merge" and resolve_result.wikilink:
                if not dry_run:
                    merged_path = merge_into_existing(
                        vault,
                        resolve_result.wikilink,
                        card.model_dump(mode="python"),
                        prepared.provenance,
                        prepared.body,
                    )
                    identity_cache.upsert(resolve_result.wikilink, self._person_identity_aliases(card))
                    if merged_path is not None:
                        _refresh_person_indexes(
                            resolve_result.wikilink,
                            Path(merged_path).relative_to(vault),
                            delta_people_index,
                        )
                    person_uid_index[card.uid] = resolve_result.wikilink
                result.merged += 1
                processed_successfully += 1
                _checkpoint(prepared.raw_item, card, prepared.item_index)
                return f"merge-resolved:{resolve_result.wikilink}"

            if resolve_result.action == "conflict" and resolve_result.wikilink:
                if not dry_run:
                    log_conflict(
                        vault,
                        card.model_dump(mode="python"),
                        resolve_result.wikilink,
                        resolve_result.confidence,
                        resolve_result.reasons,
                    )
                result.conflicted += 1
                processed_successfully += 1
                _checkpoint(prepared.raw_item, card, prepared.item_index)
                return f"conflict:{resolve_result.wikilink}"

            rel_path = self._person_rel_path(vault, card)
            if not dry_run:
                write_card(vault, rel_path, card, body=prepared.body, provenance=prepared.provenance)
                wikilink = f"[[{Path(rel_path).stem}]]"
                identity_cache.upsert(wikilink, self._person_identity_aliases(card))
                people_index.upsert(wikilink, card.model_dump(mode="python"))
                delta_people_index.upsert(wikilink, card.model_dump(mode="python"))
                person_uid_index[card.uid] = wikilink
            result.created += 1
            processed_successfully += 1
            _checkpoint(prepared.raw_item, card, prepared.item_index)
            return "create"

        _log("fetch batches iterator start")
        for batch_index, raw_batch in enumerate(self.fetch_batches(str(vault), cursor, config=config, **kwargs)):
            materialize_started_at = perf_counter()
            _log(f"batch {batch_index} materialize start")
            batch = (
                raw_batch
                if isinstance(raw_batch, FetchedBatch)
                else FetchedBatch(items=list(raw_batch), sequence=batch_index)
            )
            batch_items = list(batch.items)
            _log(f"batch {batch_index} materialize done: elapsed_s={perf_counter() - materialize_started_at:.2f}")
            _log(f"batch {batch.sequence} start: items={len(batch_items)} skipped={batch.skipped_count}")
            seen_items += len(batch_items)
            batch_error_count = 0
            result.skipped += int(batch.skipped_count or 0)
            for key, value in dict(batch.skip_details).items():
                result.skip_details[key] = result.skip_details.get(key, 0) + int(value or 0)

            for chunk_start in range(0, len(batch_items), max(1, person_match_chunk_size)):
                chunk_started_at = perf_counter()
                created_before = result.created
                merged_before = result.merged
                conflicted_before = result.conflicted
                errors_before = len(result.errors)
                prepared_people: list[PreparedIngestItem] = []
                chunk_items = batch_items[chunk_start : chunk_start + max(1, person_match_chunk_size)]
                chunk_end = chunk_start + len(chunk_items)
                prepare_started_at = perf_counter()
                _log(f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} prepare start: items={len(chunk_items)}")
                for item in chunk_items:
                    item_index = global_index
                    global_index += 1
                    try:
                        _log(f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} item {item_index} to_card start")
                        card, provenance, body = self.to_card(item)
                        _log(
                            f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} item {item_index} "
                            f"to_card done: type={card.type} {self._item_log_label(card)}"
                        )
                    except Exception as exc:
                        batch_error_count += 1
                        result.errors.append(f"item {item_index}: {exc}")
                        _log(
                            f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} item {item_index} to_card error: {exc}"
                        )
                        continue

                    prepared = PreparedIngestItem(
                        raw_item=item,
                        item_index=item_index,
                        card=card,
                        provenance=provenance,
                        body=body,
                    )
                    if isinstance(card, PersonCard):
                        prepared_people.append(prepared)
                    else:
                        try:
                            action = _process_nonperson(prepared)
                            _log(
                                f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} "
                                f"item {prepared.item_index} action={action} {self._item_log_label(card)}"
                            )
                        except Exception as exc:
                            batch_error_count += 1
                            result.errors.append(f"item {item_index}: {exc}")
                            _log(
                                f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} "
                                f"item {item_index} nonperson error: {exc}"
                            )
                _log(
                    f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} prepare done: "
                    f"people={len(prepared_people)} nonpeople={len(chunk_items) - len(prepared_people)} "
                    f"elapsed_s={perf_counter() - prepare_started_at:.2f}"
                )

                if not prepared_people:
                    _log(
                        f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} done: "
                        f"people=0 created_delta={result.created - created_before} "
                        f"merged_delta={result.merged - merged_before} "
                        f"conflicted_delta={result.conflicted - conflicted_before} "
                        f"errors_delta={len(result.errors) - errors_before} "
                        f"elapsed_s={perf_counter() - chunk_started_at:.2f}"
                    )
                    continue

                resolution_hints: list[ResolveResult | None] = [None] * len(prepared_people)
                if self.parallel_person_matching and person_match_workers > 1 and len(prepared_people) > 1:
                    assert identity_cache is not None
                    assert people_index is not None
                    snapshot_started_at = perf_counter()
                    _log(f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} snapshot build start")
                    alias_entries = dict(identity_cache.entries)
                    people_snapshot = people_index.snapshot()
                    _log(
                        f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} snapshot build done: "
                        f"aliases={len(alias_entries)} people={len(people_snapshot.records)} "
                        f"elapsed_s={perf_counter() - snapshot_started_at:.2f}"
                    )
                    _log(
                        f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} "
                        f"parallel resolve start: people={len(prepared_people)} workers={min(person_match_workers, len(prepared_people))}"
                    )

                    def _resolve_prepared(prepared: PreparedIngestItem) -> ResolveResult:
                        assert isinstance(prepared.card, PersonCard)
                        return resolve_person_snapshot(
                            self._person_identifiers(prepared.card),
                            alias_entries=alias_entries,
                            people_snapshot=people_snapshot,
                            nicknames=nicknames,
                            config=config,
                        )

                    with ThreadPoolExecutor(
                        max_workers=max(1, min(person_match_workers, len(prepared_people)))
                    ) as executor:
                        resolution_hints = list(executor.map(_resolve_prepared, prepared_people))
                    action_counts = Counter(
                        (hint.action if hint is not None else "unknown") for hint in resolution_hints
                    )
                    _log(
                        f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} "
                        f"parallel resolve done: people={len(prepared_people)} "
                        f"actions={dict(sorted(action_counts.items()))}"
                    )
                else:
                    _log(f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} parallel resolve skipped")

                _log(f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} delta people index init start")
                delta_people_index = PersonIndex(vault, preload=False)
                _log(f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} delta people index init done")
                commit_started_at = perf_counter()
                _log(
                    f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} "
                    f"serial commit start: people={len(prepared_people)}"
                )
                for commit_index, (prepared, resolve_hint) in enumerate(
                    zip(prepared_people, resolution_hints), start=1
                ):
                    try:
                        action = _commit_person(prepared, resolve_hint, delta_people_index)
                        _log(
                            f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} "
                            f"commit {commit_index}/{len(prepared_people)} action={action} "
                            f"{self._item_log_label(prepared.card)}"
                        )
                    except Exception as exc:
                        batch_error_count += 1
                        result.errors.append(f"item {prepared.item_index}: {exc}")
                        _log(
                            f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} "
                            f"commit {commit_index}/{len(prepared_people)} error: {exc}"
                        )
                _log(
                    f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} "
                    f"serial commit done: people={len(prepared_people)} "
                    f"elapsed_s={perf_counter() - commit_started_at:.2f}"
                )
                _log(
                    f"batch {batch.sequence} chunk {chunk_start}:{chunk_end} done: "
                    f"people={len(prepared_people)} "
                    f"created_delta={result.created - created_before} "
                    f"merged_delta={result.merged - merged_before} "
                    f"conflicted_delta={result.conflicted - conflicted_before} "
                    f"errors_delta={len(result.errors) - errors_before} "
                    f"elapsed_s={perf_counter() - chunk_started_at:.2f}"
                )

            if not dry_run:
                flush_started_at = perf_counter()
                _log("identity cache flush start")
                if identity_cache is not None:
                    identity_cache.flush()
                _log(f"identity cache flush done: elapsed_s={perf_counter() - flush_started_at:.2f}")
                if batch.cursor_patch and batch_error_count == 0:
                    _log(f"batch {batch.sequence} cursor patch apply start: keys={sorted(batch.cursor_patch.keys())}")
                    cursor.update(batch.cursor_patch)
                    _log(f"batch {batch.sequence} cursor patch apply done")
                _write_progress()
            _log(
                f"batch {batch.sequence} done: created={result.created} merged={result.merged} "
                f"conflicted={result.conflicted} skipped={result.skipped} errors={len(result.errors)}"
            )
        _log("fetch batches iterator done")

        if not dry_run:
            final_cursor = _run_logged("finalize cursor", lambda: self.finalize_cursor(cursor, **kwargs))
            if final_cursor:
                _log(f"final cursor patch apply start: keys={sorted(final_cursor.keys())}")
                cursor.update(final_cursor)
                _log("final cursor patch apply done")
            _write_progress()
        _log(
            f"ingest done: created={result.created} merged={result.merged} conflicted={result.conflicted} "
            f"skipped={result.skipped} errors={len(result.errors)} elapsed_s={perf_counter() - ingest_started_at:.2f}"
        )
        return result
