"""Join-key census and vault rewrite jobs."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_engine.identity_resolution import auto_merge_eligible
from archive_vault.canon import phone as canon_phone
from archive_vault.canon import wikilink as canon_wikilink
from archive_vault.canon.email import canonical as canon_email
from archive_vault.identity import IdentityCache, canonicalize_people_list, canonicalize_wikilink, load_identity_map
from archive_vault.identity_quality import (
    is_shared_mailbox_email,
    looks_like_event_title,
    shared_mailbox_domain,
    should_strip_invitation_alias,
)
from archive_vault.identity_resolver import load_nicknames
from archive_vault.vault import update_frontmatter_fields

log = logging.getLogger("ppa.identity_repair")

ALWAYS_REWRITE = ("phones", "person_refs", "thread_rollups")


def _frontmatter_rows(vault: Path, types: list[str] | None = None):
    """Yield cache rows. Never rebuild a living-seed cache from this worktree."""

    from archive_cli.vault_cache import VaultScanCache

    cache_path = VaultScanCache.cache_path_for_vault(vault)
    if cache_path.is_file():
        try:
            import archive_crate

            kwargs: dict[str, Any] = {"types": types} if types else {}
            log.info("identity-repair cache dump path=%s rebuild=no", cache_path)
            return archive_crate.frontmatter_dicts_from_cache(str(cache_path), **kwargs)
        except Exception:
            log.warning("identity-repair cache dump failed; falling back")
    log.info("identity-repair cache missing; building lookup tables vault=%s", vault)
    scan_cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=5000)
    by_type, _rel_by_uid, uid_by_path, _uid_by_stem, frontmatter_by_uid = scan_cache.slice_lookup_tables()
    rows: list[dict[str, Any]] = []
    wanted = set(types) if types else None
    for card_type, rels in by_type.items():
        if wanted and card_type not in wanted:
            continue
        for rel in rels:
            uid = uid_by_path.get(rel, "")
            fm = dict(frontmatter_by_uid.get(uid) or {})
            if fm:
                rows.append({"rel_path": rel, "frontmatter": fm})
    return rows


def _phone_bucket(raw: str) -> str:
    parsed = canon_phone.parse(raw)
    if parsed.validity == "opaque":
        return "opaque_handle"
    if parsed.validity == "unknown":
        return "unknown"
    if parsed.validity == "invalid":
        return "empty" if parsed.reason == "empty" else "non_phone"
    if parsed.region == "INTL" or str(raw or "").strip() == parsed.canonical:
        return "e164"
    return "dirty_but_canonicalizable"


def _wikilink_shape(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return "empty"
    if value.startswith("[[") and value.endswith("]]"):
        inner = canon_wikilink.parse(value)
        if inner.startswith("hfa-"):
            return "uid_wikilink"
        return "slug_wikilink"
    if value.startswith("hfa-"):
        return "bare_uid"
    return "display_or_other"


def run_census(vault: Path) -> dict[str, Any]:
    rows = _frontmatter_rows(vault)
    phone_buckets: Counter[str] = Counter()
    dirty_phone_paths: list[str] = []
    stale_thread_paths: list[str] = []
    email_dirty = 0
    handle_urlish = 0
    wikilink_shapes: Counter[str] = Counter()
    missing_people = 0
    stale_rollups = 0
    person_count = 0
    comms = 0
    thread_children: dict[str, list[str]] = defaultdict(list)
    thread_declared: dict[str, dict[str, Any]] = {}
    person_summaries: dict[str, list[str]] = defaultdict(list)
    person_alias_rows: list[dict[str, Any]] = []

    scanned = 0
    for row in rows:
        scanned += 1
        if scanned % 20000 == 0:
            log.info("identity-repair census scanned=%d", scanned)
        fm = row.get("frontmatter") or {}
        card_type = str(fm.get("type") or "")
        uid = str(fm.get("uid") or "")
        if card_type == "person":
            person_count += 1
            summary = str(fm.get("summary") or "").strip()
            if summary:
                person_summaries[summary.lower()].append(uid)
            person_alias_rows.append(
                {
                    "rel_path": str(row.get("rel_path") or ""),
                    "uid": uid,
                    "summary": summary,
                    "aliases": [str(item).strip() for item in (fm.get("aliases") or []) if str(item).strip()],
                    "emails": [str(item).strip() for item in (fm.get("emails") or []) if str(item).strip()],
                }
            )
            for raw in fm.get("phones") or []:
                bucket = _phone_bucket(str(raw))
                phone_buckets[bucket] += 1
                if bucket in {"dirty_but_canonicalizable", "national10", "digits11"}:
                    dirty_phone_paths.append(str(row.get("rel_path") or ""))
            for raw in fm.get("emails") or []:
                if canon_email(str(raw)) != str(raw).strip():
                    email_dirty += 1
            for field in ("linkedin", "github", "twitter"):
                val = str(fm.get(field) or "")
                if "http" in val.lower() or "/" in val:
                    handle_urlish += 1
        if card_type in {
            "imessage_thread",
            "imessage_message",
            "email_thread",
            "email_message",
            "beeper_thread",
            "beeper_message",
        }:
            comms += 1
            people = fm.get("people") or []
            if not people:
                missing_people += 1
            for raw in people:
                wikilink_shapes[_wikilink_shape(str(raw))] += 1
        if card_type == "imessage_thread":
            thread_declared[uid] = {
                "message_count": fm.get("message_count"),
                "last_message_at": str(fm.get("last_message_at") or ""),
                "rel_path": str(row.get("rel_path") or ""),
            }
        if card_type == "imessage_message":
            thread_ref = canon_wikilink.parse(str(fm.get("thread") or ""))
            sent = str(fm.get("sent_at") or fm.get("created") or "")
            if thread_ref:
                thread_children[thread_ref].append(sent)

    for uid, declared in thread_declared.items():
        children = thread_children.get(uid) or []
        if not children:
            continue
        last_child = max(children)
        declared_last = declared.get("last_message_at") or ""
        declared_count = declared.get("message_count")
        if declared_count != len(children) or (declared_last and declared_last < last_child[:19]):
            stale_rollups += 1
            if declared.get("rel_path"):
                stale_thread_paths.append(str(declared["rel_path"]))

    rewrite_families = list(ALWAYS_REWRITE)
    code_only = ["email", "handle", "slug", "instant", "place"]
    if email_dirty:
        rewrite_families.append("email")
        code_only = [item for item in code_only if item != "email"]

    alias_hygiene = classify_alias_hygiene(person_alias_rows, person_summaries)
    if alias_hygiene["shared_mailbox_email_count"] or alias_hygiene["junk_alias_count"] or alias_hygiene["stolen_alias_count"]:
        rewrite_families.append("aliases")

    report = {
        "vault": str(vault),
        "cards_scanned": scanned if scanned else len(rows),
        "person_count": person_count,
        "comms_count": comms,
        "phone_buckets": dict(phone_buckets),
        "email_dirty": email_dirty,
        "handle_urlish": handle_urlish,
        "wikilink_shapes": dict(wikilink_shapes),
        "missing_people": missing_people,
        "stale_thread_rollups": stale_rollups,
        "rewrite_families": sorted(set(rewrite_families)),
        "code_only_families": code_only,
        "dirty_phone_paths": sorted(set(p for p in dirty_phone_paths if p)),
        "stale_thread_paths": sorted(set(p for p in stale_thread_paths if p)),
        "shared_mailbox_person_count": alias_hygiene["shared_mailbox_person_count"],
        "shared_mailbox_email_count": alias_hygiene["shared_mailbox_email_count"],
        "junk_alias_count": alias_hygiene["junk_alias_count"],
        "stolen_alias_count": alias_hygiene["stolen_alias_count"],
        "alias_hygiene_samples": alias_hygiene["samples"],
    }
    return report


def classify_alias_hygiene(
    person_rows: list[dict[str, Any]],
    person_summaries: dict[str, list[str]],
) -> dict[str, Any]:
    """Flag invitation-mailbox emails and stolen or event-title aliases."""

    samples: list[dict[str, Any]] = []
    shared_people: set[str] = set()
    shared_emails = 0
    junk_aliases = 0
    stolen_aliases = 0
    strip_aliases = 0
    for row in person_rows:
        uid = str(row.get("uid") or "")
        summary = str(row.get("summary") or "").strip()
        emails = [str(item) for item in (row.get("emails") or [])]
        aliases = [str(item) for item in (row.get("aliases") or [])]
        shared = [email for email in emails if is_shared_mailbox_email(email)]
        junk = [alias for alias in aliases if looks_like_event_title(alias)]
        stolen = []
        strip = []
        for alias in aliases:
            owners = [other for other in person_summaries.get(alias.lower(), []) if other and other != uid]
            if owners:
                stolen.append({"alias": alias, "owner_uids": owners[:8]})
            if should_strip_invitation_alias(
                alias,
                card_uid=uid,
                emails=emails,
                person_summaries=person_summaries,
            ):
                strip.append(alias)
        if shared:
            shared_people.add(uid)
            shared_emails += len(shared)
        junk_aliases += len(junk)
        stolen_aliases += len(stolen)
        strip_aliases += len(strip)
        if shared or strip:
            if len(samples) < 50:
                samples.append(
                    {
                        "rel_path": row.get("rel_path") or "",
                        "uid": uid,
                        "summary": summary,
                        "shared_mailbox_emails": shared,
                        "shared_mailbox_domains": sorted(
                            {shared_mailbox_domain(email) or "" for email in shared} - {""}
                        ),
                        "junk_aliases": junk,
                        "stolen_aliases": stolen,
                        "strip_aliases": strip,
                    }
                )
    return {
        "shared_mailbox_person_count": len(shared_people),
        "shared_mailbox_email_count": shared_emails,
        "junk_alias_count": junk_aliases,
        "stolen_alias_count": stolen_aliases,
        "strip_alias_count": strip_aliases,
        "samples": samples,
    }


def alias_hygiene(vault: Path, *, apply: bool) -> dict[str, Any]:
    """Strip invitation-mailbox emails and junk/stolen aliases from person cards."""

    rows = _iter_person_rows(vault)
    person_summaries: dict[str, list[str]] = defaultdict(list)
    packed: list[dict[str, Any]] = []
    for row in rows:
        fm = dict(row.get("frontmatter") or {})
        uid = str(fm.get("uid") or "")
        summary = str(fm.get("summary") or "").strip()
        if summary:
            person_summaries[summary.lower()].append(uid)
        packed.append(
            {
                "rel_path": str(row.get("rel_path") or ""),
                "uid": uid,
                "summary": summary,
                "aliases": [str(item).strip() for item in (fm.get("aliases") or []) if str(item).strip()],
                "emails": [str(item).strip() for item in (fm.get("emails") or []) if str(item).strip()],
            }
        )
    report = classify_alias_hygiene(packed, person_summaries)
    changed = 0
    for row in packed:
        emails = list(row["emails"])
        aliases = list(row["aliases"])
        uid = row["uid"]
        if not any(is_shared_mailbox_email(email) for email in emails):
            continue
        keep_emails = [email for email in emails if not is_shared_mailbox_email(email)]
        keep_aliases = [
            alias
            for alias in aliases
            if not should_strip_invitation_alias(
                alias,
                card_uid=uid,
                emails=emails,
                person_summaries=person_summaries,
            )
        ]
        if keep_emails == emails and keep_aliases == aliases:
            continue
        changed += 1
        if not apply or not row["rel_path"]:
            continue
        try:
            update_frontmatter_fields(
                vault,
                str(row["rel_path"]),
                {"emails": keep_emails, "aliases": keep_aliases},
            )
        except Exception as exc:
            log.warning("alias-hygiene skip rel=%s err=%s", row["rel_path"], exc)
            changed -= 1
    report["changed_people"] = changed
    report["applied"] = apply
    return report


def preflight(vault: Path, *, tar_path: str = "") -> dict[str, Any]:
    """Read-only census plus optional tarball of dirty paths and identity-map."""

    report = run_census(vault)
    touch = sorted(
        set(report.get("dirty_phone_paths") or [])
        | set(report.get("stale_thread_paths") or [])
        | {"_meta/identity-map.json"}
    )
    report["touch_paths"] = touch
    if tar_path:
        import tarfile

        dest = Path(tar_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(dest, "w") as archive:
            for rel in touch:
                src = vault / rel
                if src.is_file():
                    archive.add(src, arcname=rel)
        report["tar_path"] = str(dest)
        report["tar_members"] = sum(1 for rel in touch if (vault / rel).is_file())
    return report


def _iter_person_rows(vault: Path) -> list[dict[str, Any]]:
    return _frontmatter_rows(vault, types=["person"])


def canonicalize_people(vault: Path, *, apply: bool) -> dict[str, Any]:
    changed = 0
    alias_upserts = 0
    cache = IdentityCache(vault) if apply else None
    for row in _iter_person_rows(vault):
        rel = str(row.get("rel_path") or "")
        fm = dict(row.get("frontmatter") or {})
        raw_phones = [str(item) for item in (fm.get("phones") or []) if str(item).strip()]
        new_phones = []
        seen: set[str] = set()
        for raw in raw_phones:
            canon = canon_phone.canonical(raw) or raw.strip()
            if canon and canon not in seen:
                seen.add(canon)
                new_phones.append(canon)
        phones_changed = new_phones != raw_phones
        if phones_changed:
            changed += 1
        if not apply:
            continue
        if phones_changed:
            try:
                update_frontmatter_fields(vault, rel, {"phones": new_phones})
            except Exception as exc:
                log.warning("canonicalize skip rel=%s err=%s", rel, exc)
                changed -= 1
                continue
        aliases: dict[str, Any] = {
            "phones": [],
            "emails": [str(item) for item in (fm.get("emails") or []) if str(item).strip()],
            "name": str(fm.get("summary") or ""),
        }
        for raw in raw_phones + new_phones:
            aliases["phones"].extend(canon_phone.alias_forms(raw))
        if cache is not None and (aliases["phones"] or aliases["emails"] or aliases["name"]):
            cache.upsert(f"[[{Path(rel).stem}]]", aliases)
            alias_upserts += 1
    if cache is not None and alias_upserts:
        cache.flush()
    return {"changed_people": changed, "identity_map_upserts": alias_upserts, "applied": apply}


COMMS_PEOPLE_TYPES = (
    "imessage_thread",
    "imessage_message",
    "email_thread",
    "email_message",
    "beeper_thread",
    "beeper_message",
)


def _canonical_people_link(identity: dict[str, str], raw: str) -> str:
    parsed = canon_wikilink.person_ref(canon_wikilink.parse(str(raw)))
    if not parsed:
        return ""
    redirected = canonicalize_wikilink(identity, parsed, lookup=False)
    return redirected or parsed


def _identity_target(identity: dict[str, str], key: str) -> str:
    if not key:
        return ""
    target = identity.get(key)
    if not target:
        return ""
    return _canonical_people_link(identity, str(target))


def _handle_identity_keys(handle: str) -> list[str]:
    raw = str(handle or "").strip()
    if not raw:
        return []
    if "@" in raw:
        email = canon_email(raw)
        return [f"email:{email}"] if email else []
    forms = canon_phone.alias_forms(raw)
    if forms:
        return [f"phone:{form}" for form in forms]
    phone = canon_phone.canonical(raw)
    return [f"phone:{phone}"] if phone else []


def resolve_people_fields(vault: Path, *, apply: bool) -> dict[str, Any]:
    identity = load_identity_map(vault)
    updated = 0
    gained_by_type: dict[str, int] = {}
    replaced_by_type: dict[str, int] = {}
    rows = _frontmatter_rows(vault, types=list(COMMS_PEOPLE_TYPES))
    for row in rows:
        rel = str(row.get("rel_path") or "")
        fm = dict(row.get("frontmatter") or {})
        card_type = str(fm.get("type") or "")
        handles = [str(item) for item in (fm.get("participant_handles") or [])]
        sender = str(fm.get("sender_handle") or "")
        if sender:
            handles.append(sender)
        emails = [str(item) for item in (fm.get("participant_emails") or [])]
        from_email = str(fm.get("from_email") or "")
        if from_email:
            emails.append(from_email)
        resolved: list[str] = []
        for handle in handles:
            for key in _handle_identity_keys(handle):
                link = _identity_target(identity, key)
                if link and link not in resolved:
                    resolved.append(link)
        for mail in emails:
            link = _identity_target(identity, f"email:{canon_email(mail)}")
            if link and link not in resolved:
                resolved.append(link)
        existing = [str(item) for item in (fm.get("people") or [])]
        canonical_existing = canonicalize_people_list(identity, existing)
        merged = list(dict.fromkeys([*canonical_existing, *resolved]))
        if merged == existing:
            continue
        added = [link for link in merged if link not in existing]
        replaced = existing != canonical_existing
        if added:
            gained_by_type[card_type] = gained_by_type.get(card_type, 0) + 1
        if replaced:
            replaced_by_type[card_type] = replaced_by_type.get(card_type, 0) + 1
        updated += 1
        if not apply:
            continue
        try:
            update_frontmatter_fields(vault, rel, {"people": merged})
        except Exception as exc:
            log.warning("resolve-people skip rel=%s err=%s", rel, exc)
            updated -= 1
            continue
    return {
        "updated_cards": updated,
        "applied": apply,
        "gained_wikilink_by_type": gained_by_type,
        "replaced_wikilink_by_type": replaced_by_type,
    }


def rollup_imessage_threads(vault: Path, *, apply: bool) -> dict[str, Any]:
    from archive_engine.thread_projection import project_from_rows

    rows = _frontmatter_rows(vault, types=["imessage_thread", "imessage_message"])
    if not apply:
        return {"updated_threads": 0, "applied": False, "pending": []}
    from archive_engine.thread_projection import drain_pending, pending_thread_uids

    if pending_thread_uids(vault):
        return drain_pending(vault, rows)
    return project_from_rows(vault, rows)


def _compatible_name(left: dict[str, Any], right: dict[str, Any], nicknames: dict[str, list[str]]) -> bool:
    from archive_engine.identity_resolution import given_names_compatible

    return given_names_compatible(left, right, nicknames)


def merge_people(vault: Path, *, apply: bool) -> dict[str, Any]:
    nicknames = load_nicknames(vault)
    people = _iter_person_rows(vault)
    by_email: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_phone: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in people:
        fm = row.get("frontmatter") or {}
        row_rec = {"rel_path": row.get("rel_path"), "frontmatter": fm}
        for raw in fm.get("emails") or []:
            key = canon_email(str(raw))
            if key:
                by_email[key].append(row_rec)
        for raw in fm.get("phones") or []:
            key = canon_phone.canonical(str(raw))
            if key:
                by_phone[key].append(row_rec)
    redirected: list[dict[str, str]] = []
    queued: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()

    def _pair_key(a: str, b: str) -> tuple[str, str]:
        return (a, b) if a < b else (b, a)

    def _richer(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
        def score(item: dict[str, Any]) -> tuple[int, int, int]:
            fm = item["frontmatter"]
            return (
                len(fm.get("source") or []),
                int(fm.get("emails_seen_count") or 0),
                len(fm.get("emails") or []) + len(fm.get("phones") or []),
            )

        return left if score(left) >= score(right) else right

    for key, group in list(by_email.items()) + list(by_phone.items()):
        if len(group) < 2:
            continue
        unique: dict[str, dict[str, Any]] = {}
        for item in group:
            unique[str(item["frontmatter"].get("uid"))] = item
        items = list(unique.values())
        if len(items) < 2:
            continue
        for i, left in enumerate(items):
            for right in items[i + 1 :]:
                lu = str(left["frontmatter"].get("uid"))
                ru = str(right["frontmatter"].get("uid"))
                pair = _pair_key(lu, ru)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                eligible, reason = auto_merge_eligible(left["frontmatter"], right["frontmatter"], nicknames)
                if eligible:
                    winner = _richer(left, right)
                    loser = right if winner is left else left
                    redirected.append(
                        {
                            "winner": str(winner["frontmatter"].get("uid")),
                            "loser": str(loser["frontmatter"].get("uid")),
                            "reason": reason,
                        }
                    )
                    if apply:
                        from archive_engine.corrections import CorrectionCommandRequest, merge_identities

                        merge_identities(
                            vault,
                            CorrectionCommandRequest(
                                action="merge_identities",
                                winner_uid=str(winner["frontmatter"].get("uid")),
                                loser_uid=str(loser["frontmatter"].get("uid")),
                                author="identity-repair",
                                reason=reason,
                            ),
                        )
                else:
                    queued.append({"uids": [lu, ru], "reason": reason})

    if apply and queued:
        from archive_engine.journaled_state import (
            IDENTITY_PROPOSALS_REL,
            IDENTITY_PROPOSALS_UID,
            load_json_state,
            merge_identity_proposals,
            persist_json_state,
        )

        existing_payload = load_json_state(vault, IDENTITY_PROPOSALS_REL)
        existing = list(existing_payload.get("proposals") or [])
        merged = merge_identity_proposals(existing, queued)
        persist_json_state(
            vault,
            uid=IDENTITY_PROPOSALS_UID,
            rel=IDENTITY_PROPOSALS_REL,
            payload={"proposals": merged, "status": "queued"},
            source="identity-repair",
        )
    return {"redirected": redirected, "queued": queued, "applied": apply}


def emit_same_conversation_edges(vault: Path, *, apply: bool = False) -> dict[str, Any]:
    """Propose email-handle vs phone-handle pairs. Preview writes nothing."""

    from archive_engine.conversation_links import preview_or_apply
    from archive_vault.vault import read_note_frontmatter_file

    threads: list[dict[str, Any]] = []
    for row in _frontmatter_rows(vault, types=["imessage_thread"]):
        rel = str(row.get("rel_path") or "")
        fm = dict(row.get("frontmatter") or {})
        if rel:
            try:
                fm = dict(read_note_frontmatter_file(vault / rel, vault_root=vault).frontmatter)
            except Exception as exc:
                log.warning("same-conversation skip stale-cache rel=%s err=%s", rel, exc)
        threads.append(fm)
    result = preview_or_apply(vault, threads, apply=apply)
    return {
        "pairs": result.get("count", 0),
        "path": result.get("path") or "",
        "applied": apply,
        "truncated": result.get("truncated"),
    }


def _review_queue_counts(vault: Path) -> dict[str, int]:
    from archive_engine.journaled_state import IDENTITY_PROPOSALS_REL, load_json_state

    payload = load_json_state(vault, IDENTITY_PROPOSALS_REL)
    counts = {"open": 0, "accepted": 0, "rejected": 0, "applied": 0, "other": 0}
    for item in payload.get("proposals") or []:
        status = str(item.get("status") or "open").strip().lower() or "open"
        if status in counts:
            counts[status] += 1
        else:
            counts["other"] += 1
    counts["total"] = sum(counts.values())
    return counts


def _comms_has_join_key(fm: dict[str, Any]) -> bool:
    if any(str(item).strip() for item in (fm.get("participant_handles") or [])):
        return True
    if any(str(item).strip() for item in (fm.get("participant_emails") or [])):
        return True
    if str(fm.get("sender_handle") or "").strip():
        return True
    if str(fm.get("from_email") or "").strip():
        return True
    return False


def discoverability_snapshot(vault: Path) -> dict[str, Any]:
    """Census people discoverability: empty people lists, missing links, identity-map reach."""

    identity = load_identity_map(vault)
    empty_people_by_type: dict[str, int] = {card_type: 0 for card_type in COMMS_PEOPLE_TYPES}
    missing_people_by_type: dict[str, int] = {card_type: 0 for card_type in COMMS_PEOPLE_TYPES}
    people_with_contacts_apple = 0
    people_with_phone = 0
    people_with_email = 0
    person_count = 0
    rows = _frontmatter_rows(vault, types=["person", *COMMS_PEOPLE_TYPES])
    for row in rows:
        fm = dict(row.get("frontmatter") or {})
        card_type = str(fm.get("type") or "")
        if card_type == "person":
            person_count += 1
            sources = [str(item) for item in (fm.get("source") or [])]
            if "contacts.apple" in sources:
                people_with_contacts_apple += 1
            if any(canon_phone.canonical(str(item)) for item in (fm.get("phones") or [])):
                people_with_phone += 1
            if any(canon_email(str(item)) for item in (fm.get("emails") or [])):
                people_with_email += 1
            continue
        if card_type not in empty_people_by_type:
            continue
        people = [str(item) for item in (fm.get("people") or []) if str(item).strip()]
        if not people:
            empty_people_by_type[card_type] += 1
        if _comms_has_join_key(fm) and not people:
            missing_people_by_type[card_type] += 1
    phone_keys = sum(1 for key in identity if str(key).startswith("phone:"))
    email_keys = sum(1 for key in identity if str(key).startswith("email:"))
    return {
        "person_count": person_count,
        "people_with_contacts_apple": people_with_contacts_apple,
        "people_with_phone": people_with_phone,
        "people_with_email": people_with_email,
        "empty_people_by_type": empty_people_by_type,
        "missing_people_by_type": missing_people_by_type,
        "missing_people": sum(missing_people_by_type.values()),
        "identity_phone_keys": phone_keys,
        "identity_email_keys": email_keys,
        "review_queue": _review_queue_counts(vault),
    }


def discoverability_report(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    match_outcomes: dict[str, int] | None = None,
    match_reasons: dict[str, int] | None = None,
    resolve_people: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Before/after discoverability score for an Apple Contacts catch-up."""

    def _delta_map(key: str) -> dict[str, int]:
        left = dict(before.get(key) or {})
        right = dict(after.get(key) or {})
        keys = sorted(set(left) | set(right))
        return {item: int(right.get(item, 0) or 0) - int(left.get(item, 0) or 0) for item in keys}

    resolve_people = resolve_people or {}
    return {
        "before": before,
        "after": after,
        "delta": {
            "person_count": int(after.get("person_count") or 0) - int(before.get("person_count") or 0),
            "people_with_contacts_apple": int(after.get("people_with_contacts_apple") or 0)
            - int(before.get("people_with_contacts_apple") or 0),
            "missing_people": int(after.get("missing_people") or 0) - int(before.get("missing_people") or 0),
            "identity_phone_keys": int(after.get("identity_phone_keys") or 0)
            - int(before.get("identity_phone_keys") or 0),
            "identity_email_keys": int(after.get("identity_email_keys") or 0)
            - int(before.get("identity_email_keys") or 0),
            "empty_people_by_type": _delta_map("empty_people_by_type"),
            "missing_people_by_type": _delta_map("missing_people_by_type"),
        },
        "match_outcomes": dict(match_outcomes or {}),
        "match_reasons": dict(match_reasons or {}),
        "gained_wikilink_by_type": dict(resolve_people.get("gained_wikilink_by_type") or {}),
        "replaced_wikilink_by_type": dict(resolve_people.get("replaced_wikilink_by_type") or {}),
        "review_queue": dict(after.get("review_queue") or {}),
    }


def apply_accepted_reviews(vault: Path, *, apply: bool) -> dict[str, Any]:
    """Apply human-accepted identity-proposals. Open and rejected rows stay untouched."""

    from archive_engine.journaled_state import (
        IDENTITY_PROPOSALS_REL,
        IDENTITY_PROPOSALS_UID,
        load_json_state,
        persist_json_state,
    )
    from archive_vault.identity_resolver import merge_into_existing, provenance_from_incoming

    payload = load_json_state(vault, IDENTITY_PROPOSALS_REL)
    proposals = [dict(item) for item in (payload.get("proposals") or [])]
    accepted = [item for item in proposals if str(item.get("status") or "open").lower() == "accepted"]
    applied_rows: list[dict[str, str]] = []
    skipped_rows: list[dict[str, str]] = []
    people_by_uid: dict[str, dict[str, Any]] = {}
    if accepted:
        for row in _iter_person_rows(vault):
            fm = dict(row.get("frontmatter") or {})
            uid = str(fm.get("uid") or "").strip()
            if uid:
                people_by_uid[uid] = row
    for proposal in accepted:
        incoming = dict(proposal.get("incoming") or {})
        existing_wikilink = str(proposal.get("existing_wikilink") or "").strip()
        uids = [str(uid).strip() for uid in (proposal.get("uids") or []) if str(uid).strip()]
        incoming_uid = str(incoming.get("uid") or "").strip()
        existing_uid = ""
        for uid in uids:
            if uid and uid != incoming_uid:
                existing_uid = uid
                break
        if not incoming_uid and uids:
            incoming_uid = uids[0]
        if not existing_uid and existing_wikilink:
            for row in people_by_uid.values():
                fm = dict(row.get("frontmatter") or {})
                slug = Path(str(row.get("rel_path") or "")).stem
                if f"[[{slug}]]" == existing_wikilink:
                    existing_uid = str(fm.get("uid") or "")
                    break
        incoming_row = people_by_uid.get(incoming_uid)
        existing_row = people_by_uid.get(existing_uid)
        if incoming_row and existing_row and incoming_uid != existing_uid:
            if apply:
                from archive_engine.corrections import CorrectionCommandRequest, merge_identities

                merge_identities(
                    vault,
                    CorrectionCommandRequest(
                        action="merge_identities",
                        winner_uid=existing_uid,
                        loser_uid=incoming_uid,
                        author="identity-review",
                        reason=str(proposal.get("reason") or "accepted_review"),
                    ),
                )
            applied_rows.append({"mode": "merge_identities", "winner": existing_uid, "loser": incoming_uid})
            proposal["status"] = "applied"
            continue
        if existing_wikilink and incoming:
            if apply:
                merge_into_existing(
                    vault,
                    existing_wikilink,
                    incoming,
                    provenance_from_incoming(incoming),
                    "",
                )
            applied_rows.append({"mode": "merge_into_existing", "existing": existing_wikilink})
            proposal["status"] = "applied"
            continue
        skipped_rows.append({"reason": "no_existing_target", "uids": ",".join(uids)})
    if apply and accepted:
        persist_json_state(
            vault,
            uid=IDENTITY_PROPOSALS_UID,
            rel=IDENTITY_PROPOSALS_REL,
            payload={"proposals": proposals, "status": "queued"},
            source="identity-review",
        )
    return {
        "accepted": len(accepted),
        "applied": apply,
        "applied_rows": applied_rows,
        "skipped_rows": skipped_rows,
    }


def _report_for_json(result: dict[str, Any]) -> dict[str, Any]:
    """Cap path lists so a living-seed census JSON stays usable."""

    out = dict(result)
    for key in ("dirty_phone_paths", "stale_thread_paths", "touch_paths"):
        vals = [str(item) for item in (out.get(key) or [])]
        out[f"{key}_count"] = len(vals)
        if len(vals) > 500:
            out[key] = vals[:500]
            out[f"{key}_truncated"] = True
    return out


def dispatch(args: Any) -> dict[str, Any]:
    from archive_cli.commands._resolve import resolve_vault

    vault = Path(resolve_vault())
    action = str(getattr(args, "identity_action", "") or "")
    apply = bool(getattr(args, "apply", False))
    output = str(getattr(args, "output", "") or "")
    started = datetime.now(timezone.utc).isoformat()
    log.info("identity-repair start action=%s apply=%s vault=%s", action, apply, vault)
    if action == "census":
        result = run_census(vault)
    elif action == "preflight":
        result = preflight(vault, tar_path=str(getattr(args, "tar", "") or ""))
    elif action == "canonicalize":
        result = canonicalize_people(vault, apply=apply)
    elif action == "resolve-people":
        result = resolve_people_fields(vault, apply=apply)
    elif action == "rollup-threads":
        result = rollup_imessage_threads(vault, apply=apply)
    elif action == "merge":
        result = merge_people(vault, apply=apply)
    elif action == "same-conversation":
        result = emit_same_conversation_edges(vault, apply=apply)
    elif action == "alias-hygiene":
        result = alias_hygiene(vault, apply=apply)
    elif action == "discoverability":
        result = discoverability_snapshot(vault)
    elif action == "apply-reviews":
        result = apply_accepted_reviews(vault, apply=apply)
    else:
        raise SystemExit(f"unknown identity-repair action: {action}")
    result["started_at"] = started
    result["action"] = action
    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(json.dumps(_report_for_json(result), indent=2, default=str) + "\n", encoding="utf-8")
        result["output"] = output
    log.info("identity-repair done action=%s keys=%s", action, sorted(result))
    return result
