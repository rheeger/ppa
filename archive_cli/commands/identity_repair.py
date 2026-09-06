"""Join-key census and vault rewrite jobs."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_vault.canon import phone as canon_phone
from archive_vault.canon import wikilink as canon_wikilink
from archive_vault.canon.email import canonical as canon_email
from archive_vault.identity import IdentityCache, load_identity_map
from archive_vault.identity_resolver import is_same_person, load_nicknames, normalize_person_name
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
    value = str(raw or "").strip()
    if not value:
        return "empty"
    canon = canon_phone.canonical(value)
    if value == canon:
        return "e164"
    if value.isdigit() and len(value) == 10:
        return "national10"
    if value.isdigit() and len(value) == 11 and value.startswith("1"):
        return "digits11"
    if canon:
        return "dirty_but_canonicalizable"
    return "non_phone"


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
        if card_type in {"imessage_thread", "imessage_message", "email_thread", "email_message", "beeper_thread", "beeper_message"}:
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
    }
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


def resolve_people_fields(vault: Path, *, apply: bool) -> dict[str, Any]:
    identity = load_identity_map(vault)
    updated = 0
    rows = _frontmatter_rows(
        vault,
        types=["imessage_thread", "imessage_message", "email_thread", "email_message", "beeper_thread", "beeper_message"],
    )
    for row in rows:
        rel = str(row.get("rel_path") or "")
        fm = dict(row.get("frontmatter") or {})
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
            key = f"phone:{canon_phone.canonical(handle)}" if "@" not in handle else f"email:{canon_email(handle)}"
            if "@" in handle:
                key = f"email:{canon_email(handle)}"
            else:
                phone = canon_phone.canonical(handle)
                key = f"phone:{phone}" if phone else ""
            target = identity.get(key) if key else None
            if target:
                link = canon_wikilink.person_ref(canon_wikilink.parse(str(target)))
                if link and link not in resolved:
                    resolved.append(link)
        for mail in emails:
            target = identity.get(f"email:{canon_email(mail)}")
            if target:
                link = canon_wikilink.person_ref(canon_wikilink.parse(str(target)))
                if link and link not in resolved:
                    resolved.append(link)
        existing = [str(item) for item in (fm.get("people") or [])]
        if not resolved or set(existing) >= set(resolved):
            continue
        merged = list(dict.fromkeys(existing + resolved))
        updated += 1
        if not apply:
            continue
        try:
            update_frontmatter_fields(vault, rel, {"people": merged})
        except Exception as exc:
            log.warning("resolve-people skip rel=%s err=%s", rel, exc)
            updated -= 1
            continue
    return {"updated_cards": updated, "applied": apply}


def rollup_imessage_threads(vault: Path, *, apply: bool) -> dict[str, Any]:
    rows = _frontmatter_rows(vault, types=["imessage_thread", "imessage_message"])
    children: dict[str, list[dict[str, Any]]] = defaultdict(list)
    threads: dict[str, str] = {}
    for row in rows:
        fm = row.get("frontmatter") or {}
        rel = str(row.get("rel_path") or "")
        if fm.get("type") == "imessage_thread":
            threads[str(fm.get("uid") or "")] = rel
        elif fm.get("type") == "imessage_message":
            parent = canon_wikilink.parse(str(fm.get("thread") or ""))
            if parent:
                children[parent].append(fm)
    updated = 0
    for uid, rel in threads.items():
        msgs = children.get(uid) or []
        if not msgs:
            continue
        times = [str(m.get("sent_at") or m.get("created") or "") for m in msgs if str(m.get("sent_at") or m.get("created") or "")]
        if not times:
            continue
        first_at, last_at = min(times), max(times)
        count = len(msgs)
        thread_fm = next((r["frontmatter"] for r in rows if str(r.get("frontmatter", {}).get("uid")) == uid), {})
        if thread_fm.get("message_count") == count and str(thread_fm.get("last_message_at") or "") == last_at:
            continue
        updated += 1
        if not apply:
            continue
        try:
            update_frontmatter_fields(
                vault,
                rel,
                {"message_count": count, "first_message_at": first_at, "last_message_at": last_at},
            )
        except Exception as exc:
            log.warning("rollup skip rel=%s err=%s", rel, exc)
            updated -= 1
            continue
    return {"updated_threads": updated, "applied": apply}


def _compatible_name(left: dict[str, Any], right: dict[str, Any], nicknames: dict[str, list[str]]) -> bool:
    left_tokens = set(normalize_person_name(f"{left.get('first_name', '')} {left.get('last_name', '')}").split())
    right_tokens = set(normalize_person_name(f"{right.get('first_name', '')} {right.get('last_name', '')}").split())
    if left_tokens & right_tokens:
        return True
    _same, _conf, reasons = is_same_person(left, right, nicknames)
    return any(item in reasons for item in ("exact_name", "nickname_name", "fuzzy_name"))


def merge_people(vault: Path, *, apply: bool) -> dict[str, Any]:
    nicknames = load_nicknames(vault)
    identity_cache = IdentityCache(vault) if apply else None
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
                same_email = bool(
                    {canon_email(x) for x in left["frontmatter"].get("emails") or []}
                    & {canon_email(x) for x in right["frontmatter"].get("emails") or []}
                    - {""}
                )
                same_phone = bool(
                    {canon_phone.canonical(x) for x in left["frontmatter"].get("phones") or []}
                    & {canon_phone.canonical(x) for x in right["frontmatter"].get("phones") or []}
                    - {""}
                )
                names_ok = _compatible_name(left["frontmatter"], right["frontmatter"], nicknames)
                if same_email or (same_phone and names_ok):
                    winner = _richer(left, right)
                    loser = right if winner is left else left
                    redirected.append(
                        {
                            "winner": str(winner["frontmatter"].get("uid")),
                            "loser": str(loser["frontmatter"].get("uid")),
                            "reason": "exact_email" if same_email else "exact_phone_compatible_name",
                        }
                    )
                    if apply:
                        rel = str(loser["rel_path"])
                        update_frontmatter_fields(
                            vault,
                            rel,
                            {"redirect_to": str(winner["frontmatter"].get("uid"))},
                        )
                        assert identity_cache is not None
                        identity_cache.upsert(
                            f"[[{Path(str(winner['rel_path'])).stem}]]",
                            {
                                "redirect": [str(loser["frontmatter"].get("uid"))],
                                "redirect-wikilink": [f"[[{Path(rel).stem}]]"],
                            },
                        )
                elif same_phone and not names_ok:
                    queued.append({"uids": [lu, ru], "reason": "phone_incompatible_name"})
                else:
                    queued.append({"uids": [lu, ru], "reason": "ambiguous"})

    if apply and queued:
        queue_path = vault / "_meta" / "dedup-candidates.json"
        existing: list[Any] = []
        if queue_path.is_file():
            try:
                existing = json.loads(queue_path.read_text(encoding="utf-8"))
                if not isinstance(existing, list):
                    existing = []
            except json.JSONDecodeError:
                existing = []
        existing.extend(queued)
        queue_path.parent.mkdir(parents=True, exist_ok=True)
        queue_path.write_text(json.dumps(existing, indent=2) + "\n", encoding="utf-8")
    if identity_cache is not None:
        identity_cache.flush()
    return {"redirected": redirected, "queued": queued, "applied": apply}


def emit_same_conversation_edges(vault: Path) -> dict[str, Any]:
    """Record 1:1 email-handle vs phone-handle pairs that share one person. Does not smash chat IDs."""

    rows = _frontmatter_rows(vault, types=["imessage_thread"])
    by_person: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        fm = row.get("frontmatter") or {}
        handles = [str(h) for h in (fm.get("participant_handles") or [])]
        people = [canon_wikilink.parse(str(p)) for p in (fm.get("people") or [])]
        if len(handles) != 1 or len(people) != 1:
            continue
        by_person[people[0]].append({"uid": fm.get("uid"), "handle": handles[0], "rel_path": row.get("rel_path")})
    pairs: list[dict[str, Any]] = []
    for person, threads in by_person.items():
        emails = [t for t in threads if "@" in t["handle"]]
        phones = [t for t in threads if "@" not in t["handle"]]
        if emails and phones:
            pairs.append(
                {
                    "person": person,
                    "email_thread": emails[0]["uid"],
                    "phone_thread": phones[0]["uid"],
                    "edge_type": "same_conversation",
                }
            )
    out = vault / "_meta" / "same-conversation.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(pairs, indent=2) + "\n", encoding="utf-8")
    return {"pairs": len(pairs), "path": str(out)}


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
        result = emit_same_conversation_edges(vault)
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
