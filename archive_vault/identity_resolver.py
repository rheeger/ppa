"""Identity resolution and merge helpers."""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from archive_vault.config import PPAConfig, load_config
from archive_vault.identity import IdentityCache, _normalize_identifier, resolve_any, upsert_identity_map
from archive_vault.identity_quality import alias_is_trustworthy, is_shared_mailbox_email, looks_like_person_name
from archive_vault.provenance import PROVENANCE_EXEMPT_FIELDS, ProvenanceEntry
from pydantic import ValidationError

from archive_vault.schema import validate_card_permissive, validate_card_strict
from archive_vault.vault import find_note_by_slug, iter_notes, read_note, read_note_by_uid, write_card

try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    from difflib import SequenceMatcher

    class _FallbackFuzz:
        @staticmethod
        def token_sort_ratio(a: str, b: str) -> float:
            left = " ".join(sorted(a.split()))
            right = " ".join(sorted(b.split()))
            return SequenceMatcher(None, left, right).ratio() * 100

    fuzz = _FallbackFuzz()


# Close-name and name-conflict rows at or above this score merge without a human.
# Below it they stay in identity-proposals as open review.
REVIEW_AUTO_APPROVE_MIN_CONFIDENCE = 80


@dataclass
class ResolveResult:
    action: str
    wikilink: str | None
    confidence: int
    reasons: list[str]


def _auto_approve_if_confident(result: ResolveResult) -> ResolveResult:
    if result.action != "conflict" or not result.wikilink:
        return result
    if result.confidence < REVIEW_AUTO_APPROVE_MIN_CONFIDENCE:
        return result
    reasons = list(result.reasons)
    if "auto_approved" not in reasons:
        reasons.append("auto_approved")
    return ResolveResult("merge", result.wikilink, result.confidence, reasons)


@dataclass(frozen=True)
class PersonIndexSnapshot:
    records: dict[str, dict[str, Any]]
    by_last_name: dict[str, tuple[str, ...]]
    by_first_initial_last: dict[tuple[str, str], tuple[str, ...]]


class PersonIndex:
    """In-memory candidate index for person resolution during batch imports."""

    def __init__(
        self,
        vault_path: str | Path,
        *,
        preload: bool = True,
        log=None,
        progress_every: int = 0,
    ):
        self.vault_path = Path(vault_path)
        self.records: dict[str, dict[str, Any]] = {}
        self.by_last_name: dict[str, set[str]] = {}
        self.by_first_initial_last: dict[tuple[str, str], set[str]] = {}
        self._log = log
        self._progress_every = max(0, int(progress_every or 0))
        if preload:
            self._load()

    def _load_people_rows_from_cache(self) -> list[dict[str, Any]] | None:
        """Load ``People/`` frontmatter from the vault scan cache (no full-vault walk)."""

        try:
            from archive_cli.vault_cache import VaultScanCache

            cache_path = VaultScanCache.cache_path_for_vault(self.vault_path)
            if not cache_path.is_file():
                return None
            import archive_crate

            return list(archive_crate.frontmatter_dicts_from_cache(str(cache_path), types=["person"]))
        except Exception:
            return None

    def _load(self) -> None:
        if self._log is not None:
            self._log(f"person index preload start: vault={self.vault_path}")
        loaded = 0
        skipped = 0
        cache_rows = self._load_people_rows_from_cache()
        if cache_rows is not None:
            if self._log is not None:
                self._log(f"person index preload source=cache rows={len(cache_rows)}")
            for row in cache_rows:
                rel = str(row.get("rel_path") or "")
                if not rel.startswith("People/"):
                    continue
                fm = dict(row.get("frontmatter") or {})
                if not fm:
                    continue
                data = self._person_record_from_frontmatter(rel, fm)
                if data is None:
                    skipped += 1
                    continue
                self.upsert(f"[[{Path(rel).stem}]]", data)
                loaded += 1
                if self._log is not None and self._progress_every and loaded % self._progress_every == 0:
                    self._log(f"person index preload progress: loaded={loaded}")
        else:
            if self._log is not None:
                self._log("person index preload source=vault People/ only")
            for rel_path, _ in iter_notes(self.vault_path):
                if not rel_path.parts or rel_path.parts[0] != "People":
                    continue
                frontmatter, _, _ = read_note(self.vault_path, str(rel_path))
                data = self._person_record_from_frontmatter(str(rel_path), frontmatter)
                if data is None:
                    skipped += 1
                    continue
                self.upsert(f"[[{rel_path.stem}]]", data)
                loaded += 1
                if self._log is not None and self._progress_every and loaded % self._progress_every == 0:
                    self._log(f"person index preload progress: loaded={loaded}")
        if self._log is not None:
            self._log(f"person index preload done: loaded={loaded} skipped={skipped}")

    def _person_record_from_frontmatter(self, rel: str, frontmatter: dict[str, Any]) -> dict[str, Any] | None:
        try:
            return validate_card_permissive(frontmatter).model_dump(mode="python")
        except ValidationError as exc:
            if self._log is not None:
                self._log(f"person index skip invalid card rel={rel} errors={exc.error_count()}")
            return None

    def _remove_indexes(self, wikilink: str) -> None:
        old = self.records.get(wikilink)
        if not old:
            return
        first, last = _name_parts(old)
        if last and last in self.by_last_name:
            self.by_last_name[last].discard(wikilink)
        if first and last and (last, first[:1]) in self.by_first_initial_last:
            self.by_first_initial_last[(last, first[:1])].discard(wikilink)

    def upsert(self, wikilink: str, data: dict[str, Any]) -> None:
        self._remove_indexes(wikilink)
        self.records[wikilink] = data
        first, last = _name_parts(data)
        if last:
            self.by_last_name.setdefault(last, set()).add(wikilink)
        if first and last:
            self.by_first_initial_last.setdefault((last, first[:1]), set()).add(wikilink)

    def candidates(self, identifiers: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
        first, last = _name_parts(identifiers)
        candidate_links: set[str] = set()
        if last:
            candidate_links.update(self.by_last_name.get(last, set()))
            if first:
                candidate_links.update(self.by_first_initial_last.get((last, first[:1]), set()))
        if not candidate_links:
            candidate_links = set(self.records)
        return [(wikilink, self.records[wikilink]) for wikilink in candidate_links]

    def snapshot(self) -> PersonIndexSnapshot:
        return PersonIndexSnapshot(
            records={wikilink: dict(data) for wikilink, data in self.records.items()},
            by_last_name={key: tuple(sorted(value)) for key, value in self.by_last_name.items()},
            by_first_initial_last={key: tuple(sorted(value)) for key, value in self.by_first_initial_last.items()},
        )


def load_nicknames(vault_path: str | Path) -> dict[str, list[str]]:
    """Load nickname mappings from vault metadata."""

    path = Path(vault_path) / "_meta" / "nicknames.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        normalize_person_name(str(canonical)): [
            normalize_person_name(str(alias)) for alias in aliases if str(alias).strip()
        ]
        for canonical, aliases in payload.items()
        if isinstance(aliases, list)
    }


def normalize_person_name(name: str) -> str:
    """Lowercase, strip punctuation, and collapse whitespace."""

    normalized = re.sub(r"[^\w\s]", " ", name.lower().strip())
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _canonicalize_name(name: str, nicknames: dict[str, list[str]]) -> str:
    tokens = normalize_person_name(name).split()
    if not tokens:
        return ""

    alias_to_canonical = {canonical: canonical for canonical in nicknames}
    for canonical, aliases in nicknames.items():
        for alias in aliases:
            alias_to_canonical[alias] = canonical
    tokens[0] = alias_to_canonical.get(tokens[0], tokens[0])
    return " ".join(tokens)


def _name_variants(name: str, nicknames: dict[str, list[str]]) -> list[str]:
    normalized = normalize_person_name(name)
    canonicalized = _canonicalize_name(name, nicknames)
    variants = [value for value in {normalized, canonicalized} if value]
    return variants


def _as_list(data: dict[str, Any], field: str) -> list[str]:
    value = data.get(field, [])
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _string_value(data: dict[str, Any], field: str) -> str:
    value = data.get(field, "")
    return str(value).strip() if value is not None else ""


def _name_parts(data: dict[str, Any]) -> tuple[str, str]:
    first = normalize_person_name(_string_value(data, "first_name"))
    last = normalize_person_name(_string_value(data, "last_name"))
    if first and last:
        return first, last
    summary = normalize_person_name(str(data.get("summary") or data.get("name") or ""))
    tokens = summary.split()
    if not first and tokens:
        first = tokens[0]
    if not last and len(tokens) > 1:
        last = tokens[-1]
    return first, last


def _name_candidates(data: dict[str, Any]) -> list[str]:
    values = [
        _string_value(data, "summary"),
        _string_value(data, "name"),
        *(_as_list(data, "aliases")),
    ]
    first, last = _name_parts(data)
    if first or last:
        values.append(" ".join(part for part in [first, last] if part))
    return [value for value in dict.fromkeys(value for value in values if value)]


def _company_values(data: dict[str, Any]) -> list[str]:
    values = [_string_value(data, "company"), *(_as_list(data, "companies"))]
    return [value for value in dict.fromkeys(value for value in values if value)]


def _title_values(data: dict[str, Any]) -> list[str]:
    values = [_string_value(data, "title"), *(_as_list(data, "titles"))]
    return [value for value in dict.fromkeys(value for value in values if value)]


def _social_value(data: dict[str, Any], field: str) -> str:
    return _string_value(data, field).lower()


def _email_domains(data: dict[str, Any]) -> set[str]:
    return {item.split("@", 1)[1] for item in _as_list(data, "emails") if "@" in item}


def _resolve_from_alias_entries(entries: dict[str, str], prefix: str, value: str) -> str | None:
    normalized = _normalize_identifier(prefix, value)
    if not normalized:
        return None
    return entries.get(f"{prefix}:{normalized}")


def _snapshot_candidates(
    snapshot: PersonIndexSnapshot,
    identifiers: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    first, last = _name_parts(identifiers)
    candidate_links: set[str] = set()
    if last:
        candidate_links.update(snapshot.by_last_name.get(last, ()))
        if first:
            candidate_links.update(snapshot.by_first_initial_last.get((last, first[:1]), ()))
    if not candidate_links:
        candidate_links = set(snapshot.records)
    return [(wikilink, snapshot.records[wikilink]) for wikilink in candidate_links if wikilink in snapshot.records]


def _best_name_match(
    candidate: dict[str, Any],
    existing: dict[str, Any],
    nicknames: dict[str, list[str]],
) -> tuple[bool, float]:
    best_match = False
    best_score = 0.0
    for left in _name_candidates(candidate):
        for right in _name_candidates(existing):
            matched, score = names_match(left, right, nicknames)
            if score > best_score:
                best_score = score
                best_match = matched
    return best_match, best_score


def _best_similarity(values_a: list[str], values_b: list[str]) -> float:
    best = 0.0
    for left in values_a:
        normalized_left = normalize_person_name(left)
        if not normalized_left:
            continue
        for right in values_b:
            normalized_right = normalize_person_name(right)
            if not normalized_right:
                continue
            best = max(best, float(fuzz.token_sort_ratio(normalized_left, normalized_right)))
    return best


def _has_exact_social_match(candidate: dict[str, Any], existing: dict[str, Any]) -> tuple[bool, str]:
    for field in ("linkedin", "github", "twitter", "instagram", "telegram", "discord"):
        candidate_value = _social_value(candidate, field)
        existing_value = _social_value(existing, field)
        if candidate_value and existing_value and candidate_value == existing_value:
            return True, field
    return False, ""


def _candidate_is_plausible(
    candidate: dict[str, Any],
    existing: dict[str, Any],
    nicknames: dict[str, list[str]],
    config: PPAConfig,
) -> bool:
    exact_social, _ = _has_exact_social_match(candidate, existing)
    if exact_social:
        return True
    candidate_first, candidate_last = _name_parts(candidate)
    existing_first, existing_last = _name_parts(existing)
    if candidate_last and existing_last and candidate_last != existing_last:
        return False
    if candidate_first and existing_first:
        if candidate_first == existing_first:
            return True
        candidate_canonical = _canonicalize_name(candidate_first, nicknames)
        existing_canonical = _canonicalize_name(existing_first, nicknames)
        if candidate_canonical and candidate_canonical == existing_canonical:
            return True
        if candidate_first[:1] and candidate_first[:1] == existing_first[:1] and candidate_last and existing_last:
            return True
    name_match, name_score = _best_name_match(candidate, existing, nicknames)
    if name_match and name_score >= config.fuzzy_name_threshold + 5:
        return True
    return False


def names_match(a: str, b: str, nicknames: dict[str, list[str]]) -> tuple[bool, float]:
    """Compare names using exact, nickname-expanded, and fuzzy matching."""

    left = normalize_person_name(a)
    right = normalize_person_name(b)
    if not left or not right:
        return False, 0.0
    if left == right:
        return True, 100.0

    left_canonical = _canonicalize_name(left, nicknames)
    right_canonical = _canonicalize_name(right, nicknames)
    if left_canonical and left_canonical == right_canonical:
        return True, 95.0

    score = float(fuzz.token_sort_ratio(left, right))
    return score >= 85.0, score


def is_same_person(
    candidate: dict[str, Any],
    existing: dict[str, Any],
    nicknames: dict[str, list[str]],
    config: PPAConfig | None = None,
) -> tuple[bool, int, list[str]]:
    """Score whether two person payloads likely refer to the same human."""

    config = config or PPAConfig()
    reasons: list[str] = []
    confidence = 0
    support_score = 0

    exact_social, social_field = _has_exact_social_match(candidate, existing)
    if exact_social:
        confidence += 95
        reasons.append(f"exact_{social_field}")
        support_score += 95

    name_match, name_score = _best_name_match(candidate, existing, nicknames)
    if name_match:
        if name_score >= 100:
            confidence += 80
            reasons.append("exact_name")
        elif name_score >= 95:
            confidence += 70
            reasons.append("nickname_name")
        elif name_score >= config.fuzzy_name_threshold:
            confidence += 50 if name_score < 90 else 60
            reasons.append("fuzzy_name")

    from archive_vault.canon.email import canonical as _email
    from archive_vault.canon.phone import canonical as _phone

    candidate_emails = [_email(item) for item in _as_list(candidate, "emails")]
    existing_emails = [_email(item) for item in _as_list(existing, "emails")]
    if set(filter(None, candidate_emails)) & set(filter(None, existing_emails)):
        confidence += 100
        reasons.append("exact_email")
        support_score += 100
    candidate_phones = {_phone(item) for item in _as_list(candidate, "phones")} - {""}
    existing_phones = {_phone(item) for item in _as_list(existing, "phones")} - {""}
    if candidate_phones & existing_phones:
        confidence += 100
        reasons.append("exact_phone")
        support_score += 100

    if _email_domains(candidate) & _email_domains(existing):
        confidence += 15
        reasons.append("same_email_domain")
        support_score += 15

    company_score = _best_similarity(_company_values(candidate), _company_values(existing))
    if company_score >= 90:
        confidence += 20
        reasons.append("same_company")
        support_score += 20
    elif company_score >= 75:
        confidence += 10
        reasons.append("close_company")
        support_score += 10

    title_score = _best_similarity(_title_values(candidate), _title_values(existing))
    if title_score >= 90:
        confidence += 10
        reasons.append("same_title")
        support_score += 10
    elif title_score >= 75:
        confidence += 5
        reasons.append("close_title")
        support_score += 5

    if not exact_social and not name_match:
        return False, min(confidence, 100), reasons

    confidence = min(confidence, 100)
    if "fuzzy_name" in reasons and support_score == 0:
        return False, confidence, reasons
    return confidence >= config.conflict_threshold, confidence, reasons


def _is_unnamed_stub(data: dict[str, Any]) -> bool:
    first, last = _name_parts(data)
    if first or last:
        return False
    summary = str(data.get("summary") or data.get("name") or "").strip()
    if not summary:
        return True
    if "@" in summary:
        return True
    from archive_vault.canon.phone import canonical as canon_phone

    if canon_phone(summary):
        return True
    return not looks_like_person_name(summary)


def _names_safe_for_auto_merge(
    candidate: dict[str, Any],
    existing: dict[str, Any],
    nicknames: dict[str, list[str]],
) -> bool:
    if _is_unnamed_stub(candidate) or _is_unnamed_stub(existing):
        return True
    name_match, name_score = _best_name_match(candidate, existing, nicknames)
    if name_match and name_score >= 95:
        return True
    candidate_first, candidate_last = _name_parts(candidate)
    existing_first, existing_last = _name_parts(existing)
    if (
        candidate_first
        and existing_first
        and candidate_last
        and existing_last
        and candidate_first == existing_first
        and candidate_last == existing_last
    ):
        return True
    if candidate_last and existing_last and candidate_last == existing_last and candidate_first and existing_first:
        if _canonicalize_name(candidate_first, nicknames) == _canonicalize_name(existing_first, nicknames):
            return True
    return False


def _record_for_wikilink(
    wikilink: str,
    *,
    records: dict[str, dict[str, Any]] | None,
    candidate_people: list[tuple[str, dict[str, Any]]],
) -> dict[str, Any] | None:
    if records and wikilink in records:
        return records[wikilink]
    for link, data in candidate_people:
        if link == wikilink:
            return data
    return None


def _identifier_auto_merge_or_review(
    identifiers: dict[str, Any],
    *,
    match: str,
    reason: str,
    nicknames: dict[str, list[str]],
    records: dict[str, dict[str, Any]] | None,
    candidate_people: list[tuple[str, dict[str, Any]]],
) -> ResolveResult:
    existing = _record_for_wikilink(match, records=records, candidate_people=candidate_people)
    if existing is None or _names_safe_for_auto_merge(identifiers, existing, nicknames):
        return ResolveResult("merge", match, 100, [reason])
    return _auto_approve_if_confident(ResolveResult("conflict", match, 100, [reason, "name_conflict"]))


def _create_or_skip(identifiers: dict[str, Any]) -> ResolveResult:
    emails = _as_list(identifiers, "emails")
    phones = _as_list(identifiers, "phones")
    if not emails and not phones:
        return ResolveResult("skip", None, 0, ["no_identifier"])
    summary = str(identifiers.get("summary") or identifiers.get("name") or "").strip()
    first, last = _name_parts(identifiers)
    composed = " ".join(part for part in [first, last] if part)
    if looks_like_person_name(summary) or looks_like_person_name(composed):
        return ResolveResult("create", None, 0, ["no_match"])
    return ResolveResult("skip", None, 0, ["implausible_name"])


def _resolve_person_from_candidates(
    identifiers: dict[str, Any],
    *,
    resolver,
    candidate_people: list[tuple[str, dict[str, Any]]],
    nicknames: dict[str, list[str]],
    config: PPAConfig,
    records: dict[str, dict[str, Any]] | None = None,
) -> ResolveResult:
    for email in _as_list(identifiers, "emails"):
        if match := resolver("email", email):
            return _identifier_auto_merge_or_review(
                identifiers,
                match=match,
                reason="exact_email",
                nicknames=nicknames,
                records=records,
                candidate_people=candidate_people,
            )
    for phone in _as_list(identifiers, "phones"):
        if match := resolver("phone", phone):
            return _identifier_auto_merge_or_review(
                identifiers,
                match=match,
                reason="exact_phone",
                nicknames=nicknames,
                records=records,
                candidate_people=candidate_people,
            )
    for field in ("linkedin", "github", "twitter", "instagram", "telegram", "discord"):
        for value in _as_list(identifiers, field):
            if match := resolver(field, value):
                return _identifier_auto_merge_or_review(
                    identifiers,
                    match=match,
                    reason=f"exact_{field}",
                    nicknames=nicknames,
                    records=records,
                    candidate_people=candidate_people,
                )

    best: ResolveResult | None = None
    for wikilink, existing in candidate_people:
        if not _candidate_is_plausible(identifiers, existing, nicknames, config):
            continue
        is_match, confidence, reasons = is_same_person(identifiers, existing, nicknames, config=config)
        if not is_match and confidence < config.conflict_threshold:
            continue
        candidate = ResolveResult("conflict", wikilink, confidence, reasons or ["close_name"])
        if best is None or candidate.confidence > best.confidence:
            best = candidate

    if best and best.confidence >= config.conflict_threshold:
        if "fuzzy_name" not in best.reasons and "close_name" not in best.reasons:
            best = ResolveResult("conflict", best.wikilink, best.confidence, [*best.reasons, "close_name"])
        return _auto_approve_if_confident(best)
    return _create_or_skip(identifiers)


def resolve_person(
    vault_path: str | Path,
    identifiers: dict[str, Any],
    cache: IdentityCache | None = None,
    people_index: PersonIndex | None = None,
    *,
    nicknames: dict[str, list[str]] | None = None,
    config: PPAConfig | None = None,
) -> ResolveResult:
    """Resolve a person to merge/create/conflict using aliases plus fuzzy matching."""

    vault_path = Path(vault_path)
    nicknames = nicknames if nicknames is not None else load_nicknames(vault_path)
    config = config or load_config(vault_path)

    resolver = cache.resolve if cache is not None else lambda prefix, value: resolve_any(vault_path, prefix, value)
    candidate_people = (
        people_index.candidates(identifiers)
        if people_index is not None
        else [
            (
                f"[[{rel_path.stem}]]",
                validate_card_permissive(read_note(vault_path, str(rel_path))[0]).model_dump(mode="python"),
            )
            for rel_path, _ in iter_notes(vault_path)
            if rel_path.parts and rel_path.parts[0] == "People"
        ]
    )
    result = _resolve_person_from_candidates(
        identifiers,
        resolver=resolver,
        candidate_people=candidate_people,
        nicknames=nicknames,
        config=config,
        records=people_index.records if people_index is not None else None,
    )
    if result.wikilink:
        from archive_vault.identity import canonicalize_wikilink, load_identity_map

        canonical = canonicalize_wikilink(
            load_identity_map(vault_path) if cache is None else cache.entries, result.wikilink
        )
        if canonical and canonical != result.wikilink:
            return ResolveResult(result.action, canonical, result.confidence, [*result.reasons, "identity_redirect"])
    return result


def resolve_person_batch(
    vault_path: str | Path,
    identifiers_batch: list[dict[str, Any]],
    *,
    cache: IdentityCache | None = None,
    people_index: PersonIndex | None = None,
    nicknames: dict[str, list[str]] | None = None,
    config: PPAConfig | None = None,
) -> list[ResolveResult]:
    """Run :func:`resolve_person` for each identifier dict, sharing cache and index."""

    vault_path = Path(vault_path)
    nicknames = nicknames if nicknames is not None else load_nicknames(vault_path)
    config = config or load_config(vault_path)
    cache = cache or IdentityCache(vault_path)
    people_index = people_index or PersonIndex(vault_path, preload=True)
    return [
        resolve_person(
            vault_path,
            identifiers,
            cache=cache,
            people_index=people_index,
            nicknames=nicknames,
            config=config,
        )
        for identifiers in identifiers_batch
    ]


def resolve_person_snapshot(
    identifiers: dict[str, Any],
    *,
    alias_entries: dict[str, str],
    people_snapshot: PersonIndexSnapshot,
    nicknames: dict[str, list[str]],
    config: PPAConfig,
) -> ResolveResult:
    return _resolve_person_from_candidates(
        identifiers,
        resolver=lambda prefix, value: _resolve_from_alias_entries(alias_entries, prefix, value),
        candidate_people=_snapshot_candidates(people_snapshot, identifiers),
        nicknames=nicknames,
        config=config,
        records=people_snapshot.records,
    )


def _union_preserve_order(existing: list[Any], incoming: list[Any]) -> list[Any]:
    merged: list[Any] = []
    seen: set[Any] = set()
    for item in [*existing, *incoming]:
        marker = item
        if marker in seen:
            continue
        seen.add(marker)
        merged.append(item)
    return merged


def _merge_body(existing_body: str, incoming_body: str) -> tuple[str, bool]:
    existing = existing_body.strip()
    incoming = incoming_body.strip()
    if not incoming:
        return existing_body, False
    if not existing:
        return incoming, True
    if incoming in existing:
        return existing_body, False
    return f"{existing}\n\n{incoming}", True


def _source_rank(source: str, field_name: str) -> int:
    if field_name in {"company", "title"}:
        if source == "linkedin":
            return 3
        if source.startswith("contacts"):
            return 2
        return 1
    if field_name == "summary":
        if source.startswith("contacts"):
            return 3
        if source == "linkedin":
            return 2
    return 1


def _text_quality(value: Any) -> int:
    text = str(value).strip()
    if not text:
        return 0
    return len(re.findall(r"[A-Za-z0-9]", text)) + (len(text.split()) * 2) - len(re.findall(r"[^\w\s&/.,'-]", text))


def _clone_provenance(entry: ProvenanceEntry) -> ProvenanceEntry:
    return ProvenanceEntry(
        source=entry.source,
        date=entry.date,
        method=entry.method,
        model=entry.model,
        enrichment_version=entry.enrichment_version,
        input_hash=entry.input_hash,
    )


def _source_from_incoming(new_data: dict[str, Any]) -> str:
    source = new_data.get("source")
    if isinstance(source, list) and source:
        return str(source[0] or "").strip() or "contacts.apple"
    if isinstance(source, str) and source.strip():
        return source.strip()
    return "contacts.apple"


def provenance_from_incoming(new_data: dict[str, Any]) -> dict[str, ProvenanceEntry]:
    """Build deterministic provenance for non-empty incoming person fields."""

    source = _source_from_incoming(new_data)
    today = date.today().isoformat()
    out: dict[str, ProvenanceEntry] = {}
    for field_name, value in new_data.items():
        if field_name in PROVENANCE_EXEMPT_FIELDS:
            continue
        if value in ("", [], None, 0):
            continue
        out[field_name] = ProvenanceEntry(source, today, "deterministic")
    return out


def _backfill_missing_provenance(
    card_data: dict[str, Any],
    provenance: dict[str, ProvenanceEntry],
    new_data: dict[str, Any],
) -> dict[str, ProvenanceEntry]:
    """Fill provenance for populated fields so write_card does not reject the merge."""

    filled = dict(provenance)
    fallback = provenance_from_incoming(new_data)
    today = date.today().isoformat()
    source = _source_from_incoming(new_data)
    for field_name, value in card_data.items():
        if field_name in PROVENANCE_EXEMPT_FIELDS:
            continue
        if value in ("", [], None, 0):
            continue
        if field_name in filled:
            continue
        filled[field_name] = fallback.get(field_name) or ProvenanceEntry(source, today, "deterministic")
    return filled


def _should_replace_scalar(
    field_name: str,
    existing_value: Any,
    incoming_value: Any,
    existing_source: str,
    incoming_source: str,
) -> bool:
    if existing_value in ("", [], None, 0):
        return incoming_value not in ("", [], None, 0)
    if incoming_value in ("", [], None, 0):
        return False
    if field_name in {"company", "title"}:
        if _source_rank(incoming_source, field_name) > _source_rank(existing_source, field_name):
            return True
        return _text_quality(incoming_value) > _text_quality(existing_value) + 4
    if field_name == "summary":
        existing_text = str(existing_value).strip()
        incoming_text = str(incoming_value).strip()
        if "@" in existing_text and "@" not in incoming_text:
            return True
        if existing_text.lower() == "unknown":
            return True
        if (
            existing_source == "linkedin"
            and incoming_source.startswith("contacts")
            and _text_quality(incoming_text) >= _text_quality(existing_text)
        ):
            return True
        return False
    if field_name == "emails_seen_count":
        return int(incoming_value) > int(existing_value)
    return False


def merge_into_existing(
    vault_path: str | Path,
    wikilink: str,
    new_data: dict[str, Any],
    new_provenance: dict[str, ProvenanceEntry],
    new_body: str = "",
    *,
    identity_cache: IdentityCache | None = None,
    target_rel_path: str | Path | None = None,
) -> str | None:
    """Merge incoming data into an existing person card."""

    from archive_vault.identity import canonicalize_wikilink, load_identity_map, redirect_target_uid

    vault_root = Path(vault_path)
    canonical_link = canonicalize_wikilink(load_identity_map(vault_root), wikilink) or wikilink
    if target_rel_path is not None:
        target = vault_root / Path(target_rel_path)
        if not target.exists():
            return None
        frontmatter_probe, _, _ = read_note(vault_root, str(target.relative_to(vault_root)))
        redirect_uid = str(frontmatter_probe.get("redirect_to") or "") or redirect_target_uid(
            vault_root, str(frontmatter_probe.get("uid") or "")
        )
        if redirect_uid:
            redirected = read_note_by_uid(vault_root, redirect_uid)
            if redirected is not None:
                target = vault_root / redirected[0]
                wikilink = canonical_link
    else:
        slug = canonical_link.removeprefix("[[").removesuffix("]]")
        target = find_note_by_slug(vault_root, slug)
    if target is None:
        return None

    frontmatter, body, existing_provenance = read_note(vault_root, str(target.relative_to(vault_root)))
    existing_card = validate_card_permissive(frontmatter)
    merged_data = existing_card.model_dump(mode="python")
    if merged_data.get("company") and not merged_data.get("companies"):
        merged_data["companies"] = [merged_data["company"]]
        if "company" in existing_provenance and "companies" not in existing_provenance:
            existing_provenance["companies"] = existing_provenance["company"]
    if merged_data.get("title") and not merged_data.get("titles"):
        merged_data["titles"] = [merged_data["title"]]
        if "title" in existing_provenance and "titles" not in existing_provenance:
            existing_provenance["titles"] = existing_provenance["title"]
    changed = False
    changed_fields: set[str] = set()
    existing_summary = str(merged_data.get("summary", "")).strip()
    incoming_summary = str(new_data.get("summary", "")).strip()
    incoming_emails = [str(item).strip() for item in _as_list(new_data, "emails") if str(item).strip()]
    if (
        incoming_summary
        and existing_summary
        and normalize_person_name(incoming_summary) != normalize_person_name(existing_summary)
        and alias_is_trustworthy(incoming_summary, incoming_emails=incoming_emails)
    ):
        if "aliases" not in new_provenance and "summary" in new_provenance:
            new_provenance = {**new_provenance, "aliases": _clone_provenance(new_provenance["summary"])}
        merged_aliases = _union_preserve_order(merged_data.get("aliases", []), [incoming_summary])
        if merged_aliases != merged_data.get("aliases", []):
            merged_data["aliases"] = merged_aliases
            changed = True
            changed_fields.add("aliases")

    for field_name, incoming_value in new_data.items():
        if field_name not in merged_data:
            continue
        existing_value = merged_data[field_name]
        if isinstance(existing_value, list) and isinstance(incoming_value, list):
            if field_name == "emails":
                incoming_value = [
                    item for item in incoming_value if not is_shared_mailbox_email(str(item))
                ]
            elif field_name == "aliases":
                incoming_value = [
                    item
                    for item in incoming_value
                    if alias_is_trustworthy(str(item), incoming_emails=incoming_emails)
                ]
            merged_value = _union_preserve_order(existing_value, incoming_value)
            if merged_value != existing_value:
                merged_data[field_name] = merged_value
                changed = True
                changed_fields.add(field_name)
            continue
        if field_name == "updated":
            continue
        existing_source = existing_provenance.get(field_name, ProvenanceEntry("", "", "")).source
        incoming_source = new_provenance.get(field_name, ProvenanceEntry("", "", "")).source
        if _should_replace_scalar(field_name, existing_value, incoming_value, existing_source, incoming_source):
            merged_data[field_name] = incoming_value
            changed = True
            changed_fields.add(field_name)

    merged_body, body_changed = _merge_body(body, new_body)
    if body_changed:
        changed = True

    if changed:
        merged_data["updated"] = date.today().isoformat()
    if not changed:
        return None

    merged_card = validate_card_strict(merged_data)
    merged_prov = dict(existing_provenance)
    for field_name in changed_fields:
        if field_name in new_provenance:
            merged_prov[field_name] = new_provenance[field_name]
    if merged_data.get("aliases") and "aliases" not in merged_prov:
        if "summary" in merged_prov:
            merged_prov["aliases"] = _clone_provenance(merged_prov["summary"])
        elif "summary" in new_provenance:
            merged_prov["aliases"] = _clone_provenance(new_provenance["summary"])
    merged_prov = _backfill_missing_provenance(merged_card.model_dump(mode="python"), merged_prov, new_data)
    write_card(vault_root, str(target.relative_to(vault_root)), merged_card, body=merged_body, provenance=merged_prov)
    aliases = {
        "name": merged_card.summary,
        "emails": getattr(merged_card, "emails", []),
        "phones": getattr(merged_card, "phones", []),
        "github": getattr(merged_card, "github", ""),
        "linkedin": getattr(merged_card, "linkedin", ""),
        "twitter": getattr(merged_card, "twitter", ""),
        "instagram": getattr(merged_card, "instagram", ""),
        "telegram": getattr(merged_card, "telegram", ""),
        "discord": getattr(merged_card, "discord", ""),
    }
    if identity_cache is not None:
        identity_cache.upsert(wikilink, aliases)
    else:
        upsert_identity_map(vault_path, wikilink, aliases)
    return str(target)


def log_conflict(
    vault_path: str | Path,
    incoming: dict[str, Any],
    existing_wikilink: str,
    confidence: int,
    reasons: list[str],
) -> None:
    """Append a possible duplicate to the dedup candidate log."""

    path = Path(vault_path) / "_meta" / "dedup-candidates.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            data = []
    except (OSError, json.JSONDecodeError):
        data = []

    data.append(
        {
            "timestamp": datetime.now().isoformat(),
            "incoming": incoming,
            "existing": existing_wikilink,
            "confidence": confidence,
            "reasons": reasons,
        }
    )

    fd, tmp_path = tempfile.mkstemp(prefix="dedup-candidates-", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def queue_identity_review(
    vault_path: str | Path,
    *,
    incoming: dict[str, Any],
    existing_wikilink: str,
    existing_uid: str,
    confidence: int,
    reasons: list[str],
    would_have_auto_merged: bool = False,
) -> dict[str, Any]:
    """Queue a fuzzy or close match for human review in identity-proposals."""

    from archive_engine.journaled_state import (
        IDENTITY_PROPOSALS_REL,
        IDENTITY_PROPOSALS_UID,
        load_json_state,
        merge_identity_proposals,
        persist_json_state,
    )
    from archive_vault.canon import phone as canon_phone
    from archive_vault.canon.email import canonical as canon_email

    incoming_uid = str(incoming.get("uid") or "").strip()
    existing_uid = str(existing_uid or "").strip()
    proposal = {
        "uids": [uid for uid in (incoming_uid, existing_uid) if uid],
        "reason": ",".join(reasons) if reasons else "close_match",
        "status": "open",
        "confidence": confidence,
        "reasons": list(reasons),
        "existing_wikilink": existing_wikilink,
        "would_have_auto_merged": bool(would_have_auto_merged),
        "incoming": {
            "uid": incoming_uid,
            "summary": str(incoming.get("summary") or incoming.get("name") or ""),
            "emails": [canon_email(item) for item in incoming.get("emails") or [] if canon_email(str(item))],
            "phones": [canon_phone.canonical(str(item)) for item in incoming.get("phones") or [] if canon_phone.canonical(str(item))],
            "phones_original": [str(item) for item in incoming.get("phones") or []],
            "company": str(incoming.get("company") or ""),
        },
    }
    if len(proposal["uids"]) < 2:
        if incoming_uid and existing_wikilink:
            proposal["uids"] = [incoming_uid, existing_uid or existing_wikilink]
        else:
            return proposal
    vault = Path(vault_path)
    existing_payload = load_json_state(vault, IDENTITY_PROPOSALS_REL)
    existing = list(existing_payload.get("proposals") or [])
    merged = merge_identity_proposals(existing, [proposal])
    # Keep rich fields from the newest proposal with the same uid pair.
    by_id = {str(item.get("proposal_id") or ""): item for item in merged}
    pair = tuple(sorted(proposal["uids"][:2]))
    key = f"{pair[0]}::{pair[1]}"
    by_id[key] = {**by_id.get(key, {}), **proposal, "uids": list(pair), "proposal_id": key}
    persist_json_state(
        vault,
        uid=IDENTITY_PROPOSALS_UID,
        rel=IDENTITY_PROPOSALS_REL,
        payload={"proposals": list(by_id.values()), "status": "queued"},
        source="identity-review",
    )
    return proposal
