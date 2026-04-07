"""Base abstractions for email-derived card extraction."""

from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.extractors.field_metrics import compute_extraction_confidence
from archive_sync.extractors.field_validation import SYSTEM_FIELDS, validate_field
from hfa.provenance import ProvenanceEntry
from hfa.schema import CARD_TYPES, BaseCard

_BODY_KEY = "_body"
_DISCRIMINATOR_KEY = "_discriminator"


def _sent_date_string(frontmatter: dict[str, Any]) -> str:
    """Return YYYY-MM-DD from email sent_at (or today)."""
    sent = str(frontmatter.get("sent_at") or "").strip()
    if len(sent) >= 10 and sent[4] == "-" and sent[7] == "-":
        return sent[:10]
    return date.today().isoformat()


def _parse_iso_date(value: str) -> date | None:
    value = (value or "").strip()
    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _in_date_range(sent_at: str, start_iso: str, end_iso: str) -> bool:
    """Inclusive date range on calendar dates."""
    d = _parse_iso_date(sent_at)
    start = _parse_iso_date(start_iso)
    end = _parse_iso_date(end_iso)
    if d is None or start is None or end is None:
        return True
    return start <= d <= end


@dataclass(frozen=True)
class TemplateVersion:
    """A parser tied to a date range when a sender's email format was active."""

    name: str
    date_range: tuple[str, str]  # (start_iso, end_iso) inclusive
    parser: Callable[[dict[str, Any], str], list[dict[str, Any]]]


@dataclass(frozen=True)
class ExtractionResult:
    """One derived card produced by an extractor."""

    card: BaseCard
    provenance: dict[str, ProvenanceEntry]
    body: str
    source_email_uid: str
    source_email_rel_path: str
    extraction_confidence: float = 1.0


class EmailExtractor(ABC):
    """Base class for all email extractors."""

    sender_patterns: list[str] = []
    subject_patterns: list[str] = []
    output_card_type: str = ""
    # Reject marketing/promo/account emails (checked on cleaned body).
    reject_subject_patterns: list[str] = []
    receipt_indicators: list[str] = []

    @property
    def extractor_id(self) -> str:
        """CLI --sender filter id (e.g. doordash, uber_rides)."""
        name = self.__class__.__name__
        if name.endswith("Extractor"):
            name = name[: -len("Extractor")]
        return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

    @abstractmethod
    def template_versions(self) -> list[TemplateVersion]:
        """Return template versions ordered newest-first."""

    def _compile_patterns(self, patterns: list[str]) -> list[re.Pattern[str]]:
        return [re.compile(p, re.IGNORECASE) for p in patterns]

    def matches(self, from_email: str, subject: str) -> bool:
        """Check if this extractor handles the given email."""
        from_email = (from_email or "").strip().lower()
        subject = subject or ""
        if not self.sender_patterns:
            return False
        sender_ok = any(p.search(from_email) for p in self._compile_patterns(self.sender_patterns))
        if not sender_ok:
            return False
        if not self.subject_patterns:
            return True
        return any(p.search(subject) for p in self._compile_patterns(self.subject_patterns))

    def should_extract(self, subject: str, body: str) -> bool:
        """Return False to reject marketing/promotional emails.

        Called after matches() returns True (typically from the runner on cleaned body).
        Default: accept all. Subclasses override with specific classification logic.
        """
        subject = subject or ""
        if self.reject_subject_patterns:
            for p in self._compile_patterns(self.reject_subject_patterns):
                if p.search(subject):
                    return False
        if self.receipt_indicators:
            body_lower = body.lower()
            return any(ind.lower() in body_lower for ind in self.receipt_indicators)
        return True

    def _instantiate_card(
        self,
        payload: dict[str, Any],
        *,
        uid: str,
        created: str,
        updated: str,
        source_email_wikilink: str,
    ) -> BaseCard:
        card_type = self.output_card_type
        model_cls = CARD_TYPES[card_type]
        data: dict[str, Any] = {
            **payload,
            "uid": uid,
            "type": card_type,
            "source": ["email_extraction"],
            "source_id": uid,
            "created": created,
            "updated": updated,
            "source_email": source_email_wikilink,
        }
        for key in list(data.keys()):
            if key.startswith("_") or key in SYSTEM_FIELDS:
                continue
            cleaned = validate_field(card_type, key, data[key])
            if cleaned is None:
                data.pop(key, None)
            else:
                data[key] = cleaned
        return model_cls.model_validate(data)

    def extract(
        self,
        frontmatter: dict[str, Any],
        body: str,
        source_uid: str,
        source_rel_path: str,
        *,
        raw_body: str | None = None,
    ) -> list[ExtractionResult]:
        """Try each template version; return first non-empty result. Fall back to summary_only_fallback."""
        _ = raw_body  # reserved for extractors that need original HTML
        subject = str(frontmatter.get("subject") or "")
        if not self.should_extract(subject, body):
            return []
        sent_at = str(frontmatter.get("sent_at") or "")
        source_wikilink = f"[[{source_uid}]]"
        created = updated = _sent_date_string(frontmatter)

        for tv in self.template_versions():
            if not _in_date_range(sent_at, tv.date_range[0], tv.date_range[1]):
                continue
            raw_rows = tv.parser(frontmatter, body)
            if not raw_rows:
                continue
            out: list[ExtractionResult] = []
            for row in raw_rows:
                row_copy = dict(row)
                markdown_body = str(row_copy.pop(_BODY_KEY, "") or "")
                disc = row_copy.pop(_DISCRIMINATOR_KEY, None)
                if disc is None:
                    msg = f"{self.__class__.__name__} parser row missing {_DISCRIMINATOR_KEY!r}"
                    raise ValueError(msg)
                uid = self.generate_derived_uid(source_uid, str(disc))
                card = self._instantiate_card(
                    row_copy,
                    uid=uid,
                    created=created,
                    updated=updated,
                    source_email_wikilink=source_wikilink,
                )
                confidence = compute_extraction_confidence(self.output_card_type, card.model_dump(mode="python"))
                prov = deterministic_provenance(card, "email_extraction")
                out.append(
                    ExtractionResult(
                        card=card,
                        provenance=prov,
                        body=markdown_body,
                        source_email_uid=source_uid,
                        source_email_rel_path=source_rel_path,
                        extraction_confidence=confidence,
                    )
                )
            return out

        return self.summary_only_fallback(frontmatter, body, source_uid, source_rel_path)

    def summary_only_fallback(
        self,
        frontmatter: dict[str, Any],
        body: str,
        source_uid: str,
        source_rel_path: str,
    ) -> list[ExtractionResult]:
        """Default: no card when no template matches.

        Subclasses may override only when the email is definitively a receipt and
        critical fields can be extracted. Generic stub cards are worse than none.
        """
        return []

    def generate_derived_uid(self, source_uid: str, discriminator: str) -> str:
        """Deterministic UID: hfa-{output_card_type}-{sha256(source_uid:discriminator)[:12]}."""
        raw = f"{source_uid}:{discriminator}".encode("utf-8")
        short = hashlib.sha256(raw).hexdigest()[:12]
        return f"hfa-{self.output_card_type}-{short}"
