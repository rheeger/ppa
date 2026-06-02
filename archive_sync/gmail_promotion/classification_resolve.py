"""Section C classification precedence (includes email_corpus_decisions)."""

from __future__ import annotations

from typing import Any, Callable

from archive_cli.corpus_hygiene.classification_reuse import (
    ClassificationReuseLoader,
    EmailThreadRecord,
    ReusedClassification,
    _hit,
    _parse_card_types,
    stage0_classification,
)
from archive_cli.corpus_hygiene.decisions import EmailCorpusDecisionRecord
from archive_sync.llm_enrichment.classify_index import ClassifyIndex
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION


class GmailClassificationResolver:
    """Resolve classification for sync using Section C precedence."""

    def __init__(
        self,
        *,
        corpus_decisions: dict[str, EmailCorpusDecisionRecord] | None = None,
        card_classifications: dict[str, ReusedClassification] | None = None,
        classify_index: ClassifyIndex | None = None,
        allow_new_llm: bool = False,
        llm_classify_fn: Callable[[EmailThreadRecord], ReusedClassification | None] | None = None,
        user_domains: frozenset[str] | None = None,
    ) -> None:
        self._corpus_by_thread = corpus_decisions or {}
        self._fallback = ClassificationReuseLoader(
            card_classifications=card_classifications,
            classify_index=classify_index,
            allow_new_llm=allow_new_llm,
            llm_classify_fn=llm_classify_fn,
            user_domains=user_domains,
        )
        self.source_counts: dict[str, int] = {}
        self.new_llm_call_count = 0

    def resolve(self, thread: EmailThreadRecord) -> ReusedClassification:
        hit = self._resolve_inner(thread)
        src = hit.classification_source
        self.source_counts[src] = self.source_counts.get(src, 0) + 1
        if src == "new_llm":
            self.new_llm_call_count += 1
        return hit

    def _resolve_inner(self, thread: EmailThreadRecord) -> ReusedClassification:
        tid = thread.gmail_thread_id.strip()
        sha = thread.thread_body_sha.strip()
        if tid and tid in self._corpus_by_thread:
            rec = self._corpus_by_thread[tid]
            if (
                rec.policy_version == EMAIL_PROMOTION_POLICY_VERSION
                and (not sha or not rec.thread_body_sha or rec.thread_body_sha == sha)
                and rec.classification_source
            ):
                payload = rec.to_dict() if hasattr(rec, "to_dict") else {}
                return _hit(
                    str(rec.classification or payload.get("classification") or ""),
                    float(rec.confidence),
                    tuple(rec.card_types),
                    "email_corpus_decisions",
                    classify_prompt_version=str(rec.classify_prompt_version or ""),
                    classify_model=str(rec.classify_model or ""),
                )

        return self._fallback.resolve(thread)


def corpus_decisions_index(
    records: list[EmailCorpusDecisionRecord],
) -> dict[str, EmailCorpusDecisionRecord]:
    out: dict[str, EmailCorpusDecisionRecord] = {}
    for rec in records:
        tid = rec.gmail_thread_id.strip()
        if tid:
            out[tid] = rec
    return out


def load_card_classifications_from_index_rows(rows: list[dict[str, Any]]) -> dict[str, ReusedClassification]:
    from archive_cli.corpus_hygiene.classification_reuse import load_card_classifications_from_rows

    return load_card_classifications_from_rows(rows)
