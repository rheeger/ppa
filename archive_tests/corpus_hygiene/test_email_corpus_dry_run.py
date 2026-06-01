"""Section B — email corpus hygiene dry-run tests (no production mutation, no LLM)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_cli.corpus_hygiene.census import CensusContext, compute_input_hash, run_email_census_dry_run
from archive_cli.corpus_hygiene.classification_reuse import (
    ClassificationReuseLoader,
    EmailThreadRecord,
    ReusedClassification,
    load_card_classifications_from_rows,
    stage0_classification,
)
from archive_cli.corpus_hygiene.review_buckets import assign_review_bucket, bucket_samples
from archive_sync.llm_enrichment.classify_index import ClassifyIndex
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE_RUN_ID = "section-b-dry-run-test"


def _thread(**kwargs: object) -> EmailThreadRecord:
    defaults = {
        "thread_uid": "uid-default",
        "gmail_thread_id": "g-thread-default",
        "account_email": "owner@example.com",
        "source_key": "gmail-messages:owner@example.com",
    }
    defaults.update(kwargs)
    return EmailThreadRecord(**defaults)  # type: ignore[arg-type]


def _classify_db(tmp_path: Path, entries: dict[str, dict[str, object]]) -> Path:
    db = tmp_path / "classify_index.db"
    with ClassifyIndex(db) as idx:
        for tid, row in entries.items():
            idx.put_classification(
                tid,
                str(row.get("category") or "marketing"),
                float(row.get("confidence") or 0.9),
                list(row.get("card_types") or []),
                first_subject=str(row.get("subject") or ""),
            )
    return db


# ---------------------------------------------------------------------------
# Classification precedence
# ---------------------------------------------------------------------------


def test_classification_precedence_card_classifications_wins(tmp_path: Path) -> None:
    thread = _thread(
        thread_uid="uid-1",
        gmail_thread_id="g-1",
        triage_classification="personal",
        triage_confidence=0.99,
    )
    card_rows = [
        {
            "card_uid": "uid-1",
            "classification": "marketing",
            "confidence": 0.91,
            "card_types": [],
        }
    ]
    db = _classify_db(tmp_path, {"g-1": {"category": "automated", "confidence": 0.88}})
    card_map = load_card_classifications_from_rows(card_rows)
    loader = ClassificationReuseLoader(
        card_classifications=card_map,
        classify_index=ClassifyIndex(db),
    )
    hit = loader.resolve(thread)
    assert hit.classification_source == "card_classifications"
    assert hit.classification == "marketing"


def test_classification_precedence_classify_index_over_frontmatter(tmp_path: Path) -> None:
    thread = _thread(
        thread_uid="uid-2",
        gmail_thread_id="g-2",
        triage_classification="personal",
        triage_confidence=0.99,
    )
    db = _classify_db(tmp_path, {"g-2": {"category": "marketing", "confidence": 0.92}})
    loader = ClassificationReuseLoader(classify_index=ClassifyIndex(db))
    hit = loader.resolve(thread)
    assert hit.classification_source == "classify_index"
    assert hit.classification == "marketing"


def test_classification_precedence_frontmatter_over_stage0() -> None:
    thread = _thread(
        thread_uid="uid-3",
        gmail_thread_id="g-3",
        from_emails=("newsletter@mailchimp.com",),
        subject="Weekly deals — 50% off",
        triage_classification="personal",
        triage_confidence=0.88,
    )
    loader = ClassificationReuseLoader()
    hit = loader.resolve(thread)
    assert hit.classification_source == "frontmatter"
    assert hit.classification == "personal"


def test_classification_precedence_stage0_when_no_higher_source() -> None:
    thread = _thread(
        thread_uid="uid-4",
        gmail_thread_id="g-4",
        from_emails=("noreply@facebookmail.com",),
        subject="Someone tagged you",
    )
    loader = ClassificationReuseLoader()
    hit = loader.resolve(thread)
    assert hit.classification_source == "stage0"
    stage0 = stage0_classification(thread.from_emails, (thread.subject,))
    assert stage0 is not None
    assert hit.classification == stage0.classification


# ---------------------------------------------------------------------------
# No LLM when classification reusable
# ---------------------------------------------------------------------------


def test_no_llm_call_when_classification_reusable() -> None:
    called = {"n": 0}

    def _llm(_thread: EmailThreadRecord) -> ReusedClassification:
        called["n"] += 1
        return ReusedClassification("marketing", 0.9, (), "new_llm")

    thread = _thread(
        thread_uid="uid-5",
        gmail_thread_id="g-5",
        triage_classification="marketing",
        triage_confidence=0.91,
        label_ids=("CATEGORY_PROMOTIONS",),
    )
    loader = ClassificationReuseLoader(allow_new_llm=True, llm_classify_fn=_llm)
    hit = loader.resolve(thread)
    assert hit.classification_source == "frontmatter"
    assert called["n"] == 0
    assert loader.new_llm_call_count == 0


def test_missing_classification_without_llm_opt_in() -> None:
    thread = _thread(thread_uid="uid-6", gmail_thread_id="g-6", subject="Hello there")
    loader = ClassificationReuseLoader(allow_new_llm=False)
    hit = loader.resolve(thread)
    assert hit.classification_source == "missing"
    assert loader.new_llm_call_count == 0


# ---------------------------------------------------------------------------
# Deterministic dry-run report
# ---------------------------------------------------------------------------


def _fixture_threads() -> list[EmailThreadRecord]:
    return [
        _thread(
            thread_uid="uid-mkt",
            gmail_thread_id="g-mkt",
            label_ids=("CATEGORY_PROMOTIONS",),
            triage_classification="marketing",
            triage_confidence=0.91,
        ),
        _thread(
            thread_uid="uid-star",
            gmail_thread_id="g-star",
            label_ids=("CATEGORY_PROMOTIONS", "STARRED"),
            triage_classification="marketing",
            triage_confidence=0.91,
        ),
        _thread(
            thread_uid="uid-derived",
            gmail_thread_id="g-derived",
            label_ids=("CATEGORY_PROMOTIONS",),
            triage_classification="marketing",
            triage_confidence=0.91,
            derived_uids=("meal-order-1",),
        ),
        _thread(
            thread_uid="uid-txn",
            gmail_thread_id="g-txn",
            from_emails=("receipts@doordash.com",),
            triage_classification="transactional",
            triage_confidence=0.84,
            triage_card_types=("meal_order",),
        ),
    ]


def test_dry_run_report_is_deterministic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    threads = _fixture_threads()
    ctx = CensusContext(
        decision_run_id=_FIXTURE_RUN_ID,
        deterministic=True,
        engine_mode="n/a",
    )

    r1 = run_email_census_dry_run(threads, context=ctx, repo_root=tmp_path)
    r2 = run_email_census_dry_run(threads, context=ctx, repo_root=tmp_path)

    assert r1.input_hash == r2.input_hash
    assert r1.corpus_counts == r2.corpus_counts
    assert r1.classification_source_counts == r2.classification_source_counts

    sig1 = [
        (
            d.gmail_thread_id,
            d.classification_source,
            d.corpus_decision,
            d.processor_decision,
            d.decision_reason,
        )
        for d in r1.records
    ]
    sig2 = [
        (
            d.gmail_thread_id,
            d.classification_source,
            d.corpus_decision,
            d.processor_decision,
            d.decision_reason,
        )
        for d in r2.records
    ]
    assert sig1 == sig2

    report_path = Path(r1.artifact_paths["report"])
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["new_llm_call_count"] == 0
    assert report["corpus_counts"]["suppressed"] >= 1
    assert report["details"]["safety"]["dry_run_only"] is True


def test_decision_records_include_required_fields() -> None:
    threads = _fixture_threads()
    ctx = CensusContext(decision_run_id=_FIXTURE_RUN_ID, engine_mode="n/a")
    result = run_email_census_dry_run(threads, context=ctx)
    for rec in result.records:
        assert rec.decision_run_id == _FIXTURE_RUN_ID
        assert rec.classification_source
        assert rec.corpus_decision in {"active", "suppressed", "quarantine"}
        assert rec.processor_decision
        assert rec.policy_version == EMAIL_PROMOTION_POLICY_VERSION
        assert rec.applied_at == ""


# ---------------------------------------------------------------------------
# Review buckets
# ---------------------------------------------------------------------------


def test_review_bucket_high_confidence_marketing() -> None:
    threads = [_thread(
        thread_uid="uid-mkt",
        gmail_thread_id="g-mkt",
        label_ids=("CATEGORY_PROMOTIONS",),
        triage_classification="marketing",
        triage_confidence=0.91,
    )]
    result = run_email_census_dry_run(threads, context=CensusContext(decision_run_id=_FIXTURE_RUN_ID))
    buckets = bucket_samples(result.records)
    assert buckets["high_confidence_marketing"]
    assert buckets["high_confidence_marketing"][0]["corpus_decision"] == "suppressed"


def test_review_bucket_quarantine_conflicts() -> None:
    threads = [_thread(
        thread_uid="uid-star",
        gmail_thread_id="g-star",
        label_ids=("CATEGORY_PROMOTIONS", "STARRED"),
        triage_classification="marketing",
        triage_confidence=0.91,
    )]
    result = run_email_census_dry_run(threads, context=CensusContext(decision_run_id=_FIXTURE_RUN_ID))
    rec = result.records[0]
    assert rec.corpus_decision == "quarantine"
    assert assign_review_bucket(rec) == "quarantine_conflicts"
    assert bucket_samples(result.records)["quarantine_conflicts"]


def test_review_bucket_suppressed_with_derived_cards() -> None:
    threads = [_thread(
        thread_uid="uid-derived",
        gmail_thread_id="g-derived",
        label_ids=("CATEGORY_PROMOTIONS",),
        triage_classification="marketing",
        triage_confidence=0.91,
        derived_uids=("meal-order-1",),
    )]
    result = run_email_census_dry_run(threads, context=CensusContext(decision_run_id=_FIXTURE_RUN_ID))
    rec = result.records[0]
    assert rec.corpus_decision == "quarantine"
    assert assign_review_bucket(rec) == "suppressed_with_derived_cards"
    assert bucket_samples(result.records)["suppressed_with_derived_cards"]


def test_input_hash_stable_for_fixed_threads() -> None:
    threads = _fixture_threads()
    h1 = compute_input_hash(threads, policy_version=EMAIL_PROMOTION_POLICY_VERSION)
    h2 = compute_input_hash(list(reversed(threads)), policy_version=EMAIL_PROMOTION_POLICY_VERSION)
    assert h1 == h2
