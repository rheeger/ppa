"""P01-C: EvidenceEnvelope and honest confidence on retrieval responses."""

from __future__ import annotations

from archive_cli.commands.confidence import (
    ASSESSMENT_VERSION,
    CONFIDENCE_MEANING,
    attach_retrieval_envelope,
    client_failure_payload,
    is_client_failure,
)
from archive_engine.contracts import UNKNOWN, EvidenceEnvelope


def golden_zero_hit() -> dict:
    return attach_retrieval_envelope({"rows": []}, query="no-such-card")


def golden_exact() -> dict:
    return attach_retrieval_envelope(
        {"rows": [{"card_uid": "hfa-person-exact", "exact_match": True, "matched_by": "exact"}]},
        query="hfa-person-exact",
        method="exact",
        evidence_kind="source_reported",
    )


def golden_eleven_irrelevant() -> dict:
    rows = [{"card_uid": f"hfa-noise-{i:02d}", "summary": f"unrelated {i}"} for i in range(11)]
    return attach_retrieval_envelope({"rows": rows}, query="eleven weak hits", limit=20)


def test_zero_hit_is_low_with_unknown_coverage() -> None:
    payload = golden_zero_hit()
    envelope = EvidenceEnvelope.from_payload(payload["evidence"])
    assert payload["ok"] is True
    assert payload["confidence"] == "low"
    assert payload["confidence_reason"].startswith("no_results|")
    assert payload["confidence_meaning"] == CONFIDENCE_MEANING
    assert envelope.coverage == UNKNOWN
    assert envelope.freshness == UNKNOWN
    assert envelope.complete is False
    assert envelope.truncated is False
    assert envelope.query == "no-such-card"


def test_exact_match_is_high_without_completeness() -> None:
    payload = golden_exact()
    envelope = EvidenceEnvelope.from_payload(payload["evidence"])
    assert payload["confidence"] == "high"
    assert payload["confidence_reason"].startswith("exact_identifier|")
    assert "completeness not implied" in payload["confidence_reason"]
    assert envelope.complete is False
    assert envelope.coverage == UNKNOWN
    assert envelope.freshness == UNKNOWN
    assert envelope.method == "exact"
    assert envelope.evidence_kind == "source_reported"


def test_eleven_irrelevant_hits_are_not_high() -> None:
    payload = golden_eleven_irrelevant()
    envelope = EvidenceEnvelope.from_payload(payload["evidence"])
    assert payload["confidence"] == "low"
    assert payload["confidence_reason"].startswith("volume_without_relevance|")
    assert envelope.complete is False
    assert envelope.coverage == UNKNOWN
    assert envelope.freshness == UNKNOWN
    assert envelope.truncated is False
    assert payload["assessment_version"] == ASSESSMENT_VERSION


def test_known_truncation_is_flagged_without_guessing_freshness() -> None:
    rows = [{"card_uid": f"hfa-lex-{i}", "matched_by": "lexical"} for i in range(5)]
    payload = attach_retrieval_envelope({"rows": rows}, query="q", limit=5)
    envelope = EvidenceEnvelope.from_payload(payload["evidence"])
    assert envelope.truncated is True
    assert envelope.coverage == UNKNOWN
    assert envelope.freshness == UNKNOWN
    assert payload["confidence"] == "medium"


def test_unknown_truncation_stays_false_and_coverage_unknown() -> None:
    payload = attach_retrieval_envelope(
        {"rows": [{"card_uid": "hfa-1", "matched_by": "lexical"}]},
        query="q",
    )
    envelope = EvidenceEnvelope.from_payload(payload["evidence"])
    assert envelope.truncated is False
    assert envelope.coverage == UNKNOWN


def test_denial_cannot_look_like_empty_success() -> None:
    empty = golden_zero_hit()
    denied = client_failure_payload(
        status="denied",
        error="tool_disabled",
        message="Tool disabled by PPA_MCP_TOOL_PROFILE=remote-read",
        tool="archive_read",
    )
    assert empty["ok"] is True
    assert empty["confidence"] == "low"
    assert "error" not in empty
    assert denied["ok"] is False
    assert denied["confidence"] is None
    assert denied.get("rows") is None
    assert is_client_failure(denied)
    assert not is_client_failure(empty)
    loaded = EvidenceEnvelope.from_payload(denied["evidence"])
    assert loaded.errors[0] == "tool_disabled"
