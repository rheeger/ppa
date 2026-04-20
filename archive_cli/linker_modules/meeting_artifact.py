"""MODULE_MEETING_ARTIFACT -- Phase 6.5 deterministic meeting-transcript linker.

Finds the calendar_event each meeting_transcript originated from. Three tiers:
  - MEETING_TIER_ICAL_UID  (1.00): transcript.ical_uid == event.ical_uid.
  - MEETING_TIER_TITLE_TIME (0.88): normalized title match + start_at within 15 min.
    Generic titles ("meeting", "1:1", "sync") are rejected at this tier.
  - MEETING_TIER_PARTICIPANT_TIME (0.70, MEDIUM band): participant overlap >= 2
    + start_at within 1 hour, same day. Review-only band.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.seed_links import (
    LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT,
    LinkEvidence,
    SeedCardSketch,
    SeedLinkCandidate,
    SeedLinkCatalog,
    _append_candidate,
    _clean_text,
    _normalize_alias,
)

MODULE_MEETING_ARTIFACT = "meetingArtifactLinker"

_GENERIC_TITLE_TOKENS = frozenset({
    "meeting", "1:1", "one on one", "sync", "call", "catch up", "standup",
})


def _parse_ts(value: Any) -> datetime | None:
    """Best-effort ISO timestamp parse. Returns None on any failure."""
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Replace Z with +00:00 for fromisoformat pre-3.11.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # Try date-only.
        try:
            dt = datetime.fromisoformat(s[:10])
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _is_generic_meeting_title(title_norm: str) -> bool:
    return title_norm in _GENERIC_TITLE_TOKENS


def _build_title_bucket_index(catalog: SeedLinkCatalog) -> None:
    """post_build_hook: index calendar_events by normalized title."""
    buckets: dict[str, list[SeedCardSketch]] = {}
    for event in catalog.cards_by_type.get("calendar_event", []):
        title = event.summary or event.frontmatter.get("title", "") or ""
        key = _normalize_alias(title)
        if not key:
            continue
        buckets.setdefault(key, []).append(event)
    lf.set_private_index(catalog, "calendar_events_by_title_bucket", buckets)


def _generate_meeting_artifact_candidates(
    catalog: SeedLinkCatalog, source: SeedCardSketch,
) -> list[SeedLinkCandidate]:
    if source.card_type != "meeting_transcript":
        return []
    results: list[SeedLinkCandidate] = []
    seen: set[str] = set()

    ical_uid = _clean_text(source.frontmatter.get("ical_uid", ""))
    title_raw = source.summary or source.frontmatter.get("title", "") or ""
    title_norm = _normalize_alias(title_raw)
    start_at_raw = source.frontmatter.get("start_at") or source.activity_at
    start_at = _parse_ts(start_at_raw)
    t_participants = set(source.participant_emails or [])

    # Tier 1 — exact ical_uid (framework-core index already on SeedLinkCatalog).
    if ical_uid:
        for event in catalog.calendar_events_by_ical_uid.get(ical_uid, []):
            if event.uid == source.uid or event.uid in seen:
                continue
            seen.add(event.uid)
            _append_meeting_artifact(
                results, source, event,
                tier="MEETING_TIER_ICAL_UID",
                deterministic_score=1.00, risk_penalty=0.0,
                extra_features={"matched_ical_uid": ical_uid},
            )

    # Tier 2 — title + time. Skip generic titles (they fall to Tier 3).
    if title_norm and start_at is not None and not _is_generic_meeting_title(title_norm):
        title_buckets = lf.get_private_index(catalog, "calendar_events_by_title_bucket")
        for event in title_buckets.get(title_norm, []):
            if event.uid == source.uid or event.uid in seen:
                continue
            event_start = _parse_ts(
                event.frontmatter.get("start_at") or event.activity_at
            )
            if event_start is None:
                continue
            delta = abs((event_start - start_at).total_seconds())
            if delta > 15 * 60:
                continue
            seen.add(event.uid)
            _append_meeting_artifact(
                results, source, event,
                tier="MEETING_TIER_TITLE_TIME",
                deterministic_score=0.88, risk_penalty=0.0,
                extra_features={"title_norm": title_norm, "time_delta_s": int(delta)},
            )

    # Tier 3 — participant overlap + time window (review band, catches generic titles).
    if start_at is not None and t_participants:
        day_key = start_at.date().isoformat()
        for event in catalog.events_by_day.get(day_key, []):
            if event.uid == source.uid or event.uid in seen:
                continue
            if event.card_type != "calendar_event":
                continue
            event_start = _parse_ts(
                event.frontmatter.get("start_at") or event.activity_at
            )
            if event_start is None:
                continue
            delta = abs((event_start - start_at).total_seconds())
            if delta > 3600:
                continue
            e_participants = set(event.participant_emails or [])
            overlap = len(t_participants & e_participants)
            if overlap < 2:
                continue
            seen.add(event.uid)
            _append_meeting_artifact(
                results, source, event,
                tier="MEETING_TIER_PARTICIPANT_TIME",
                deterministic_score=0.70, risk_penalty=0.05,
                extra_features={
                    "participant_overlap": overlap,
                    "time_delta_s": int(delta),
                },
            )

    return results


def _append_meeting_artifact(
    results: list[SeedLinkCandidate],
    source: SeedCardSketch,
    target: SeedCardSketch,
    *,
    tier: str,
    deterministic_score: float,
    risk_penalty: float,
    extra_features: dict[str, Any],
) -> None:
    features: dict[str, Any] = {
        "tier": tier,
        "deterministic_score": deterministic_score,
        "risk_penalty": risk_penalty,
    }
    features.update(extra_features)
    evidences = [
        LinkEvidence(
            evidence_type="predicate_match",
            evidence_source="meeting_artifact",
            feature_name="tier",
            feature_value=tier,
            feature_weight=deterministic_score,
            raw_payload_json=dict(extra_features),
        ),
    ]
    _append_candidate(
        results,
        module_name=MODULE_MEETING_ARTIFACT,
        source=source,
        target=target,
        proposed_link_type=LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT,
        candidate_group=f"meeting_artifact:{tier}",
        features=features,
        evidences=evidences,
    )


def _score_meeting_artifact_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    det = float(features.get("deterministic_score", 0.0))
    risk = float(features.get("risk_penalty", 0.0))
    return det, 0.0, 0.0, 0.0, risk


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_MEETING_ARTIFACT,
    source_card_types=("meeting_transcript",),
    emits_link_types=(LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT,),
    generator=_generate_meeting_artifact_candidates,
    scoring_fn=_score_meeting_artifact_features,
    scoring_mode="deterministic",
    # Framework-core calendar_events_by_ical_uid and events_by_day are already
    # on SeedLinkCatalog; no CatalogIndexSpec declarations needed for them.
    # calendar_events_by_title_bucket is a linker-owned index built via the
    # post_build_hook below.
    policies=(),  # reuses existing LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT policy
    requires_llm_judge=False,
    lifecycle_state="active",
    phase_owner="phase_6.5",
    post_promotion_action="edges_only",
    description=(
        "Links meeting transcripts to their originating calendar events via "
        "ical_uid, normalized title + start_at window, or participant overlap."
    ),
    post_build_hook=_build_title_bucket_index,
))
