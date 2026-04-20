"""MODULE_COMMUNICATION -- Phase 6.5 Step 18 file-split from seed_links.py.

Email/iMessage/Beeper thread, message, attachment, and participant linkage from exact identifiers.

Behavior is byte-equivalent to the pre-Step-18 dispatch.
"""

from __future__ import annotations

from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.seed_links import (
    CARD_TYPE_MODULES,
    LINK_TYPE_MESSAGE_HAS_ATTACHMENT,
    LINK_TYPE_MESSAGE_IN_THREAD,
    LINK_TYPE_MESSAGE_MENTIONS_PERSON,
    LINK_TYPE_THREAD_HAS_ATTACHMENT,
    LINK_TYPE_THREAD_HAS_MESSAGE,
    LINK_TYPE_THREAD_HAS_PERSON,
    MODULE_COMMUNICATION,
    MODULE_GRAPH,
    SeedCardSketch,
    SeedLinkCandidate,
    SeedLinkCatalog,
    _append_candidate,
    _clean_text,
    _generate_person_link_candidates,
    _iter_string_values,
    _make_evidence,
    _message_thread_features,
    _normalize_email,
    _normalize_handle,
    _normalize_slug,
    _slug_from_ref,
    get_link_surface_policies,
)


def _generate_communication_candidates(catalog: SeedLinkCatalog, source: SeedCardSketch) -> list[SeedLinkCandidate]:
    results: list[SeedLinkCandidate] = []
    if source.card_type == "email_message":
        thread_id = _clean_text(source.frontmatter.get("gmail_thread_id", ""))
        for thread in catalog.email_threads_by_thread_id.get(thread_id, []):
            if thread.uid == source.uid:
                continue
            features, evidences = _message_thread_features(source, thread)
            if features["exact_thread_id"] and _normalize_slug(
                _slug_from_ref(source.frontmatter.get("thread", ""))
            ) != _normalize_slug(thread.slug):
                _append_candidate(
                    results,
                    module_name=MODULE_COMMUNICATION,
                    source=source,
                    target=thread,
                    proposed_link_type=LINK_TYPE_MESSAGE_IN_THREAD,
                    candidate_group="thread_membership",
                    features=features,
                    evidences=evidences,
                )
            if features["exact_thread_id"] and not features["reverse_messages_present"]:
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=thread,
                    target=source,
                    proposed_link_type=LINK_TYPE_THREAD_HAS_MESSAGE,
                    candidate_group="thread_membership",
                    features=features,
                    evidences=evidences,
                )
        attachment_matches = catalog.email_attachments_by_message_id.get(
            _clean_text(source.frontmatter.get("gmail_message_id", "")), []
        )
        existing_attachments = {
            _normalize_slug(_slug_from_ref(item))
            for item in _iter_string_values(source.frontmatter.get("attachments", []))
        }
        for attachment in attachment_matches:
            if _normalize_slug(attachment.slug) in existing_attachments:
                continue
            evidences = [
                _make_evidence(
                    "exact_parent_message",
                    "frontmatter",
                    "gmail_message_id",
                    source.frontmatter.get("gmail_message_id", ""),
                    1.0,
                    source_uid=source.uid,
                    target_uid=attachment.uid,
                )
            ]
            _append_candidate(
                results,
                module_name=MODULE_COMMUNICATION,
                source=source,
                target=attachment,
                proposed_link_type=LINK_TYPE_MESSAGE_HAS_ATTACHMENT,
                candidate_group="attachment_membership",
                features={"exact_parent_message": 1, "ambiguous_target_count": len(attachment_matches)},
                evidences=evidences,
            )
        person_emails = source.participant_emails | (
            {_normalize_email(source.frontmatter.get("from_email", ""))}
            if _normalize_email(source.frontmatter.get("from_email", ""))
            else set()
        )
        results.extend(
            _generate_person_link_candidates(
                catalog,
                source=source,
                emails=person_emails,
                handles=set(),
                link_type=LINK_TYPE_MESSAGE_MENTIONS_PERSON,
                candidate_group="participant_resolution",
            )
        )
    elif source.card_type == "email_thread":
        thread_id = _clean_text(source.frontmatter.get("gmail_thread_id", ""))
        attachment_matches = catalog.email_attachments_by_thread_id.get(thread_id, [])
        for attachment in attachment_matches:
            evidences = [
                _make_evidence(
                    "exact_parent_thread",
                    "frontmatter",
                    "gmail_thread_id",
                    thread_id,
                    1.0,
                    source_uid=source.uid,
                    target_uid=attachment.uid,
                )
            ]
            _append_candidate(
                results,
                module_name=MODULE_GRAPH,
                source=source,
                target=attachment,
                proposed_link_type=LINK_TYPE_THREAD_HAS_ATTACHMENT,
                candidate_group="attachment_membership",
                features={"exact_parent_thread": 1, "ambiguous_target_count": len(attachment_matches)},
                evidences=evidences,
            )
        results.extend(
            _generate_person_link_candidates(
                catalog,
                source=source,
                emails=source.participant_emails
                | (
                    {_normalize_email(source.frontmatter.get("account_email", ""))}
                    if _normalize_email(source.frontmatter.get("account_email", ""))
                    else set()
                ),
                handles=set(),
                link_type=LINK_TYPE_THREAD_HAS_PERSON,
                candidate_group="participant_resolution",
            )
        )
    elif source.card_type == "email_attachment":
        message_matches = catalog.email_messages_by_message_id.get(
            _clean_text(source.frontmatter.get("gmail_message_id", "")), []
        )
        for message in message_matches:
            existing_attachments = {
                _normalize_slug(_slug_from_ref(item))
                for item in _iter_string_values(message.frontmatter.get("attachments", []))
            }
            if _normalize_slug(source.slug) in existing_attachments:
                continue
            evidences = [
                _make_evidence(
                    "exact_parent_message",
                    "frontmatter",
                    "gmail_message_id",
                    source.frontmatter.get("gmail_message_id", ""),
                    1.0,
                    source_uid=message.uid,
                    target_uid=source.uid,
                )
            ]
            _append_candidate(
                results,
                module_name=MODULE_COMMUNICATION,
                source=message,
                target=source,
                proposed_link_type=LINK_TYPE_MESSAGE_HAS_ATTACHMENT,
                candidate_group="attachment_membership",
                features={"exact_parent_message": 1, "ambiguous_target_count": len(message_matches)},
                evidences=evidences,
            )
    elif source.card_type == "imessage_message":
        chat_id = _clean_text(source.frontmatter.get("imessage_chat_id", ""))
        for thread in catalog.imessage_threads_by_chat_id.get(chat_id, []):
            reverse_list = {
                _normalize_slug(_slug_from_ref(item))
                for item in _iter_string_values(thread.frontmatter.get("messages", []))
            }
            features = {
                "exact_thread_id": int(bool(chat_id)),
                "reverse_messages_present": int(_normalize_slug(source.slug) in reverse_list),
                "message_thread_field_present": int(bool(_clean_text(source.frontmatter.get("thread", "")))),
            }
            evidences = [
                _make_evidence(
                    "exact_thread_id",
                    "frontmatter",
                    "imessage_chat_id",
                    chat_id,
                    1.0,
                    source_uid=source.uid,
                    target_uid=thread.uid,
                )
            ]
            if not features["message_thread_field_present"]:
                _append_candidate(
                    results,
                    module_name=MODULE_COMMUNICATION,
                    source=source,
                    target=thread,
                    proposed_link_type=LINK_TYPE_MESSAGE_IN_THREAD,
                    candidate_group="thread_membership",
                    features=features,
                    evidences=evidences,
                )
            if not features["reverse_messages_present"]:
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=thread,
                    target=source,
                    proposed_link_type=LINK_TYPE_THREAD_HAS_MESSAGE,
                    candidate_group="thread_membership",
                    features=features,
                    evidences=evidences,
                )
        results.extend(
            _generate_person_link_candidates(
                catalog,
                source=source,
                emails=set(),
                handles=source.participant_handles
                | (
                    {_normalize_handle(source.frontmatter.get("sender_handle", ""))}
                    if _normalize_handle(source.frontmatter.get("sender_handle", ""))
                    else set()
                ),
                link_type=LINK_TYPE_MESSAGE_MENTIONS_PERSON,
                candidate_group="participant_resolution",
            )
        )
    elif source.card_type == "imessage_thread":
        results.extend(
            _generate_person_link_candidates(
                catalog,
                source=source,
                emails=set(),
                handles=source.participant_handles,
                link_type=LINK_TYPE_THREAD_HAS_PERSON,
                candidate_group="participant_resolution",
            )
        )
    return results



def _score_communication_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    """Branch lifted from ``_component_scores`` for MODULE_COMMUNICATION."""
    deterministic_hits = len(features.get("deterministic_hits", []))
    deterministic_score = 0.0
    lexical_score = 0.0
    graph_score = 0.0
    embedding_score = 0.0
    risk_penalty = 0.0
    deterministic_score = min(
        1.0,
        (1.0 if int(features.get("exact_thread_id", 0)) else 0.0)
        + (1.0 if int(features.get("exact_parent_message", 0)) else 0.0)
        + (0.85 if deterministic_hits else 0.0),
    )
    lexical_score = min(
        1.0,
        float(features.get("subject_similarity", 0.0)) * 0.6
        + min(int(features.get("participant_overlap", 0)), 3) * 0.12,
    )
    graph_score = min(
        1.0,
        (0.8 if int(features.get("reverse_messages_present", 0)) else 0.0)
        + (0.6 if int(features.get("path_bucket_match", 0)) else 0.0),
    )
    if int(features.get("ambiguous_target_count", 0)) > 3:
        risk_penalty += 0.12
    return (
        max(0.0, min(deterministic_score, 1.0)),
        max(0.0, min(lexical_score, 1.0)),
        max(0.0, min(graph_score, 1.0)),
        max(0.0, min(embedding_score, 1.0)),
        max(0.0, min(risk_penalty, 0.8)),
    )



def _policies() -> tuple:
    return tuple(p for p in get_link_surface_policies() if p.module_name == MODULE_COMMUNICATION)


def _source_types() -> tuple[str, ...]:
    return tuple(ct for ct, mods in CARD_TYPE_MODULES.items() if MODULE_COMMUNICATION in mods)


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_COMMUNICATION,
    source_card_types=_source_types(),
    emits_link_types=(
        LINK_TYPE_MESSAGE_IN_THREAD,
        LINK_TYPE_MESSAGE_HAS_ATTACHMENT,
        LINK_TYPE_THREAD_HAS_PERSON,
        LINK_TYPE_MESSAGE_MENTIONS_PERSON,
    ),
    generator=_generate_communication_candidates,
    scoring_fn=_score_communication_features,
    scoring_mode="weighted",
    policies=_policies(),
    requires_llm_judge=False,
    lifecycle_state="active",
    phase_owner="phase_2.875",
    post_promotion_action="edges_only",
    description="Email/iMessage/Beeper thread + message + attachment linkage from exact gmail/imessage IDs.",
))
