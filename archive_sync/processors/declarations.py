"""Processor declaration registry (read without running processors)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from .constants import (
    EMAIL_PROMOTION_PROCESSOR_VERSION,
    EMAIL_THREAD_ENRICHMENT_VERSION,
    EMAIL_TYPED_EXTRACTION_VERSION,
    EMBEDDING_PROCESSOR_VERSION,
    ENTITY_RESOLUTION_VERSION,
    LINKERS_PROCESSOR_VERSION,
    MATERIALIZATION_VERSION,
    OUTPUT_KIND_CARDS,
    OUTPUT_KIND_CHUNKS,
    OUTPUT_KIND_DERIVED_CARDS,
    OUTPUT_KIND_EMAIL_CORPUS_DECISIONS,
    OUTPUT_KIND_EMBEDDINGS,
    OUTPUT_KIND_ENTITIES,
    OUTPUT_KIND_GRAPH_EDGES,
    OUTPUT_KIND_LINK_DECISIONS,
    OUTPUT_KIND_MATCHES,
    OUTPUT_KIND_ORG_LINKS,
    OUTPUT_KIND_PERSON_LINKS,
    OUTPUT_KIND_PLACE_LINKS,
    OUTPUT_KIND_PROJECTIONS,
    OUTPUT_KIND_SUMMARIES,
    PROCESSOR_EMAIL_PROMOTION_POLICY,
    PROCESSOR_EMAIL_THREAD_ENRICHMENT,
    PROCESSOR_EMAIL_TYPED_EXTRACTION,
    PROCESSOR_EMBEDDING,
    PROCESSOR_ENTITY_RESOLUTION,
    PROCESSOR_KEYS,
    PROCESSOR_LINKERS,
    PROCESSOR_MATERIALIZATION,
    ROLLBACK_BY_RUN_ID,
    ROLLBACK_MARK_INACTIVE,
    ROLLBACK_SUPERSEDE,
)


@dataclass(frozen=True)
class ProcessorDeclaration:
    processor_key: str
    processor_version: str
    input_card_types: tuple[str, ...]
    input_filters: dict[str, str] = field(default_factory=dict)
    output_kinds: tuple[str, ...] = ()
    output_identity: str = ""
    input_hash_fields: tuple[str, ...] = ()
    active_only: bool = True
    depends_on: tuple[str, ...] = ()
    idempotent: bool = True
    llm_dependent: bool = False
    rollback_strategy: str = ROLLBACK_SUPERSEDE
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "processor_key": self.processor_key,
            "processor_version": self.processor_version,
            "input_card_types": list(self.input_card_types),
            "input_filters": dict(self.input_filters),
            "output_kinds": list(self.output_kinds),
            "output_identity": self.output_identity,
            "input_hash_fields": list(self.input_hash_fields),
            "active_only": self.active_only,
            "depends_on": list(self.depends_on),
            "idempotent": self.idempotent,
            "llm_dependent": self.llm_dependent,
            "rollback_strategy": self.rollback_strategy,
            "enabled": self.enabled,
        }


def _email_promotion_policy() -> ProcessorDeclaration:
    return ProcessorDeclaration(
        processor_key=PROCESSOR_EMAIL_PROMOTION_POLICY,
        processor_version=EMAIL_PROMOTION_PROCESSOR_VERSION,
        input_card_types=("gmail_thread", "email_thread"),
        input_filters={},
        output_kinds=(OUTPUT_KIND_EMAIL_CORPUS_DECISIONS,),
        output_identity="email_corpus_decisions:{input_uid}",
        input_hash_fields=("thread_body_sha", "classification", "policy_version"),
        active_only=False,
        depends_on=(),
        idempotent=True,
        llm_dependent=False,
        rollback_strategy=ROLLBACK_SUPERSEDE,
    )


def _email_typed_extraction() -> ProcessorDeclaration:
    return ProcessorDeclaration(
        processor_key=PROCESSOR_EMAIL_TYPED_EXTRACTION,
        processor_version=EMAIL_TYPED_EXTRACTION_VERSION,
        input_card_types=("email_thread",),
        input_filters={"processor_decision": "typed_extraction", "corpus_decision": "active"},
        output_kinds=(OUTPUT_KIND_DERIVED_CARDS,),
        output_identity="{processor_key}:{input_uid}:{extractor_version}",
        input_hash_fields=("body_sha", "thread_uid", "processor_decision"),
        active_only=True,
        depends_on=(PROCESSOR_EMAIL_PROMOTION_POLICY, PROCESSOR_MATERIALIZATION),
        idempotent=True,
        llm_dependent=True,
        rollback_strategy=ROLLBACK_SUPERSEDE,
    )


def _email_thread_enrichment() -> ProcessorDeclaration:
    return ProcessorDeclaration(
        processor_key=PROCESSOR_EMAIL_THREAD_ENRICHMENT,
        processor_version=EMAIL_THREAD_ENRICHMENT_VERSION,
        input_card_types=("email_thread",),
        input_filters={"processor_decision": "thread_enrichment", "corpus_decision": "active"},
        output_kinds=(OUTPUT_KIND_SUMMARIES, OUTPUT_KIND_ENTITIES, OUTPUT_KIND_MATCHES),
        output_identity="{processor_key}:{input_uid}:{prompt_version}",
        input_hash_fields=("body_sha", "thread_uid", "processor_decision"),
        active_only=True,
        depends_on=(PROCESSOR_EMAIL_PROMOTION_POLICY, PROCESSOR_MATERIALIZATION),
        idempotent=False,
        llm_dependent=True,
        rollback_strategy=ROLLBACK_BY_RUN_ID,
    )


def _materialization() -> ProcessorDeclaration:
    return ProcessorDeclaration(
        processor_key=PROCESSOR_MATERIALIZATION,
        processor_version=MATERIALIZATION_VERSION,
        input_card_types=(
            "email_thread",
            "email_message",
            "calendar_event",
            "imessage_thread",
            "photo_metadata",
            "health_record",
        ),
        input_filters={},
        output_kinds=(OUTPUT_KIND_CARDS, OUTPUT_KIND_CHUNKS, OUTPUT_KIND_PROJECTIONS),
        output_identity="{processor_key}:{input_uid}:chunk",
        input_hash_fields=("body_sha", "frontmatter_hash", "corpus_state"),
        active_only=False,
        depends_on=(),
        idempotent=True,
        llm_dependent=False,
        rollback_strategy=ROLLBACK_SUPERSEDE,
    )


def _embedding() -> ProcessorDeclaration:
    return ProcessorDeclaration(
        processor_key=PROCESSOR_EMBEDDING,
        processor_version=EMBEDDING_PROCESSOR_VERSION,
        input_card_types=("email_thread", "email_message", "calendar_event", "imessage_thread"),
        input_filters={"corpus_decision": "active"},
        output_kinds=(OUTPUT_KIND_EMBEDDINGS,),
        output_identity="{processor_key}:{input_uid}:{chunk_key}:{model_id}",
        input_hash_fields=("chunk_hash", "corpus_state"),
        active_only=True,
        depends_on=(PROCESSOR_MATERIALIZATION,),
        idempotent=True,
        llm_dependent=True,
        rollback_strategy=ROLLBACK_MARK_INACTIVE,
    )


def _linkers() -> ProcessorDeclaration:
    return ProcessorDeclaration(
        processor_key=PROCESSOR_LINKERS,
        processor_version=LINKERS_PROCESSOR_VERSION,
        input_card_types=(
            "email_thread",
            "email_message",
            "calendar_event",
            "imessage_thread",
            "derived_card",
        ),
        input_filters={"corpus_decision": "active"},
        output_kinds=(OUTPUT_KIND_GRAPH_EDGES, OUTPUT_KIND_LINK_DECISIONS),
        output_identity="{processor_key}:{source_uid}:{target_uid}:{relation}:{linker_version}",
        input_hash_fields=("source_hash", "target_hash", "corpus_state"),
        active_only=True,
        depends_on=(PROCESSOR_MATERIALIZATION, PROCESSOR_EMBEDDING),
        idempotent=True,
        llm_dependent=True,
        rollback_strategy=ROLLBACK_SUPERSEDE,
    )


def _entity_resolution() -> ProcessorDeclaration:
    return ProcessorDeclaration(
        processor_key=PROCESSOR_ENTITY_RESOLUTION,
        processor_version=ENTITY_RESOLUTION_VERSION,
        input_card_types=("email_thread", "email_message", "calendar_event", "imessage_thread"),
        input_filters={"corpus_decision": "active"},
        output_kinds=(OUTPUT_KIND_PERSON_LINKS, OUTPUT_KIND_PLACE_LINKS, OUTPUT_KIND_ORG_LINKS),
        output_identity="{processor_key}:{input_uid}:{entity_mention_hash}",
        input_hash_fields=("entity_mentions", "corpus_state"),
        active_only=True,
        depends_on=(PROCESSOR_MATERIALIZATION,),
        idempotent=True,
        llm_dependent=True,
        rollback_strategy=ROLLBACK_SUPERSEDE,
    )


_PROCESSOR_DECLARATIONS: tuple[ProcessorDeclaration, ...] = (
    _email_promotion_policy(),
    _email_typed_extraction(),
    _email_thread_enrichment(),
    _materialization(),
    _embedding(),
    _linkers(),
    _entity_resolution(),
)

_DECLARATIONS_BY_KEY: dict[str, ProcessorDeclaration] = {decl.processor_key: decl for decl in _PROCESSOR_DECLARATIONS}


def iter_processor_declarations() -> tuple[ProcessorDeclaration, ...]:
    return _PROCESSOR_DECLARATIONS


def declaration_for_key(processor_key: str) -> ProcessorDeclaration | None:
    return _DECLARATIONS_BY_KEY.get(processor_key)


def validate_declaration(decl: ProcessorDeclaration) -> list[str]:
    errors: list[str] = []
    if not decl.processor_key.strip():
        errors.append("processor_key is required")
    elif decl.processor_key not in PROCESSOR_KEYS:
        errors.append(f"unknown processor_key: {decl.processor_key}")
    if not decl.processor_version.strip():
        errors.append("processor_version is required")
    if not decl.input_card_types:
        errors.append("input_card_types is required")
    if not decl.output_kinds:
        errors.append("output_kinds is required")
    if not decl.output_identity.strip():
        errors.append("output_identity is required")
    if not decl.input_hash_fields:
        errors.append("input_hash_fields is required")
    if not decl.rollback_strategy.strip():
        errors.append("rollback_strategy is required")
    for dep in decl.depends_on:
        if dep not in PROCESSOR_KEYS:
            errors.append(f"depends_on references unknown processor: {dep}")
    return errors


def validate_all_declarations(
    declarations: Iterable[ProcessorDeclaration] | None = None,
) -> dict[str, list[str]]:
    decls = list(declarations) if declarations is not None else list(_PROCESSOR_DECLARATIONS)
    return {d.processor_key: validate_declaration(d) for d in decls if validate_declaration(d)}


def topological_order(
    declarations: Iterable[ProcessorDeclaration] | None = None,
) -> list[str]:
    """Return processor keys in dependency order (no execution)."""

    decls = list(declarations) if declarations is not None else list(_PROCESSOR_DECLARATIONS)
    by_key = {d.processor_key: d for d in decls}
    visited: set[str] = set()
    order: list[str] = []

    def visit(key: str) -> None:
        if key in visited:
            return
        visited.add(key)
        decl = by_key.get(key)
        if decl:
            for dep in decl.depends_on:
                if dep in by_key:
                    visit(dep)
        order.append(key)

    for decl in decls:
        visit(decl.processor_key)
    return order
