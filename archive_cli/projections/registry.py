"""Projection and rule registries for ppa.

Typed projections, chunk rule specs, and edge rule specs are all derived
from the unified card registry (card_registry.py).  Generic projections
are still defined here since they are card-type-independent.
"""

from __future__ import annotations

from archive_cli.card_registry import CARD_TYPE_REGISTRATIONS, _float, _int, _json, _text, _timestamptz
from archive_cli.contracts import ChunkRuleSpec, EdgeRuleSpec, ProjectionColumnSpec, ProjectionSpec
from archive_cli.projections.base import SHARED_TYPED_COLUMNS

PROJECTION_REGISTRY_VERSION = 2
TYPED_PROJECTION_VERSION = 1


# ---------------------------------------------------------------------------
# Generic projections (card-type-independent, defined inline)
# ---------------------------------------------------------------------------

GENERIC_PROJECTIONS: tuple[ProjectionSpec, ...] = (
    ProjectionSpec(
        name="cards",
        table_name="cards",
        applies_to_types=tuple(),
        kind="generic",
        columns=(
            _text("uid", nullable=False, indexed=True),
            _text("rel_path", nullable=False, indexed=True),
            _text("slug", nullable=False, indexed=True),
            _text("type", nullable=False, indexed=True),
            _text("summary", nullable=False),
            _text("source_id", nullable=False),
            _text("created", nullable=False, indexed=True),
            _text("updated", nullable=False),
            _timestamptz("activity_at", indexed=True),
            _timestamptz("activity_end_at", indexed=True),
            _text("sent_at", nullable=False),
            _text("start_at", nullable=False),
            _text("first_message_at", nullable=False),
            _text("last_message_at", nullable=False),
            _float("quality_score", nullable=False, default=0.0),
            ProjectionColumnSpec(
                "quality_flags",
                "TEXT[]",
                nullable=False,
                indexed=False,
                source_field="quality_flags",
                value_mode="text_array",
                default="",
            ),
            _int("enrichment_version", nullable=False, default=0),
            _text("enrichment_status", nullable=False, default="none"),
            _timestamptz("last_enriched_at"),
            _text("content_hash", nullable=False),
            _text("search_text", nullable=False),
        ),
        load_order=10,
        clear_order=10,
        builder_name="build_cards_projection",
    ),
    ProjectionSpec(
        name="card_sources",
        table_name="card_sources",
        applies_to_types=tuple(),
        kind="generic",
        columns=(_text("card_uid", nullable=False), _text("source", nullable=False, indexed=True)),
        load_order=11,
        clear_order=11,
        builder_name="build_card_sources_projection",
    ),
    ProjectionSpec(
        name="card_people",
        table_name="card_people",
        applies_to_types=tuple(),
        kind="generic",
        columns=(_text("card_uid", nullable=False), _text("person", nullable=False, indexed=True)),
        load_order=12,
        clear_order=12,
        builder_name="build_card_people_projection",
    ),
    ProjectionSpec(
        name="card_orgs",
        table_name="card_orgs",
        applies_to_types=tuple(),
        kind="generic",
        columns=(_text("card_uid", nullable=False), _text("org", nullable=False, indexed=True)),
        load_order=13,
        clear_order=13,
        builder_name="build_card_orgs_projection",
    ),
    ProjectionSpec(
        name="external_ids",
        table_name="external_ids",
        applies_to_types=tuple(),
        kind="generic",
        columns=(
            _text("card_uid", nullable=False),
            _text("field_name", nullable=False),
            _text("provider", nullable=False, indexed=True),
            _text("external_id", nullable=False, indexed=True),
        ),
        load_order=14,
        clear_order=14,
        builder_name="build_external_ids_projection",
    ),
    ProjectionSpec(
        name="duplicate_uid_rows",
        table_name="duplicate_uid_rows",
        applies_to_types=tuple(),
        kind="generic",
        columns=(
            _text("uid", nullable=False),
            _text("preferred_rel_path", nullable=False),
            _text("preferred_type", nullable=False),
            _text("preferred_source_id", nullable=False),
            _text("preferred_summary", nullable=False),
            _text("duplicate_rel_path", nullable=False),
            _text("duplicate_type", nullable=False),
            _text("duplicate_source_id", nullable=False),
            _text("duplicate_summary", nullable=False),
            _int("duplicate_group_size", nullable=False, default=2),
        ),
        load_order=15,
        clear_order=15,
        builder_name="build_duplicate_uid_rows_projection",
    ),
    ProjectionSpec(
        name="edges",
        table_name="edges",
        applies_to_types=tuple(),
        kind="generic",
        columns=(
            _text("source_uid", nullable=False),
            _text("source_path", nullable=False),
            _text("target_uid", nullable=False),
            _text("target_slug", nullable=False),
            _text("target_path", nullable=False, indexed=True),
            _text("target_kind", nullable=False),
            _text("edge_type", nullable=False, indexed=True),
            _text("field_name", nullable=False),
        ),
        load_order=16,
        clear_order=16,
        builder_name="build_edges_projection",
    ),
    ProjectionSpec(
        name="chunks",
        table_name="chunks",
        applies_to_types=tuple(),
        kind="generic",
        columns=(
            _text("chunk_key", nullable=False),
            _text("card_uid", nullable=False, indexed=True),
            _text("rel_path", nullable=False),
            _text("chunk_type", nullable=False),
            _int("chunk_index", nullable=False),
            _int("chunk_schema_version", nullable=False),
            _json("source_fields", source_field="source_fields"),
            _text("content", nullable=False),
            _text("content_hash", nullable=False),
            _int("token_count", nullable=False),
        ),
        load_order=17,
        clear_order=17,
        builder_name="build_chunks_projection",
    ),
)


# ---------------------------------------------------------------------------
# Typed projections (derived from the unified card registry)
# ---------------------------------------------------------------------------


def _build_typed_projection(reg) -> ProjectionSpec:
    return ProjectionSpec(
        name=reg.projection_table,
        table_name=reg.projection_table,
        applies_to_types=(reg.card_type,),
        kind="typed",
        columns=SHARED_TYPED_COLUMNS + reg.projection_columns,
        load_order=100,
        clear_order=100,
        builder_name=f"build_{reg.projection_table}_projection",
        explain_name=f"explain_{reg.projection_table}_projection",
    )


TYPED_PROJECTIONS: tuple[ProjectionSpec, ...] = tuple(_build_typed_projection(reg) for reg in CARD_TYPE_REGISTRATIONS)


# ---------------------------------------------------------------------------
# Combined registry and lookup indexes
# ---------------------------------------------------------------------------

PROJECTION_REGISTRY: tuple[ProjectionSpec, ...] = GENERIC_PROJECTIONS + TYPED_PROJECTIONS
PROJECTION_BY_TABLE = {projection.table_name: projection for projection in PROJECTION_REGISTRY}
TYPED_PROJECTION_BY_CARD_TYPE = {projection.applies_to_types[0]: projection for projection in TYPED_PROJECTIONS}


def projection_for_card_type(card_type: str) -> ProjectionSpec | None:
    return TYPED_PROJECTION_BY_CARD_TYPE.get(card_type)


# ---------------------------------------------------------------------------
# Chunk and edge rule specs (derived from the unified card registry)
# ---------------------------------------------------------------------------

CHUNK_RULE_SPECS: tuple[ChunkRuleSpec, ...] = tuple(
    ChunkRuleSpec(reg.card_type, reg.chunk_builder_name or "default", reg.chunk_types)
    for reg in CARD_TYPE_REGISTRATIONS
    if reg.chunk_builder_name is not None
)

EDGE_RULE_SPECS: tuple[EdgeRuleSpec, ...] = tuple(
    EdgeRuleSpec(
        reg.card_type,
        reg.card_type,
        tuple(sorted(set(rule.edge_type for rule in reg.edge_rules) | {"wikilink"})),
    )
    for reg in CARD_TYPE_REGISTRATIONS
    if reg.edge_rules
)
