"""Shared contracts for the ppa service and projection layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

JsonDict = dict[str, Any]


@dataclass(frozen=True)
class ProjectionColumnSpec:
    name: str
    sql_type: str
    nullable: bool = True
    indexed: bool = False
    source_field: str | None = None
    value_mode: str = "text"
    default: Any = ""


@dataclass(frozen=True)
class ProjectionSpec:
    name: str
    table_name: str
    applies_to_types: tuple[str, ...]
    kind: Literal["generic", "typed"]
    columns: tuple[ProjectionColumnSpec, ...]
    load_order: int
    clear_order: int
    builder_name: str
    explain_name: str | None = None
    indexes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ChunkRuleSpec:
    card_type: str
    profile_name: str
    chunk_types: tuple[str, ...]


@dataclass(frozen=True)
class EdgeRuleSpec:
    card_type: str
    profile_name: str
    derived_edge_types: tuple[str, ...]


@dataclass(frozen=True)
class DeclEdgeRule:
    """Declarative edge rule: extract values from source fields and emit edges."""

    field_name: str
    edge_type: str
    target: Literal["card", "person"]
    source_fields: tuple[str, ...]
    multi: bool = True
    # When set, resolve target by this field on cards of target_card_type (e.g.
    # shipment.linked_purchase order id -> purchase.order_number).
    target_lookup_field: str | None = None
    target_card_type: str | None = None


@dataclass(frozen=True)
class CardTypeRegistration:
    """Unified declarative spec for one card type in the derived index.

    Combines typed projection columns, edge rules, chunk builder reference,
    and person-edge labelling into a single registration so adding a card
    type is a two-file change (schema model + registration).
    """

    card_type: str
    projection_table: str
    projection_columns: tuple[ProjectionColumnSpec, ...]
    person_edge_type: str
    edge_rules: tuple[DeclEdgeRule, ...]
    chunk_builder_name: str | None
    chunk_types: tuple[str, ...]
    quality_critical_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class ArchiveContext:
    card_type: str
    source_labels: tuple[str, ...] = ()
    people: tuple[str, ...] = ()
    orgs: tuple[str, ...] = ()
    time_span: tuple[str, ...] = ()
    provenance_bias: float = 0.0
    graph_neighbor_types: tuple[str, ...] = ()
    typed_projection_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class ArchiveConfig:
    vault_path: str
    index_dsn: str | None
    index_schema: str
    retrieval_defaults: JsonDict = field(default_factory=dict)
    retrieval: JsonDict = field(default_factory=dict)
    runtime: JsonDict = field(default_factory=dict)
    embeddings: JsonDict = field(default_factory=dict)
    seed_links: JsonDict = field(default_factory=dict)


class ArchiveStore(Protocol):
    def bootstrap(self) -> JsonDict: ...

    def rebuild(
        self,
        *,
        workers: int | None = None,
        batch_size: int | None = None,
        commit_interval: int | None = None,
        progress_every: int | None = None,
        executor_kind: str | None = None,
    ) -> JsonDict: ...

    def status(self) -> JsonDict: ...

    def read(self, path_or_uid: str) -> JsonDict: ...

    def query(
        self,
        *,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        org_filter: str = "",
        limit: int = 20,
    ) -> JsonDict: ...

    def search(self, query: str, *, limit: int = 20) -> JsonDict: ...

    def graph(self, note_path: str, *, hops: int = 2) -> JsonDict: ...

    def timeline(self, *, start_date: str = "", end_date: str = "", limit: int = 20) -> JsonDict: ...

    def vector_search(self, query: str, **kwargs) -> JsonDict: ...

    def hybrid_search(self, query: str, **kwargs) -> JsonDict: ...

    def embedding_status(self, *, embedding_model: str = "", embedding_version: int = 0) -> JsonDict: ...

    def embedding_backlog(
        self, *, limit: int = 20, embedding_model: str = "", embedding_version: int = 0
    ) -> JsonDict: ...

    def embed_pending(self, *, limit: int = 0, embedding_model: str = "", embedding_version: int = 0) -> JsonDict: ...

    def projection_inventory(self) -> JsonDict: ...

    def projection_status(self) -> JsonDict: ...

    def projection_explain(self, card_uid: str) -> JsonDict: ...

    def retrieval_explain(self, query: str, **kwargs) -> JsonDict: ...

    def seed_link_surface(self) -> JsonDict: ...

    def seed_link_enqueue(self, **kwargs) -> JsonDict: ...

    def seed_link_backfill(self, **kwargs) -> JsonDict: ...

    def seed_link_refresh(self, **kwargs) -> JsonDict: ...

    def seed_link_worker(self, **kwargs) -> JsonDict: ...

    def seed_link_promote(self, **kwargs) -> JsonDict: ...

    def seed_link_report(self, **kwargs) -> JsonDict: ...
