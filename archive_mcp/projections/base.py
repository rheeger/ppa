"""Base helpers for projection row materialization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from archive_mcp.contracts import ProjectionColumnSpec, ProjectionSpec
from archive_mcp.features import (card_activity_at, external_ids_by_provider,
                                  iter_string_values, json_text,
                                  primary_person, relationship_payload)
from hfa.schema import BaseCard

SHARED_TYPED_COLUMNS: tuple[ProjectionColumnSpec, ...] = (
    ProjectionColumnSpec("card_uid", "TEXT", nullable=False, source_field="uid", value_mode="card_uid"),
    ProjectionColumnSpec("rel_path", "TEXT", nullable=False, source_field="rel_path", indexed=True, value_mode="rel_path"),
    ProjectionColumnSpec("card_type", "TEXT", nullable=False, source_field="type", indexed=True, value_mode="card_type"),
    ProjectionColumnSpec("summary", "TEXT", nullable=False, source_field="summary", indexed=False, value_mode="summary"),
    ProjectionColumnSpec("created", "TEXT", nullable=False, source_field="created"),
    ProjectionColumnSpec("updated", "TEXT", nullable=False, source_field="updated"),
    ProjectionColumnSpec("primary_source", "TEXT", nullable=False, source_field="source", indexed=True, value_mode="primary_source"),
    ProjectionColumnSpec("source_id", "TEXT", nullable=False, source_field="source_id", indexed=True),
    ProjectionColumnSpec("activity_at", "TEXT", nullable=False, source_field="activity_at", indexed=True, value_mode="activity_at"),
    ProjectionColumnSpec("external_ids_json", "JSONB", nullable=False, source_field="external_ids", value_mode="external_ids_json", default="{}"),
    ProjectionColumnSpec("relationships_json", "JSONB", nullable=False, source_field="relationships", value_mode="relationships_json", default="{}"),
    ProjectionColumnSpec("typed_projection_version", "INTEGER", nullable=False, value_mode="typed_projection_version", default=1),
    ProjectionColumnSpec("canonical_ready", "BOOLEAN", nullable=False, value_mode="canonical_ready", default=True),
    ProjectionColumnSpec("migration_notes", "TEXT", nullable=False, value_mode="migration_notes", default=""),
)


@dataclass(slots=True)
class ProjectionRowBuffer:
    rows_by_table: dict[str, list[tuple[Any, ...]]] = field(default_factory=dict)

    def add(self, table_name: str, row: tuple[Any, ...]) -> None:
        self.rows_by_table.setdefault(table_name, []).append(row)

    def extend(self, other: "ProjectionRowBuffer") -> None:
        for table_name, rows in other.rows_by_table.items():
            self.rows_by_table.setdefault(table_name, []).extend(rows)

    def clear(self) -> None:
        self.rows_by_table.clear()

    def rows_for(self, table_name: str) -> list[tuple[Any, ...]]:
        return self.rows_by_table.get(table_name, [])

    def __getattr__(self, name: str) -> list[tuple[Any, ...]]:
        if name == "rows_by_table":
            raise AttributeError(name)
        return self.rows_by_table.get(name, [])


def _column_value(
    column: ProjectionColumnSpec,
    *,
    card: BaseCard,
    rel_path: str,
    frontmatter: dict[str, Any],
    typed_projection_version: int,
    canonical_ready: bool,
    migration_notes: str,
) -> Any:
    if column.value_mode == "card_uid":
        return str(card.uid)
    if column.value_mode == "rel_path":
        return rel_path
    if column.value_mode == "card_type":
        return card.type
    if column.value_mode == "summary":
        return card.summary
    if column.value_mode == "primary_source":
        sources = iter_string_values(frontmatter.get("source", []))
        return sources[0] if sources else ""
    if column.value_mode == "activity_at":
        return card_activity_at(frontmatter)
    if column.value_mode == "primary_person":
        return primary_person(frontmatter.get(column.source_field or column.name, []))
    if column.value_mode == "external_ids_json":
        return json_text(external_ids_by_provider(frontmatter))
    if column.value_mode == "relationships_json":
        return json_text(relationship_payload(frontmatter))
    if column.value_mode == "typed_projection_version":
        return int(typed_projection_version)
    if column.value_mode == "canonical_ready":
        return bool(canonical_ready)
    if column.value_mode == "migration_notes":
        return migration_notes

    source_field = column.source_field or column.name
    value = frontmatter.get(source_field, column.default)
    if column.value_mode == "json":
        return json_text(value or ([] if isinstance(column.default, list) else {} if column.sql_type == "JSONB" else value))
    if column.value_mode == "bool":
        return bool(value)
    if column.value_mode == "float":
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return float(column.default or 0)
    if column.value_mode == "int":
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return int(column.default or 0)
    if isinstance(value, list):
        values = iter_string_values(value)
        return values[0] if values else str(column.default or "")
    return str(value or column.default or "")


def build_projection_row(
    projection: ProjectionSpec,
    *,
    card: BaseCard,
    rel_path: str,
    frontmatter: dict[str, Any],
    typed_projection_version: int = 1,
) -> tuple[tuple[Any, ...], bool, str]:
    missing_fields: list[str] = []
    for column in projection.columns:
        source_field = column.source_field
        if not source_field or column.value_mode in {
            "card_uid",
            "rel_path",
            "card_type",
            "summary",
            "primary_source",
            "activity_at",
            "external_ids_json",
            "relationships_json",
            "typed_projection_version",
            "canonical_ready",
            "migration_notes",
        }:
            continue
        value = frontmatter.get(source_field)
        if not column.nullable and value in (None, "", []):
            missing_fields.append(source_field)
    canonical_ready = not missing_fields
    migration_notes = ", ".join(sorted(set(missing_fields)))
    row = tuple(
        _column_value(
            column,
            card=card,
            rel_path=rel_path,
            frontmatter=frontmatter,
            typed_projection_version=typed_projection_version,
            canonical_ready=canonical_ready,
            migration_notes=migration_notes,
        )
        for column in projection.columns
    )
    return row, canonical_ready, migration_notes
