"""JSON Schema fragments for LLM prompts — derived from Pydantic card models."""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

from archive_vault.schema import CARD_TYPES, BaseCard

# Filled by the enrichment runner / promotion pipeline, not the extraction model.
LLM_OMIT_FIELDS: frozenset[str] = frozenset(
    {
        "uid",
        "type",
        "source",
        "source_id",
        "created",
        "updated",
        "extraction_confidence",
        "people",
        "orgs",
        "tags",
        "source_email",
    }
)

# Grouping for documentation / triage hints (Phase 2.75 plan).
EXTRACTABLE_TYPES: dict[str, list[str]] = {
    "transaction": [
        "meal_order",
        "grocery_order",
        "purchase",
        "ride",
        "flight",
        "accommodation",
        "car_rental",
        "shipment",
        "subscription",
        "event_ticket",
        "payroll",
    ],
    "entity": ["place", "organization"],
    "knowledge": ["knowledge", "observation"],
}


def all_extractable_card_types() -> list[str]:
    out: list[str] = []
    for _cat, types in EXTRACTABLE_TYPES.items():
        out.extend(types)
    return out


def _prune_omit_fields(obj: Any) -> None:
    if isinstance(obj, dict):
        props = obj.get("properties")
        if isinstance(props, dict):
            for k in list(props):
                if k in LLM_OMIT_FIELDS:
                    del props[k]
            req = obj.get("required")
            if isinstance(req, list):
                obj["required"] = [x for x in req if x not in LLM_OMIT_FIELDS]
        for v in obj.values():
            _prune_omit_fields(v)
    elif isinstance(obj, list):
        for item in obj:
            _prune_omit_fields(item)


LLM_FIELD_DESCRIPTIONS: dict[tuple[str, str], str] = {
    ("ride", "pickup_at"): "ISO-8601 datetime, e.g. 2024-01-15T14:30:00",
    ("ride", "dropoff_at"): "ISO-8601 datetime, e.g. 2024-01-15T14:45:00",
    ("ride", "service"): "Company name: Uber, Lyft, Via, etc.",
    ("flight", "origin_airport"): "3-letter IATA code only, e.g. SFO",
    ("flight", "destination_airport"): "3-letter IATA code only, e.g. JFK",
    ("flight", "departure_at"): "ISO-8601 datetime, e.g. 2024-01-15T08:30:00",
    ("flight", "arrival_at"): "ISO-8601 datetime, e.g. 2024-01-15T11:45:00",
    ("accommodation", "check_in"): "ISO date YYYY-MM-DD only",
    ("accommodation", "check_out"): "ISO date YYYY-MM-DD only",
    ("accommodation", "property_name"): "Listing or hotel name — not dates or marketing",
    ("car_rental", "pickup_at"): "ISO-8601 datetime",
    ("car_rental", "dropoff_at"): "ISO-8601 datetime",
    ("payroll", "pay_date"): "ISO date YYYY-MM-DD",
    ("event_ticket", "event_at"): "ISO-8601 datetime",
}

_ITEMS_SCHEMA: dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "qty": {"type": "integer", "default": 1},
            "price": {"type": "number", "default": 0},
        },
        "required": ["name"],
    },
}


def _inject_field_descriptions(schema: dict[str, Any], card_type: str) -> None:
    props = schema.get("properties")
    if not isinstance(props, dict):
        return
    for field_name, prop in props.items():
        if not isinstance(prop, dict):
            continue
        desc = LLM_FIELD_DESCRIPTIONS.get((card_type, field_name))
        if desc:
            prop["description"] = desc
        if field_name == "items" and prop.get("type") == "array":
            prop.update(_ITEMS_SCHEMA)
        if field_name == "summary":
            prop["description"] = "Human-readable 1-line description. Never use IDs or hashes."


def card_type_to_llm_json_schema(card_type: str) -> dict[str, Any]:
    """Full JSON Schema for ``card_type`` with system fields stripped and descriptions injected."""

    model = CARD_TYPES.get(card_type)
    if model is None:
        raise KeyError(f"Unknown card type: {card_type}")
    if not isinstance(model, type) or not issubclass(model, BaseCard):
        raise KeyError(f"Not a card model: {card_type!r}")
    schema = model.model_json_schema()
    schema = copy.deepcopy(schema)
    _prune_omit_fields(schema)
    _inject_field_descriptions(schema, card_type)
    return schema


def schema_version_for_card_type(card_type: str) -> str:
    """SHA-256 of the pruned JSON Schema (cache / invalidation key)."""

    sch = card_type_to_llm_json_schema(card_type)
    raw = json.dumps(sch, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def combined_schema_version(card_types: list[str]) -> str:
    """Single hash for a set of target card types (sorted)."""

    parts = [schema_version_for_card_type(t) for t in sorted(set(card_types))]
    blob = "\n".join(parts)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def extractable_types_reference_markdown() -> str:
    """Human-readable catalog for triage / extraction prompts."""

    lines: list[str] = []
    for cat, types in EXTRACTABLE_TYPES.items():
        lines.append(f"- **{cat}**: {', '.join(types)}")
    return "\n".join(lines)
