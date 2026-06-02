"""Deterministic input hashing for processor staleness."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def compute_input_hash(
    *,
    input_uid: str,
    fields: dict[str, Any],
    hash_field_names: tuple[str, ...],
    processor_version: str = "",
) -> str:
    """Hash only declared input_hash_fields plus processor_version when provided."""

    payload: dict[str, Any] = {"input_uid": input_uid}
    for name in hash_field_names:
        if name in fields:
            payload[name] = fields[name]
    if processor_version:
        payload["processor_version"] = processor_version
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def format_output_identity(template: str, **values: str) -> str:
    """Render deterministic output identity from declaration template."""

    merged = {"processor_key": "", "input_uid": "", **values}
    try:
        return template.format(**merged)
    except KeyError:
        return template
