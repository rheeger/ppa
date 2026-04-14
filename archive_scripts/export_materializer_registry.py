#!/usr/bin/env python3
"""Emit archive_crate/materializer_registry.json from the live Python card + projection registries.

Run from repo root after changing archive_cli/card_registry.py or projections/base.py:

  python scripts/export_materializer_registry.py
"""

from __future__ import annotations

import json
from pathlib import Path

from archive_cli.card_registry import CARD_TYPE_REGISTRATIONS
from archive_cli.projections.base import SHARED_TYPED_COLUMNS


def _col_spec(col) -> dict:
    d = col.default
    if hasattr(d, "item"):  # numpy scalar
        d = d.item()
    return {
        "name": col.name,
        "sql_type": col.sql_type,
        "nullable": col.nullable,
        "indexed": col.indexed,
        "source_field": col.source_field,
        "value_mode": col.value_mode,
        "default": d,
    }


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    out_path = root / "archive_crate" / "materializer_registry.json"

    shared = [_col_spec(c) for c in SHARED_TYPED_COLUMNS]

    card_types = []
    for reg in CARD_TYPE_REGISTRATIONS:
        typed_cols = [_col_spec(c) for c in reg.projection_columns]
        card_types.append(
            {
                "card_type": reg.card_type,
                "projection_table": reg.projection_table,
                "person_edge_type": reg.person_edge_type,
                "quality_critical_fields": list(reg.quality_critical_fields),
                "edge_rules": [
                    {
                        "field_name": rule.field_name,
                        "edge_type": rule.edge_type,
                        "target": rule.target,
                        "source_fields": list(rule.source_fields),
                        "multi": rule.multi,
                    }
                    for rule in reg.edge_rules
                ],
                "shared_typed_columns": shared,
                "typed_columns": typed_cols,
            }
        )

    payload = {
        "registry_version": 1,
        "projection_registry_version": __import__(
            "archive_cli.projections.registry", fromlist=["PROJECTION_REGISTRY_VERSION"]
        ).PROJECTION_REGISTRY_VERSION,
        "card_types": card_types,
    }

    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
