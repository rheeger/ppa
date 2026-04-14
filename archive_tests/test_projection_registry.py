from __future__ import annotations

from archive_cli.projections.registry import PROJECTION_REGISTRY, TYPED_PROJECTIONS, projection_for_card_type
from archive_vault.schema import CARD_TYPES


def test_every_current_card_type_has_registered_typed_projection():
    for card_type in CARD_TYPES:
        projection = projection_for_card_type(card_type)
        assert projection is not None
        assert projection.table_name


def test_projection_registry_contains_generic_and_typed_layers():
    names = {projection.table_name for projection in PROJECTION_REGISTRY}
    assert "cards" in names
    assert "chunks" in names
    assert "people" in names
    assert "git_messages" in names
    assert len(TYPED_PROJECTIONS) == len(CARD_TYPES)
