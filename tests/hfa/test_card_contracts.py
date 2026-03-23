from __future__ import annotations

from hfa.card_contracts import CARD_TYPE_SPECS
from hfa.schema import CARD_TYPES


def test_every_card_type_has_contract_spec():
    assert set(CARD_TYPES) == set(CARD_TYPE_SPECS)


def test_every_card_type_declares_typed_projection():
    for card_type, spec in CARD_TYPE_SPECS.items():
        assert spec.card_type == card_type
        assert spec.typed_projection
        assert spec.rel_path_family
