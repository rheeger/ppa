"""Phase 6.5 Step 9 — wiring tests.

Asserts the linker framework is populated correctly + the three new modules
are active + the semantic module is retired + LLM_REVIEW_MODULES excludes
the new deterministic modules + policy version bumped to 6.
"""

from __future__ import annotations

import pytest
from archive_cli import linker_framework as lf
from archive_cli import seed_links as s


def test_policy_version_bumped_to_six():
    assert s.SEED_LINK_POLICY_VERSION == 6


def test_all_ten_linkers_registered():
    expected = {
        # legacy seven
        "identityLinker",
        "communicationLinker",
        "calendarLinker",
        "mediaLinker",
        "orphanRepairLinker",
        "graphConsistencyLinker",
        "semanticLinker",
        # phase 6.5 new three
        "meetingArtifactLinker",
        "tripClusterLinker",
        "financeReconcileLinker",
    }
    assert set(lf.ALL_LINKERS.keys()) == expected


@pytest.mark.parametrize(
    "module_name,phase_owner",
    [
        ("identityLinker", "phase_2.875"),
        ("communicationLinker", "phase_2.875"),
        ("calendarLinker", "phase_2.875"),
        ("mediaLinker", "phase_2.875"),
        ("orphanRepairLinker", "phase_2.875"),
        ("graphConsistencyLinker", "phase_2.875"),
        ("semanticLinker", "phase_6"),
        ("meetingArtifactLinker", "phase_6.5"),
        ("tripClusterLinker", "phase_6.5"),
        ("financeReconcileLinker", "phase_6.5"),
    ],
)
def test_phase_owner_tagged(module_name, phase_owner):
    assert lf.ALL_LINKERS[module_name].phase_owner == phase_owner


def test_semantic_linker_is_retired():
    spec = lf.ALL_LINKERS["semanticLinker"]
    assert spec.lifecycle_state == "retired"
    # Retired modules should NOT be wired into CARD_TYPE_MODULES.
    for modules in s.CARD_TYPE_MODULES.values():
        assert "semanticLinker" not in modules


def test_new_modules_active():
    for module_name in ("meetingArtifactLinker", "tripClusterLinker", "financeReconcileLinker"):
        assert lf.ALL_LINKERS[module_name].lifecycle_state == "active"


def test_new_modules_wired_into_card_type_modules():
    assert "financeReconcileLinker" in s.CARD_TYPE_MODULES.get("finance", ())
    assert "tripClusterLinker" in s.CARD_TYPE_MODULES.get("accommodation", ())
    assert "meetingArtifactLinker" in s.CARD_TYPE_MODULES.get("meeting_transcript", ())


def test_new_modules_are_deterministic():
    for module_name in ("meetingArtifactLinker", "tripClusterLinker", "financeReconcileLinker"):
        assert lf.ALL_LINKERS[module_name].scoring_mode == "deterministic"


def test_new_modules_not_in_llm_review():
    for module_name in ("meetingArtifactLinker", "tripClusterLinker", "financeReconcileLinker"):
        assert module_name not in s.LLM_REVIEW_MODULES


def test_new_link_types_registered():
    assert "finance_reconciles" in s.PROPOSED_LINK_TYPES
    assert "part_of_trip" in s.PROPOSED_LINK_TYPES
    assert s.LINK_SURFACE_BY_TYPE["finance_reconciles"].surface == s.SURFACE_DERIVED_ONLY
    assert s.LINK_SURFACE_BY_TYPE["part_of_trip"].surface == s.SURFACE_DERIVED_ONLY


def test_post_promotion_action_tagged():
    # Phase 6.5 linkers write edges only.
    for m in ("meetingArtifactLinker", "tripClusterLinker", "financeReconcileLinker"):
        assert lf.ALL_LINKERS[m].post_promotion_action == "edges_only"
    # Identity + Orphan canonically touch frontmatter on promotion.
    assert lf.ALL_LINKERS["identityLinker"].post_promotion_action == "frontmatter_delta"
    assert lf.ALL_LINKERS["orphanRepairLinker"].post_promotion_action == "frontmatter_delta"


def test_register_linker_duplicate_raises():
    spec = lf.ALL_LINKERS["financeReconcileLinker"]
    with pytest.raises(ValueError, match="already registered"):
        lf.register_linker(spec)


def test_list_linkers_filter_by_lifecycle():
    active = lf.list_linkers(lifecycle="active")
    retired = lf.list_linkers(lifecycle="retired")
    assert all(s.lifecycle_state == "active" for s in active)
    assert all(s.lifecycle_state == "retired" for s in retired)
    assert len(active) + len(retired) == len(lf.ALL_LINKERS)


def test_unregister_and_reregister():
    """unregister_linker is test-only, but round-trip must be clean."""
    original = lf.ALL_LINKERS["meetingArtifactLinker"]
    lf.unregister_linker("meetingArtifactLinker")
    assert "meetingArtifactLinker" not in lf.ALL_LINKERS
    assert "meetingArtifactLinker" not in s.CARD_TYPE_MODULES.get("meeting_transcript", ())
    # Re-register the exact same spec.
    lf.register_linker(original)
    assert "meetingArtifactLinker" in lf.ALL_LINKERS
    assert "meetingArtifactLinker" in s.CARD_TYPE_MODULES.get("meeting_transcript", ())
