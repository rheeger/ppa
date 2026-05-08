"""Tests for Phase 9 deployment health report types."""

from archive_cli.commands.health_check import DeploymentReport


def test_report_ok_when_no_missing_types() -> None:
    r = DeploymentReport(missing_types=[], temporal_index_present=True)
    assert r.ok


def test_report_not_ok_when_missing_types() -> None:
    r = DeploymentReport(missing_types=["meal_order"], temporal_index_present=True, ok=False)
    assert not r.ok


def test_report_not_ok_when_temporal_index_missing() -> None:
    r = DeploymentReport(missing_types=[], temporal_index_present=False, ok=False)
    assert not r.ok


def test_report_ok_when_card_count_within_tolerance() -> None:
    r = DeploymentReport(index_card_count=1_882_460, vault_file_count=1_882_463, card_count_match=True)
    assert r.ok


def test_no_knowledge_fields_in_report() -> None:
    assert not hasattr(DeploymentReport, "knowledge_card_count")
    assert not hasattr(DeploymentReport, "stale_knowledge_count")
