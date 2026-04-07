"""Tests for scripts/verify_ground_truth.py."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from tests.archive_sync.extractors.conftest import write_email_to_vault


def _load_verify():
    root = Path(__file__).resolve().parents[3]
    spec = importlib.util.spec_from_file_location("verify_ground_truth", root / "scripts" / "verify_ground_truth.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod.run_verification


def test_verify_ground_truth_emission(tmp_path, extractor_vault, sample_email_card):
    fm, body = sample_email_card(
        "hfa-email-message-gtv",
        "orders@doordash.com",
        "50% off your next order",
        "promo body",
        sent_at="2023-01-01T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/gtv.md", fm, body)
    gt = {
        "provider": "DoorDash",
        "extractor_id": "doordash",
        "holdout_emails": [{"uid": "hfa-email-message-gtv", "expected_cards": []}],
    }
    p = tmp_path / "gt.json"
    p.write_text(json.dumps(gt), encoding="utf-8")
    run_verification = _load_verify()
    text = run_verification(p, extractor_vault)
    assert "Ground Truth Verification" in text
    assert "1/1" in text or "emission" in text.lower()
