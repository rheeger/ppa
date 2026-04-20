"""Phase 6.5 Step 8.0 -- merchant-normalizer fixture gate.

Hard-gates the MODULE_FINANCE_RECONCILE TIER_HIGH tier: if these tests fail,
finance reconcile narrows to source-email-only (TIER_HIGH requires reliable
merchant matching).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from archive_cli.merchant_normalizer import (_merchants_match,
                                             _normalize_merchant)

FIXTURE_DIR = Path(__file__).parent / "fixtures"

# Gate thresholds (from the Phase 6.5 plan):
POSITIVE_GATE = 0.90   # >=90% of real pairs must match
NEGATIVE_GATE = 0.98   # <=2% false positive rate


@pytest.fixture(scope="module")
def positive_pairs():
    return json.loads((FIXTURE_DIR / "merchant_pairs.json").read_text())


@pytest.fixture(scope="module")
def negative_pairs():
    return json.loads((FIXTURE_DIR / "merchant_negatives.json").read_text())


def test_positive_fixture_size(positive_pairs):
    """Gate requires >=30 positive pairs with real-world prefixes."""
    assert len(positive_pairs) >= 30


def test_negative_fixture_size(negative_pairs):
    """Gate requires >=100 negative pairs from demonstrably different merchants."""
    assert len(negative_pairs) >= 100


def test_positive_match_rate(positive_pairs):
    hits = sum(1 for p in positive_pairs if _merchants_match(p["a"], p["b"]))
    rate = hits / len(positive_pairs)
    assert rate >= POSITIVE_GATE, (
        f"positive match rate {hits}/{len(positive_pairs)}={rate:.2%} "
        f"below gate {POSITIVE_GATE:.0%}"
    )


def test_negative_reject_rate(negative_pairs):
    correct = sum(1 for n in negative_pairs if not _merchants_match(n["a"], n["b"]))
    rate = correct / len(negative_pairs)
    assert rate >= NEGATIVE_GATE, (
        f"negative true-reject rate {correct}/{len(negative_pairs)}={rate:.2%} "
        f"below gate {NEGATIVE_GATE:.0%}"
    )


def test_normalize_is_idempotent():
    """normalize(normalize(x)) == normalize(x) for a sample of inputs."""
    samples = [
        "AMZN MKTPLACE",
        "SQ *BLUE BOTTLE",
        "Netflix.com",
        "Apple Inc.",
        "",
        "   ",
    ]
    for s in samples:
        once = _normalize_merchant(s)
        twice = _normalize_merchant(once)
        assert once == twice, f"idempotence broken on {s!r}: {once!r} != {twice!r}"


@pytest.mark.parametrize("empty_input", [None, "", "   "])
def test_empty_inputs_return_empty(empty_input):
    assert _normalize_merchant(empty_input) == ""


@pytest.mark.parametrize("a,b", [
    (None, "Amazon"),
    ("Amazon", None),
    ("", "Amazon"),
    ("Amazon", ""),
])
def test_merchants_match_empty_inputs_are_false(a, b):
    assert _merchants_match(a, b) is False
