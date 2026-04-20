"""Phase 6.5 merchant-name normalization for MODULE_FINANCE_RECONCILE.

Bank feeds mangle merchant names in predictable ways (``AMZN MKTPLACE`` vs
``Amazon.com``, ``SQ *BLUE BOTTLE`` vs ``Blue Bottle Coffee``). The normalizer
strips common processor prefixes + incorporation suffixes + punctuation and
compares the remainder with token-set similarity.

See ``archive_tests/fixtures/merchant_pairs.json`` and
``archive_tests/fixtures/merchant_negatives.json`` for the ground-truth
fixture sets; ``archive_tests/test_merchant_normalizer.py`` enforces the
>=90% positive / >=98% negative accuracy gate.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher

# Bank-feed processor prefixes that carry the real merchant name AFTER them.
# Example: "SQ *BLUE BOTTLE" -> "BLUE BOTTLE". We strip the prefix + any
# whitespace and star glyph.
_STRIP_PREFIX_RE = re.compile(
    r"^(?:"
    r"SQ\s*\*|"
    r"TST\s*\*|"
    r"SP\s*\*|"
    r"PAYPAL\s*\*|"
    r"PY\s*\*|"
    r"DOORDASH\s*\*|"
    r"UBER\s*\*|"
    r"AMZ\s*\*"
    r")\s*",
    re.IGNORECASE,
)

# Bank-feed tokens that ARE themselves the merchant (Amazon variants). These
# get rewritten to the canonical name rather than stripped.
_AMAZON_ALIASES_RE = re.compile(
    r"^\s*(?:AMZN(?:\s+MKTPLACE|\s+MKTPL)?|AMAZON(?:\s+MKTPL)?)\b",
    re.IGNORECASE,
)

# Common corporate suffixes. Stripped only at the end of the string after
# punctuation normalization. ".com" becomes " com" after _PUNCT so we match
# the bare "com" token too.
_SUFFIX_RE = re.compile(
    r"\s+(?:\.COM|COM|INC|LLC|LTD|CO|CORP|INCORPORATED)\b\.?\s*$",
    re.IGNORECASE,
)

_PUNCT = re.compile(r"[^\w\s]")
_WS = re.compile(r"\s+")


def _normalize_merchant(value: str | None) -> str:
    """Lowercase + strip processor prefix + rewrite common aliases + strip
    corporate suffix + collapse whitespace and punctuation. Returns an empty
    string for falsy input.
    """
    if not value:
        return ""
    s = str(value)
    # Processor prefixes: "SQ *BLUE BOTTLE" -> "BLUE BOTTLE".
    s = _STRIP_PREFIX_RE.sub("", s)
    # Amazon aliases: "AMZN MKTPLACE", "AMZN", "AMAZON" -> canonical "amazon".
    s = _AMAZON_ALIASES_RE.sub("amazon", s)
    s = _PUNCT.sub(" ", s)
    s = s.lower().strip()
    # Strip corporate suffix after punctuation normalization.
    s = _SUFFIX_RE.sub("", s)
    s = _WS.sub(" ", s).strip()
    return s


def _merchants_match(a: str | None, b: str | None) -> bool:
    """Return True iff two merchant strings plausibly refer to the same entity.

    Heuristic: normalize both sides; match if either
      (a) token-set similarity >= 0.70 via difflib.SequenceMatcher, OR
      (b) one is a substring of the other AND min(len) >= 4
    """
    na = _normalize_merchant(a)
    nb = _normalize_merchant(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    ratio = SequenceMatcher(None, na, nb).ratio()
    if ratio >= 0.70:
        return True
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    if len(shorter) >= 4 and shorter in longer:
        return True
    return False
