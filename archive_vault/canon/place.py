"""Place / merchant / restaurant profiles — do not smash semantics."""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Literal

Profile = Literal["merchant_bank", "place_geo", "restaurant_receipt"]

_WS = re.compile(r"\s+")
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
_AMAZON_ALIASES_RE = re.compile(
    r"^\s*(?:AMZN(?:\s+MKTPLACE|\s+MKTPL)?|AMAZON(?:\s+MKTPL)?)\b",
    re.IGNORECASE,
)
_SUFFIX_RE = re.compile(
    r"\s+(?:\.COM|COM|INC|LLC|LTD|CO|CORP|INCORPORATED)\b\.?\s*$",
    re.IGNORECASE,
)
_PUNCT = re.compile(r"[^\w\s]")


def _merchant_bank(value: str) -> str:
    s = _STRIP_PREFIX_RE.sub("", value)
    s = _AMAZON_ALIASES_RE.sub("amazon", s)
    s = _PUNCT.sub(" ", s)
    s = s.lower().strip()
    s = _SUFFIX_RE.sub("", s)
    return _WS.sub(" ", s).strip()


def _place_geo(value: str) -> str:
    n = value.lower().strip()
    if n.startswith("the "):
        n = n[4:]
    n = re.sub(r"\bst\b\.?", "street", n)
    n = re.sub(r"\bave\b\.?", "avenue", n)
    return _WS.sub(" ", n).strip()


def _restaurant_receipt(value: str) -> str:
    return _WS.sub(" ", value.strip().lower())


def canonical(value: str | None, profile: Profile = "restaurant_receipt") -> str:
    """Normalize a place-like string under a named profile."""

    raw = str(value or "")
    if not raw.strip():
        return ""
    if profile == "merchant_bank":
        return _merchant_bank(raw)
    if profile == "place_geo":
        return _place_geo(raw)
    if profile == "restaurant_receipt":
        return _restaurant_receipt(raw)
    raise ValueError(f"unknown place profile: {profile}")


def merchants_match(left: str | None, right: str | None) -> bool:
    """Bank-feed merchant equality used by finance reconcile."""

    na = canonical(left, profile="merchant_bank")
    nb = canonical(right, profile="merchant_bank")
    if not na or not nb:
        return False
    if na == nb:
        return True
    if SequenceMatcher(None, na, nb).ratio() >= 0.70:
        return True
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    return len(shorter) >= 4 and shorter in longer
