"""MODULE_FINANCE_RECONCILE -- Phase 6.5 finance-to-derived-transaction linker.

Tier ladder (highest precision first; first tier that matches wins):

  TIER_SOURCE_EMAIL           (0.98):  finance.source_email wikilink == other.source_email wikilink
                                       AND same currency
                                       AND >=2 tight-bound corroborating signals (per
                                       archive_docs/runbooks/linker-quality-gates.md) drawn
                                       from {amount-within-cents, date-within-2-days,
                                       merchant-tokens-agree}. The wikilink alone is not
                                       sufficient -- the upstream resolver is LLM-driven and
                                       its bare output is review-only, never auto-promote.
                                       1 corroborating signal -> review-only at 0.72
                                       (TIER_SOURCE_EMAIL_WEAK).
                                       0 corroborating signals -> rejected.
  TIER_HIGH                   (0.90):  abs(|finance.amount| - other.amount) <= 0.01
                                       AND abs(date_delta) <= 2 days
                                       AND _merchants_match(finance.counterparty, other.merchant)
                                       AND same currency
  TIER_MEDIUM                 (0.78):  amount match + <=5 days + shared gmail_thread_id
                                       via source_email resolution
  TIER_LOW (review-only)      (0.55):  amount match + <=3 days only  (retirable at calibration)

**Retired (Phase 6.5 Step 17):** ``TIER_PHASE2875_LLM_SIGNAL`` — Phase 2.875
``match_candidates.jsonl`` rows targeted ``email_message``, not derived
transaction cards, so the tier was misleading. See
``archive_docs/runbooks/linker-retirement-protocol.md`` Appendix A.

Refund handling: finance.amount<0 and transaction_type in {refund, credit,
return, chargeback} -> match on abs(amount) with extended date window per
tier. Tag features.refund=true.

The subplan at .cursor/plans/phase_6_5_finance_reconcile_d2e1f3a4.plan.md has
the full motivation and per-sub-step DoD; this file implements it.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.merchant_normalizer import (_merchants_match,
                                             _normalize_merchant)
from archive_cli.seed_links import (LINK_TYPE_FINANCE_RECONCILES, LinkEvidence,
                                    SeedCardSketch, SeedLinkCandidate,
                                    SeedLinkCatalog, _append_candidate)

log = logging.getLogger("ppa.linkers.finance_reconcile")

MODULE_FINANCE_RECONCILE = "financeReconcileLinker"

# Derived card types that can reconcile against a finance charge.
RECONCILE_TARGET_TYPES: tuple[str, ...] = (
    "purchase", "meal_order", "ride", "subscription", "payroll",
    "flight", "accommodation", "car_rental", "grocery_order", "event_ticket",
)

_REFUND_TRANSACTION_TYPES = frozenset({"refund", "credit", "return", "chargeback"})

# Standard / refund date windows per tier (in days).
# Refund windows are wider because credit-card refunds settle 3-60 days after.
_WINDOW_DAYS = {
    "RECONCILE_TIER_HIGH":   {"standard": 2, "refund": 60},
    "RECONCILE_TIER_MEDIUM": {"standard": 5, "refund": 60},
    "RECONCILE_TIER_LOW":    {"standard": 3, "refund": 14},
}

# LOW tier can be retired at calibration sub-step 8.8 if spot-check < 80%.
LOW_TIER_RETIRED = False


# --- Parsing / normalization helpers --------------------------------------


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.fromisoformat(s[:10])
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _wikilink_key(value: Any) -> str:
    """Normalize a source_email wikilink to its bare target (lowercase, stripped)."""
    if not value:
        return ""
    s = str(value).strip()
    if s.startswith("[[") and s.endswith("]]"):
        s = s[2:-2]
    # Split on '|' for aliased wikilinks [[target|alias]].
    s = s.split("|", 1)[0]
    return s.strip().lower()


def _is_refund(fm: dict[str, Any]) -> bool:
    try:
        amt = float(fm.get("amount") or 0.0)
    except (TypeError, ValueError):
        return False
    tt = str(fm.get("transaction_type") or "").lower().strip()
    return amt < 0 and tt in _REFUND_TRANSACTION_TYPES


def _same_currency(a: dict[str, Any], b: dict[str, Any]) -> bool:
    ca = str(a.get("currency") or "USD").upper().strip()
    cb = str(b.get("currency") or "USD").upper().strip()
    return ca == cb


# --- Per-card-type amount extraction --------------------------------------


def _amount_for(card: SeedCardSketch) -> float:
    """Return the card's reconcile-target amount (always positive)."""
    fm = card.frontmatter
    try:
        if card.card_type == "finance":
            return abs(float(fm.get("amount") or 0.0))
        if card.card_type == "purchase":
            return float(fm.get("total") or 0.0)
        if card.card_type == "meal_order":
            return float(fm.get("total") or 0.0)
        if card.card_type == "grocery_order":
            return float(fm.get("total") or 0.0)
        if card.card_type == "ride":
            return float(fm.get("fare") or 0.0) + float(fm.get("tip") or 0.0)
        if card.card_type == "flight":
            return float(fm.get("fare_amount") or 0.0)
        if card.card_type == "accommodation":
            return float(fm.get("total_cost") or 0.0)
        if card.card_type == "car_rental":
            return float(fm.get("total_cost") or 0.0)
        if card.card_type == "subscription":
            p = float(fm.get("price") or 0.0)
            # Skip $0 trials / cancellation events.
            return p if p > 0 else 0.0
        if card.card_type == "payroll":
            return float(fm.get("net_amount") or 0.0)
        if card.card_type == "event_ticket":
            price = float(fm.get("price") or 0.0)
            qty = int(fm.get("quantity") or 1)
            return price * max(qty, 1)
    except (TypeError, ValueError):
        return 0.0
    return 0.0


def _merchant_for(card: SeedCardSketch) -> str:
    fm = card.frontmatter
    ct = card.card_type
    if ct == "purchase":
        return str(fm.get("vendor") or "")
    if ct == "meal_order":
        restaurant = str(fm.get("restaurant") or "")
        service = str(fm.get("service") or "")
        return restaurant or service
    if ct == "ride":
        return str(fm.get("service") or "")
    if ct == "flight":
        return str(fm.get("airline") or "")
    if ct == "accommodation":
        return str(fm.get("property_name") or fm.get("booking_source") or "")
    if ct == "car_rental":
        return str(fm.get("company") or "")
    if ct == "subscription":
        return str(fm.get("service_name") or "")
    if ct == "payroll":
        return str(fm.get("employer") or "")
    if ct == "grocery_order":
        store = str(fm.get("store") or "")
        service = str(fm.get("service") or "")
        return store or service
    if ct == "event_ticket":
        return str(fm.get("venue") or fm.get("event_name") or "")
    return ""


# --- Catalog indexes (built via post_build_hook) --------------------------


def _derived_amount_key(card: SeedCardSketch) -> str | None:
    amt = _amount_for(card)
    if amt <= 0:
        return None
    return f"{card.card_type}|{round(amt, 2):.2f}"


def _build_finance_indexes(catalog: SeedLinkCatalog) -> None:
    """post_build_hook: populate the indexes finance_reconcile needs."""
    finances_by_amount: dict[str, list[SeedCardSketch]] = {}
    derived_by_amount: dict[str, list[SeedCardSketch]] = {}
    cards_by_source_email: dict[str, list[SeedCardSketch]] = {}

    for fin in catalog.cards_by_type.get("finance", []):
        amt = _amount_for(fin)
        if amt > 0:
            key = f"{round(amt, 2):.2f}"
            finances_by_amount.setdefault(key, []).append(fin)
        se = _wikilink_key(fin.frontmatter.get("source_email", ""))
        if se:
            cards_by_source_email.setdefault(se, []).append(fin)

    for ct in RECONCILE_TARGET_TYPES:
        for card in catalog.cards_by_type.get(ct, []):
            key = _derived_amount_key(card)
            if key is not None:
                derived_by_amount.setdefault(key, []).append(card)
            se = _wikilink_key(card.frontmatter.get("source_email", ""))
            if se:
                cards_by_source_email.setdefault(se, []).append(card)

    lf.set_private_index(catalog, "finances_by_amount_rounded2", finances_by_amount)
    lf.set_private_index(catalog, "derived_by_amount_rounded2", derived_by_amount)
    lf.set_private_index(catalog, "cards_by_source_email_wikilink", cards_by_source_email)


# --- Tier matchers --------------------------------------------------------


def _source_email_corroboration(
    source: SeedCardSketch,
    other: SeedCardSketch,
    *,
    fin_amount: float,
    fin_at: datetime | None,
    fin_merchant_norm: str,
) -> dict[str, Any]:
    """Compute tight-bound corroboration between a finance card and an
    other card that share a `source_email` wikilink. Returns a dict with
    the per-signal booleans and the integer count.

    See archive_docs/runbooks/linker-quality-gates.md for the bounds.
    """
    other_amount = _amount_for(other)
    amount_match = bool(
        fin_amount > 0 and other_amount > 0
        and abs(fin_amount - other_amount) <= 0.01
    )
    date_match = False
    date_delta_days: float | None = None
    if fin_at is not None:
        other_at = _parse_ts(other.activity_at)
        if other_at is not None:
            date_delta_days = abs((fin_at - other_at).total_seconds()) / 86400.0
            date_match = date_delta_days <= 2.0
    other_merchant_norm = _normalize_merchant(_merchant_for(other))
    merchant_match = bool(
        fin_merchant_norm and other_merchant_norm
        and _merchants_match(fin_merchant_norm, other_merchant_norm)
    )
    count = int(amount_match) + int(date_match) + int(merchant_match)
    return {
        "amount_match": amount_match,
        "date_match": date_match,
        "merchant_match": merchant_match,
        "corroborating_signal_count": count,
        "date_delta_days": (
            round(date_delta_days, 2) if date_delta_days is not None else None
        ),
        "merchant_norm_finance": fin_merchant_norm,
        "merchant_norm_other": other_merchant_norm,
    }


def _tier1_source_email(
    catalog: SeedLinkCatalog,
    source: SeedCardSketch,
    *,
    seen: set[str],
    is_refund: bool,
    fin_amount: float,
    fin_at: datetime | None,
    fin_merchant_norm: str,
) -> list[tuple[SeedCardSketch, dict[str, Any]]]:
    se_key = _wikilink_key(source.frontmatter.get("source_email", ""))
    if not se_key:
        return []
    cards_by_se = lf.get_private_index(catalog, "cards_by_source_email_wikilink")
    out: list[tuple[SeedCardSketch, dict[str, Any]]] = []
    for other in cards_by_se.get(se_key, []):
        if other.uid == source.uid or other.uid in seen:
            continue
        if other.card_type not in RECONCILE_TARGET_TYPES:
            continue
        if not _same_currency(source.frontmatter, other.frontmatter):
            continue
        corr = _source_email_corroboration(
            source, other,
            fin_amount=fin_amount,
            fin_at=fin_at,
            fin_merchant_norm=fin_merchant_norm,
        )
        signals = corr["corroborating_signal_count"]
        if signals >= 2:
            tier = "RECONCILE_TIER_SOURCE_EMAIL"
            det = 0.98
            risk = 0.0
        elif signals == 1:
            tier = "RECONCILE_TIER_SOURCE_EMAIL_WEAK"
            det = 0.72
            risk = 0.06
        else:
            # Bare wikilink with no tight-bound corroboration is rejected
            # per archive_docs/runbooks/linker-quality-gates.md. Fall through
            # to lower tiers (or no candidate at all).
            continue
        features: dict[str, Any] = {
            "tier": tier,
            "deterministic_score": det,
            "risk_penalty": risk,
            "source_email_key": se_key,
            "refund": is_refund,
        }
        features.update(corr)
        out.append((other, features))
    return out


def _tier3_high(
    catalog: SeedLinkCatalog,
    source: SeedCardSketch,
    *,
    seen: set[str],
    fin_amount: float,
    fin_at: datetime,
    fin_merchant_norm: str,
    is_refund: bool,
) -> list[tuple[SeedCardSketch, dict[str, Any]]]:
    if not fin_merchant_norm:
        return []
    window_days = _WINDOW_DAYS["RECONCILE_TIER_HIGH"]["refund" if is_refund else "standard"]
    derived_by_amount = lf.get_private_index(catalog, "derived_by_amount_rounded2")
    out: list[tuple[SeedCardSketch, dict[str, Any]]] = []
    for ct in RECONCILE_TARGET_TYPES:
        key = f"{ct}|{round(fin_amount, 2):.2f}"
        for other in derived_by_amount.get(key, []):
            if other.uid == source.uid or other.uid in seen:
                continue
            if not _same_currency(source.frontmatter, other.frontmatter):
                continue
            other_at = _parse_ts(other.activity_at)
            if other_at is None:
                continue
            delta_days = abs((fin_at - other_at).total_seconds()) / 86400.0
            if delta_days > window_days:
                continue
            other_merchant_norm = _normalize_merchant(_merchant_for(other))
            if not _merchants_match(fin_merchant_norm, other_merchant_norm):
                continue
            out.append((other, {
                "tier": "RECONCILE_TIER_HIGH",
                "deterministic_score": 0.90,
                "risk_penalty": 0.0,
                "date_delta_days": round(delta_days, 2),
                "merchant_norm_finance": fin_merchant_norm,
                "merchant_norm_other": other_merchant_norm,
                "refund": is_refund,
            }))
    return out


def _thread_id_from_source_email(
    catalog: SeedLinkCatalog, card: SeedCardSketch,
) -> str | None:
    """Resolve a card's source_email wikilink to its gmail_thread_id."""
    se = _wikilink_key(card.frontmatter.get("source_email", ""))
    if not se:
        return None
    msgs = catalog.email_messages_by_message_id.get(se, [])
    # Also try slug-based lookup in case the wikilink uses slug rather than uid.
    if not msgs:
        msgs = catalog.cards_by_slug.get(se, [])
        if not isinstance(msgs, list):
            msgs = [msgs] if msgs else []
    for msg in msgs:
        if not isinstance(msg, SeedCardSketch):
            continue
        tid = str(msg.frontmatter.get("gmail_thread_id") or "").strip()
        if tid:
            return tid
    return None


def _tier4_medium(
    catalog: SeedLinkCatalog,
    source: SeedCardSketch,
    *,
    seen: set[str],
    fin_amount: float,
    fin_at: datetime,
    is_refund: bool,
) -> list[tuple[SeedCardSketch, dict[str, Any]]]:
    fin_thread = _thread_id_from_source_email(catalog, source)
    if not fin_thread:
        return []
    window_days = _WINDOW_DAYS["RECONCILE_TIER_MEDIUM"]["refund" if is_refund else "standard"]
    derived_by_amount = lf.get_private_index(catalog, "derived_by_amount_rounded2")
    out: list[tuple[SeedCardSketch, dict[str, Any]]] = []
    for ct in RECONCILE_TARGET_TYPES:
        key = f"{ct}|{round(fin_amount, 2):.2f}"
        for other in derived_by_amount.get(key, []):
            if other.uid == source.uid or other.uid in seen:
                continue
            if not _same_currency(source.frontmatter, other.frontmatter):
                continue
            other_at = _parse_ts(other.activity_at)
            if other_at is None:
                continue
            if abs((fin_at - other_at).total_seconds()) / 86400.0 > window_days:
                continue
            other_thread = _thread_id_from_source_email(catalog, other)
            if not other_thread or other_thread != fin_thread:
                continue
            out.append((other, {
                "tier": "RECONCILE_TIER_MEDIUM",
                "deterministic_score": 0.78,
                "risk_penalty": 0.05,
                "shared_thread_id": fin_thread,
                "refund": is_refund,
            }))
    return out


def _tier5_low(
    catalog: SeedLinkCatalog,
    source: SeedCardSketch,
    *,
    seen: set[str],
    fin_amount: float,
    fin_at: datetime,
    is_refund: bool,
) -> list[tuple[SeedCardSketch, dict[str, Any]]]:
    if LOW_TIER_RETIRED:
        return []
    window_days = _WINDOW_DAYS["RECONCILE_TIER_LOW"]["refund" if is_refund else "standard"]
    derived_by_amount = lf.get_private_index(catalog, "derived_by_amount_rounded2")
    out: list[tuple[SeedCardSketch, dict[str, Any]]] = []
    for ct in RECONCILE_TARGET_TYPES:
        key = f"{ct}|{round(fin_amount, 2):.2f}"
        for other in derived_by_amount.get(key, []):
            if other.uid == source.uid or other.uid in seen:
                continue
            if not _same_currency(source.frontmatter, other.frontmatter):
                continue
            other_at = _parse_ts(other.activity_at)
            if other_at is None:
                continue
            delta_days = abs((fin_at - other_at).total_seconds()) / 86400.0
            if delta_days > window_days:
                continue
            out.append((other, {
                "tier": "RECONCILE_TIER_LOW",
                "deterministic_score": 0.55,
                "risk_penalty": 0.15,
                "date_delta_days": round(delta_days, 2),
                "refund": is_refund,
            }))
    return out


# --- Generator ------------------------------------------------------------


def _generate_finance_reconcile_candidates(
    catalog: SeedLinkCatalog, source: SeedCardSketch,
) -> list[SeedLinkCandidate]:
    if source.card_type != "finance":
        return []

    fin_amount = _amount_for(source)
    if fin_amount <= 0:
        return []

    fin_at = _parse_ts(source.activity_at)
    if fin_at is None:
        return []

    fin_merchant_norm = _normalize_merchant(
        source.frontmatter.get("counterparty", "")
    )
    is_refund = _is_refund(source.frontmatter)

    results: list[SeedLinkCandidate] = []
    seen: set[str] = set()

    for matcher in (
        lambda: _tier1_source_email(
            catalog, source, seen=seen, is_refund=is_refund,
            fin_amount=fin_amount, fin_at=fin_at,
            fin_merchant_norm=fin_merchant_norm,
        ),
        lambda: _tier3_high(
            catalog, source, seen=seen, fin_amount=fin_amount, fin_at=fin_at,
            fin_merchant_norm=fin_merchant_norm, is_refund=is_refund,
        ),
        lambda: _tier4_medium(
            catalog, source, seen=seen, fin_amount=fin_amount, fin_at=fin_at,
            is_refund=is_refund,
        ),
        lambda: _tier5_low(
            catalog, source, seen=seen, fin_amount=fin_amount, fin_at=fin_at,
            is_refund=is_refund,
        ),
    ):
        for other, features in matcher():
            if other.uid in seen:
                continue
            seen.add(other.uid)
            _append_finance_reconcile(results, source, other, features)

    return results


def _append_finance_reconcile(
    results: list[SeedLinkCandidate],
    source: SeedCardSketch,
    target: SeedCardSketch,
    features: dict[str, Any],
) -> None:
    tier = features.get("tier", "RECONCILE_TIER_UNKNOWN")
    evidences = [
        LinkEvidence(
            evidence_type="predicate_match",
            evidence_source="finance_reconcile",
            feature_name="tier",
            feature_value=tier,
            feature_weight=float(features.get("deterministic_score", 0.0)),
            raw_payload_json=dict(features),
        ),
    ]
    _append_candidate(
        results,
        module_name=MODULE_FINANCE_RECONCILE,
        source=source,
        target=target,
        proposed_link_type=LINK_TYPE_FINANCE_RECONCILES,
        candidate_group=f"finance_reconcile:{tier}",
        features=features,
        evidences=evidences,
    )


def _score_finance_reconcile_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    det = float(features.get("deterministic_score", 0.0))
    risk = float(features.get("risk_penalty", 0.0))
    return det, 0.0, 0.0, 0.0, risk


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_FINANCE_RECONCILE,
    source_card_types=("finance",),
    emits_link_types=(LINK_TYPE_FINANCE_RECONCILES,),
    generator=_generate_finance_reconcile_candidates,
    scoring_fn=_score_finance_reconcile_features,
    scoring_mode="deterministic",
    policies=(),  # LINK_TYPE_FINANCE_RECONCILES policy already registered in seed_links
    requires_llm_judge=False,
    lifecycle_state="active",
    phase_owner="phase_6.5",
    post_promotion_action="edges_only",
    description=(
        "Matches bank/credit-card finance cards to merchant-side derived cards "
        "(purchase, meal_order, ride, subscription, payroll, flight, accommodation, "
        "car_rental, grocery_order, event_ticket) via a deterministic tier ladder."
    ),
    post_build_hook=_build_finance_indexes,
))
