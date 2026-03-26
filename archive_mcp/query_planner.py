"""Deterministic-first, model-optional query planning for hybrid retrieval."""

from __future__ import annotations

import re
from typing import Any

from .retrieval_pipeline import FilterInference, PlannedQuery, QueryPlan

# Obvious source tokens -> index source_filter substring (caller may refine).
_SOURCE_ALIAS_MAP: tuple[tuple[str, str], ...] = (
    ("gmail", "gmail"),
    ("google mail", "gmail"),
    ("otter", "otter"),
    # Avoid "calendar" alone — it appears in natural phrases ("calendar dinner") unrelated to source=calendar.
    ("google calendar", "calendar"),
    ("ical ", "calendar"),
    ("photos", "photos"),
    ("github", "github"),
    ("git ", "github"),
    ("notion", "notion"),
    ("imessage", "imessage"),
    ("slack", "slack"),
)

# Natural phrases -> card type filter (HFA card `type` field).
_TYPE_HINT_MAP: tuple[tuple[str, str], ...] = (
    ("meeting transcript", "meeting_transcript"),
    ("transcript", "meeting_transcript"),
    ("meeting", "meeting_transcript"),
    ("email thread", "email_thread"),
    ("thread", "email_thread"),
    ("person", "person"),
    ("contact", "person"),
    ("document", "document"),
    ("medical", "medical_record"),
    ("commit", "git_commit"),
    ("pull request", "git_thread"),
    ("pr ", "git_thread"),
)

_QUOTED = re.compile(r'"([^"]+)"')
_EMAIL = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
_YEAR = re.compile(r"\b(19|20)\d{2}\b")
_ISO_DATE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")


def _merge_filter_str(explicit: str, inferred: str) -> str:
    explicit = (explicit or "").strip()
    inferred = (inferred or "").strip()
    if not inferred:
        return explicit
    if not explicit:
        return inferred
    if inferred.lower() in explicit.lower():
        return explicit
    return explicit


class DeterministicQueryPlanner:
    """Extract phrases, dates, source/type hints without a model."""

    def plan(self, query: str, *, max_variants: int = 2, **_: Any) -> QueryPlan:
        text = (query or "").strip()
        if not text:
            return QueryPlan(
                queries=(PlannedQuery(text="", role="primary"),),
                inferred=FilterInference(),
                planner_provider="deterministic",
            )

        phrases = tuple(m.group(1).strip() for m in _QUOTED.finditer(text) if m.group(1).strip())
        emails = tuple(sorted(set(_EMAIL.findall(text))))
        external_ids = tuple(e for e in emails)

        lowered = text.lower()
        source_hints: list[str] = []
        for needle, src in _SOURCE_ALIAS_MAP:
            if needle in lowered:
                source_hints.append(src)
        type_hints: list[str] = []
        for needle, ctype in _TYPE_HINT_MAP:
            if needle in lowered:
                type_hints.append(ctype)

        start_hint = ""
        end_hint = ""
        years = [m.group(0) for m in _YEAR.finditer(text)]
        if years:
            y = years[0]
            start_hint = f"{y}-01-01"
            end_hint = f"{y}-12-31"
        for m in _ISO_DATE.finditer(text):
            start_hint = m.group(0)
            end_hint = m.group(0)
            break

        inferred = FilterInference(
            type_hints=tuple(dict.fromkeys(type_hints))[:3],
            source_hints=tuple(dict.fromkeys(source_hints))[:3],
            start_date_hint=start_hint,
            end_date_hint=end_hint,
            phrases=phrases,
            emails=emails,
            external_ids=external_ids,
        )

        queries: list[PlannedQuery] = [PlannedQuery(text=text, role="primary", weight=1.0)]
        for phrase in phrases[: max(0, max_variants)]:
            if phrase.lower() != text.lower() and phrase not in (q.text for q in queries):
                queries.append(PlannedQuery(text=phrase, role="phrase_variant", weight=0.85))
        for email in emails[: max(0, max_variants)]:
            if email not in (q.text for q in queries):
                queries.append(PlannedQuery(text=email, role="email_anchor", weight=0.9))

        return QueryPlan(queries=tuple(queries), inferred=inferred, planner_provider="deterministic")


def augment_plan_with_model(plan: QueryPlan, **_kwargs: Any) -> QueryPlan:
    """Optional model-assisted expansion; disabled until a provider is wired."""
    return plan


def build_query_plan(
    query: str,
    *,
    config: dict[str, Any] | None = None,
    **_: Any,
) -> QueryPlan:
    cfg = config or {}
    qp = cfg.get("query_planner") or {}
    if not qp.get("enabled", True):
        return QueryPlan(
            queries=(PlannedQuery(text=(query or "").strip(), role="primary"),),
            inferred=FilterInference(),
            planner_provider="disabled",
        )

    max_variants = int(qp.get("max_variants", 2) or 2)
    planner = DeterministicQueryPlanner()
    plan = planner.plan(query, max_variants=max_variants)
    if (qp.get("provider") or "deterministic") == "model":
        plan = augment_plan_with_model(plan)
    if not qp.get("allow_filter_inference", True):
        plan = QueryPlan(
            queries=plan.queries, inferred=FilterInference(), planner_provider=plan.planner_provider + "+no_inference"
        )

    return plan


def effective_filters_from_plan(
    plan: QueryPlan,
    *,
    type_filter: str,
    source_filter: str,
    start_date: str,
    end_date: str,
    allow_merge: bool = True,
) -> tuple[str, str, str, str]:
    if not allow_merge:
        return type_filter, source_filter, start_date, end_date
    inf = plan.inferred
    t = type_filter
    s = source_filter
    sd = start_date
    ed = end_date
    if inf.type_hints and not (t or "").strip():
        t = inf.type_hints[0]
    if inf.source_hints and not (s or "").strip():
        s = inf.source_hints[0]
    if inf.start_date_hint and not (sd or "").strip():
        sd = inf.start_date_hint
    if inf.end_date_hint and not (ed or "").strip():
        ed = inf.end_date_hint
    return t, s, sd, ed
