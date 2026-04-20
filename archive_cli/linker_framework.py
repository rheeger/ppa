"""Phase 6.5 linker framework.

Declarative registry for seed-link modules. Every linker is a ``LinkerSpec``
registered via ``register_linker`` at module import time; everything else --
dispatch, scoring, lifecycle gating, CLI surface, health reporting, and the
setup-wizard integration scheduled for v3 Phase 11 -- reads off the registry.

During Phase 6.5 this framework augments the existing ``archive_cli.seed_links``
rather than replacing it. All ten modules live under
``archive_cli/linker_modules/`` and register themselves as ``LinkerSpec``s at
import time.

A follow-up cleanup phase (tentatively Phase 6.75) can split ``seed_links.py``
into the full package layout described in
``.cursor/plans/phase_6_5_linker_framework_e4f2a9b7.plan.md``. This module
defines the public framework contract that survives that split.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Literal

if TYPE_CHECKING:  # pragma: no cover - import cycles avoided at runtime
    from archive_cli.seed_links import (LinkSurfacePolicy, SeedCardSketch,
                                        SeedLinkCandidate, SeedLinkCatalog,
                                        SeedLinkDecision)


log = logging.getLogger("ppa.linkers.framework")


# --- Type aliases ----------------------------------------------------------

Lifecycle = Literal["active", "deprecated", "retired"]
ScoringMode = Literal["deterministic", "weighted", "semantic", "bespoke"]
PostPromotionAction = Literal["edges_only", "frontmatter_delta", "new_cards"]
ScoringFn = Callable[[dict[str, Any]], tuple[float, float, float, float, float]]
BespokeEvaluator = Callable[
    [Any, "SeedLinkCatalog", "SeedLinkCandidate"],
    "SeedLinkDecision",
]
PostBuildHook = Callable[["SeedLinkCatalog"], None]
KeyFn = Callable[["SeedCardSketch"], "str | int | Iterable[str] | None"]
GeneratorFn = Callable[["SeedLinkCatalog", "SeedCardSketch"], list["SeedLinkCandidate"]]


# --- CatalogIndexSpec ------------------------------------------------------


@dataclass(frozen=True)
class CatalogIndexSpec:
    """Declarative description of a catalog index.

    ``build_seed_link_catalog`` collects all ``CatalogIndexSpec``s from
    registered linkers and populates them in one pass over the vault.

    ``key_fn`` return values:
      - ``str`` / ``int``: card indexed under that single key.
      - ``Iterable[str]``: card indexed under each key (multi-key).
      - ``None``: card skipped for this index.
    """

    name: str
    source_card_types: tuple[str, ...]
    key_fn: KeyFn
    description: str = ""


# --- LinkerSpec ------------------------------------------------------------


@dataclass(frozen=True)
class LinkerSpec:
    """Single source of truth for a linker.

    Required fields
    ---------------
    module_name
        Stable identifier e.g. ``"financeReconcileLinker"``. Used as the
        registry key, in ``CARD_TYPE_MODULES``, and in worker logs.
    source_card_types
        Tuple of card types whose cards are enqueued for this module. Empty
        tuple means "any card type" (used by MODULE_ORPHAN).
    emits_link_types
        Tuple of ``LINK_TYPE_*`` string constants this module emits. Drives
        the ``ppa linker impact`` query filter.
    generator
        Callable ``(catalog, source) -> list[SeedLinkCandidate]``.
    scoring_fn
        Callable ``(features) -> (det, lex, graph, emb, risk_penalty)``.
    scoring_mode
        ``"deterministic"`` -> ``final = det - risk``.
        ``"weighted"``      -> legacy 0.45 det + 0.12 lex + 0.13 graph + 0.18 llm + 0.12 emb - risk.
        ``"semantic"``      -> dual-tier gate ``llm * emb - risk``.
        ``"bespoke"``       -> delegates to ``bespoke_evaluator``.

    Optional fields follow the plan's Implementation Readiness Contract.
    """

    module_name: str
    source_card_types: tuple[str, ...]
    emits_link_types: tuple[str, ...]
    generator: GeneratorFn
    scoring_fn: ScoringFn
    scoring_mode: ScoringMode
    catalog_indexes: tuple[CatalogIndexSpec, ...] = ()
    policies: tuple["LinkSurfacePolicy", ...] = ()
    requires_llm_judge: bool = False
    lifecycle_state: Lifecycle = "active"
    phase_owner: str = ""
    post_promotion_action: PostPromotionAction = "edges_only"
    description: str = ""
    bespoke_evaluator: BespokeEvaluator | None = None
    post_build_hook: PostBuildHook | None = None


# --- Registry + wiring tables ---------------------------------------------

# Populated by ``register_linker`` calls from modules at import time.
ALL_LINKERS: dict[str, LinkerSpec] = {}

# Updated by ``register_linker``; external callers import these from
# ``archive_cli.seed_links`` as before. The framework augments the mutable
# dicts/sets on-the-fly rather than shadowing them, so legacy callers see
# the combined wiring.
_CARD_TYPE_MODULES_REF: dict[str, tuple[str, ...]] | None = None
_LLM_REVIEW_MODULES_REF: set[str] | None = None
_PROPOSED_LINK_TYPES_REF: set[str] | None = None
_LINK_SURFACE_BY_TYPE_REF: dict[str, Any] | None = None


def _bind_wiring_tables(
    card_type_modules: dict[str, tuple[str, ...]],
    llm_review_modules: set[str],
    proposed_link_types: set[str],
    link_surface_by_type: dict[str, Any],
) -> None:
    """Called by ``archive_cli.seed_links`` at import to give the framework
    pointers to the legacy wiring tables so ``register_linker`` can mutate them.

    This is a one-time binding; subsequent calls are no-ops (idempotent).
    """
    global _CARD_TYPE_MODULES_REF, _LLM_REVIEW_MODULES_REF
    global _PROPOSED_LINK_TYPES_REF, _LINK_SURFACE_BY_TYPE_REF
    if _CARD_TYPE_MODULES_REF is None:
        _CARD_TYPE_MODULES_REF = card_type_modules
        _LLM_REVIEW_MODULES_REF = llm_review_modules
        _PROPOSED_LINK_TYPES_REF = proposed_link_types
        _LINK_SURFACE_BY_TYPE_REF = link_surface_by_type


# --- Lifecycle override file ----------------------------------------------

_LIFECYCLE_OVERRIDES_PATH = "_artifacts/_linkers/_lifecycle_overrides.json"


def _load_lifecycle_overrides() -> dict[str, Lifecycle]:
    """Read ops-supplied lifecycle overrides, if present.

    The file is an escape hatch for operators who need to toggle a linker's
    lifecycle state without a code change. Format::

        {"overrides": {"moduleNameLinker": "retired", ...}}

    Absent file, malformed JSON, or missing ``overrides`` key -> empty dict.
    """
    path = Path(_LIFECYCLE_OVERRIDES_PATH)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Failed to parse lifecycle overrides at %s: %s", path, exc)
        return {}
    overrides = raw.get("overrides") if isinstance(raw, dict) else None
    if not isinstance(overrides, dict):
        return {}
    return {
        str(k): v for k, v in overrides.items()
        if v in ("active", "deprecated", "retired")
    }


# --- register_linker / unregister_linker -----------------------------------


def register_linker(spec: LinkerSpec) -> None:
    """Register a linker. Idempotent by ``module_name``; duplicate raises.

    Wires the spec into:
      - ``ALL_LINKERS[module_name]`` (always)
      - ``LINK_SURFACE_BY_TYPE`` (always, for every policy in ``spec.policies``)
      - ``PROPOSED_LINK_TYPES`` (always, for every emit_link_type + policy)
      - ``CARD_TYPE_MODULES`` (only if ``lifecycle_state == "active"``)
      - ``LLM_REVIEW_MODULES`` (only if ``lifecycle_state != "retired"``
        and ``requires_llm_judge`` is True)
    """
    if spec.module_name in ALL_LINKERS:
        raise ValueError(
            f"linker {spec.module_name!r} already registered "
            f"(previous phase_owner={ALL_LINKERS[spec.module_name].phase_owner!r})"
        )

    overrides = _load_lifecycle_overrides()
    effective_state: Lifecycle = overrides.get(spec.module_name, spec.lifecycle_state)
    if effective_state != spec.lifecycle_state:
        spec = replace(spec, lifecycle_state=effective_state)
        log.info(
            "linker %s lifecycle_state overridden to %r via %s",
            spec.module_name, effective_state, _LIFECYCLE_OVERRIDES_PATH,
        )

    ALL_LINKERS[spec.module_name] = spec

    # Policy table + proposed link types.
    if _LINK_SURFACE_BY_TYPE_REF is not None:
        for policy in spec.policies:
            _LINK_SURFACE_BY_TYPE_REF[policy.link_type] = policy
    if _PROPOSED_LINK_TYPES_REF is not None:
        for policy in spec.policies:
            _PROPOSED_LINK_TYPES_REF.add(policy.link_type)
        for link_type in spec.emits_link_types:
            _PROPOSED_LINK_TYPES_REF.add(link_type)

    # Active-only wiring.
    if spec.lifecycle_state == "active" and _CARD_TYPE_MODULES_REF is not None:
        for card_type in spec.source_card_types:
            current = _CARD_TYPE_MODULES_REF.get(card_type, ())
            if spec.module_name not in current:
                _CARD_TYPE_MODULES_REF[card_type] = (*current, spec.module_name)
    if (
        spec.lifecycle_state != "retired"
        and spec.requires_llm_judge
        and _LLM_REVIEW_MODULES_REF is not None
    ):
        _LLM_REVIEW_MODULES_REF.add(spec.module_name)


def unregister_linker(module_name: str) -> None:
    """Tear down a registration. Test-only; production code must not call."""
    spec = ALL_LINKERS.pop(module_name, None)
    if spec is None:
        return
    if _LINK_SURFACE_BY_TYPE_REF is not None:
        for policy in spec.policies:
            _LINK_SURFACE_BY_TYPE_REF.pop(policy.link_type, None)
    if _PROPOSED_LINK_TYPES_REF is not None:
        for policy in spec.policies:
            _PROPOSED_LINK_TYPES_REF.discard(policy.link_type)
        # Don't remove emits_link_types from PROPOSED (another module may also
        # emit the same type).
    if _CARD_TYPE_MODULES_REF is not None:
        for card_type, modules in list(_CARD_TYPE_MODULES_REF.items()):
            _CARD_TYPE_MODULES_REF[card_type] = tuple(
                m for m in modules if m != module_name
            )
    if _LLM_REVIEW_MODULES_REF is not None:
        _LLM_REVIEW_MODULES_REF.discard(module_name)


# --- Score via registered spec --------------------------------------------


def score_via_spec(
    candidate: "SeedLinkCandidate",
) -> tuple[float, float, float, float, float]:
    """Generic ``_component_scores`` wrapper. Returns zeros if module_name is
    unknown (stale link_decisions rows during policy-version bump transitions).
    """
    spec = ALL_LINKERS.get(candidate.module_name)
    if spec is None:
        return 0.0, 0.0, 0.0, 0.0, 0.2
    return spec.scoring_fn(candidate.features)


def generate_via_spec(
    catalog: "SeedLinkCatalog",
    source: "SeedCardSketch",
    module_name: str,
) -> list["SeedLinkCandidate"]:
    """Generic dispatcher. Empty list for unknown / retired modules."""
    spec = ALL_LINKERS.get(module_name)
    if spec is None or spec.lifecycle_state == "retired":
        return []
    return spec.generator(catalog, source)


# --- Post-build hook dispatcher -------------------------------------------


def run_post_build_hooks(catalog: "SeedLinkCatalog") -> None:
    """Invoke every registered spec's ``post_build_hook`` in registration order.

    Called by ``build_seed_link_catalog`` at the end of catalog construction.
    Hooks are expected to mutate ``catalog.private_indexes`` (or equivalent
    extension fields) directly.
    """
    for spec in ALL_LINKERS.values():
        if spec.post_build_hook is None:
            continue
        try:
            spec.post_build_hook(catalog)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning(
                "post_build_hook for %s raised %s; continuing.",
                spec.module_name, exc,
            )


# --- Linker-owned catalog-index access ------------------------------------


def get_spec_catalog_indexes() -> tuple[CatalogIndexSpec, ...]:
    """All ``CatalogIndexSpec``s declared by registered specs, deduped by name."""
    seen: dict[str, CatalogIndexSpec] = {}
    for spec in ALL_LINKERS.values():
        for idx in spec.catalog_indexes:
            if idx.name in seen and seen[idx.name].key_fn is not idx.key_fn:
                log.warning(
                    "CatalogIndexSpec name collision: %s (from %s vs earlier)",
                    idx.name, spec.module_name,
                )
            seen.setdefault(idx.name, idx)
    return tuple(seen.values())


# --- Private-index helper on SeedLinkCatalog ------------------------------

# ``SeedLinkCatalog`` predates this framework and does not have a
# ``private_indexes`` field. Rather than add one (and churn every legacy
# dataclass usage), we keep indexes in a process-level dict keyed on
# ``id(catalog)``. Cleanup is opportunistic: when a new catalog is built with
# the same id() (Python id-reuse after GC), the setter overwrites the entry.
# For long-lived processes that build many catalogs, call ``clear_private_indexes``.

_PRIVATE_INDEXES: dict[int, dict[str, dict[Any, list[Any]]]] = {}


def set_private_index(
    catalog: Any,
    name: str,
    data: dict[Any, list[Any]],
) -> None:
    """Attach a linker-owned index to a ``SeedLinkCatalog`` instance."""
    buckets = _PRIVATE_INDEXES.setdefault(id(catalog), {})
    buckets[name] = data


def get_private_index(catalog: Any, name: str) -> dict[Any, list[Any]]:
    """Fetch a linker-owned index; returns an empty dict if absent."""
    buckets = _PRIVATE_INDEXES.get(id(catalog), {})
    return buckets.get(name, {})


def all_private_indexes(catalog: Any) -> dict[str, dict[Any, list[Any]]]:
    """Return all linker-owned indexes attached to this catalog."""
    return dict(_PRIVATE_INDEXES.get(id(catalog), {}))


def clear_private_indexes(catalog: Any) -> None:
    """Drop all linker-owned indexes attached to a specific catalog."""
    _PRIVATE_INDEXES.pop(id(catalog), None)


# --- Registry introspection helpers ---------------------------------------


def list_linkers(
    lifecycle: Lifecycle | None = None,
    phase_owner: str | None = None,
) -> list[LinkerSpec]:
    """Registry filter helper. Returns specs sorted by module_name."""
    specs = list(ALL_LINKERS.values())
    if lifecycle is not None:
        specs = [s for s in specs if s.lifecycle_state == lifecycle]
    if phase_owner is not None:
        specs = [s for s in specs if s.phase_owner == phase_owner]
    return sorted(specs, key=lambda s: s.module_name)


def get_linker(module_name: str) -> LinkerSpec | None:
    """Return the registered spec or ``None``."""
    return ALL_LINKERS.get(module_name)


# --- Lifecycle override writer (for `ppa linker deprecate/revive/retire`) --


def set_lifecycle_override(module_name: str, state: Lifecycle) -> None:
    """Persist a lifecycle override. Takes effect on next process start.

    Writes to ``_artifacts/_linkers/_lifecycle_overrides.json`` atomically.
    """
    path = Path(_LIFECYCLE_OVERRIDES_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    overrides = _load_lifecycle_overrides()
    overrides[module_name] = state
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"overrides": overrides}, indent=2, sort_keys=True))
    os.replace(tmp, path)
