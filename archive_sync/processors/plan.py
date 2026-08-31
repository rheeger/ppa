"""Map dirty inputs to processor staleness plans (read-only)."""

from __future__ import annotations

from collections import Counter
from typing import Iterable

from .batch import ProcessorPlanItem, ProcessorPlanSummary
from .declarations import ProcessorDeclaration, iter_processor_declarations, topological_order
from .input_hash import compute_input_hash, format_output_identity
from .staleness import ProcessorInputSnapshot, evaluate_staleness


def build_processor_plan(
    inputs: Iterable[ProcessorInputSnapshot],
    *,
    declarations: Iterable[ProcessorDeclaration] | None = None,
    processor_keys: Iterable[str] | None = None,
) -> ProcessorPlanSummary:
    """Evaluate which processors would run for inputs without executing them."""

    decls = list(declarations) if declarations is not None else list(iter_processor_declarations())
    if processor_keys is not None:
        allowed = set(processor_keys)
        decls = [d for d in decls if d.processor_key in allowed]
    order = topological_order(decls)
    decl_by_key = {d.processor_key: d for d in decls}
    ordered_decls = [decl_by_key[k] for k in order if k in decl_by_key]

    snapshots = list(inputs)
    summary = ProcessorPlanSummary(input_count=len(snapshots))
    skip_counter: Counter[str] = Counter()
    stale_counter: Counter[str] = Counter()
    triggered: set[str] = set()

    dirty_uids = {s.input_uid for s in snapshots if s.source_dirty}
    summary.dirty_count = len(dirty_uids)

    for snapshot in snapshots:
        for decl in ordered_decls:
            if not decl.enabled:
                continue
            if snapshot.card_type not in decl.input_card_types:
                continue

            current_hash = compute_input_hash(
                input_uid=snapshot.input_uid,
                fields={
                    **snapshot.field_values,
                    "corpus_state": snapshot.corpus_state,
                    "processor_decision": snapshot.processor_decision,
                },
                hash_field_names=decl.input_hash_fields,
                processor_version=decl.processor_version,
            )
            evaluation = evaluate_staleness(decl, snapshot, current_input_hash=current_hash)
            output_id = format_output_identity(
                decl.output_identity,
                processor_key=decl.processor_key,
                input_uid=snapshot.input_uid,
                extractor_version=decl.processor_version,
                prompt_version=decl.processor_version,
                chunk_key=snapshot.field_values.get("chunk_key", ""),
                model_id=snapshot.field_values.get("model_id", ""),
                source_uid=snapshot.input_uid,
                target_uid=str(snapshot.field_values.get("target_uid", "")),
                relation=str(snapshot.field_values.get("relation", "")),
                linker_version=decl.processor_version,
                entity_mention_hash=str(snapshot.field_values.get("entity_mention_hash", "")),
            )
            item = ProcessorPlanItem(
                processor_key=decl.processor_key,
                input_uid=snapshot.input_uid,
                stale=evaluation.stale,
                skipped=evaluation.skipped,
                skip_reason=evaluation.skip_reason,
                stale_reasons=list(evaluation.stale_reasons),
                current_input_hash=current_hash,
                output_identity=output_id,
            )
            summary.items.append(item)
            if evaluation.skipped:
                summary.skipped_count += 1
                if evaluation.skip_reason:
                    skip_counter[evaluation.skip_reason] += 1
            elif evaluation.stale:
                summary.stale_count += 1
                summary.pending_count += 1
                triggered.add(decl.processor_key)
                for reason in evaluation.stale_reasons:
                    stale_counter[reason] += 1

    summary.skip_reasons = dict(skip_counter)
    summary.stale_reasons = dict(stale_counter)
    summary.processors_triggered = [k for k in order if k in triggered]
    return summary


def processors_for_dirty_input(
    snapshot: ProcessorInputSnapshot,
    *,
    declarations: Iterable[ProcessorDeclaration] | None = None,
) -> list[str]:
    """Return processor keys that would evaluate (not skip) for one dirty input."""

    plan = build_processor_plan([snapshot], declarations=declarations)
    return sorted({item.processor_key for item in plan.items if not item.skipped})
