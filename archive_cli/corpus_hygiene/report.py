"""Human-readable census summaries and staged decision exports."""

from __future__ import annotations

import json
from pathlib import Path

from archive_cli.validation_gates.report import GateRunReport

from .decisions import EmailCorpusDecisionRecord


def write_decision_records_jsonl(path: str | Path, records: list[EmailCorpusDecisionRecord]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(rec.to_dict(), sort_keys=True) for rec in records]
    p.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def render_census_summary(result: object, report: GateRunReport) -> str:
    from .census import CensusResult

    assert isinstance(result, CensusResult)
    lines = [
        f"# Section B — email corpus hygiene dry-run",
        "",
        f"- decision_run_id: `{report.decision_run_id}`",
        f"- policy_version: `{report.policy_version}`",
        f"- archive_instance: `{report.archive_instance}`",
        f"- engine_mode: `{report.engine_mode}`",
        f"- completion_state: `{report.completion_state}`",
        "",
        "## Classification sources",
        "",
    ]
    for src, count in sorted(result.classification_source_counts.items()):
        lines.append(f"- {src}: {count}")
    lines.extend(
        [
            "",
            f"- new_llm_calls: {result.new_llm_call_count}",
            "",
            "## Proposed corpus decisions",
            "",
        ]
    )
    for decision, count in sorted(result.corpus_counts.items()):
        lines.append(f"- {decision}: {count}")
    lines.extend(
        [
            "",
            f"- unchanged: {result.unchanged_count}",
            f"- newly_demoted: {result.newly_demoted_count}",
            "",
            "## Index impact (estimate)",
            "",
        ]
    )
    for key, val in sorted(result.index_impact.items()):
        lines.append(f"- {key}: {val}")
    lines.extend(["", "## Review buckets", ""])
    for bucket, samples in sorted(result.review_buckets.items()):
        if samples:
            lines.append(f"- {bucket}: {len(samples)} sample(s)")
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- production mutation: no",
            "- dry-run only: yes",
            "- apply unlocked: no",
            "",
            f"next_recommended_gate: `{report.next_recommended_gate}`",
        ]
    )
    return "\n".join(lines) + "\n"


def render_apply_summary(result: object, report: GateRunReport) -> str:
    from .apply import ApplyResult

    assert isinstance(result, ApplyResult)
    lines = [
        "# Section B — email corpus hygiene apply",
        "",
        f"- decision_run_id: `{report.decision_run_id}`",
        f"- archive_instance: `{report.archive_instance}`",
        f"- engine_mode: `{report.engine_mode}`",
        f"- completion_state: `{report.completion_state}`",
        "",
        "## Apply counts",
        "",
        f"- threads_applied: {result.counts.threads_applied}",
        f"- cards_updated: {result.counts.cards_updated}",
        f"- files_deleted: {result.counts.files_deleted}",
        f"- uids_purged: {result.counts.uids_purged}",
        "",
    ]
    if result.counts.by_corpus_state:
        lines.append("## Corpus state updates")
        lines.append("")
        for state, count in sorted(result.counts.by_corpus_state.items()):
            lines.append(f"- {state}: {count}")
    deleted_label = "yes" if result.vault_markdown_deleted else "no"
    lines.extend(
        [
            "",
            "## Safety",
            "",
            f"- vault markdown deleted: {deleted_label}",
            "- rollback available: yes",
            "",
            f"next_recommended_gate: `{report.next_recommended_gate}`",
        ]
    )
    return "\n".join(lines) + "\n"


def render_rollback_summary(result: object, report: GateRunReport) -> str:
    from .rollback import RollbackResult

    assert isinstance(result, RollbackResult)
    lines = [
        "# Section B — email corpus hygiene rollback",
        "",
        f"- decision_run_id: `{report.decision_run_id}`",
        f"- archive_instance: `{report.archive_instance}`",
        f"- engine_mode: `{report.engine_mode}`",
        "",
        "## Rollback counts",
        "",
        f"- cards_restored: {result.counts.cards_restored}",
        f"- threads_restored: {result.counts.threads_restored}",
        f"- kit_files_restored: {result.counts.kit_files_restored}",
        "",
        "## Safety",
        "",
        "- llm_calls: no",
        f"- vault markdown deleted: {'yes' if result.vault_markdown_deleted else 'no'}",
        "",
        f"next_recommended_gate: `{report.next_recommended_gate}`",
    ]
    return "\n".join(lines) + "\n"
