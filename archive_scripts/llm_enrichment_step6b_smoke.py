#!/usr/bin/env python3
"""Phase 2.75 Step 6b — run triage + extraction for one thread (human review aid).

Requires local Ollama. Example::

  .venv/bin/python scripts/llm_enrichment_step6b_smoke.py \\
    --vault /path/to/vault \\
    --thread-id thread_fix001

Use ``--list-threads`` on a small vault to discover ``gmail_thread_id`` values.

Exit 0 when the pipeline completes (including triage-skip). Exit 1 on errors.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from archive_sync.llm_enrichment.cache import InferenceCache  # noqa: E402
from archive_sync.llm_enrichment.runner import (  # noqa: E402
    build_thread_index,
    load_email_stubs_for_vault,
    run_thread_pipeline,
)
from archive_vault.llm_provider import OllamaProvider  # noqa: E402


def _md_block(title: str, body: str) -> str:
    return f"## {title}\n\n{body}\n\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="LLM enrichment single-thread smoke (Step 6b)")
    ap.add_argument("--vault", type=Path, required=True, help="Path to vault root")
    ap.add_argument("--thread-id", dest="thread_id", default="", help="gmail_thread_id")
    ap.add_argument("--list-threads", action="store_true", help="Print thread ids + counts, then exit")
    ap.add_argument("--base-url", default="http://localhost:11434", help="Ollama base URL")
    ap.add_argument("--triage-model", default="", help="Override triage model (default: gemma4:31b)")
    ap.add_argument("--extract-model", default="", help="Override extract model (default: same as triage)")
    ap.add_argument("--cache-db", type=Path, default=None, help="Inference cache SQLite path")
    ap.add_argument("--run-id", default="step6b-smoke", help="run_id for cache rows")
    args = ap.parse_args()

    vault = args.vault.expanduser().resolve()
    if not vault.is_dir():
        print(f"vault not found: {vault}", file=sys.stderr)
        return 1

    stubs = load_email_stubs_for_vault(vault)
    index = build_thread_index(stubs)
    if args.list_threads:
        print(f"threads: {len(index)} (from {len(stubs)} email_message stubs)\n")
        for tid, group in sorted(index.items(), key=lambda x: (-len(x[1]), x[0]))[:80]:
            subj = group[0].subject[:60] if group else ""
            print(f"  {tid}\tmsgs={len(group)}\t{subj}")
        if len(index) > 80:
            print(f"  ... ({len(index) - 80} more)")
        return 0

    if not args.thread_id.strip():
        print("Provide --thread-id or use --list-threads", file=sys.stderr)
        return 1

    model = args.triage_model.strip() or "gemma4:31b"
    extract_model = args.extract_model.strip() or model
    provider = OllamaProvider(model=model, base_url=args.base_url)
    if not provider.health_check():
        print("Ollama health_check failed — is `ollama serve` running and the model installed?", file=sys.stderr)
        return 1

    cache = InferenceCache(args.cache_db) if args.cache_db else None
    try:
        tri, ex, doc = run_thread_pipeline(
            vault,
            args.thread_id.strip(),
            provider,
            inference_cache=cache,
            triage_model=model,
            extract_model=extract_model,
            run_id=args.run_id,
        )
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1
    finally:
        if cache is not None:
            cache.close()

    out: list[str] = []
    out.append("# Step 6b — single thread LLM enrichment\n")
    out.append(_md_block("Triage", json.dumps(tri.raw, indent=2)))
    out.append(
        _md_block(
            "Triage decision",
            f"- skip: **{tri.skip}**\n- classification: `{tri.classification}`\n"
            f"- confidence: {tri.confidence}\n- card_types: {tri.card_types}\n"
            f"- cache_hit: {tri.cache_hit}\n",
        )
    )
    if tri.skip or ex is None:
        out.append("*(Extraction skipped — triage negative or no card types.)*\n")
        print("".join(out))
        return 0

    out.append(_md_block("Extraction reasoning", ex.reasoning or "(empty)"))
    out.append(f"- extract cache_hit: {ex.cache_hit}\n\n")
    for i, c in enumerate(ex.cards, start=1):
        out.append(f"### Card {i}: `{c.card_type}`\n\n")
        if c.validated is not None:
            out.append("**Pydantic:** OK\n\n")
        else:
            out.append("**Pydantic:** validation failed (see raw JSON)\n\n")
        out.append("```json\n" + json.dumps(c.data, indent=2)[:12000] + "\n```\n\n")
        if c.round_trip_warnings:
            out.append("**Round-trip warnings:**\n")
            for w in c.round_trip_warnings[:30]:
                out.append(f"- {w}\n")
            out.append("\n")
    if doc is not None:
        out.append(_md_block("Thread meta", f"- message_count: {doc.message_count}\n- content_hash: `{doc.content_hash}`\n"))

    print("".join(out))
    print(
        "\n---\n**Step 6b:** Review prompts in `archive_sync/llm_enrichment/prompts/` "
        "and this output. Reply PROCEED / FIX PROMPTS / FIX PLUMBING.\n",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
