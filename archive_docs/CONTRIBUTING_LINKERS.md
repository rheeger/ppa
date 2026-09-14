# Contributing a linker

A linker helps someone follow a record to the evidence around it. It can connect a charge to a purchase, a flight to an accommodation, or a meeting to its transcript. Those connections make questions across services easier to answer without repeatedly matching records inside a chat.

A wrong link can attach a charge to the wrong trip or attribute a conversation to the wrong person. Start with a relationship that has structural evidence, and test the similar-looking records that should stay separate. Read the [linker architecture](LINKER_ARCHITECTURE.md) before implementing a module.

## Decide whether a linker is needed

1. Check whether the records already contain the relationship. If a `DeclEdgeRule` in `archive_cli/card_registry.py` should materialize it, fix that path first.
2. Identify a shared identifier or a set of corroborating fields. Booking codes, account identity, amounts, and bounded dates can support a relationship; topic similarity alone does not establish one.
3. Confirm that you are linking existing cards. New records derived from source material generally belong in an extractor.
4. Write a short tier table that explains each match rule, its bounds, and a plausible false positive.

## Implement and register

1. Create a module under `archive_cli/linker_modules/`. The existing scaffold command can write a starting file:

   ```bash
   ppa linker scaffold --module exampleLinker --source-types purchase --emits example_relation
   ```

2. Implement the candidate generator, scoring function, any `CatalogIndexSpec` indexes, and `LinkSurfacePolicy` rules.
3. Register a `LinkerSpec` at module import and import the module from `archive_cli/linker_modules/__init__.py`.
4. Add synthetic tests with a positive and negative case for each tier, plus unrelated records that yield no candidates.
5. Verify registration with `ppa linker info --module exampleLinker` and `ppa linker list --json`.

The scaffold names above are placeholders. Choose a relation whose meaning you can define and support with evidence. Keep account and provider identity in the match rule where it affects uniqueness.

## Check match accuracy

Start in a disposable vault that contains the relevant synthetic records:

```bash
ppa linker calibrate --module exampleLinker --mode vault --vault /tmp/ppa-linker-fixture --limit 25
```

The preview checks generator behavior and tier distribution. A small synthetic fixture is not production precision evidence. Before automatic promotion on a real archive, follow the [quality gates](runbooks/linker-quality-gates.md), including a stratified sample of at least 30 candidates per tier and at least 95% precision.

Automatic promotion needs a deterministic identity match or the required independent signals within tight bounds. Two-signal agreement remains review-only; similarity or one weak signal cannot establish the relationship. An upstream model-written reference needs independent corroboration.

After generating a calibration cache, iterate thresholds offline:

```bash
ppa linker replay --cache /path/to/candidates.jsonl
```

Regenerate when feature extraction changes. Index-mode calibration uses `--mode index --scope <schema>` and can enqueue work, so bind an owned test instance and follow the [detached-job instructions](../.cursor/skills/long-running-jobs/SKILL.md) for long runs.

## Submit a reviewable contribution

Include the module, synthetic tests, and a description of each tier's evidence and bounds. Include calibration results when the change affects promotion. Keep private source records and credentials out of committed artifacts; use redacted examples or aggregate verdicts where necessary.

Explain the user-visible question the new relationship helps answer. Report false positives as well as matches. Show how the rule avoids attaching unrelated events to the user's history.

## Retire a rule that cannot meet the standard

Tighten a failing rule, lower it to review-only, or retire it. `lifecycle_state="retired"` keeps the module visible without scheduling it. Record the reason in the [retirement protocol](runbooks/linker-retirement-protocol.md) so another contributor can understand the evidence before reviving it.
