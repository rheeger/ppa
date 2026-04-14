# HFA Archive Cleanup Runbook

## Scope

This runbook is for documenting and verifying cleanup of generated seed artifacts and archive intermediates.

This runbook does **not** authorize immediate deletion.

## Hard Safety Rules

- Do not modify, move, or delete local machine archive copies on Robbie's Mac by default.
- Do not modify, move, or delete seed files or source exports on Robbie's Mac by default.
- If provenance is unclear, preserve the file and escalate instead of deleting it.
- Cleanup execution requires a separate explicit approval step after this runbook has been reviewed.

## Protected Local Source Material

Treat all of the following as protected unless separately approved for deletion later:

- local archive copies
- seed vaults
- Photos/iPhoto exports
- iMessage snapshots
- contacts exports
- original files and source dumps used for seeding

## Inventory Targets

### VM-generated or derived artifacts

- temporary vaults
- benchmark/sample vaults
- checkpoint directories
- local archive Postgres data
- runbook logs
- manifests
- derived backup staging artifacts

### Local-machine derived artifacts

- copied seed vaults
- test exports
- temporary scratch restores
- generated manifests or verification outputs

## Verification Before Any Deletion

- Identify the file or directory.
- Classify it as one of:
  - original source material
  - canonical archive material
  - generated intermediate
  - unclear provenance
- Confirm whether it lives on the VM or on Robbie's Mac.
- Confirm whether a surviving original exists and remains untouched.
- Confirm whether the item is still needed for rollback, audit, or restore.

## Default-Safe Decision Table

- original source material: preserve
- canonical archive material: preserve unless a different runbook authorizes migration or rotation
- generated intermediate with clear provenance and replacement path: eligible for later explicit deletion
- unclear provenance: preserve and escalate

## Suggested Cleanup Candidates

These are candidates for later explicit review, not auto-deletion:

- benchmark/sample vault copies
- generated validation manifests
- checkpoint bundles
- temporary restore directories
- stale local Postgres test data
- obsolete encrypted backup staging outputs

## Agent Instructions

- Never delete protected local source material.
- Never infer that “seed” means disposable.
- Prefer reporting, classification, and evidence gathering over action.
- When uncertain, emit a hold recommendation with the reason.
