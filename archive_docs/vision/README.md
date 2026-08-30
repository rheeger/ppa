# Product vision (historical roadmap)

Long-form planning documents for PPA — kept out of the repo root for clarity.

| File                           | Notes                                                                                                                                                                       |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [v2vision.md](v2vision.md)     | Phase 2 / extraction + index era                                                                                                                                            |
| [v2.5vision.md](v2.5vision.md) | Production-hardening for a **local seed living archive**. v2.5-done = this machine’s canonical seed is the main corpus. Arnold is down and is **not** the long-term home.   |
| [v3vision.md](v3vision.md)     | Phase 3+ product shape                                                                                                                                                      |
| [v4vision.md](v4vision.md)     | Consumer product / native app direction                                                                                                                                     |

Cross-links between these files use **relative** paths in this directory.

**v2.5 done means:** the canonical seed at `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127` (schema `ppa` on this machine) is the living high-signal archive. Suppressed marketing is deleted; quarantine stays as labeled cards (`retrieval_weight=0.35`). Live updaters that this Mac (or later Helga Pataki) can run have been applied here; soak has run; Otter MCP auth persists; F freshness uses live keys. Arnold is down and is not the long-term home — do not copy the seed, deploy Arnold, or restage a fake validation ladder. Formal `ready: false` leftover from missing `validation_gates` / `corpus_cleanup` review rows is an accepted local exception (`local_seed_living_corpus`). Photos, Apple Health, and `--catch-up` stay parked.
