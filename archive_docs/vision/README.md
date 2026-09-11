# Product vision (historical roadmap)

**Living status:** [STATUS.md](../STATUS.md) (2026-09-09). When these files conflict with STATUS or [PRODUCT_CAPABILITY_MATRIX.md](../PRODUCT_CAPABILITY_MATRIX.md), the living page wins. There is no v2.75. Do not treat HEAD SHAs inside the long vision files as current.

Long-form planning documents for PPA. Kept out of the repo root for clarity.

| File                           | Notes                                                                                                                                                                       |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [v2vision.md](v2vision.md)     | Phase 2 / extraction + index era                                                                                                                                            |
| [v2.5vision.md](v2.5vision.md) | Living-seed thesis, then ten-plan retrieval hardening (PR 29). v2.5-done = this machine’s canonical seed is the main corpus. Arnold is a remote MCP client, not the home.   |
| [v3vision.md](v3vision.md)     | Phase 3+ product shape (independent install). Not started as a stranger-ready product.                                                                                      |
| [v4vision.md](v4vision.md)     | Consumer product / native app direction                                                                                                                                     |

Cross-links between these files use **relative** paths in this directory.

**v2.5 done means:** the canonical seed at `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127` (schema `ppa` on this machine) is the living high-signal archive. Suppressed marketing is deleted; quarantine stays as labeled cards (`retrieval_weight=0.35`). Live updaters that this Mac (or later Helga Pataki) can run have been applied here. The ten-plan hardening in [PR 29](https://github.com/rheeger/ppa/pull/29) is part of that close: trained hybrid retrieval, conversation bursts, complete serving generations, access-bounded hops, independent instances, and `archive_analytics`. PRs 30–33 added honest person lookup and content-keyed embeddings.

Arnold is **not** the long-term home of the corpus. It may talk to this machine over HTTP MCP. Do not copy the seed there. Formal `ready: false` leftover from missing `validation_gates` / `corpus_cleanup` review rows is an accepted local exception (`local_seed_living_corpus`). Photos, Apple Health, and `--catch-up` stay parked. Nightly maintain exists in code and is currently unloaded. See [STATUS.md](../STATUS.md).

That seed path is a **historical instance note**, not a default for `ppa setup`. New archives evaluate fail-closed on the current instance only and **do not inherit** `local_seed_living_corpus`. Knowledge-cache / 46-facet claims stay deferred. `ppa analytics` / `archive_analytics` are shipped. `production_proven` stays false for fixture smokes.
