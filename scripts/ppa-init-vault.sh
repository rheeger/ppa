#!/bin/bash
set -euo pipefail

VAULT="${1:-${PPA_PATH:-/srv/hfa-secure/vault}}"
PYTHON="${PYTHON:-python3}"

mkdir -p "$VAULT/People" "$VAULT/Finance" "$VAULT/Photos" "$VAULT/_meta" "$VAULT/_templates" "$VAULT/Attachments"

"$PYTHON" - "$VAULT" <<'PY'
import json
import sys
from pathlib import Path

vault = Path(sys.argv[1])
meta = vault / "_meta"
meta.mkdir(parents=True, exist_ok=True)

files = {
    "identity-map.json": {},
    "sync-state.json": {},
    "own-emails.json": [],
    "dedup-candidates.json": [],
    "enrichment-log.json": [],
    "llm-cache.json": {},
    "ppa-config.json": {
        "merge_threshold": 90,
        "conflict_threshold": 75,
        "fuzzy_name_threshold": 85.0,
        "finance_min_amount": 20.0,
        "dedup_sweep_auto_merge": True,
        "max_enrichment_log_entries": 100,
        "imessage_thread_body_sha_cache_enabled": True,
        "gmail_thread_body_sha_cache_enabled": True,
        "calendar_event_body_sha_cache_enabled": True,
    },
    "llm-config.json": {
        "primary": {"provider": "gemini", "model": "gemini-2.0-flash-lite"},
        "fallback": {"provider": "openai", "model": "gpt-4o-mini"},
        "max_tokens_tiebreak": 4,
        "max_tokens_enrichment": 256,
    },
    "nicknames.json": {
        "robert": ["rob", "robbie", "bob", "bobby", "bert"],
        "william": ["will", "bill", "billy", "willy", "liam"],
        "james": ["jim", "jimmy", "jamie"],
        "richard": ["rick", "rich", "dick", "ricky"],
        "michael": ["mike", "mikey", "mick"],
        "elizabeth": ["liz", "lizzy", "beth", "betty", "eliza"],
        "jennifer": ["jen", "jenny", "jenn"],
        "katherine": ["kate", "kathy", "katie", "kat", "cathy"],
        "margaret": ["maggie", "meg", "peggy", "marge"],
        "alexander": ["alex", "al", "xander"],
        "christopher": ["chris", "topher"],
        "jonathan": ["jon", "jonny"],
        "benjamin": ["ben", "benji", "benny"],
        "nicholas": ["nick", "nicky"],
        "matthew": ["matt", "matty"],
        "daniel": ["dan", "danny"],
        "anthony": ["tony"],
        "joseph": ["joe", "joey"],
        "thomas": ["tom", "tommy"],
        "timothy": ["tim", "timmy"],
        "samuel": ["sam", "sammy"],
        "andrew": ["andy", "drew"],
        "theodore": ["ted", "teddy", "theo"],
        "edward": ["ed", "eddie", "ted", "teddy"],
        "stephen": ["steve", "stevie"],
        "steven": ["steve", "stevie"],
        "patricia": ["pat", "patty", "trish"],
        "rebecca": ["becca", "becky"],
        "jessica": ["jess", "jessie"],
        "victoria": ["vicky", "tori"],
    },
}

for name, payload in files.items():
    path = meta / name
    if path.exists():
        continue
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

print(f"Initialized PPA vault at {vault}")
PY
