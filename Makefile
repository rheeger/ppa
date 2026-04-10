PYTHON ?= .venv/bin/python
PG_ENV_FILE ?= .env.pgvector
COMPOSE_FILE ?= docker-compose.pgvector.yml
DOCKER_COMPOSE ?= docker compose --env-file $(PG_ENV_FILE) -f $(COMPOSE_FILE)

PPA_PATH ?= /Users/rheeger/Archive/production/hf-archives
PPA_BENCHMARK_SOURCE_VAULT ?= /Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127
# Gitignored vault trees from make slice-local-* (see .slices/README.md)
SLICES_LOCAL_DIR ?= .slices
# Same vault/schema as run-local-seed-mcp.sh (derived index in Postgres schema archive_seed)
PPA_SEED_VAULT ?= $(PPA_BENCHMARK_SOURCE_VAULT)
PPA_SEED_INDEX_SCHEMA ?= archive_seed
PPA_BENCHMARK_OUTPUT_VAULT ?= /Users/rheeger/Archive/tests/hf-archives-benchmark-sample
PPA_INDEX_SCHEMA ?= archive_mcp
PPA_BENCHMARK_PROFILE ?= local-laptop
PPA_BENCHMARK_SAMPLE_PERCENT ?= 0
PPA_BENCHMARK_SAMPLE_PER_GROUP ?= 40
PPA_BENCHMARK_SAMPLE_MAX_NOTES ?= 2000
PPA_BENCHMARK_SAMPLE_OVERSAMPLE_FACTOR ?= 10
PPA_REBUILD_WORKERS ?=
PPA_REBUILD_BATCH_SIZE ?=
PPA_REBUILD_COMMIT_INTERVAL ?=
PPA_REBUILD_PROGRESS_EVERY ?=
PPA_REBUILD_EXECUTOR ?=
PPA_EMBEDDING_PROVIDER ?= hash
PPA_EMBEDDING_MODEL ?= archive-hash-dev
PPA_EMBEDDING_VERSION ?= 1
PPA_EMBED_LIMIT ?= 0
PPA_EMBED_CONCURRENCY ?=
PPA_EMBED_WRITE_BATCH_SIZE ?=
PPA_EMBED_PROGRESS_EVERY ?=
PPA_EMBED_DEFER_VECTOR_INDEX ?=
PPA_SEED_LINK_INCLUDE_LLM ?= 0
PPA_SEED_LINK_APPLY_PROMOTIONS ?= 0
PPA_SEED_LINK_MODULES ?=
PPA_USE_ARNOLD_OPENAI_KEY ?= 0
PPA_OPENAI_API_KEY_OP_REF ?= op://Arnold/OPENAI_API_KEY/credential
PPA_GEMINI_API_KEY_OP_REF ?= op://Arnold/GEMINI_API_KEY/credential
PPA_OP_SERVICE_ACCOUNT_TOKEN_OP_REF ?= op://Arnold-Passkey-Gate/Service Account Auth Token: Arnold-Passkey-Gate/credential
PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE ?=

PG_HOST_PORT_CMD = $(DOCKER_COMPOSE) port archive-postgres 5432 | awk -F: '{print $$NF}'
LOCAL_PPA_INDEX_DSN_CMD = POSTGRES_USER="$$(grep '^POSTGRES_USER=' $(PG_ENV_FILE) | cut -d= -f2)"; \
	POSTGRES_PASSWORD="$$(grep '^POSTGRES_PASSWORD=' $(PG_ENV_FILE) | cut -d= -f2)"; \
	POSTGRES_DB="$$(grep '^POSTGRES_DB=' $(PG_ENV_FILE) | cut -d= -f2)"; \
	POSTGRES_PORT="$$( $(PG_HOST_PORT_CMD) )"; \
	printf 'postgresql://%s:%s@127.0.0.1:%s/%s' "$$POSTGRES_USER" "$$POSTGRES_PASSWORD" "$$POSTGRES_PORT" "$$POSTGRES_DB"

# Logical dumps: always use pg_dump from the running archive-postgres container (PG major matches image, currently 17).
# Avoid host Homebrew pg_dump (often older) against this server.
PPA_DUMP_OUT ?= archive_seed.dump
DUMP_SCHEMA ?= $(PPA_SEED_INDEX_SCHEMA)
# Parallel jobs for pg_restore on Arnold (directory restore); override: PG_RESTORE_JOBS=16 make scp-restore-seed-arnold
PG_RESTORE_JOBS ?= 32
# 1 = pg_dump -Z0 (faster CPU, larger artifact). >=2 = parallel directory dump (-Fd -j) + docker cp (often fastest locally).
PG_DUMP_FAST ?= 0
PG_DUMP_JOBS ?= 0

DOMAIN ?= example.com
CATEGORY ?=
# Expansive sender-census against PPA_SEED_VAULT (full Email/ walk; override SAMPLE + DETAIL).
SENDER_CENSUS_SAMPLE ?= 2000
SENDER_CENSUS_DETAIL ?= 40
SENDER_CENSUS_TOP_FROM ?= 60
SENDER_CENSUS_TOP_EXACT ?= 60
SENDER_CENSUS_TOP_SHAPES ?= 60
# Step 11a template-sampler on full seed → specs/samples_seed/ (very long wall time).
STEP_11A_PER_YEAR_SEED ?= 15
# Step 11a on 10pct slice (override per-year without exporting env).
STEP_11A_PER_YEAR ?= 5

.PHONY: install pg-up pg-down pg-logs pg-psql dump-schema dump-seed-schema dump-seed-schema-fast dump-seed-schema-stream-arnold pipe-restore-seed-arnold scp-restore-seed-arnold dump-and-scp-restore-seed-arnold watch-scp-restore-continue-hfa bootstrap-postgres bootstrap-seed-postgres rebuild-indexes rebuild-seed-indexes index-status index-status-seed embed-pending migrate migrate-seed migrate-dry-run migration-status migration-status-seed build-benchmark-sample benchmark-rebuild benchmark-seed-links smoke smoke-queries arnold-smoke test-unit ollama-llm-smoke llm-enrichment-6b-smoke test-integration test-slice test-slice-smoke test-slice-verify test-slice-verify-10pct test-slice-verify-smoke verify-incremental benchmark-1pct benchmark-5pct health-check extract-emails-staging extract-emails-full extract-benchmark extract-dry-run enrich-emails-staging build-enrichment-benchmark build-enrichment-benchmark-smoke build-enrichment-benchmark-1pct build-enrichment-benchmark-5pct build-enrichment-benchmark-10pct build-enrichment-benchmark-slices build-enrichment-benchmark-slices-all step8b-review-packet run-enrichment-benchmark run-enrichment-benchmark-matrix aggregate-benchmark-results staging-report promote-staging promote-staging-dry-run resolve-entities resolve-entities-full clean-phase3-derived clean-phase3-derived-slices clean-phase3-derived-local-slices extract-emails-slice-smoke extract-emails-slice-full slice-local-1pct slice-local-5pct slice-local-10pct slice-local-all clean-ppa-machine-artifacts clean-ppa-machine-artifacts-dry-run clean-ppa-local-slices extract-emails-1pct-slice extract-emails-5pct-slice extract-emails-10pct-slice extraction-quality-reports sender-census template-sampler sender-census-slice-smoke template-sampler-slice-smoke step-11a-template-samplers sender-census-seed step-11a-template-samplers-seed step-11d-slice-yield-report

install:
	$(PYTHON) -m pip install -e .

pg-up:
	$(DOCKER_COMPOSE) up -d

pg-down:
	$(DOCKER_COMPOSE) down

pg-logs:
	$(DOCKER_COMPOSE) logs -f archive-postgres

pg-psql:
	$(DOCKER_COMPOSE) exec archive-postgres psql -U "$$(grep '^POSTGRES_USER=' $(PG_ENV_FILE) | cut -d= -f2)" -d "$$(grep '^POSTGRES_DB=' $(PG_ENV_FILE) | cut -d= -f2)"

# Dump/restore shell scripts still read ARCHIVE_* env var names internally.
# The Makefile maps PPA_* Make variables to the env var names each script expects.
dump-schema:
	@DUMP_SCHEMA=$(DUMP_SCHEMA) ARCHIVE_DUMP_OUT=$(PPA_DUMP_OUT) \
		ARCHIVE_STREAM_BYTES=$(PPA_STREAM_BYTES) PG_ENV_FILE=$(PG_ENV_FILE) COMPOSE_FILE=$(COMPOSE_FILE) \
		PG_DUMP_FAST=$(PG_DUMP_FAST) PG_DUMP_JOBS=$(PG_DUMP_JOBS) PG_DUMP_CONTAINER_NAME=$(PG_DUMP_CONTAINER_NAME) \
		ARCHIVE_DUMP_LOG=$(PPA_DUMP_LOG) ARCHIVE_DUMP_LOG_FILE=$(PPA_DUMP_LOG_FILE) \
		bash scripts/dump-schema-local.sh

dump-seed-schema:
	@$(MAKE) dump-schema DUMP_SCHEMA=$(PPA_SEED_INDEX_SCHEMA) PPA_DUMP_OUT=$(PPA_DUMP_OUT)

# Fast local dump: -Z0 + parallel -Fd -j (default 8 workers). Artifact is PPA_DUMP_OUT with .dump → sibling .dump.d directory.
dump-seed-schema-fast:
	@$(MAKE) dump-seed-schema PG_DUMP_FAST=1 PG_DUMP_JOBS=$(if $(filter-out 0,$(PG_DUMP_JOBS)),$(PG_DUMP_JOBS),8)

# Stream archive_seed custom dump to Arnold (see scripts/stream-seed-dump-to-arnold.sh).
# Progress: install `pv` (brew install pv). Optional ETA: PPA_STREAM_BYTES=... make dump-seed-schema-stream-arnold
dump-seed-schema-stream-arnold:
	@bash scripts/stream-seed-dump-to-arnold.sh

# Pipe pg_dump → pg_restore on Arnold (no archive_seed.dump on VM). Use when encrypted LV is too small for dump + PGDATA + vault.
pipe-restore-seed-arnold:
	@bash scripts/stream-seed-pipe-restore-arnold.sh

# Local dump → rsync to Arnold staging on /srv/hfa-secure (default) → copy into PGDATA on same volume → pg_restore --jobs.
# Override REMOTE_STAGING_DIR=/home/arnold/... only if root LV has room (~100+ GiB).
scp-restore-seed-arnold:
	@PG_RESTORE_JOBS=$(PG_RESTORE_JOBS) ARCHIVE_DUMP_OUT=$(PPA_DUMP_OUT) ARCHIVE_DUMP_ARTIFACT=$(PPA_DUMP_ARTIFACT) \
		ARCHIVE_SCP_RESTORE_LOG=$(PPA_SCP_RESTORE_LOG) ARCHIVE_SCP_RESTORE_LOG_FILE=$(PPA_SCP_RESTORE_LOG_FILE) \
		RSYNC_HEARTBEAT_SEC=$(RSYNC_HEARTBEAT_SEC) RESTORE_HEARTBEAT_SEC=$(RESTORE_HEARTBEAT_SEC) RESTORE_LOG_INTERVAL=$(RESTORE_LOG_INTERVAL) \
		bash scripts/scp-restore-seed-arnold.sh

dump-and-scp-restore-seed-arnold: dump-seed-schema
	@$(MAKE) scp-restore-seed-arnold PG_RESTORE_JOBS=$(PG_RESTORE_JOBS)

# Poll logs/archive-scp-restore.log every POLL_INTERVAL_SEC (default 120); on success run hey-arnold policy-check + post-seed-restore.
watch-scp-restore-continue-hfa:
	@POLL_INTERVAL_SEC=$(POLL_INTERVAL_SEC) POLL_MAX_WAIT_SEC=$(POLL_MAX_WAIT_SEC) bash scripts/poll-scp-restore-and-continue-hfa.sh

bootstrap-postgres:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=$(PPA_PATH) \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=$(PPA_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp bootstrap-postgres

rebuild-indexes:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=$(PPA_PATH) \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=$(PPA_INDEX_SCHEMA) \
	$(if $(PPA_REBUILD_WORKERS),PPA_REBUILD_WORKERS=$(PPA_REBUILD_WORKERS)) \
	$(if $(PPA_REBUILD_BATCH_SIZE),PPA_REBUILD_BATCH_SIZE=$(PPA_REBUILD_BATCH_SIZE)) \
	$(if $(PPA_REBUILD_COMMIT_INTERVAL),PPA_REBUILD_COMMIT_INTERVAL=$(PPA_REBUILD_COMMIT_INTERVAL)) \
	$(if $(PPA_REBUILD_PROGRESS_EVERY),PPA_REBUILD_PROGRESS_EVERY=$(PPA_REBUILD_PROGRESS_EVERY)) \
	$(if $(PPA_REBUILD_EXECUTOR),PPA_REBUILD_EXECUTOR=$(PPA_REBUILD_EXECUTOR)) \
	$(PYTHON) -m archive_mcp rebuild-indexes

bootstrap-seed-postgres:
	@$(MAKE) bootstrap-postgres PPA_PATH=$(PPA_SEED_VAULT) PPA_INDEX_SCHEMA=$(PPA_SEED_INDEX_SCHEMA)

rebuild-seed-indexes:
	@$(MAKE) rebuild-indexes PPA_PATH=$(PPA_SEED_VAULT) PPA_INDEX_SCHEMA=$(PPA_SEED_INDEX_SCHEMA)

index-status:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=$(PPA_PATH) \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=$(PPA_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp index-status

index-status-seed:
	@$(MAKE) index-status PPA_PATH=$(PPA_SEED_VAULT) PPA_INDEX_SCHEMA=$(PPA_SEED_INDEX_SCHEMA)

embed-pending:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=$(PPA_PATH) \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=$(PPA_INDEX_SCHEMA) \
	PPA_EMBEDDING_PROVIDER=$(PPA_EMBEDDING_PROVIDER) \
	PPA_EMBEDDING_MODEL=$(PPA_EMBEDDING_MODEL) \
	PPA_EMBEDDING_VERSION=$(PPA_EMBEDDING_VERSION) \
	$(if $(PPA_EMBED_CONCURRENCY),PPA_EMBED_CONCURRENCY=$(PPA_EMBED_CONCURRENCY)) \
	$(if $(PPA_EMBED_WRITE_BATCH_SIZE),PPA_EMBED_WRITE_BATCH_SIZE=$(PPA_EMBED_WRITE_BATCH_SIZE)) \
	$(if $(PPA_EMBED_PROGRESS_EVERY),PPA_EMBED_PROGRESS_EVERY=$(PPA_EMBED_PROGRESS_EVERY)) \
	$(if $(PPA_EMBED_DEFER_VECTOR_INDEX),PPA_EMBED_DEFER_VECTOR_INDEX=$(PPA_EMBED_DEFER_VECTOR_INDEX)) \
	PPA_USE_ARNOLD_OPENAI_KEY=$(PPA_USE_ARNOLD_OPENAI_KEY) \
	PPA_OPENAI_API_KEY_OP_REF='$(PPA_OPENAI_API_KEY_OP_REF)' \
	PPA_OP_SERVICE_ACCOUNT_TOKEN_OP_REF='$(PPA_OP_SERVICE_ACCOUNT_TOKEN_OP_REF)' \
	PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE='$(PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE)' \
	$(PYTHON) -m archive_mcp embed-pending --limit $(PPA_EMBED_LIMIT) --embedding-model "$(PPA_EMBEDDING_MODEL)" --embedding-version $(PPA_EMBEDDING_VERSION)

build-benchmark-sample:
	$(PYTHON) -m archive_mcp build-benchmark-sample \
		--source-vault "$(PPA_BENCHMARK_SOURCE_VAULT)" \
		--output-vault "$(PPA_BENCHMARK_OUTPUT_VAULT)" \
		--per-group-limit $(PPA_BENCHMARK_SAMPLE_PER_GROUP) \
		--max-notes $(PPA_BENCHMARK_SAMPLE_MAX_NOTES) \
		--oversample-factor $(PPA_BENCHMARK_SAMPLE_OVERSAMPLE_FACTOR) \
		--sample-percent $(PPA_BENCHMARK_SAMPLE_PERCENT)

benchmark-rebuild:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	$(PYTHON) -m archive_mcp benchmark-rebuild \
		--vault "$(PPA_PATH)" \
		--schema "$(PPA_INDEX_SCHEMA)" \
		--profile "$(PPA_BENCHMARK_PROFILE)" \
		$(if $(PPA_REBUILD_WORKERS),--workers $(PPA_REBUILD_WORKERS)) \
		$(if $(PPA_REBUILD_BATCH_SIZE),--batch-size $(PPA_REBUILD_BATCH_SIZE)) \
		$(if $(PPA_REBUILD_COMMIT_INTERVAL),--commit-interval $(PPA_REBUILD_COMMIT_INTERVAL)) \
		$(if $(PPA_REBUILD_PROGRESS_EVERY),--progress-every $(PPA_REBUILD_PROGRESS_EVERY)) \
		$(if $(PPA_REBUILD_EXECUTOR),--executor $(PPA_REBUILD_EXECUTOR))

benchmark-seed-links:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	$(PYTHON) -m archive_mcp benchmark-seed-links \
		--vault "$(PPA_PATH)" \
		--schema "$(PPA_INDEX_SCHEMA)" \
		--profile "$(PPA_BENCHMARK_PROFILE)" \
		$(if $(PPA_REBUILD_WORKERS),--workers $(PPA_REBUILD_WORKERS)) \
		$(if $(PPA_REBUILD_BATCH_SIZE),--batch-size $(PPA_REBUILD_BATCH_SIZE)) \
		$(if $(PPA_REBUILD_COMMIT_INTERVAL),--commit-interval $(PPA_REBUILD_COMMIT_INTERVAL)) \
		$(if $(PPA_REBUILD_PROGRESS_EVERY),--progress-every $(PPA_REBUILD_PROGRESS_EVERY)) \
		$(if $(PPA_REBUILD_EXECUTOR),--executor $(PPA_REBUILD_EXECUTOR)) \
		$(if $(filter 1,$(PPA_SEED_LINK_INCLUDE_LLM)),--include-llm) \
		$(if $(filter 1,$(PPA_SEED_LINK_APPLY_PROMOTIONS)),--apply-promotions) \
		$(if $(PPA_SEED_LINK_MODULES),--modules "$(PPA_SEED_LINK_MODULES)")

migrate:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=$(PPA_PATH) \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=$(PPA_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp migrate

migrate-seed:
	@$(MAKE) migrate PPA_PATH=$(PPA_SEED_VAULT) PPA_INDEX_SCHEMA=$(PPA_SEED_INDEX_SCHEMA)

migrate-dry-run:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=$(PPA_PATH) \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=$(PPA_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp migrate --dry-run

migration-status:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=$(PPA_PATH) \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=$(PPA_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp migration-status

migration-status-seed:
	@$(MAKE) migration-status PPA_PATH=$(PPA_SEED_VAULT) PPA_INDEX_SCHEMA=$(PPA_SEED_INDEX_SCHEMA)

smoke-queries:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=$(PPA_PATH) \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=$(PPA_INDEX_SCHEMA) \
	PPA_EMBEDDING_PROVIDER=$(PPA_EMBEDDING_PROVIDER) \
	PPA_EMBEDDING_MODEL=$(PPA_EMBEDDING_MODEL) \
	PPA_EMBEDDING_VERSION=$(PPA_EMBEDDING_VERSION) \
	PATH="$$(dirname $(PYTHON)):$$PATH" \
	bash scripts/ppa-smoke-test.sh

arnold-smoke:
	ssh arnold@192.168.50.27 'cd /srv/ppa && ./scripts/ppa-smoke-test.sh'

smoke: install bootstrap-postgres rebuild-indexes index-status embed-pending smoke-queries

test-unit:
	$(PYTHON) -m pytest tests/ -v --tb=short -m "not integration and not slow"

# Phase 2.75 Step 1b — requires local Ollama + model (e.g. gemma4:31b). Non-strict: exits 0 if Ollama down.
ollama-llm-smoke:
	$(PYTHON) scripts/ollama_llm_smoke.py

# Phase 2.75 Step 6b — triage + extract one thread (human review). Example: --vault tests/fixtures --list-threads
llm-enrichment-6b-smoke:
	@echo "Run: $(PYTHON) scripts/llm_enrichment_step6b_smoke.py --vault <VAULT> --list-threads"
	@echo "Then: $(PYTHON) scripts/llm_enrichment_step6b_smoke.py --vault <VAULT> --thread-id <ID>"

test-integration:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test \
	PPA_EMBEDDING_PROVIDER=hash \
	PPA_EMBEDDING_MODEL=archive-hash-dev \
	PPA_EMBEDDING_VERSION=1 \
	$(PYTHON) -m pytest tests/ -v --tb=short -m "integration"

# Full stratified slice (~5% target_percent): scans entire seed vault once — wall time scales with note count
# (often tens of minutes on a multi-million-note seed). Logs: ppa.slice on stderr; optional PPA_SLICE_LOG.
test-slice:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	$(PYTHON) -m archive_mcp slice-seed \
		--config tests/slice_config.json \
		--output /tmp/ppa-test-slice \
		--source-vault "$(PPA_BENCHMARK_SOURCE_VAULT)" \
		--progress-every 10000

# Tiny slice (0.5%% per type, cluster_cap 60) for pipeline / agent smoke — faster copy, still exercises closure.
test-slice-smoke:
	@mkdir -p logs; \
	PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	$(PYTHON) -m archive_mcp --log-file logs/ppa-slice-smoke.log slice-seed \
		--config tests/slice_config.smoke.json \
		--output /tmp/ppa-test-slice-smoke \
		--source-vault "$(PPA_BENCHMARK_SOURCE_VAULT)" \
		--progress-every 2000

# Local 1% / 5% / 10% slices under $(SLICES_LOCAL_DIR)/ (gitignored); configs tests/slice_config.{1,5,10}pct.json
slice-local-1pct:
	@mkdir -p $(SLICES_LOCAL_DIR)/1pct
	$(PYTHON) -m archive_mcp slice-seed \
		--config tests/slice_config.1pct.json \
		--output $(SLICES_LOCAL_DIR)/1pct \
		--source-vault "$(PPA_BENCHMARK_SOURCE_VAULT)" \
		--progress-every 5000

slice-local-5pct:
	@mkdir -p $(SLICES_LOCAL_DIR)/5pct
	$(PYTHON) -m archive_mcp slice-seed \
		--config tests/slice_config.5pct.json \
		--output $(SLICES_LOCAL_DIR)/5pct \
		--source-vault "$(PPA_BENCHMARK_SOURCE_VAULT)" \
		--progress-every 15000

slice-local-10pct:
	@mkdir -p $(SLICES_LOCAL_DIR)/10pct
	$(PYTHON) -m archive_mcp slice-seed \
		--config tests/slice_config.10pct.json \
		--output $(SLICES_LOCAL_DIR)/10pct \
		--source-vault "$(PPA_BENCHMARK_SOURCE_VAULT)" \
		--progress-every 30000

slice-local-all: slice-local-1pct slice-local-5pct slice-local-10pct
	@echo "Local slices under $(CURDIR)/$(SLICES_LOCAL_DIR)/{1pct,5pct,10pct}"

test-slice-verify:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=/tmp/ppa-test-slice \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test_slice \
	PPA_EMBEDDING_PROVIDER=hash \
	PPA_EMBEDDING_MODEL=archive-hash-dev \
	PPA_EMBEDDING_VERSION=1 \
	$(PYTHON) -m archive_mcp bootstrap-postgres && \
	PPA_PATH=/tmp/ppa-test-slice \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test_slice \
	PPA_EMBEDDING_PROVIDER=hash \
	PPA_EMBEDDING_MODEL=archive-hash-dev \
	PPA_EMBEDDING_VERSION=1 \
	$(PYTHON) -m archive_mcp rebuild-indexes && \
	PPA_PATH=/tmp/ppa-test-slice \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test_slice \
	$(PYTHON) -m archive_mcp health-check --manifest tests/slice_manifest.json --report-format both

# Same as test-slice-verify but vault = gitignored .slices/10pct (no slice-seed to /tmp). Requires slice-local-10pct + pg-up.
test-slice-verify-10pct:
	@test -d "$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" || { echo "Missing $(SLICES_LOCAL_DIR)/10pct — run: make slice-local-10pct"; exit 1; }
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH="$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test_slice_10pct \
	PPA_EMBEDDING_PROVIDER=hash \
	PPA_EMBEDDING_MODEL=archive-hash-dev \
	PPA_EMBEDDING_VERSION=1 \
	$(PYTHON) -m archive_mcp bootstrap-postgres && \
	PPA_PATH="$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test_slice_10pct \
	PPA_EMBEDDING_PROVIDER=hash \
	PPA_EMBEDDING_MODEL=archive-hash-dev \
	PPA_EMBEDDING_VERSION=1 \
	$(PYTHON) -m archive_mcp rebuild-indexes && \
	PPA_PATH="$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test_slice_10pct \
	$(PYTHON) -m archive_mcp health-check --manifest tests/slice_manifest.json --report-format both

# Rebuild + health-check on smoke slice output (run test-slice-smoke first). Uses separate schema from full slice.
test-slice-verify-smoke:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=/tmp/ppa-test-slice-smoke \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test_slice_smoke \
	PPA_EMBEDDING_PROVIDER=hash \
	PPA_EMBEDDING_MODEL=archive-hash-dev \
	PPA_EMBEDDING_VERSION=1 \
	$(PYTHON) -m archive_mcp bootstrap-postgres && \
	PPA_PATH=/tmp/ppa-test-slice-smoke \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test_slice_smoke \
	PPA_EMBEDDING_PROVIDER=hash \
	PPA_EMBEDDING_MODEL=archive-hash-dev \
	PPA_EMBEDDING_VERSION=1 \
	$(PYTHON) -m archive_mcp --log-file logs/ppa-rebuild-smoke.log rebuild-indexes && \
	PPA_PATH=/tmp/ppa-test-slice-smoke \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test_slice_smoke \
	$(PYTHON) -m archive_mcp health-check --manifest tests/slice_manifest.smoke.json --report-format both \
		--report-dir logs

verify-incremental:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=archive_test_incr \
	PPA_EMBEDDING_PROVIDER=hash \
	PPA_EMBEDDING_MODEL=archive-hash-dev \
	PPA_EMBEDDING_VERSION=1 \
	$(PYTHON) -m pytest tests/test_rebuild_incremental.py -v --tb=short

benchmark-1pct:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	$(PYTHON) -m archive_mcp benchmark --slice-percent 1 --output /tmp/bench-results/

benchmark-5pct:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	$(PYTHON) -m archive_mcp benchmark --slice-percent 5 --output /tmp/bench-results/

health-check:
	@PPA_INDEX_DSN="$$( $(LOCAL_PPA_INDEX_DSN_CMD) )"; \
	PPA_PATH=$(PPA_PATH) \
	PPA_INDEX_DSN="$$PPA_INDEX_DSN" \
	PPA_INDEX_SCHEMA=$(PPA_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp health-check

# Remove Phase-3 email-derived cards from seed or slice vaults (MealOrders, Rides, …; Entities/Places, Organizations).
clean-phase3-derived:
	bash scripts/clean-phase3-derived-dirs.sh "$(PPA_PATH)"

# Same cleanup for default Makefile slice outputs (paths only; no-op if missing).
clean-phase3-derived-slices:
	bash scripts/clean-phase3-derived-dirs.sh /tmp/ppa-test-slice-smoke /tmp/ppa-test-slice

clean-phase3-derived-local-slices:
	bash scripts/clean-phase3-derived-dirs.sh \
		"$(CURDIR)/$(SLICES_LOCAL_DIR)/1pct" \
		"$(CURDIR)/$(SLICES_LOCAL_DIR)/5pct" \
		"$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct"

# Remove /tmp slice vaults, extract staging, bench dirs (see scripts/clean-ppa-machine-artifacts.sh --help).
clean-ppa-machine-artifacts:
	bash scripts/clean-ppa-machine-artifacts.sh

clean-ppa-machine-artifacts-dry-run:
	bash scripts/clean-ppa-machine-artifacts.sh --dry-run

# Deletes .slices/{1pct,5pct,10pct} only (multi-GB). Seed vault untouched.
clean-ppa-local-slices:
	bash scripts/clean-ppa-machine-artifacts.sh --remove-local-slices

# Extract from local stratified slices (see .slices/README.md). Cleans Phase-3 dirs on that slice first.
extract-emails-1pct-slice:
	bash scripts/clean-phase3-derived-dirs.sh "$(CURDIR)/$(SLICES_LOCAL_DIR)/1pct"
	rm -rf _staging-1pct/
	PPA_PATH="$(CURDIR)/$(SLICES_LOCAL_DIR)/1pct" $(PYTHON) -m archive_mcp --log-file logs/extract-1pct-slice.log extract-emails \
		--staging-dir _staging-1pct/ --workers 8 --full-report

extract-emails-5pct-slice:
	bash scripts/clean-phase3-derived-dirs.sh "$(CURDIR)/$(SLICES_LOCAL_DIR)/5pct"
	rm -rf _staging-5pct/
	PPA_PATH="$(CURDIR)/$(SLICES_LOCAL_DIR)/5pct" $(PYTHON) -m archive_mcp --log-file logs/extract-5pct-slice.log extract-emails \
		--staging-dir _staging-5pct/ --workers 8 --full-report

extract-emails-10pct-slice:
	bash scripts/clean-phase3-derived-dirs.sh "$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct"
	rm -rf _staging-10pct/
	PPA_PATH="$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" $(PYTHON) -m archive_mcp --log-file logs/extract-10pct-slice.log extract-emails \
		--staging-dir _staging-10pct/ --workers 8 --full-report

# Regenerate markdown quality reports (requires matching _staging-*pct/ from extract-emails-*pct-slice).
# --vault must be the slice tree so round-trip checks resolve source_email UIDs (not repo root).
extraction-quality-reports:
	$(PYTHON) scripts/generate_extraction_quality_report.py --staging-dir _staging-1pct --vault "$(CURDIR)/$(SLICES_LOCAL_DIR)/1pct" --label 1pct --out docs/reports/extraction-quality/1pct.md --problem-samples 8 --clean-samples 4
	$(PYTHON) scripts/generate_extraction_quality_report.py --staging-dir _staging-5pct --vault "$(CURDIR)/$(SLICES_LOCAL_DIR)/5pct" --label 5pct --out docs/reports/extraction-quality/5pct.md --problem-samples 8 --clean-samples 4
	$(PYTHON) scripts/generate_extraction_quality_report.py --staging-dir _staging-10pct --vault "$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" --label 10pct --out docs/reports/extraction-quality/10pct.md --problem-samples 8 --clean-samples 4

# EDL Phase 1–2: taxonomy + per-year template samples (set DOMAIN=, optional CATEGORY=, PPA_PATH=).
sender-census:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp sender-census --domain $(DOMAIN) --sample 100

template-sampler:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp template-sampler \
		--domain $(DOMAIN) --category $(CATEGORY) --per-year 3 --out-dir /tmp/era-samples/$(DOMAIN)

# Fast EDL smoke: use gitignored .slices/10pct (same tree as extract-emails-10pct-slice). DOMAIN= required.
sender-census-slice-smoke:
	@test -d "$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" || { echo "Missing $(SLICES_LOCAL_DIR)/10pct — run: make slice-local-10pct"; exit 1; }
	PPA_PATH="$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" $(PYTHON) -m archive_mcp sender-census --domain $(DOMAIN) --sample 50

template-sampler-slice-smoke:
	@test -d "$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" || { echo "Missing $(SLICES_LOCAL_DIR)/10pct — run: make slice-local-10pct"; exit 1; }
	PPA_PATH="$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" $(PYTHON) -m archive_mcp template-sampler \
		--domain $(DOMAIN) --category $(CATEGORY) --per-year 2 --out-dir /tmp/era-samples-smoke/$(DOMAIN)

# Phase 2.5 Step 11a: all Tier 1–3 template-samplers → archive_sync/extractors/specs/samples/<provider>/ (long-running).
step-11a-template-samplers:
	PER_YEAR="$(STEP_11A_PER_YEAR)" $(CURDIR)/scripts/run_step_11a_template_samplers.sh

# Sender census on full seed vault (same Email/ walk as slice; counts are always totals). DOMAIN= required.
# Optional: CENSUS_OUT=…  SENDER_CENSUS_SAMPLE=5000  SENDER_CENSUS_TOP_EXACT=80  etc.
sender-census-seed:
	@test -n "$(DOMAIN)" && [ "$(DOMAIN)" != "example.com" ] || { echo >&2 "Usage: make sender-census-seed DOMAIN=amazon.com [CENSUS_OUT=archive_sync/extractors/specs/foo-census-seed.md]"; exit 1; }
	@test -d "$(PPA_SEED_VAULT)" || { echo >&2 "Missing PPA_SEED_VAULT: $(PPA_SEED_VAULT)"; exit 1; }
	PPA_PATH="$(PPA_SEED_VAULT)" $(PYTHON) -m archive_mcp sender-census \
		--domain $(DOMAIN) --sample $(SENDER_CENSUS_SAMPLE) --detail-rows $(SENDER_CENSUS_DETAIL) \
		--top-from $(SENDER_CENSUS_TOP_FROM) \
		--top-exact-subjects $(SENDER_CENSUS_TOP_EXACT) \
		--top-subject-shapes $(SENDER_CENSUS_TOP_SHAPES) \
		$(if $(CENSUS_OUT),--out $(CENSUS_OUT),)

# Step 11a batch on full seed → archive_sync/extractors/specs/samples_seed/<provider>/ (one walk; often 30–90+ min).
# Phase 11b–11c: cite paths under samples_seed/ in provider specs. See specs/samples_seed/README.md
# Override: STEP_11A_PER_YEAR_SEED=20
step-11a-template-samplers-seed:
	@test -d "$(PPA_SEED_VAULT)" || { echo >&2 "Missing PPA_SEED_VAULT: $(PPA_SEED_VAULT)"; exit 1; }
	RUN_ON_SEED=1 SEED_VAULT="$(PPA_SEED_VAULT)" PER_YEAR="$(STEP_11A_PER_YEAR_SEED)" $(CURDIR)/scripts/run_step_11a_template_samplers.sh

# Phase 2.5 Step 11d: one dry-run pass, per-extractor matched/extracted/yield (default: .slices/10pct). Long-running.
STEP_11D_VAULT ?= $(CURDIR)/$(SLICES_LOCAL_DIR)/10pct
step-11d-slice-yield-report:
	@test -d "$(STEP_11D_VAULT)" || { echo "Missing $(STEP_11D_VAULT) — run: make slice-local-10pct"; exit 1; }
	$(PYTHON) scripts/step_11d_slice_yield_report.py --vault "$(STEP_11D_VAULT)"

# Extract from smoke slice → /tmp (run test-slice-smoke first). Cleans slice derived dirs first.
extract-emails-slice-smoke: clean-phase3-derived-slices
	PPA_PATH=/tmp/ppa-test-slice-smoke $(PYTHON) -m archive_mcp extract-emails \
		--staging-dir /tmp/ppa-slice-smoke-extract-staging --workers 4 --full-report

# Extract from full 5% slice → /tmp (run test-slice first). Cleans slice derived dirs first.
extract-emails-slice-full: clean-phase3-derived-slices
	PPA_PATH=/tmp/ppa-test-slice $(PYTHON) -m archive_mcp extract-emails \
		--staging-dir /tmp/ppa-slice-full-extract-staging --workers 4 --full-report

extract-emails-staging:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp extract-emails \
		--staging-dir _staging/ --workers 4

extract-emails-full:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp extract-emails \
		--staging-dir _staging/ --workers 8 --full-report

extract-benchmark:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp extract-emails \
		--staging-dir /tmp/extract-bench/ --workers 4 --limit-vault-percent 5

extract-dry-run:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp extract-emails --dry-run

# Phase 2.75 LLM pipeline — writes _staging-llm/. Provider: ollama (local) or gemini (API).
ENRICH_PROVIDER ?= ollama
ENRICH_EXTRACT_MODEL ?= gemma4:31b
ENRICH_WORKERS ?= 4
enrich-emails-staging:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp --log-file logs/enrich-emails.log enrich-emails \
		--staging-dir _staging-llm/ --cache-db _enrichment_cache.db --progress-every 25 --full-report \
		--provider "$(ENRICH_PROVIDER)" --extract-model "$(ENRICH_EXTRACT_MODEL)" --workers $(ENRICH_WORKERS)

# Gemini API variant — pulls key from 1Password via op CLI. Default model: gemini-2.0-flash.
enrich-emails-gemini:
	GEMINI_API_KEY=$$(op read '$(PPA_GEMINI_API_KEY_OP_REF)') \
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp --log-file logs/enrich-emails-gemini.log enrich-emails \
		--staging-dir _staging-llm/ --cache-db _enrichment_cache.db --progress-every 25 --full-report \
		--provider gemini --workers 8

# Phase 2.75 Step 8 — ground truth JSON from regex staging + vault (writes gitignored _benchmark/).
# Vault must be the same tree as extract-emails for that staging (so source_email UIDs resolve).
# Always smoke-test 1pct before 5pct/10pct/full seed — Email/ walks are expensive on huge vaults.
build-enrichment-benchmark:
	$(PYTHON) scripts/build_enrichment_benchmark.py \
		--staging-dir _staging/ \
		--vault "$(PPA_PATH)" \
		--out _benchmark/enrichment_ground_truth.json

# Default Step 8 check: smallest slice only (override STEP8_MIN_POSITIVE_1PCT=200 to require ≥200 on 1pct).
STEP8_MIN_POSITIVE_1PCT ?= 1

build-enrichment-benchmark-smoke: build-enrichment-benchmark-1pct

# Same Step 8 against slice stagings: _staging-Npct must come from extract-emails-Npct-slice against .slices/Npct.
# If you never ran the 5pct extractor, _staging-5pct/ is empty/stale — skip build-enrichment-benchmark-5pct.
build-enrichment-benchmark-1pct:
	$(PYTHON) scripts/build_enrichment_benchmark.py \
		--staging-dir _staging-1pct/ \
		--vault "$(CURDIR)/$(SLICES_LOCAL_DIR)/1pct" \
		--out _benchmark/enrichment_ground_truth_1pct.json \
		--min-positive-cards $(STEP8_MIN_POSITIVE_1PCT)

build-enrichment-benchmark-5pct:
	$(PYTHON) scripts/build_enrichment_benchmark.py \
		--staging-dir _staging-5pct/ \
		--vault "$(CURDIR)/$(SLICES_LOCAL_DIR)/5pct" \
		--out _benchmark/enrichment_ground_truth_5pct.json

# Typical workflow when only 1pct + 10pct extract runs exist (no 5pct staging).
build-enrichment-benchmark-slices: build-enrichment-benchmark-1pct build-enrichment-benchmark-10pct

build-enrichment-benchmark-10pct:
	$(PYTHON) scripts/build_enrichment_benchmark.py \
		--staging-dir _staging-10pct/ \
		--vault "$(CURDIR)/$(SLICES_LOCAL_DIR)/10pct" \
		--out _benchmark/enrichment_ground_truth_10pct.json

# All three slice stagings — only if you ran extract-emails-{1,5,10}pct-slice for each.
build-enrichment-benchmark-slices-all: build-enrichment-benchmark-1pct build-enrichment-benchmark-5pct build-enrichment-benchmark-10pct

# Phase 2.75 Step 8b — markdown review packet from ground truth (next step: human APPROVE / FIX before Step 9).
STEP8B_GROUND_TRUTH ?= _benchmark/enrichment_ground_truth_10pct.json
step8b-review-packet:
	$(PYTHON) scripts/prepare_step8b_human_review.py --ground-truth "$(STEP8B_GROUND_TRUTH)" --out _benchmark/step8b_review_packet.md

# Phase 2.75 Step 9 — LLM vs ground truth (Ollama). Gemma 4 only: gemma4:e2b,e4b,26b,31b (see docs/gemma4-local-models.md).
STEP9_MODELS ?= gemma4:31b
# Full matrix for Step 10 (long run — days on full ground truth; use LIMIT args for smoke).
STEP9_MATRIX_MODELS ?= gemma4:e2b,gemma4:e4b,gemma4:26b,gemma4:31b
STEP9_OUT ?= _benchmark/results
run-enrichment-benchmark:
	$(PYTHON) scripts/run_enrichment_benchmark.py \
		--ground-truth "$(STEP8B_GROUND_TRUTH)" \
		--models "$(STEP9_MODELS)" \
		--output "$(STEP9_OUT)/"

run-enrichment-benchmark-matrix:
	$(PYTHON) scripts/run_enrichment_benchmark.py \
		--ground-truth "$(STEP8B_GROUND_TRUTH)" \
		--models "$(STEP9_MATRIX_MODELS)" \
		--output "$(STEP9_OUT)/"

# Phase 2.75 Step 10 — comparison table from all per-model scores.json under STEP9_OUT.
aggregate-benchmark-results:
	$(PYTHON) scripts/aggregate_benchmark_results.py --results-dir "$(STEP9_OUT)"

staging-report:
	$(PYTHON) -m archive_mcp staging-report --staging-dir _staging/

promote-staging:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp promote-staging --staging-dir _staging/

promote-staging-dry-run:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp promote-staging --staging-dir _staging/ --dry-run

resolve-entities:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp resolve-entities

resolve-entities-full:
	PPA_PATH=$(PPA_PATH) $(PYTHON) -m archive_mcp resolve-entities --report-dir _reports/
