PYTHON ?= .venv/bin/python
PG_ENV_FILE ?= .env.pgvector
COMPOSE_FILE ?= docker-compose.pgvector.yml
DOCKER_COMPOSE ?= docker compose --env-file $(PG_ENV_FILE) -f $(COMPOSE_FILE)

PPA_PATH ?= /Users/rheeger/Archive/production/hf-archives
PPA_BENCHMARK_SOURCE_VAULT ?= /Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127
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

.PHONY: install pg-up pg-down pg-logs pg-psql dump-schema dump-seed-schema dump-seed-schema-fast dump-seed-schema-stream-arnold pipe-restore-seed-arnold scp-restore-seed-arnold dump-and-scp-restore-seed-arnold watch-scp-restore-continue-hfa bootstrap-postgres bootstrap-seed-postgres rebuild-indexes rebuild-seed-indexes index-status index-status-seed embed-pending migrate migrate-seed migrate-dry-run migration-status migration-status-seed build-benchmark-sample benchmark-rebuild benchmark-seed-links smoke smoke-queries arnold-smoke

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
