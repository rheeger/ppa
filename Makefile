PYTHON ?= .venv/bin/python
PG_ENV_FILE ?= .env.pgvector
COMPOSE_FILE ?= docker-compose.pgvector.yml
DOCKER_COMPOSE ?= docker compose --env-file $(PG_ENV_FILE) -f $(COMPOSE_FILE)

HFA_VAULT_PATH ?= /Users/rheeger/Archive/production/hf-archives
ARCHIVE_BENCHMARK_SOURCE_VAULT ?= /Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127
# Same vault/schema as run-local-seed-mcp.sh (derived index in Postgres schema archive_seed)
ARCHIVE_SEED_VAULT ?= $(ARCHIVE_BENCHMARK_SOURCE_VAULT)
ARCHIVE_SEED_INDEX_SCHEMA ?= archive_seed
ARCHIVE_BENCHMARK_OUTPUT_VAULT ?= /Users/rheeger/Archive/tests/hf-archives-benchmark-sample
ARCHIVE_INDEX_SCHEMA ?= archive_mcp
ARCHIVE_BENCHMARK_PROFILE ?= local-laptop
ARCHIVE_BENCHMARK_SAMPLE_PERCENT ?= 0
ARCHIVE_BENCHMARK_SAMPLE_PER_GROUP ?= 40
ARCHIVE_BENCHMARK_SAMPLE_MAX_NOTES ?= 2000
ARCHIVE_BENCHMARK_SAMPLE_OVERSAMPLE_FACTOR ?= 10
ARCHIVE_REBUILD_WORKERS ?=
ARCHIVE_REBUILD_BATCH_SIZE ?=
ARCHIVE_REBUILD_COMMIT_INTERVAL ?=
ARCHIVE_REBUILD_PROGRESS_EVERY ?=
ARCHIVE_REBUILD_EXECUTOR ?=
ARCHIVE_EMBEDDING_PROVIDER ?= hash
ARCHIVE_EMBEDDING_MODEL ?= archive-hash-dev
ARCHIVE_EMBEDDING_VERSION ?= 1
ARCHIVE_EMBED_LIMIT ?= 0
ARCHIVE_EMBED_CONCURRENCY ?=
ARCHIVE_EMBED_WRITE_BATCH_SIZE ?=
ARCHIVE_EMBED_PROGRESS_EVERY ?=
ARCHIVE_EMBED_DEFER_VECTOR_INDEX ?=
ARCHIVE_SEED_LINK_INCLUDE_LLM ?= 0
ARCHIVE_SEED_LINK_APPLY_PROMOTIONS ?= 0
ARCHIVE_SEED_LINK_MODULES ?=
ARCHIVE_USE_ARNOLD_OPENAI_KEY ?= 0
ARCHIVE_OPENAI_API_KEY_OP_REF ?= op://Arnold/OPENAI_API_KEY/credential
ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_OP_REF ?= op://Arnold-Passkey-Gate/Service Account Auth Token: Arnold-Passkey-Gate/credential
ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_FILE ?=

PG_HOST_PORT_CMD = $(DOCKER_COMPOSE) port archive-postgres 5432 | awk -F: '{print $$NF}'
LOCAL_ARCHIVE_INDEX_DSN_CMD = POSTGRES_USER="$$(grep '^POSTGRES_USER=' $(PG_ENV_FILE) | cut -d= -f2)"; \
	POSTGRES_PASSWORD="$$(grep '^POSTGRES_PASSWORD=' $(PG_ENV_FILE) | cut -d= -f2)"; \
	POSTGRES_DB="$$(grep '^POSTGRES_DB=' $(PG_ENV_FILE) | cut -d= -f2)"; \
	POSTGRES_PORT="$$( $(PG_HOST_PORT_CMD) )"; \
	printf 'postgresql://%s:%s@127.0.0.1:%s/%s' "$$POSTGRES_USER" "$$POSTGRES_PASSWORD" "$$POSTGRES_PORT" "$$POSTGRES_DB"

# Logical dumps: always use pg_dump from the running archive-postgres container (PG major matches image, currently 17).
# Avoid host Homebrew pg_dump (often older) against this server.
ARCHIVE_DUMP_OUT ?= archive_seed.dump
DUMP_SCHEMA ?= $(ARCHIVE_SEED_INDEX_SCHEMA)
# Parallel jobs for pg_restore on Arnold (directory restore); override: PG_RESTORE_JOBS=16 make scp-restore-seed-arnold
PG_RESTORE_JOBS ?= 32
# 1 = pg_dump -Z0 (faster CPU, larger artifact). >=2 = parallel directory dump (-Fd -j) + docker cp (often fastest locally).
PG_DUMP_FAST ?= 0
PG_DUMP_JOBS ?= 0

.PHONY: install pg-up pg-down pg-logs pg-psql dump-schema dump-seed-schema dump-seed-schema-fast dump-seed-schema-stream-arnold pipe-restore-seed-arnold scp-restore-seed-arnold dump-and-scp-restore-seed-arnold watch-scp-restore-continue-hfa bootstrap-postgres bootstrap-seed-postgres rebuild-indexes rebuild-seed-indexes index-status index-status-seed embed-pending migrate migrate-seed migrate-dry-run migration-status migration-status-seed build-benchmark-sample benchmark-rebuild benchmark-seed-links smoke smoke-queries

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

dump-schema:
	@DUMP_SCHEMA=$(DUMP_SCHEMA) ARCHIVE_DUMP_OUT=$(ARCHIVE_DUMP_OUT) \
		ARCHIVE_STREAM_BYTES=$(ARCHIVE_STREAM_BYTES) PG_ENV_FILE=$(PG_ENV_FILE) COMPOSE_FILE=$(COMPOSE_FILE) \
		PG_DUMP_FAST=$(PG_DUMP_FAST) PG_DUMP_JOBS=$(PG_DUMP_JOBS) PG_DUMP_CONTAINER_NAME=$(PG_DUMP_CONTAINER_NAME) \
		ARCHIVE_DUMP_LOG=$(ARCHIVE_DUMP_LOG) ARCHIVE_DUMP_LOG_FILE=$(ARCHIVE_DUMP_LOG_FILE) \
		bash scripts/dump-schema-local.sh

dump-seed-schema:
	@$(MAKE) dump-schema DUMP_SCHEMA=$(ARCHIVE_SEED_INDEX_SCHEMA) ARCHIVE_DUMP_OUT=$(ARCHIVE_DUMP_OUT)

# Fast local dump: -Z0 + parallel -Fd -j (default 8 workers). Artifact is ARCHIVE_DUMP_OUT with .dump → sibling .dump.d directory.
dump-seed-schema-fast:
	@$(MAKE) dump-seed-schema PG_DUMP_FAST=1 PG_DUMP_JOBS=$(if $(filter-out 0,$(PG_DUMP_JOBS)),$(PG_DUMP_JOBS),8)

# Stream archive_seed custom dump to Arnold (see scripts/stream-seed-dump-to-arnold.sh).
# Progress: install `pv` (brew install pv). Optional ETA: ARCHIVE_STREAM_BYTES=... make dump-seed-schema-stream-arnold
dump-seed-schema-stream-arnold:
	@bash scripts/stream-seed-dump-to-arnold.sh

# Pipe pg_dump → pg_restore on Arnold (no archive_seed.dump on VM). Use when encrypted LV is too small for dump + PGDATA + vault.
pipe-restore-seed-arnold:
	@bash scripts/stream-seed-pipe-restore-arnold.sh

# Local dump → rsync to Arnold staging on /srv/hfa-secure (default) → copy into PGDATA on same volume → pg_restore --jobs.
# Override REMOTE_STAGING_DIR=/home/arnold/... only if root LV has room (~100+ GiB).
scp-restore-seed-arnold:
	@PG_RESTORE_JOBS=$(PG_RESTORE_JOBS) ARCHIVE_DUMP_OUT=$(ARCHIVE_DUMP_OUT) ARCHIVE_DUMP_ARTIFACT=$(ARCHIVE_DUMP_ARTIFACT) \
		ARCHIVE_SCP_RESTORE_LOG=$(ARCHIVE_SCP_RESTORE_LOG) ARCHIVE_SCP_RESTORE_LOG_FILE=$(ARCHIVE_SCP_RESTORE_LOG_FILE) \
		RSYNC_HEARTBEAT_SEC=$(RSYNC_HEARTBEAT_SEC) RESTORE_HEARTBEAT_SEC=$(RESTORE_HEARTBEAT_SEC) RESTORE_LOG_INTERVAL=$(RESTORE_LOG_INTERVAL) \
		bash scripts/scp-restore-seed-arnold.sh

dump-and-scp-restore-seed-arnold: dump-seed-schema
	@$(MAKE) scp-restore-seed-arnold PG_RESTORE_JOBS=$(PG_RESTORE_JOBS)

# Poll logs/archive-scp-restore.log every POLL_INTERVAL_SEC (default 120); on success run hey-arnold-hfa policy-check + hfa-archive-post-seed-restore.
watch-scp-restore-continue-hfa:
	@POLL_INTERVAL_SEC=$(POLL_INTERVAL_SEC) POLL_MAX_WAIT_SEC=$(POLL_MAX_WAIT_SEC) bash scripts/poll-scp-restore-and-continue-hfa.sh

bootstrap-postgres:
	@ARCHIVE_INDEX_DSN="$$( $(LOCAL_ARCHIVE_INDEX_DSN_CMD) )"; \
	HFA_VAULT_PATH=$(HFA_VAULT_PATH) \
	ARCHIVE_INDEX_DSN="$$ARCHIVE_INDEX_DSN" \
	ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp bootstrap-postgres

rebuild-indexes:
	@ARCHIVE_INDEX_DSN="$$( $(LOCAL_ARCHIVE_INDEX_DSN_CMD) )"; \
	HFA_VAULT_PATH=$(HFA_VAULT_PATH) \
	ARCHIVE_INDEX_DSN="$$ARCHIVE_INDEX_DSN" \
	ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_INDEX_SCHEMA) \
	$(if $(ARCHIVE_REBUILD_WORKERS),ARCHIVE_REBUILD_WORKERS=$(ARCHIVE_REBUILD_WORKERS)) \
	$(if $(ARCHIVE_REBUILD_BATCH_SIZE),ARCHIVE_REBUILD_BATCH_SIZE=$(ARCHIVE_REBUILD_BATCH_SIZE)) \
	$(if $(ARCHIVE_REBUILD_COMMIT_INTERVAL),ARCHIVE_REBUILD_COMMIT_INTERVAL=$(ARCHIVE_REBUILD_COMMIT_INTERVAL)) \
	$(if $(ARCHIVE_REBUILD_PROGRESS_EVERY),ARCHIVE_REBUILD_PROGRESS_EVERY=$(ARCHIVE_REBUILD_PROGRESS_EVERY)) \
	$(if $(ARCHIVE_REBUILD_EXECUTOR),ARCHIVE_REBUILD_EXECUTOR=$(ARCHIVE_REBUILD_EXECUTOR)) \
	$(PYTHON) -m archive_mcp rebuild-indexes

bootstrap-seed-postgres:
	@$(MAKE) bootstrap-postgres HFA_VAULT_PATH=$(ARCHIVE_SEED_VAULT) ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_SEED_INDEX_SCHEMA)

rebuild-seed-indexes:
	@$(MAKE) rebuild-indexes HFA_VAULT_PATH=$(ARCHIVE_SEED_VAULT) ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_SEED_INDEX_SCHEMA)

index-status:
	@ARCHIVE_INDEX_DSN="$$( $(LOCAL_ARCHIVE_INDEX_DSN_CMD) )"; \
	HFA_VAULT_PATH=$(HFA_VAULT_PATH) \
	ARCHIVE_INDEX_DSN="$$ARCHIVE_INDEX_DSN" \
	ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp index-status

index-status-seed:
	@$(MAKE) index-status HFA_VAULT_PATH=$(ARCHIVE_SEED_VAULT) ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_SEED_INDEX_SCHEMA)

embed-pending:
	@ARCHIVE_INDEX_DSN="$$( $(LOCAL_ARCHIVE_INDEX_DSN_CMD) )"; \
	HFA_VAULT_PATH=$(HFA_VAULT_PATH) \
	ARCHIVE_INDEX_DSN="$$ARCHIVE_INDEX_DSN" \
	ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_INDEX_SCHEMA) \
	ARCHIVE_EMBEDDING_PROVIDER=$(ARCHIVE_EMBEDDING_PROVIDER) \
	ARCHIVE_EMBEDDING_MODEL=$(ARCHIVE_EMBEDDING_MODEL) \
	ARCHIVE_EMBEDDING_VERSION=$(ARCHIVE_EMBEDDING_VERSION) \
	$(if $(ARCHIVE_EMBED_CONCURRENCY),ARCHIVE_EMBED_CONCURRENCY=$(ARCHIVE_EMBED_CONCURRENCY)) \
	$(if $(ARCHIVE_EMBED_WRITE_BATCH_SIZE),ARCHIVE_EMBED_WRITE_BATCH_SIZE=$(ARCHIVE_EMBED_WRITE_BATCH_SIZE)) \
	$(if $(ARCHIVE_EMBED_PROGRESS_EVERY),ARCHIVE_EMBED_PROGRESS_EVERY=$(ARCHIVE_EMBED_PROGRESS_EVERY)) \
	$(if $(ARCHIVE_EMBED_DEFER_VECTOR_INDEX),ARCHIVE_EMBED_DEFER_VECTOR_INDEX=$(ARCHIVE_EMBED_DEFER_VECTOR_INDEX)) \
	ARCHIVE_USE_ARNOLD_OPENAI_KEY=$(ARCHIVE_USE_ARNOLD_OPENAI_KEY) \
	ARCHIVE_OPENAI_API_KEY_OP_REF='$(ARCHIVE_OPENAI_API_KEY_OP_REF)' \
	ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_OP_REF='$(ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_OP_REF)' \
	ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_FILE='$(ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_FILE)' \
	$(PYTHON) -m archive_mcp embed-pending --limit $(ARCHIVE_EMBED_LIMIT) --embedding-model "$(ARCHIVE_EMBEDDING_MODEL)" --embedding-version $(ARCHIVE_EMBEDDING_VERSION)

build-benchmark-sample:
	$(PYTHON) -m archive_mcp build-benchmark-sample \
		--source-vault "$(ARCHIVE_BENCHMARK_SOURCE_VAULT)" \
		--output-vault "$(ARCHIVE_BENCHMARK_OUTPUT_VAULT)" \
		--per-group-limit $(ARCHIVE_BENCHMARK_SAMPLE_PER_GROUP) \
		--max-notes $(ARCHIVE_BENCHMARK_SAMPLE_MAX_NOTES) \
		--oversample-factor $(ARCHIVE_BENCHMARK_SAMPLE_OVERSAMPLE_FACTOR) \
		--sample-percent $(ARCHIVE_BENCHMARK_SAMPLE_PERCENT)

benchmark-rebuild:
	@ARCHIVE_INDEX_DSN="$$( $(LOCAL_ARCHIVE_INDEX_DSN_CMD) )"; \
	ARCHIVE_INDEX_DSN="$$ARCHIVE_INDEX_DSN" \
	$(PYTHON) -m archive_mcp benchmark-rebuild \
		--vault "$(HFA_VAULT_PATH)" \
		--schema "$(ARCHIVE_INDEX_SCHEMA)" \
		--profile "$(ARCHIVE_BENCHMARK_PROFILE)" \
		$(if $(ARCHIVE_REBUILD_WORKERS),--workers $(ARCHIVE_REBUILD_WORKERS)) \
		$(if $(ARCHIVE_REBUILD_BATCH_SIZE),--batch-size $(ARCHIVE_REBUILD_BATCH_SIZE)) \
		$(if $(ARCHIVE_REBUILD_COMMIT_INTERVAL),--commit-interval $(ARCHIVE_REBUILD_COMMIT_INTERVAL)) \
		$(if $(ARCHIVE_REBUILD_PROGRESS_EVERY),--progress-every $(ARCHIVE_REBUILD_PROGRESS_EVERY)) \
		$(if $(ARCHIVE_REBUILD_EXECUTOR),--executor $(ARCHIVE_REBUILD_EXECUTOR))

benchmark-seed-links:
	@ARCHIVE_INDEX_DSN="$$( $(LOCAL_ARCHIVE_INDEX_DSN_CMD) )"; \
	ARCHIVE_INDEX_DSN="$$ARCHIVE_INDEX_DSN" \
	$(PYTHON) -m archive_mcp benchmark-seed-links \
		--vault "$(HFA_VAULT_PATH)" \
		--schema "$(ARCHIVE_INDEX_SCHEMA)" \
		--profile "$(ARCHIVE_BENCHMARK_PROFILE)" \
		$(if $(ARCHIVE_REBUILD_WORKERS),--workers $(ARCHIVE_REBUILD_WORKERS)) \
		$(if $(ARCHIVE_REBUILD_BATCH_SIZE),--batch-size $(ARCHIVE_REBUILD_BATCH_SIZE)) \
		$(if $(ARCHIVE_REBUILD_COMMIT_INTERVAL),--commit-interval $(ARCHIVE_REBUILD_COMMIT_INTERVAL)) \
		$(if $(ARCHIVE_REBUILD_PROGRESS_EVERY),--progress-every $(ARCHIVE_REBUILD_PROGRESS_EVERY)) \
		$(if $(ARCHIVE_REBUILD_EXECUTOR),--executor $(ARCHIVE_REBUILD_EXECUTOR)) \
		$(if $(filter 1,$(ARCHIVE_SEED_LINK_INCLUDE_LLM)),--include-llm) \
		$(if $(filter 1,$(ARCHIVE_SEED_LINK_APPLY_PROMOTIONS)),--apply-promotions) \
		$(if $(ARCHIVE_SEED_LINK_MODULES),--modules "$(ARCHIVE_SEED_LINK_MODULES)")

migrate:
	@ARCHIVE_INDEX_DSN="$$( $(LOCAL_ARCHIVE_INDEX_DSN_CMD) )"; \
	HFA_VAULT_PATH=$(HFA_VAULT_PATH) \
	ARCHIVE_INDEX_DSN="$$ARCHIVE_INDEX_DSN" \
	ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp migrate

migrate-seed:
	@$(MAKE) migrate HFA_VAULT_PATH=$(ARCHIVE_SEED_VAULT) ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_SEED_INDEX_SCHEMA)

migrate-dry-run:
	@ARCHIVE_INDEX_DSN="$$( $(LOCAL_ARCHIVE_INDEX_DSN_CMD) )"; \
	HFA_VAULT_PATH=$(HFA_VAULT_PATH) \
	ARCHIVE_INDEX_DSN="$$ARCHIVE_INDEX_DSN" \
	ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp migrate --dry-run

migration-status:
	@ARCHIVE_INDEX_DSN="$$( $(LOCAL_ARCHIVE_INDEX_DSN_CMD) )"; \
	HFA_VAULT_PATH=$(HFA_VAULT_PATH) \
	ARCHIVE_INDEX_DSN="$$ARCHIVE_INDEX_DSN" \
	ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_INDEX_SCHEMA) \
	$(PYTHON) -m archive_mcp migration-status

migration-status-seed:
	@$(MAKE) migration-status HFA_VAULT_PATH=$(ARCHIVE_SEED_VAULT) ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_SEED_INDEX_SCHEMA)

smoke-queries:
	@ARCHIVE_INDEX_DSN="$$( $(LOCAL_ARCHIVE_INDEX_DSN_CMD) )"; \
	HFA_VAULT_PATH=$(HFA_VAULT_PATH) \
	ARCHIVE_INDEX_DSN="$$ARCHIVE_INDEX_DSN" \
	ARCHIVE_INDEX_SCHEMA=$(ARCHIVE_INDEX_SCHEMA) \
	ARCHIVE_EMBEDDING_PROVIDER=$(ARCHIVE_EMBEDDING_PROVIDER) \
	ARCHIVE_EMBEDDING_MODEL=$(ARCHIVE_EMBEDDING_MODEL) \
	ARCHIVE_EMBEDDING_VERSION=$(ARCHIVE_EMBEDDING_VERSION) \
	ARCHIVE_USE_ARNOLD_OPENAI_KEY=$(ARCHIVE_USE_ARNOLD_OPENAI_KEY) \
	ARCHIVE_OPENAI_API_KEY_OP_REF='$(ARCHIVE_OPENAI_API_KEY_OP_REF)' \
	ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_OP_REF='$(ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_OP_REF)' \
	ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_FILE='$(ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_FILE)' \
	$(PYTHON) -c "from archive_mcp.server import archive_embedding_status, archive_graph, archive_hybrid_search, archive_index_status, archive_query, archive_read, archive_search, archive_stats, archive_vector_search; q = archive_query(type_filter='person', limit=1); q = q if q != 'No matches' else archive_query(limit=1); first = q.splitlines()[0] if q and q != 'No matches' else ''; rel_path = first[2:].split(':', 1)[0].strip() if first.startswith('- ') else ''; summary = first.split(':', 1)[1].strip() if ': ' in first else 'archive'; query = ' '.join(summary.split()[:4]).strip() or 'archive'; print('=== INDEX STATUS ==='); print(archive_index_status()); print('=== STATS ==='); print(archive_stats()); print('=== QUERY SAMPLE ==='); print(q); print('=== READ SAMPLE ==='); print(archive_read(rel_path)[:800] if rel_path else 'No sample path'); print('=== GRAPH SAMPLE ==='); print(archive_graph(rel_path, hops=1) if rel_path else 'No sample path'); print('=== EMBEDDING STATUS ==='); print(archive_embedding_status()); print('=== SEARCH SAMPLE ==='); print(archive_search(query, limit=3)); print('=== VECTOR SAMPLE ==='); print(archive_vector_search(query, limit=3)); print('=== HYBRID SAMPLE ==='); print(archive_hybrid_search(query, limit=3))"

smoke: install bootstrap-postgres rebuild-indexes index-status embed-pending smoke-queries
