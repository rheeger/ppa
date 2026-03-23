"""Schema DDL and table management mixin for PostgresArchiveIndex."""

from __future__ import annotations

import logging
from typing import Any

from .index_config import (CHUNK_SCHEMA_VERSION, MANIFEST_SCHEMA_VERSION,
                           get_seed_links_enabled)
from .loader import PROJECTION_NAMES
from .migrate import MigrationRunner
from .projections.registry import PROJECTION_REGISTRY, TYPED_PROJECTIONS

log = logging.getLogger(__name__)


class SchemaDDLMixin:
    """Mixin providing schema creation, projection tables, and index management."""

    schema: str
    vector_dimension: int

    def _projection_default_sql(self, value: Any, sql_type: str) -> str:
        if sql_type == "JSONB":
            literal = value if isinstance(value, str) else "{}"
            return f" DEFAULT '{literal}'::jsonb"
        if sql_type == "BOOLEAN":
            return f" DEFAULT {'TRUE' if bool(value) else 'FALSE'}"
        if sql_type in {"INTEGER", "DOUBLE PRECISION"}:
            return f" DEFAULT {value}"
        if value in (None, ""):
            return " DEFAULT ''"
        escaped = str(value).replace("'", "''")
        return f" DEFAULT '{escaped}'"

    def _create_projection_table(self, conn, projection, *, recreate: bool = False, ensure_indexes: bool = True) -> None:
        lines: list[str] = []
        for column in projection.columns:
            parts = [f"{column.name} {column.sql_type}"]
            if column.name == "card_uid":
                parts.append("PRIMARY KEY")
            if not column.nullable:
                parts.append("NOT NULL")
            parts.append(self._projection_default_sql(column.default, column.sql_type))
            lines.append(" ".join(part for part in parts if part))
        if recreate:
            conn.execute(f"DROP TABLE IF EXISTS {self.schema}.{projection.table_name} CASCADE")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{projection.table_name} (
                {", ".join(lines)}
            )
            """
        )
        if not ensure_indexes:
            return
        for column in projection.columns:
            if column.indexed and column.name != "card_uid":
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{projection.table_name}_{column.name} "
                    f"ON {self.schema}.{projection.table_name}({column.name})"
                )

    def _create_schema(self, conn, *, recreate_typed: bool = False, ensure_indexes: bool = True) -> None:
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.cards (
                uid TEXT PRIMARY KEY,
                rel_path TEXT NOT NULL UNIQUE,
                slug TEXT NOT NULL,
                type TEXT NOT NULL,
                summary TEXT NOT NULL DEFAULT '',
                source_id TEXT NOT NULL DEFAULT '',
                created TEXT NOT NULL DEFAULT '',
                updated TEXT NOT NULL DEFAULT '',
                activity_at TEXT NOT NULL DEFAULT '',
                sent_at TEXT NOT NULL DEFAULT '',
                start_at TEXT NOT NULL DEFAULT '',
                first_message_at TEXT NOT NULL DEFAULT '',
                last_message_at TEXT NOT NULL DEFAULT '',
                content_hash TEXT NOT NULL,
                search_text TEXT NOT NULL DEFAULT '',
                search_document tsvector GENERATED ALWAYS AS (
                    to_tsvector('english', coalesce(summary, '') || ' ' || coalesce(search_text, ''))
                ) STORED
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_cards_slug ON {self.schema}.cards(slug)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_cards_type ON {self.schema}.cards(type)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_cards_created ON {self.schema}.cards(created)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_cards_activity_at ON {self.schema}.cards(activity_at)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_cards_search_document ON {self.schema}.cards USING GIN(search_document)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.card_sources (
                card_uid TEXT NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY(card_uid, source)
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_card_sources_source ON {self.schema}.card_sources(source)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_card_sources_card_uid ON {self.schema}.card_sources(card_uid)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.card_people (
                card_uid TEXT NOT NULL,
                person TEXT NOT NULL,
                PRIMARY KEY(card_uid, person)
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_card_people_person ON {self.schema}.card_people(person)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_card_people_card_uid ON {self.schema}.card_people(card_uid)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.card_orgs (
                card_uid TEXT NOT NULL,
                org TEXT NOT NULL,
                PRIMARY KEY(card_uid, org)
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_card_orgs_org ON {self.schema}.card_orgs(org)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_card_orgs_card_uid ON {self.schema}.card_orgs(card_uid)")
        for projection in TYPED_PROJECTIONS:
            self._create_projection_table(conn, projection, recreate=recreate_typed, ensure_indexes=ensure_indexes)
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.external_ids (
                card_uid TEXT NOT NULL,
                field_name TEXT NOT NULL,
                provider TEXT NOT NULL,
                external_id TEXT NOT NULL,
                PRIMARY KEY(card_uid, field_name, external_id)
            )
            """
        )
        if ensure_indexes:
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_external_ids_lookup ON {self.schema}.external_ids(external_id, provider)"
            )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.duplicate_uid_rows (
                uid TEXT NOT NULL,
                preferred_rel_path TEXT NOT NULL,
                preferred_type TEXT NOT NULL,
                preferred_source_id TEXT NOT NULL DEFAULT '',
                preferred_summary TEXT NOT NULL DEFAULT '',
                duplicate_rel_path TEXT NOT NULL,
                duplicate_type TEXT NOT NULL,
                duplicate_source_id TEXT NOT NULL DEFAULT '',
                duplicate_summary TEXT NOT NULL DEFAULT '',
                duplicate_group_size INTEGER NOT NULL DEFAULT 2,
                PRIMARY KEY(uid, duplicate_rel_path)
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_duplicate_uid_rows_uid ON {self.schema}.duplicate_uid_rows(uid)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_duplicate_uid_rows_preferred_path ON {self.schema}.duplicate_uid_rows(preferred_rel_path)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.edges (
                source_uid TEXT NOT NULL,
                source_path TEXT NOT NULL,
                target_uid TEXT NOT NULL DEFAULT '',
                target_slug TEXT NOT NULL,
                target_path TEXT NOT NULL,
                target_kind TEXT NOT NULL DEFAULT 'card',
                edge_type TEXT NOT NULL,
                field_name TEXT NOT NULL,
                PRIMARY KEY(source_uid, target_path, edge_type, field_name)
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_edges_source_path ON {self.schema}.edges(source_path)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_edges_source_uid ON {self.schema}.edges(source_uid)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_edges_target_path ON {self.schema}.edges(target_path)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_edges_target_uid ON {self.schema}.edges(target_uid)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.chunks (
                chunk_key TEXT NOT NULL UNIQUE,
                chunk_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                card_uid TEXT NOT NULL,
                rel_path TEXT NOT NULL,
                chunk_type TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                chunk_schema_version INTEGER NOT NULL DEFAULT {CHUNK_SCHEMA_VERSION},
                source_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
                content TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                token_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(card_uid, chunk_type, chunk_index, content_hash)
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_chunks_card_uid ON {self.schema}.chunks(card_uid)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.embeddings (
                chunk_key TEXT NOT NULL REFERENCES {self.schema}.chunks(chunk_key) ON DELETE CASCADE,
                embedding_model TEXT NOT NULL,
                embedding_version INTEGER NOT NULL,
                embedding vector({self.vector_dimension}) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY(chunk_key, embedding_model, embedding_version)
            )
            """
        )
        if get_seed_links_enabled():
            self._create_seed_link_schema(conn, ensure_indexes=ensure_indexes)
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.note_manifest (
                rel_path TEXT PRIMARY KEY,
                card_uid TEXT NOT NULL,
                slug TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                frontmatter_hash TEXT NOT NULL,
                file_size BIGINT NOT NULL,
                mtime_ns BIGINT NOT NULL,
                card_type TEXT NOT NULL,
                typed_projection TEXT NOT NULL DEFAULT '',
                people_json TEXT NOT NULL DEFAULT '[]',
                orgs_json TEXT NOT NULL DEFAULT '[]',
                scan_version INTEGER NOT NULL,
                chunk_schema_version INTEGER NOT NULL,
                projection_registry_version INTEGER NOT NULL,
                index_schema_version INTEGER NOT NULL,
                last_built_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.rebuild_checkpoint (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                run_id TEXT NOT NULL DEFAULT '',
                mode TEXT NOT NULL DEFAULT 'full',
                last_committed_rel_path TEXT NOT NULL DEFAULT '',
                last_committed_card_uid TEXT NOT NULL DEFAULT '',
                loaded_card_count INTEGER NOT NULL DEFAULT 0,
                loaded_row_counts_json TEXT NOT NULL DEFAULT '{{}}',
                loaded_bytes_estimate BIGINT NOT NULL DEFAULT 0,
                vault_manifest_hash TEXT NOT NULL DEFAULT '',
                index_schema_version INTEGER NOT NULL DEFAULT 0,
                chunk_schema_version INTEGER NOT NULL DEFAULT 0,
                projection_registry_version INTEGER NOT NULL DEFAULT 0,
                manifest_schema_version INTEGER NOT NULL DEFAULT 0,
                duplicate_uid_rows_loaded BOOLEAN NOT NULL DEFAULT FALSE,
                status TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO {self.schema}.rebuild_checkpoint (id) VALUES (1)
            ON CONFLICT (id) DO NOTHING
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        if ensure_indexes:
            self._ensure_embeddings_vector_index(conn)
        conn.commit()
        self._mark_all_migrations_applied(conn)

    def _create_seed_link_schema(self, conn, *, ensure_indexes: bool = True) -> None:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.link_jobs (
                job_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                job_type TEXT NOT NULL,
                module_name TEXT NOT NULL,
                source_card_uid TEXT NOT NULL,
                source_rel_path TEXT NOT NULL,
                shard_key TEXT NOT NULL DEFAULT '',
                priority INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                input_hash TEXT NOT NULL,
                linker_version INTEGER NOT NULL DEFAULT 1,
                claimed_by TEXT NOT NULL DEFAULT '',
                claimed_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(job_type, module_name, source_card_uid, input_hash, linker_version)
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_link_jobs_status_priority ON {self.schema}.link_jobs(status, priority DESC, job_id ASC)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_link_jobs_source_uid ON {self.schema}.link_jobs(source_card_uid)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.link_candidates (
                candidate_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                job_id BIGINT REFERENCES {self.schema}.link_jobs(job_id) ON DELETE SET NULL,
                module_name TEXT NOT NULL,
                linker_version INTEGER NOT NULL DEFAULT 1,
                source_card_uid TEXT NOT NULL,
                source_rel_path TEXT NOT NULL,
                target_card_uid TEXT NOT NULL,
                target_rel_path TEXT NOT NULL,
                target_kind TEXT NOT NULL DEFAULT 'card',
                proposed_link_type TEXT NOT NULL,
                candidate_group TEXT NOT NULL DEFAULT '',
                input_hash TEXT NOT NULL,
                evidence_hash TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending_qc',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(module_name, linker_version, source_card_uid, target_card_uid, proposed_link_type, input_hash)
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_link_candidates_status ON {self.schema}.link_candidates(status)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_link_candidates_module ON {self.schema}.link_candidates(module_name)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_link_candidates_source_uid ON {self.schema}.link_candidates(source_card_uid)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_link_candidates_target_uid ON {self.schema}.link_candidates(target_card_uid)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.link_evidence (
                evidence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                candidate_id BIGINT NOT NULL REFERENCES {self.schema}.link_candidates(candidate_id) ON DELETE CASCADE,
                evidence_type TEXT NOT NULL,
                evidence_source TEXT NOT NULL,
                feature_name TEXT NOT NULL,
                feature_value TEXT NOT NULL DEFAULT '',
                feature_weight DOUBLE PRECISION NOT NULL DEFAULT 0,
                raw_payload_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_link_evidence_candidate_id ON {self.schema}.link_evidence(candidate_id)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.link_decisions (
                candidate_id BIGINT PRIMARY KEY REFERENCES {self.schema}.link_candidates(candidate_id) ON DELETE CASCADE,
                deterministic_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                lexical_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                graph_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                llm_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                risk_penalty DOUBLE PRECISION NOT NULL DEFAULT 0,
                final_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                decision TEXT NOT NULL DEFAULT 'discard',
                decision_reason TEXT NOT NULL DEFAULT '',
                auto_approved_floor DOUBLE PRECISION NOT NULL DEFAULT 0,
                review_floor DOUBLE PRECISION NOT NULL DEFAULT 0,
                discard_floor DOUBLE PRECISION NOT NULL DEFAULT 0,
                policy_version INTEGER NOT NULL DEFAULT 1,
                llm_model TEXT NOT NULL DEFAULT '',
                llm_output_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_link_decisions_decision_confidence ON {self.schema}.link_decisions(decision, final_confidence DESC)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.promotion_queue (
                promotion_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                candidate_id BIGINT NOT NULL REFERENCES {self.schema}.link_candidates(candidate_id) ON DELETE CASCADE,
                promotion_target TEXT NOT NULL,
                target_field_name TEXT NOT NULL DEFAULT '',
                promotion_status TEXT NOT NULL DEFAULT 'queued',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                claimed_by TEXT NOT NULL DEFAULT '',
                claimed_at TIMESTAMPTZ,
                blocked_reason TEXT NOT NULL DEFAULT '',
                applied_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(candidate_id, promotion_target, target_field_name)
            )
            """
        )
        conn.execute(f"ALTER TABLE {self.schema}.promotion_queue ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0")
        conn.execute(f"ALTER TABLE {self.schema}.promotion_queue ADD COLUMN IF NOT EXISTS claimed_by TEXT NOT NULL DEFAULT ''")
        conn.execute(f"ALTER TABLE {self.schema}.promotion_queue ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ")
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_promotion_queue_status ON {self.schema}.promotion_queue(promotion_status, promotion_target)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.review_actions (
                action_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                candidate_id BIGINT NOT NULL REFERENCES {self.schema}.link_candidates(candidate_id) ON DELETE CASCADE,
                reviewer TEXT NOT NULL,
                action TEXT NOT NULL,
                notes TEXT NOT NULL DEFAULT '',
                score_at_review DOUBLE PRECISION NOT NULL DEFAULT 0,
                decision_at_review TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        if ensure_indexes:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_review_actions_candidate_id ON {self.schema}.review_actions(candidate_id)")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.link_review_metrics (
                metric_date DATE NOT NULL,
                module_name TEXT NOT NULL,
                link_type TEXT NOT NULL,
                score_band TEXT NOT NULL,
                candidate_count INTEGER NOT NULL DEFAULT 0,
                approved_count INTEGER NOT NULL DEFAULT 0,
                rejected_count INTEGER NOT NULL DEFAULT 0,
                override_count INTEGER NOT NULL DEFAULT 0,
                auto_promoted_count INTEGER NOT NULL DEFAULT 0,
                sampled_auto_promoted_count INTEGER NOT NULL DEFAULT 0,
                sample_precision DOUBLE PRECISION NOT NULL DEFAULT 0,
                PRIMARY KEY(metric_date, module_name, link_type, score_band)
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.link_dead_ends (
                card_uid TEXT PRIMARY KEY,
                rel_path TEXT NOT NULL,
                card_type TEXT NOT NULL,
                degree INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

    def _ensure_embeddings_vector_index(self, conn) -> None:
        conn.execute("SET LOCAL maintenance_work_mem = '128MB'")
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_embeddings_vector
            ON {self.schema}.embeddings
            USING ivfflat (embedding vector_cosine_ops)
            """
        )

    def _drop_embeddings_vector_index(self, conn) -> None:
        conn.execute(f"DROP INDEX IF EXISTS {self.schema}.idx_embeddings_vector")

    def _mark_all_migrations_applied(self, conn) -> None:
        """After _create_schema() builds the full schema from scratch, mark
        all known migrations as applied so the runner doesn't try to re-apply."""
        runner = MigrationRunner(conn, self.schema)
        marked = runner.mark_all_applied()
        if marked:
            log.info("Marked %d migration(s) as applied on fresh schema: %s", len(marked), marked)

    def _run_pending_migrations(self, conn) -> None:
        """Apply any pending migrations. Called before version checks so that
        schema deltas land before the rebuild gate evaluates."""
        runner = MigrationRunner(conn, self.schema)
        if not runner.pending():
            return
        result = runner.run()
        if result.applied:
            log.info("Applied %d migration(s): %s", len(result.applied), result.applied)
        if result.failed is not None:
            log.error("Migration %d failed: %s", result.failed, result.error)
            raise RuntimeError(f"Migration {result.failed} failed: {result.error}")

    def _migration_status(self, conn) -> dict[str, Any]:
        """Return migration diagnostics."""
        runner = MigrationRunner(conn, self.schema)
        return runner.status()

    def _clear(self, conn) -> None:
        table_names = ["embeddings", *PROJECTION_NAMES, "meta"]
        qualified = ", ".join(f"{self.schema}.{table_name}" for table_name in table_names)
        conn.execute(f"TRUNCATE TABLE {qualified} RESTART IDENTITY CASCADE")
