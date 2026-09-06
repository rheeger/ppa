"""Shared two-instance restart cycle for P09-D pytest and acceptance."""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from archive_cli.commands.maintain import run_maintenance
from archive_cli.commands.setup import FIXTURE_ORG_UID, FIXTURE_PERSON_UID, apply_setup
from archive_cli.status.aggregate import build_production_status
from archive_cli.status.instance_policy import current_instance_policy
from archive_engine.changes import recovery_checkpoint_binding
from archive_engine.config import bind_instance_config, current_secret_values, resolve_instance_config
from archive_tests.acceptance.environment import IsolatedRuntime, reset_serving_handle

logger = logging.getLogger("ppa.p09d")

_ENV_KEYS = (
    "PPA_PATH",
    "PPA_INDEX_DSN",
    "PPA_INDEX_SCHEMA",
    "PPA_SERVING_INDEX_PATH",
    "PPA_QUERY_EMBED_CACHE_PATH",
    "PPA_EMBEDDING_PROVIDER",
    "PPA_EMBEDDING_MODEL",
    "PPA_EMBEDDING_VERSION",
    "PPA_VECTOR_DIMENSION",
    "PPA_ENGINE",
    "PPA_BOOTSTRAP_FORCE",
)


def instance_env(
    runtime: IsolatedRuntime,
    vault: Path,
    *,
    schema: str,
    serving: Path,
    embed_cache: Path,
) -> dict[str, str]:
    env = dict(runtime.env)
    env.pop("PPA_TEST_PG_DSN", None)
    env["PPA_PATH"] = str(vault)
    env["PPA_INDEX_SCHEMA"] = schema
    env["PPA_SERVING_INDEX_PATH"] = str(serving)
    env["PPA_QUERY_EMBED_CACHE_PATH"] = str(embed_cache)
    env["PPA_SEED_LINKS_ENABLED"] = "0"
    return env


@contextmanager
def bound_instance(vault: Path, env: dict[str, str]) -> Iterator[Any]:
    previous = {key: os.environ.get(key) for key in _ENV_KEYS}
    os.environ.pop("PPA_TEST_PG_DSN", None)
    os.environ.update({key: env[key] for key in _ENV_KEYS if key in env})
    try:
        config = resolve_instance_config(instance_dir=vault, environ=env, allow_cwd_discovery=False)
        with bind_instance_config(config, secrets=current_secret_values()):
            from archive_cli.store import DefaultArchiveStore

            store = DefaultArchiveStore(vault=vault)
            try:
                yield store, config
            finally:
                store.close()
    finally:
        reset_serving_handle()
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def setup_person_instance(root: Path, schema: str) -> dict[str, Any]:
    return apply_setup(
        {
            "root": str(root),
            "entity_name": "Ada Example",
            "entity_type": "person",
            "index_schema": schema,
            "fixture": "sample.fixture",
            "embedding_provider": "hash",
        }
    )


def setup_org_instance(root: Path, schema: str) -> dict[str, Any]:
    return apply_setup(
        {
            "root": str(root),
            "entity_name": "Acme Example",
            "entity_type": "organization",
            "index_schema": schema,
            "fixture": "sample.fixture",
            "embedding_provider": "hash",
        }
    )


def _query_fixture(store: Any, uid: str) -> dict[str, Any]:
    read = store.read(uid)
    query = store.query(limit=20)
    rows = query.get("rows") or query.get("cards") or query.get("results") or []
    if isinstance(query, list):
        rows = query
    uids = []
    for row in rows:
        if isinstance(row, dict):
            uids.append(str(row.get("uid") or row.get("card_uid") or ""))
    return {
        "found": bool(read.get("found")),
        "content": str(read.get("content") or ""),
        "query_uids": [item for item in uids if item],
    }


def cycle_instance(
    runtime: IsolatedRuntime,
    *,
    vault: Path,
    schema: str,
    serving: Path,
    embed_cache: Path,
    query_uid: str,
) -> dict[str, Any]:
    env = instance_env(runtime, vault, schema=schema, serving=serving, embed_cache=embed_cache)
    with bound_instance(vault, env) as (store, config):
        store.bootstrap()
        store.rebuild()
        first_started = time.monotonic()
        first = run_maintenance(store=store, logger=logger, dry_run=False)
        first_elapsed = time.monotonic() - first_started
        if first.publication.get("ok") is False and first.publication.get("error"):
            raise AssertionError(f"publish failed: {first.publication}")
        first_query = _query_fixture(store, query_uid)
        if not first_query["found"]:
            raise AssertionError(f"first query missed {query_uid}")
        checkpoint = recovery_checkpoint_binding(vault, archive_id=config.identity.archive_id)
        policy = current_instance_policy(
            vault_path=vault,
            archive_instance=schema,
            archive_id=config.identity.archive_id,
        )
        with store.index._connect() as conn:
            status = build_production_status(
                store=store,
                archive_instance=schema,
                conn=conn,
                schema=schema,
                require_production_soak=False,
                include_index_status=False,
            )

    reset_serving_handle()

    with bound_instance(vault, env) as (store, config):
        noop_started = time.monotonic()
        noop = run_maintenance(store=store, logger=logger, dry_run=False)
        noop_elapsed = time.monotonic() - noop_started
        restart_query = _query_fixture(store, query_uid)
        if not restart_query["found"]:
            raise AssertionError(f"restart query missed {query_uid}")
        restart_checkpoint = recovery_checkpoint_binding(vault, archive_id=config.identity.archive_id)

    cheap = bool(noop.nothing_to_do) or "processor_execution (no dirty work)" in noop.skipped_steps
    return {
        "archive_id": config.identity.archive_id,
        "root": str(vault),
        "schema": schema,
        "serving_index_path": str(serving),
        "entity_type": config.entity.entity_type,
        "entity_name": config.entity.name,
        "scope_names": [str(scope.get("name") or "") for scope in config.scopes],
        "checkpoint": restart_checkpoint,
        "first_checkpoint": checkpoint,
        "first_query": first_query,
        "restart_query": restart_query,
        "first_maintain_elapsed": round(first_elapsed, 3),
        "noop_maintain_elapsed": round(noop_elapsed, 3),
        "noop_nothing_to_do": cheap,
        "noop_skipped": list(noop.skipped_steps),
        "status": status,
        "policy": policy,
        "fresh": bool(status.get("fresh")),
        "production_proven": bool(status.get("production_proven")),
        "analytics": status.get("analytics") or {},
    }


def run_two_instance_cycle(runtime: IsolatedRuntime) -> dict[str, Any]:
    """Person + organization fixtures: import, maintain, publish, query, restart."""

    os.environ.pop("PPA_TEST_PG_DSN", None)
    person_root = runtime.root / "person"
    org_root = runtime.root / "org"
    person_schema = f"{runtime.schema}_p"
    org_schema = f"{runtime.schema}_o"
    person_setup = setup_person_instance(person_root, person_schema)
    org_setup = setup_org_instance(org_root, org_schema)
    if person_setup["archive_id"] == org_setup["archive_id"]:
        raise AssertionError("archive identities collided at setup")
    if person_setup.get("fresh") or org_setup.get("fresh"):
        raise AssertionError("setup claimed fresh from a manifest")

    person = cycle_instance(
        runtime,
        vault=person_root,
        schema=person_schema,
        serving=runtime.root / "person-index",
        embed_cache=runtime.root / "person-embed.sqlite",
        query_uid=FIXTURE_PERSON_UID,
    )
    org = cycle_instance(
        runtime,
        vault=org_root,
        schema=org_schema,
        serving=runtime.root / "org-index",
        embed_cache=runtime.root / "org-embed.sqlite",
        query_uid=FIXTURE_ORG_UID,
    )
    org_env = instance_env(
        runtime,
        org_root,
        schema=org_schema,
        serving=runtime.root / "org-index",
        embed_cache=runtime.root / "org-embed.sqlite",
    )
    reset_serving_handle()
    with bound_instance(org_root, org_env) as (store, _cfg):
        shared = _query_fixture(store, FIXTURE_PERSON_UID)
        if not shared["found"] or "Ada" not in shared["content"]:
            raise AssertionError("org instance could not read the same external person UID")
        org_person = {"archive_id": org["archive_id"], "restart_query": shared}

    if person["archive_id"] == org["archive_id"]:
        raise AssertionError("archive identities collided after maintain")
    if person["root"] == org["root"]:
        raise AssertionError("roots collided")
    if person["checkpoint"] == org["checkpoint"]:
        raise AssertionError("checkpoints collided")
    if person["entity_type"] == org["entity_type"]:
        raise AssertionError("entity types were not distinct")
    if set(person["scope_names"]) & set(org["scope_names"]):
        raise AssertionError(f"saved scopes overlapped: {person['scope_names']} {org['scope_names']}")
    if org_person["archive_id"] != org["archive_id"]:
        raise AssertionError("org archive_id drifted when reading shared person UID")
    if FIXTURE_PERSON_UID not in person["restart_query"]["content"] and "Ada" not in person["restart_query"]["content"]:
        raise AssertionError("person restart lost Ada")
    if "Acme" not in org["restart_query"]["content"]:
        raise AssertionError("org restart lost Acme")
    if "Ada" not in org_person["restart_query"]["content"]:
        raise AssertionError("org instance could not read the same external person UID")
    if person["policy"].get("historical_exception_applies") or org["policy"].get("historical_exception_applies"):
        raise AssertionError("new instance inherited local_seed_living_corpus")
    if person["fresh"] or org["fresh"]:
        raise AssertionError("status claimed fresh from a manifest")
    if person["production_proven"] or org["production_proven"]:
        raise AssertionError("smoke test claimed production_proven")
    if (person["analytics"].get("cli") != "pending") or (person["analytics"].get("mcp") != "pending"):
        raise AssertionError("analytics was not labeled pending")
    if not person["noop_nothing_to_do"] or not org["noop_nothing_to_do"]:
        raise AssertionError("no-op maintain was not cheap")

    return {
        "person": {
            "archive_id": person["archive_id"],
            "root": person["root"],
            "schema": person["schema"],
            "entity_type": person["entity_type"],
            "scopes": person["scope_names"],
            "checkpoint": person["checkpoint"],
            "external_id": FIXTURE_PERSON_UID,
        },
        "organization": {
            "archive_id": org["archive_id"],
            "root": org["root"],
            "schema": org["schema"],
            "entity_type": org["entity_type"],
            "scopes": org["scope_names"],
            "checkpoint": org["checkpoint"],
            "external_id": FIXTURE_ORG_UID,
            "shared_person_uid": FIXTURE_PERSON_UID,
        },
        "same_external_person_uid": FIXTURE_PERSON_UID,
        "restart_query_ok": True,
        "isolated": True,
        "production_proven": False,
        "analytics": "pending",
    }
