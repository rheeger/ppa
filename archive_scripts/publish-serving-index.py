#!/usr/bin/env python3
"""Publish ACTIVE from the current warehouse. Maintain/rebuild call the same function."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from archive_cli.log import attach_file_log, configure_logging
from archive_cli.serving_index import publish_serving_index
from archive_cli.store import get_archive_store


def main() -> None:
    configure_logging()
    attach_file_log(Path("logs/serving-index-publish.log"))
    log = logging.getLogger("ppa.serving_index")
    store = get_archive_store()
    log.info("publish_serving_index_cli vault=%s", store.vault)
    import os

    dest_generation = os.environ.get("PPA_SERVING_REPAIR_GENERATION", "").strip() or None
    skip_embeddings = os.environ.get("PPA_SERVING_SKIP_EMBEDDINGS", "") in {"1", "true", "yes"}
    result = publish_serving_index(
        store,
        logger=log,
        dest_generation=dest_generation,
        skip_embeddings=skip_embeddings,
    )
    print(json.dumps(result, indent=2, default=str))
    if not result.get("ok"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
