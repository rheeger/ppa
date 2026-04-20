"""Phase 6.5 linker modules.

Each submodule registers one linker via ``archive_cli.linker_framework.register_linker``
at import time. Adding a new linker is a one-file contribution — see
``archive_docs/CONTRIBUTING_LINKERS.md`` or ``.cursor/plans/_templates/linker.plan.md``.

Import order here doesn't matter — ``register_linker`` is idempotent by
``module_name``. Each module is independent.
"""

from __future__ import annotations

from . import (
    calendar,  # noqa: F401
    communication,  # noqa: F401
    finance_reconcile,  # noqa: F401
    graph,  # noqa: F401
    identity,  # noqa: F401
    media,  # noqa: F401
    meeting_artifact,  # noqa: F401
    orphan,  # noqa: F401
    semantic,  # noqa: F401
    trip_cluster,  # noqa: F401
)
