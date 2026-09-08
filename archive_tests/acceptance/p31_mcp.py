"""Real MCP stdio transport for PR31 installed-product proof.

Uses the installed environment's MCP SDK as the client so the session
speaks the same protocol as ``ppa serve``. This is not an in-process
``archive_person`` helper.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


class McpTransportError(RuntimeError):
    """stdio MCP session failed."""


_CLIENT = r"""
import asyncio
import json
import os
import sys

from mcp import ClientSession, StdioServerParameters, stdio_client


async def main() -> None:
    command = json.loads(sys.argv[1])
    tool = sys.argv[2]
    arguments = json.loads(sys.argv[3])
    params = StdioServerParameters(
        command=command[0],
        args=command[1:],
        env={key: str(value) for key, value in os.environ.items() if value is not None},
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool, arguments)
            payload = result.model_dump() if hasattr(result, "model_dump") else {"content": str(result)}
            print(json.dumps({"ok": True, "result": payload}, default=str))


asyncio.run(main())
"""


def call_tool(
    command: list[str],
    *,
    env: dict[str, str],
    tool: str,
    arguments: dict[str, Any],
    cwd: Path,
    timeout: float = 60.0,
    python: str | Path | None = None,
) -> dict[str, Any]:
    """Initialize a real ``ppa serve`` stdio session and call one tool."""

    runner = str(python or command[0])
    if runner.endswith("/ppa") or runner.endswith("/ppa.exe"):
        runner = str(Path(runner).with_name("python"))
    proc = subprocess.run(
        [runner, "-c", _CLIENT, json.dumps(command), tool, json.dumps(arguments)],
        cwd=str(cwd),
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if proc.returncode != 0:
        raise McpTransportError(f"MCP client failed rc={proc.returncode} stderr={proc.stderr[-3000:]}")
    line = (proc.stdout or "").strip().splitlines()[-1] if proc.stdout.strip() else ""
    if not line:
        raise McpTransportError(f"MCP client returned no JSON stdout={proc.stdout!r} stderr={proc.stderr[-2000:]}")
    try:
        payload = json.loads(line)
    except json.JSONDecodeError as exc:
        raise McpTransportError(f"MCP client output is not JSON: {line[:500]}") from exc
    if not payload.get("ok"):
        raise McpTransportError(f"MCP client payload not ok: {payload}")
    return payload
