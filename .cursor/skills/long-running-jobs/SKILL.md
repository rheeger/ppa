---
name: long-running-jobs
description: >-
  Launches PPA jobs that can outlive a chat (maintain, rebuild-indexes,
  embed-pending, slice-seed, enrich-emails, extract-emails, Gmail catch-up,
  source updaters, serving-index publish) in a detached, non-managed session.
  Use when starting, restarting, watching, or killing any PPA operation that
  may run longer than a couple of minutes.
---

# Detached long-running PPA jobs

ALWAYS run long running operations on non-managed terminal sessions in this
detached state. Cursor-managed shells die with the chat. Session compact,
agent end, and `block_until_ms: 0` process-group teardown have SIGTERM'd
living-seed jobs with no Python traceback. `--log-file` alone does not save
a process that is still a child of the agent shell.

Read this skill before you start `maintain`, `rebuild-indexes`,
`embed-pending`, `slice-seed`, `enrich-emails`, `extract-emails`, a Gmail
walk, a source updater, or serving-index publish. Logging details live in
`.cursor/rules/ppa-long-running-jobs.mdc`.

## Launch (required)

1. Put `--log-file` on the **parent** parser, before the subcommand. Prefer
   `logs/*.log` (gitignored).
2. Detach from Cursor with `setsid`, stdin from `/dev/null`, and a separate
   stdio file. Confirm the new PID has **PPID 1**.
3. Return. Watch the `--log-file`. Do not `AwaitShell` the launcher. Do not
   keep a Cursor terminal session as the job's parent.

```bash
setsid .venv/bin/python -m archive_cli --log-file logs/JOB.log SUBCOMMAND ARGS \
  </dev/null >logs/JOB.stdio 2>&1 &
echo "pid=$!"
sleep 2
ps -p "$PID" -o pid,ppid,etime,command
```

A wrapper script is fine if the `setsid` line is what actually execs Python.
`nohup` without `setsid` is not enough if the process stays in Cursor's
process group.

## After launch

- Prove detach: `ps -p PID -o pid,ppid` shows PPID 1.
- Prove work: the `--log-file` gains `ppa.*` lines.
- Do not start a second writer (`maintain`, rematerialize, publish) against
  the same vault or schema while one is running.
- Do not kill HTTP MCP (`serve --http`) or Cursor stdio MCP (parent looks
  like `Cursor Helper: mcp-process`). Those are warm servers, not leftover
  jobs.

## Never

- `block_until_ms: 0` (or a Cursor-owned background shell) as the lifetime
  of the job.
- A foreground Shell call that you expect to survive chat end.
- A new `archive_cli` per living-archive question (warm MCP only).
- `--log-file` after the subcommand.

## Watch and stop

```bash
# status
ps -p PID -o pid,ppid,etime,stat,command
tail -n 40 logs/JOB.log

# leftover job only (never the current writer, never MCP)
kill PID
```
