# Privacy contract

Combining records across services makes access rules more consequential: a graph hop or neighboring message can reveal content outside the record a client first requested. PPA carries access limits through retrieval and checks paths and provider destinations at their respective interfaces.

This contract covers path containment, tool profiles, record access, and routed provider egress. Host encryption and protection against code running as the same operating-system user are outside this contract. See the [security model](SECURITY_MODEL.md) and [data boundaries](DATA_BOUNDARIES.md).

## Path containment

All user-supplied and index-resolved vault paths go through
`archive_vault.paths.resolve_contained_path`.

Rejected before any file open:

- absolute paths (`/etc/passwd.md`, Windows drive prefixes)
- `..` traversal components
- NUL bytes
- empty paths
- symlink escapes: after `Path.resolve()`, the target must stay
  `relative_to(vault_root)`
- non-regular final targets (directories, devices)

Reads may follow an *internal* symlink only when the final regular file remains
inside the vault. Writes deny every symlink component, including a symlink
target file. Missing write parents are created only after each new directory is
checked to be a real directory, not a symlink.

Write-sensitive operations use a directory file descriptor with `O_DIRECTORY`
and `O_NOFOLLOW` where the platform provides them, then `openat`-style
create/replace of a single path component. `Path.resolve()` alone does not
eliminate check/open races against a same-UID attacker who can replace a
directory with a symlink between the check and the open. Temporary files stay
in the contained parent directory.

Denied reads return a stable not-found envelope (`found=false`, empty content)
and must not include outside-file bytes in the payload, exception text, or
progress logs. Callers must never test containment by opening real private
files; use a harmless sentinel outside a disposable vault.

### Applied call sites

| Site | Check |
| --- | --- |
| `DefaultArchiveStore.read()` `.md` paths | contained read |
| `DefaultArchiveStore.read()` UID→`rel_path` | contained read of the resolved path |
| `DefaultArchiveStore.person()` file open | contained read of index/slug path |
| `archive_vault.vault.read_note` | contained read |
| `archive_vault.vault.write_card` | contained write + no-follow replace |
| `archive_vault.vault.update_frontmatter_fields` | contained write |
| `archive_cli.commands.attachments.fetch_attachment` | `store.read` plus a second contained check on `rel_path` |

Attachment download destinations are chosen separately from vault paths. Recovery extraction also uses the contained-path helper.

## Tool profiles

`PPA_MCP_TOOL_PROFILE` is a closed enum. Valid values:

- `full`: all tools. This is the default when the variable is **unset**.
- `read-only`: retrieval tools only
- `remote-read`: minimal remote retrieval; no raw `archive_read`
- `admin-only`: maintenance tools (preserved; not silently removed)

Empty, whitespace-only, or unknown values **fail closed**: every tool returns
an actionable `Invalid PPA_MCP_TOOL_PROFILE=…` error listing the valid names.
Invalid values never select `full`.

The tool lists are defined in `archive_engine/access.py` and documented in the [runtime contract](PPA_RUNTIME_CONTRACT.md).

## Compatibility

- Deployments that omit the env var keep `full`.
- Deployments that set a misspelled profile, previously unrestricted, now deny
  every tool until the value is corrected.
- `store.read` of an escaping path now looks like a miss (`found=false`)
  instead of opening the outside file.
- `write_card` / `update_frontmatter_fields` reject escaping `rel_path`
  instead of writing through `vault / rel_path`.
- `.md` reads may include a contained `rel_path` field when the path is valid.

## Retrieval access

One immutable `AccessContext` is resolved at the CLI/MCP/HTTP entry point
(`archive_engine.access.resolve_access_context`) and passed through search,
hybrid/vector, graph, person, timeline, evidence, and raw reads. Native
serving applies the same predicate **before** top-k, counts, and graph
expansion. Denied records are absent from IDs, snippets, neighbor lists,
and pointer caches.

- Empty `allowed_sources` / `allowed_domains` with `deny=false` is the
  configured trusted-local context (unrestricted).
- Restricted contexts deny unknown provenance and incomplete lineage.
- Mixed-source derived records deny if any required source is denied.
- Domain labels use `p05b-domain-v1` (type/source, conservative unknown).
- Query-embed cache keys include `policy_identity`. Changing the allow-list
  or egress revision cannot reuse another principal's cache entry.
- Invalid `PPA_MCP_TOOL_PROFILE` still fails closed.

Env (optional, fail-closed): `PPA_ACCESS_PRINCIPAL`, `PPA_ACCESS_PROFILE`,
`PPA_ACCESS_ALLOWED_SOURCES`, `PPA_ACCESS_ALLOWED_DOMAINS`,
`PPA_ACCESS_ALLOWED_TOOLS`, `PPA_ACCESS_EGRESS_POLICY_REVISION`,
`PPA_ACCESS_DENY`.

## Provider egress

Routed provider calls go through `archive_engine.egress` attached to
`runtime.providers`. Destination capabilities (`hash` / `ollama` /
`openai` / `gemini` / `firecrawl` / `openclaw`) are declared here;
provider construction stays in the existing registries. Unknown
destinations fail closed.

- `local-only` (`PPA_EGRESS_MODE=local-only` or a revision containing
  `local-only`) blocks remote destinations and refuses a loopback
  `Location` that points at a remote host.
- Restricted `AccessContext` cannot send a denied source/domain to a
  remote destination.
- Provider failure does not substitute another destination.
- Tokens, DSNs, and raw card bodies are redacted from `ppa.*` logs and
  MCP error strings. Diagnostic IDs are hashes.
- PPA stores live vault files, warehouse data, and caches without built-in encryption at rest. The backup workflow uses external encryption tooling. See [data boundaries](DATA_BOUNDARIES.md) for storage and outbound paths.

## Client and host boundaries

A cloud client receives the content returned by its permitted calls. Provider controls do not govern that client's later use of the content or sandbox code running under the archive owner's operating-system account. The [security model](SECURITY_MODEL.md) defines those boundaries.
