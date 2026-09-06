# Privacy contract (P05-A)

This is the current containment and tool-profile contract. It does not claim
encrypted-at-rest deployment, OS sandboxing, or protection against a process
running as the same OS user. P05-B (retrieval policy) and P05-C (egress,
redaction, data inventory) are not implemented here.

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

### Applied call sites (P05-A)

| Site | Check |
| --- | --- |
| `DefaultArchiveStore.read()` `.md` paths | contained read |
| `DefaultArchiveStore.read()` UID→`rel_path` | contained read of the resolved path |
| `DefaultArchiveStore.person()` file open | contained read of index/slug path |
| `archive_vault.vault.read_note` | contained read |
| `archive_vault.vault.write_card` | contained write + no-follow replace |
| `archive_vault.vault.update_frontmatter_fields` | contained write |
| `archive_cli.commands.attachments.fetch_attachment` | `store.read` plus a second contained check on `rel_path` |

User-chosen attachment *download* destinations (Downloads) are not vault paths
and are unchanged. Restore-archive extraction is P07, using this helper.

## Tool profiles

`PPA_MCP_TOOL_PROFILE` is a closed enum. Valid values:

- `full` — all tools. This is the default when the variable is **unset**.
- `read-only` — retrieval tools only
- `remote-read` — minimal remote retrieval; no raw `archive_read`
- `admin-only` — maintenance tools (preserved; not silently removed)

Empty, whitespace-only, or unknown values **fail closed**: every tool returns
an actionable `Invalid PPA_MCP_TOOL_PROFILE=…` error listing the valid names.
They do not widen to unrestricted access. A typo is not `full`.

Explicit `full` remains explicit. Existing `read-only` / `remote-read` /
`admin-only` allow-lists are unchanged.

## Compatibility

- Deployments that omit the env var keep `full`.
- Deployments that set a misspelled profile, previously unrestricted, now deny
  every tool until the value is corrected. That is the intended security fix.
- `store.read` of an escaping path now looks like a miss (`found=false`)
  instead of opening the outside file.
- `write_card` / `update_frontmatter_fields` reject escaping `rel_path`
  instead of writing through `vault / rel_path`.
- `.md` reads may include a contained `rel_path` field when the path is valid.

## Later slices (not this contract's proof)

- P05-B: one `AccessContext` across search, graph, summaries, and reads.
  Mixed-source derived records (and future claims) deny if any required source
  is denied.
- P05-C: provider egress, diagnostic redaction, at-rest inventory.
