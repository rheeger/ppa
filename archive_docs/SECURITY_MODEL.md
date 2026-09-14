# PPA security model

A personal archive keeps history outside the services that originally held it. Its owner also decides which clients may see that combined history. PPA provides checks at its file, retrieval, and provider interfaces; the operator supplies the host's storage protection and network controls.

## What PPA controls

| Boundary | Implemented behavior |
| --- | --- |
| Vault paths | Contained reads reject paths outside the vault; writes reject symlink escapes |
| Tool access | MCP profiles select available operations; invalid profiles deny access |
| Record access | `AccessContext` limits eligible records before ranking, counting, reading, and expanding relationships |
| Provider requests | Routed providers check destination and source policy; unknown destinations fail closed |
| Diagnostics | Redaction removes supported credential patterns and raw card payloads from PPA logs and errors |
| Restore | Recovery validates required state and restores into a separate root before activation |

The [privacy contract](PRIVACY_CONTRACT.md) defines the interface checks. The [data inventory](DATA_BOUNDARIES.md) identifies files, caches, and outbound paths.

## What the operator controls

Vault files, attachments, warehouse data, caches, and serving generations can contain private content. PPA does not encrypt those stores at rest. Configure host permissions, disk encryption, and backups according to the deployment, and protect copies of derived data as well as the original cards.

A process running as the same operating-system user can read what that user can read. PPA's retrieval filters do not sandbox arbitrary code with file access. An agent that should only query the archive should connect through the intended retrieval interface without independent access to the vault.

HTTP MCP adds a network boundary. Configure its authentication and transport on the archive host. The [MCP guide](MCP_SETUP.md) covers connection choices; local stdio does not require exposing a network endpoint.

## Local storage and outbound content

Importing records from a source, sending text for processing, and returning records to a client are separate actions. A cloud agent receives tool results. A remote embedding, enrichment, or OCR provider receives the text sent to that provider.

`PPA_EGRESS_MODE=local-only` blocks remote destinations on the routed provider interface. It is not a machine-wide network firewall, and it does not control what a connected client does with returned records. Some source ingest and operator tools have separate outbound paths. See [provider egress](DATA_BOUNDARIES.md#provider-egress).

PPA does not silently substitute another provider when an authorized provider fails. Contributors must preserve that behavior when adding a new processing path.

## Independent archives and sharing

Separate instances keep distinct roots, identities, warehouse schemas, serving generations, and checkpoints. Saved scopes are reusable filters within the instance's access policy. They cannot grant access to records that policy denies. Separate histories through instance and record-access configuration.

## Historical deployments

The earlier [Arnold security design](runbooks/historical-arnold-security.md) describes one deployment with an encrypted volume, passkey gate, and separate runtime identities. Those host controls are not built-in PPA guarantees or prerequisites for every installation.
