# Development configuration examples

These examples show how an archive instance and an MCP client refer to the same stored history. They use disposable development settings; a supported onboarding flow is not yet available.

| File | Purpose |
| --- | --- |
| [ppa.mcp-example.json](ppa.mcp-example.json) | Local stdio client configuration |
| [ppa.example.json](ppa.example.json) | Instance configuration with an environment reference for credentials |
| [Connector template](connector-template/README.md) | Synthetic source and replay checks |

The examples use deterministic test embeddings, which do not provide meaning-based search. See the [runtime contract](../PPA_RUNTIME_CONTRACT.md) for configuration behavior.
