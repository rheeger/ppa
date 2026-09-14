# PPA documentation

PPA keeps records from the services you use in a local archive that you and your chosen agents can query together. These pages explain what that makes possible, which sources and operations the engine supports, and how the stored history stays independent of its original providers.

| Read | For |
| --- | --- |
| [Overview](../README.md) | The questions a personal archive can answer and the history it lets you keep |
| [Specification](SPECIFICATION.md) | Sources, records, queries, updates, and privacy |
| [Architecture](ARCHITECTURE.md) | The technologies and how they fit together |

## Technical references

These references define how imports, relationships, retrieval, and recovery work. Use them when investigating a result or extending PPA. Development and deployment procedures apply to the current engine; PPA has not yet shipped a supported onboarding flow.

<details>
<summary>Contracts and engineering notes</summary>

- [Card types](CARD_TYPE_CONTRACTS.md), [connector SDK](CONNECTOR_SDK.md), and [linker architecture](LINKER_ARCHITECTURE.md).
- [Retrieval](RETRIEVAL_CONTRACT.md), [evidence and analytics](EVIDENCE_QUERY_CONTRACT.md), and [indexing](INDEXING.md).
- [Runtime](PPA_RUNTIME_CONTRACT.md), [engine](ENGINE_CONTRACT.md), [publication](CHANGE_PUBLICATION_CONTRACT.md), and [processors](PROCESSOR_EXECUTION_CONTRACT.md).
- [Data boundaries](DATA_BOUNDARIES.md), [privacy](PRIVACY_CONTRACT.md), and [recovery](RECOVERY_CONTRACT.md).
- [Engineering playbook](PLAYBOOK.md), [verification](PRODUCT_VERIFICATION.md), and [operational notes](runbooks/README.md).
- [Historical plans](vision/README.md) and [run reports](reports/README.md).

</details>
