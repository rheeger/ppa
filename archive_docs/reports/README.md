# PPA validation reports

These reports record what a particular test or operational run established. Preserve their dates, scope, failures, and measurements when using them to support a claim. A fixture result applies to the tested archive and configuration; freshness and readiness need evidence from the instance being described.

| Area | Evidence |
| --- | --- |
| Retrieval and serving | [Retrieval fidelity](retrieval-fidelity-validation.md), [serving cutover](serving-index-cutover.md), [query bottleneck](query-bottleneck-phase1.md) |
| Publication and processing | [Publication](publication-validation.md), [processor execution](processor-execution-validation.md), [maintenance](maintain-living-loop.md) |
| Shared runtime and instances | [Engine convergence](engine-convergence-validation.md), [independent instances](independent-instance-validation.md) |
| Evidence clients | [Analytics client validation](p10-runtime-capability-delta.md) |
| Release and correctness | [Product hardening](product-hardening-release.md), [PR31 closeout](pr31-closeout.md) |
| Extraction quality | [Sample reports](extraction-quality/README.md) |

For a new result, describe the input, command, configuration, observed behavior, and limits. Report skipped checks and failures. Use synthetic or redacted records so another contributor can inspect the evidence without exposing a private archive.

The [specification](../SPECIFICATION.md) defines current capabilities. The [verification guide](../PRODUCT_VERIFICATION.md) explains how to run an isolated acceptance suite.
