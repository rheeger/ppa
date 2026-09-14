# Packaging verification

An installed PPA build should read and query its own fixture archive without depending on the source checkout or maintainer's accounts. These checks verify that boundary. The fixture setup command supports development testing; PPA has not shipped a supported onboarding flow.

## Verification scope

| Check | Requirement |
| --- | --- |
| Build | Produce the PPA and native Rust wheels from the same revision |
| Install | Load both wheels from a fresh environment outside the source checkout |
| Isolate | Use synthetic records, a separate archive root, and an owned warehouse schema |
| Query | Publish the fixture records, then verify exact read and search through the installed runtime |
| Report | Record the platform, interpreter, dependencies, results, and missing capabilities |

The existing packaging profile uses macOS arm64 and Python 3.12. The package declares Python 3.10 or newer; other installed-runtime profiles need their own verification.

The build helper is `archive_scripts/build-release.py`. Dependency locks are in `requirements/`. Setup behavior is defined in `archive_cli/commands/setup.py`, and the [acceptance runner](../PRODUCT_VERIFICATION.md) provides isolated integration checks.

A fixture install does not establish live source authentication, ongoing refresh, recovery, or readiness for general use. The [specification](../SPECIFICATION.md) is the current description of PPA's capabilities.
