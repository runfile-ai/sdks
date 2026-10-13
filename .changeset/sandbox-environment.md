---
"@runfile-ai/sdk": minor
---

The environment now defaults from the API key, so `RUNFILE_API_KEY` alone is enough. The key's marker only proves production or not, so the SDK seals that class — `production` for `rf_live_…` keys, `non-production` for `rf_test_…` keys — and the platform labels each run with the key's exact environment name (e.g. `sandbox`), rejecting a sealed value that contradicts the key. An explicit `environment=` or `RUNFILE_ENVIRONMENT` still wins. Requires `runfile-ai-schemas` 0.10 (environment becomes a free name), and the dependency is now pinned to that minor so the SDK and platform always hash events with the same projection.
