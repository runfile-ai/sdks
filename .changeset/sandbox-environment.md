---
"@runfile-ai/sdk": minor
---

The environment now defaults from the API key: `rf_live_…` keys report `production` and `rf_test_…` keys report `sandbox`, so `RUNFILE_API_KEY` alone is enough. An explicit `environment=` or `RUNFILE_ENVIRONMENT` still wins; the platform rejects data whose environment disagrees with its key. Requires `runfile-ai-schemas` 0.10 (adds `sandbox`), and the dependency is now pinned to that minor so the SDK and platform always hash events with the same projection.
