---
"@runfile-ai/sdk": minor
---

The environment now defaults from the API key, so `RUNFILE_API_KEY` alone is enough. The key's marker only proves production or not, so the SDK seals that class — `production` for `rf_live_…` keys, `non-production` for `rf_test_…` keys — and the platform labels each run with the key's exact environment name (e.g. `sandbox`), rejecting a sealed value that contradicts the key. An explicit `environment=` or `RUNFILE_ENVIRONMENT` still wins. Requires `runfile-ai-schemas` 0.10 (environment becomes a free name), and the dependency is now pinned to that minor so the SDK and platform always hash events with the same projection.

**`openai-agents` is now capped at `<0.18`.** On newer releases (seen on 0.23.1) the adapter captures a human-approved tool call out of order (`tool_call` before `tool_approval_requested`), which misstates human oversight in the audit trail. The cap keeps installs on the tested range until the adapter is fixed — see [#16](https://github.com/runfile-ai/sdks/issues/16). The README now lists supported framework versions.
