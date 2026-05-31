"""/v1/health schema-version negotiation.

On startup the SDK calls the unauthenticated ``GET /v1/health`` and compares its
schema version against the server's ``schema_versions_supported``. If the SDK's
version isn't supported (e.g. the server is a minor version behind during a
canary rollout), it emits a warning diagnostic but keeps running and submits
anyway — visible failures beat silently-broken capture. Best-effort: if health
is unreachable, negotiation is simply skipped.
"""

from __future__ import annotations

import httpx


def fetch_supported_schema_versions(client: httpx.Client, base_url: str) -> list[str] | None:
    """GET /v1/health and return ``schema_versions_supported``, or None if unavailable."""
    try:
        resp = client.get(f"{base_url}/v1/health")
    except httpx.HTTPError:
        return None
    if resp.status_code != 200:
        return None
    try:
        body = resp.json()
    except ValueError:
        return None
    versions = body.get("schema_versions_supported")
    if isinstance(versions, list) and all(isinstance(v, str) for v in versions):
        return versions
    return None
