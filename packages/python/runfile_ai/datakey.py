"""Per-``(tenant, agent)`` data-key cache.

The SDK holds no AWS credentials. It mints a data key from ``POST /v1/data-keys``
(authenticated with the ``rf_*`` API key), caches the ``{key_id, plaintext,
wrapped}`` triple in process memory keyed by ``(tenant, agent)`` with a 24h TTL,
encrypts payloads locally under ``plaintext`` (see :mod:`runfile_ai.encrypt`),
and attaches ``key_id`` + ``wrapped`` to each event. ``plaintext`` is never
written to disk and is zeroized on TTL expiry / shutdown.

No external cache: each instance mints its own key; the per-event ``key_id``
disambiguates at read time. Serverless (fresh process per invocation) mints
roughly per invocation — accepted.

Skeleton: signatures in place; bodies TODO.
"""

from __future__ import annotations

from dataclasses import dataclass

TTL_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class DataKey:
    key_id: str
    plaintext: bytes  # 32-byte AES-256 key; memory only, never persisted
    wrapped: str      # base64 KMS-wrapped form; travels with each event
    algorithm: str = "aes-256-gcm"


class DataKeyCache:
    """Lazily fetches and caches one data key per ``(tenant, agent)`` with a TTL."""

    def __init__(self, *, ttl_seconds: int = TTL_SECONDS) -> None:
        self._ttl = ttl_seconds
        # TODO: dict[(tenant, agent)] -> (DataKey, expires_at); httpx client ref.

    async def get_or_fetch(self, tenant_id: str, agent_identity: str) -> DataKey:
        raise NotImplementedError

    def zeroize(self) -> None:
        """Overwrite cached plaintext key bytes and drop entries (shutdown / expiry)."""
        raise NotImplementedError
