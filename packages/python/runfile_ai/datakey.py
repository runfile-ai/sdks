"""Per-``(tenant, agent)`` data-key cache.

The SDK holds no AWS credentials. It mints a data key from ``POST /v1/data-keys``
(authenticated with the ``rf_*`` API key), caches the ``{key_id, plaintext,
wrapped}`` triple in process memory keyed by ``(tenant, agent)`` with a 24h TTL,
encrypts payloads locally under ``plaintext`` (see :mod:`runfile_ai.encrypt`),
and attaches ``key_id`` + ``wrapped`` to each event. ``plaintext`` is never
written to disk and is best-effort zeroized on TTL expiry / shutdown.

No external cache: each instance mints its own key; the per-event ``key_id``
disambiguates at read time. Serverless (fresh process per invocation) mints
roughly per invocation — accepted.
"""

from __future__ import annotations

import base64
import threading
import time
from dataclasses import dataclass, field

import httpx

#: Default lifetime of a cached data key (24h). Hygiene, not a blast-radius control.
TTL_SECONDS = 24 * 60 * 60


class DataKeyError(RuntimeError):
    """Raised when the Data-Key endpoint cannot mint a key.

    ``retryable`` is True for transient conditions (KMS throttle/unavailable, 5xx)
    where the caller should back off and keep payloads in memory — never spilling
    plaintext to disk.
    """

    def __init__(self, message: str, *, retryable: bool, status: int | None = None) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.status = status


@dataclass
class DataKey:
    key_id: str
    plaintext: bytearray  # 32-byte AES-256 key; memory only, never persisted; zeroizable
    wrapped: str  # base64 KMS-wrapped form; travels with each event
    algorithm: str = "aes-256-gcm"


@dataclass
class _Entry:
    key: DataKey
    expires_monotonic: float


@dataclass
class DataKeyCache:
    """Lazily fetches and caches one data key per ``(tenant, agent)`` with a TTL.

    Thread-safe: the background flusher (a single thread today) is the only
    caller, but a lock guards against future multi-threaded flushing.
    """

    client: httpx.Client
    base_url: str
    api_key: str
    ttl_seconds: int = TTL_SECONDS
    _entries: dict[tuple[str, str], _Entry] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def get_or_fetch(self, tenant_id: str, agent_identity: str) -> DataKey:
        """Return a cached key for ``(tenant, agent)`` or mint a fresh one.

        Expired entries are zeroized and replaced.
        """
        cache_key = (tenant_id, agent_identity)
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(cache_key)
            if entry is not None and entry.expires_monotonic > now:
                return entry.key
            if entry is not None:
                _zeroize(entry.key.plaintext)
                del self._entries[cache_key]

        # Mint outside the lock (network I/O); store under the lock.
        key = self._mint(agent_identity)
        with self._lock:
            self._entries[cache_key] = _Entry(
                key=key, expires_monotonic=time.monotonic() + self.ttl_seconds
            )
        return key

    def _mint(self, agent_identity: str) -> DataKey:
        try:
            resp = self.client.post(
                f"{self.base_url}/v1/data-keys",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={"agent_identity": agent_identity},
            )
        except httpx.HTTPError as exc:  # network/timeout
            raise DataKeyError(f"data-key request failed: {exc}", retryable=True) from exc

        if resp.status_code == 200:
            body = resp.json()
            return DataKey(
                key_id=body["key_id"],
                plaintext=bytearray(base64.b64decode(body["plaintext"])),
                wrapped=body["wrapped"],
                algorithm=body.get("algorithm", "aes-256-gcm"),
            )
        # 429/503 are transient (KMS throttle / unavailable); 401/403 are terminal.
        retryable = resp.status_code in (429, 500, 502, 503, 504)
        raise DataKeyError(
            f"data-key mint returned {resp.status_code}",
            retryable=retryable,
            status=resp.status_code,
        )

    def zeroize(self) -> None:
        """Best-effort overwrite of cached plaintext key bytes; drop all entries."""
        with self._lock:
            for entry in self._entries.values():
                _zeroize(entry.key.plaintext)
            self._entries.clear()


def _zeroize(buf: bytearray) -> None:
    for i in range(len(buf)):
        buf[i] = 0
