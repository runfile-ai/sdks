"""SDK lifecycle and the HTTP client to the Ingest API.

``init()`` is called once at process start. It validates the API key shape,
constructs the HTTP client (bearer-auth, no AWS credentials), loads the
redaction policy, and starts the background flusher. The flusher — not the hot
path — does classification, redaction, encryption, batching, and shipping.
"""

from __future__ import annotations

import re
from typing import Optional

import httpx

from ._constants import DEFAULT_BASE_URL, DEFAULT_REGION
from .buffer import EventBuffer
from .datakey import DataKeyCache
from .policy import PolicyCache

_API_KEY_RE = re.compile(r"^rf_(live|test)_[a-z0-9]{32}$")

_instance: Optional["RunfileClient"] = None


class RunfileClient:
    """Process-wide SDK instance: buffer, flusher, policy, data-key cache, HTTP."""

    def __init__(
        self,
        api_key: str,
        *,
        environment: str = "production",
        region: str = DEFAULT_REGION,
        base_url: str = DEFAULT_BASE_URL,
        disabled: bool = False,
        start_flusher: bool = True,
        fetch_policy: bool = True,
    ) -> None:
        if not disabled and not _API_KEY_RE.match(api_key):
            raise ValueError("invalid API key shape; expected rf_<live|test>_<32 base32 chars>")
        self.api_key = api_key
        self.environment = environment
        self.region = region
        self.base_url = base_url.rstrip("/")
        self.disabled = disabled

        # Shared sync HTTP client (the flusher's transport). The SDK holds no AWS
        # credentials — only the bearer API key.
        self._http = httpx.Client(timeout=10.0)
        self.buffer = EventBuffer()
        self.datakeys = DataKeyCache(
            client=self._http, base_url=self.base_url, api_key=self.api_key
        )
        self._policy_cache = PolicyCache(
            client=self._http, base_url=self.base_url, api_key=self.api_key
        )
        if fetch_policy and not disabled:
            self._policy_cache.refresh_if_stale()  # best-effort; never blocks capture

        # Background flusher (chain → encrypt → ship). Importing here avoids a
        # module-level import cycle (flusher type-checks against this class).
        from .flusher import Flusher

        self._flusher = Flusher(client=self)
        if start_flusher and not disabled:
            self._flusher.start()
        # TODO (next slice): Spool (ciphertext-only disk durability) + atexit drain.

    @property
    def redaction_policy_version(self) -> str:
        """Policy version stamped on runs/events (default until a policy is fetched)."""
        return self._policy_cache.version()

    def refresh_policy_if_stale(self) -> None:
        """Best-effort policy refresh, called periodically by the flusher thread."""
        self._policy_cache.refresh_if_stale()

    def flush(self) -> None:
        """Force a synchronous buffer drain. Blocks until the in-flight batches ship."""
        self._flusher.flush_now()

    def shutdown(self) -> None:
        """Graceful shutdown: stop + final-drain the flusher, then release resources."""
        self._flusher.stop()
        self.datakeys.zeroize()
        self._http.close()


def init(
    api_key: str,
    *,
    environment: str = "production",
    region: str = DEFAULT_REGION,
    base_url: str = DEFAULT_BASE_URL,
    disabled: bool = False,
    start_flusher: bool = True,
    fetch_policy: bool = True,
) -> RunfileClient:
    """Initialise the SDK once at process start. Idempotent (returns the existing instance)."""
    global _instance
    if _instance is not None:
        return _instance
    _instance = RunfileClient(
        api_key,
        environment=environment,
        region=region,
        base_url=base_url,
        disabled=disabled,
        start_flusher=start_flusher,
        fetch_policy=fetch_policy,
    )
    return _instance


def get_instance() -> Optional[RunfileClient]:
    """The active SDK instance, or ``None`` if ``init()`` hasn't been called."""
    return _instance


def flush() -> None:
    """Force a synchronous buffer drain on the active instance."""
    if _instance is not None:
        _instance.flush()


def shutdown() -> None:
    """Graceful shutdown of the active instance."""
    global _instance
    if _instance is not None:
        _instance.shutdown()
        _instance = None
