"""SDK lifecycle and the HTTP client to the Ingest API.

``init()`` is called once at process start. It validates the API key shape,
constructs the HTTP client (bearer-auth, no AWS credentials), loads the
redaction policy, and starts the background flusher. The flusher — not the hot
path — does classification, redaction, encryption, batching, and shipping.

Skeleton: signatures and wiring are in place; the bodies are TODO.
"""

from __future__ import annotations

import re
from typing import Optional

import httpx

from ._constants import DEFAULT_BASE_URL, DEFAULT_REGION
from .buffer import EventBuffer
from .datakey import DataKeyCache

_API_KEY_RE = re.compile(r"^rf_(live|test)_[a-z0-9]{32}$")

# Default redaction policy version until GET /v1/policies/current is wired.
_DEFAULT_POLICY_VERSION = "0.0.0"

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
    ) -> None:
        if not disabled and not _API_KEY_RE.match(api_key):
            raise ValueError("invalid API key shape; expected rf_<live|test>_<32 base32 chars>")
        self.api_key = api_key
        self.environment = environment
        self.region = region
        self.base_url = base_url.rstrip("/")
        self.disabled = disabled

        # Redaction policy version stamped on runs/events. Populated by the
        # policy fetch (later slice); a safe default until then.
        self.redaction_policy_version = _DEFAULT_POLICY_VERSION

        # Shared sync HTTP client (the flusher's transport). The SDK holds no AWS
        # credentials — only the bearer API key.
        self._http = httpx.Client(timeout=10.0)
        self.buffer = EventBuffer()
        self.datakeys = DataKeyCache(
            client=self._http, base_url=self.base_url, api_key=self.api_key
        )

        # Background flusher (chain → encrypt → ship). Importing here avoids a
        # module-level import cycle (flusher type-checks against this class).
        from .flusher import Flusher

        self._flusher = Flusher(client=self)
        if start_flusher and not disabled:
            self._flusher.start()
        # TODO (next slice): Spool (ciphertext-only disk durability), PolicyCache
        # (GET /v1/policies/current), and an atexit best-effort final drain.

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
