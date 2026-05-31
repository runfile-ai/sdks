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

from ._constants import DEFAULT_BASE_URL, DEFAULT_REGION

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
    ) -> None:
        if not disabled and not _API_KEY_RE.match(api_key):
            raise ValueError("invalid API key shape; expected rf_<live|test>_<32 base32 chars>")
        self.api_key = api_key
        self.environment = environment
        self.region = region
        self.base_url = base_url
        self.disabled = disabled
        # TODO: construct httpx.AsyncClient, EventBuffer, Spool, DataKeyCache,
        # PolicyCache; start the background flusher; register atexit drain.

    async def flush(self) -> None:
        """Force a synchronous buffer drain. Blocks until in-flight batches complete."""
        raise NotImplementedError

    async def shutdown(self) -> None:
        """Graceful shutdown: final drain, then release resources."""
        raise NotImplementedError


def init(
    api_key: str,
    *,
    environment: str = "production",
    region: str = DEFAULT_REGION,
    base_url: str = DEFAULT_BASE_URL,
    disabled: bool = False,
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
    )
    return _instance


def get_instance() -> Optional[RunfileClient]:
    """The active SDK instance, or ``None`` if ``init()`` hasn't been called."""
    return _instance


async def flush() -> None:
    if _instance is not None:
        await _instance.flush()


async def shutdown() -> None:
    global _instance
    if _instance is not None:
        await _instance.shutdown()
        _instance = None
