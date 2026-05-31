"""SDK lifecycle and the HTTP client to the Ingest API.

``init()`` is called once at process start. It validates the API key shape,
constructs the HTTP client (bearer-auth, no AWS credentials), loads the
redaction policy, and starts the background flusher. The flusher — not the hot
path — does classification, redaction, encryption, batching, and shipping.
"""

from __future__ import annotations

import atexit
import os
import re
from pathlib import Path
from typing import Optional

import httpx

from ._constants import DEFAULT_REGION, default_base_url
from .buffer import EventBuffer
from .classifier import Redactor
from .datakey import DataKeyCache
from .policy import PolicyCache, RedactionPolicy
from .spool import DEFAULT_SPOOL_DIR, Spool
from .vault import VaultClient

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
        base_url: str | None = None,
        disabled: bool = False,
        start_flusher: bool = True,
        fetch_policy: bool = True,
        spool_dir: str | os.PathLike[str] | None = None,
        buffer_soft_cap: int = 10_000,
        capture_blocking: bool = True,
    ) -> None:
        if not disabled and not _API_KEY_RE.match(api_key):
            raise ValueError("invalid API key shape; expected rf_<live|test>_<32 base32 chars>")
        self.api_key = api_key
        self.environment = environment
        self.region = region
        # One regional host fronts every endpoint; derived from region unless the
        # caller overrides base_url (OpenAI/Anthropic-style).
        self.base_url = (base_url or default_base_url(region)).rstrip("/")
        self.disabled = disabled
        # Overflow policy: backpressure (synchronous flush) when True; drop whole
        # runs atomically (never mid-run events) when False.
        self.capture_blocking = capture_blocking

        # Shared sync HTTP client (the flusher's transport). The SDK holds no AWS
        # credentials — only the bearer API key.
        self._http = httpx.Client(timeout=10.0)
        self.buffer = EventBuffer(soft_cap=buffer_soft_cap)
        self.datakeys = DataKeyCache(
            client=self._http, base_url=self.base_url, api_key=self.api_key
        )
        self._policy_cache = PolicyCache(
            client=self._http, base_url=self.base_url, api_key=self.api_key
        )
        if fetch_policy and not disabled:
            self._policy_cache.refresh_if_stale()  # best-effort; never blocks capture

        resolved_spool = spool_dir or os.environ.get("RUNFILE_SPOOL_DIR") or DEFAULT_SPOOL_DIR
        self.spool = Spool(directory=Path(resolved_spool))

        # Vault tokenization — POST /v1/tokenize on the same regional host.
        # Drives the redactor's tokenize / tokenize_with_fallback treatments.
        self._vault = VaultClient(
            client=self._http, base_url=self.base_url, api_key=self.api_key
        )
        self.redactor = Redactor(tokenizer=self._vault.tokenize)

        # Background flusher (chain → encrypt → ship). Importing here avoids a
        # module-level import cycle (flusher type-checks against this class).
        from .flusher import Flusher

        self._flusher = Flusher(client=self)
        if start_flusher and not disabled:
            self._flusher.start()

        # Best-effort final drain on clean interpreter exit (atexit doesn't run on
        # SIGKILL/OOM — the on-disk spool is the real durability mechanism).
        if not disabled:
            atexit.register(self._atexit_drain)

    def _atexit_drain(self) -> None:
        try:
            self._flusher.flush_now()
        except Exception:
            pass

    @property
    def redaction_policy_version(self) -> str:
        """Policy version stamped on runs/events (default until a policy is fetched)."""
        return self._policy_cache.version()

    def current_policy(self) -> "RedactionPolicy | None":
        """The current redaction policy (drives the flusher's redactor), if fetched."""
        return self._policy_cache.current()

    def refresh_policy_if_stale(self) -> None:
        """Best-effort policy refresh, called periodically by the flusher thread."""
        self._policy_cache.refresh_if_stale()

    def flush(self) -> None:
        """Force a synchronous buffer drain. Blocks until the in-flight batches ship."""
        self._flusher.flush_now()

    def shutdown(self) -> None:
        """Graceful shutdown: stop + final-drain the flusher, then release resources."""
        atexit.unregister(self._atexit_drain)
        self._flusher.stop()
        self.datakeys.zeroize()
        self._http.close()


def init(
    api_key: str,
    *,
    environment: str = "production",
    region: str = DEFAULT_REGION,
    base_url: str | None = None,
    disabled: bool = False,
    start_flusher: bool = True,
    fetch_policy: bool = True,
    spool_dir: str | os.PathLike[str] | None = None,
    buffer_soft_cap: int = 10_000,
    capture_blocking: bool = True,
) -> RunfileClient:
    """Initialise the SDK once at process start. Idempotent (returns the existing instance).

    ``base_url`` defaults to ``https://api.<region>.runfile.ai``; pass it only to
    target a non-standard host.
    """
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
        spool_dir=spool_dir,
        buffer_soft_cap=buffer_soft_cap,
        capture_blocking=capture_blocking,
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
