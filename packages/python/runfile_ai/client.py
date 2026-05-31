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
from typing import Any, Callable, Optional

import httpx

from ._clock import utc_now_iso
from ._constants import DEFAULT_REGION, SCHEMA_VERSION, default_base_url
from .buffer import EventBuffer
from .classifier import Redactor
from .datakey import DataKeyCache
from .health import fetch_supported_schema_versions
from .policy import PolicyCache, RedactionPolicy
from .spool import DEFAULT_SPOOL_DIR, Spool
from .vault import VaultClient

_API_KEY_RE = re.compile(r"^rf_(live|test)_[a-z0-9]{32}$")

# Keep a bounded tail of recent diagnostics for introspection.
_MAX_DIAGNOSTICS = 256

#: A diagnostic record: {code, severity, detail, at}.
Diagnostic = dict[str, Any]

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
        on_diagnostic: Callable[[Diagnostic], None] | None = None,
        check_health: bool | None = None,
    ) -> None:
        if not disabled and not _API_KEY_RE.match(api_key):
            raise ValueError("invalid API key shape; expected rf_<live|test>_<32 base32 chars>")
        self.api_key = api_key
        self.environment = environment
        self.region = region
        # Diagnostics surface. Default is silent (no stdout/stderr) per spec; the
        # customer opts in via on_diagnostic to wire it to their logger/SIEM.
        self._on_diagnostic = on_diagnostic
        self.diagnostics: list[Diagnostic] = []
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
            self.refresh_policy_if_stale()  # best-effort; emits a diagnostic on failure

        # Schema-version negotiation via GET /v1/health. Defaults to fetch_policy
        # so "no startup network" (the offline/test posture) covers both probes.
        self.server_schema_versions: list[str] | None = None
        do_health = fetch_policy if check_health is None else check_health
        if do_health and not disabled:
            self._negotiate_schema_version()

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

    def _negotiate_schema_version(self) -> None:
        """Warn (but don't block) if the server doesn't support our schema version."""
        supported = fetch_supported_schema_versions(self._http, self.base_url)
        if supported is None:
            return  # best-effort: /v1/health unreachable
        self.server_schema_versions = supported
        if SCHEMA_VERSION not in supported:
            self.emit_diagnostic(
                "schema_version_unsupported",
                detail=f"server supports {supported}; SDK emits schema {SCHEMA_VERSION} "
                "— batches will be rejected until versions align",
            )

    def refresh_policy_if_stale(self) -> None:
        """Best-effort policy refresh; emits a diagnostic on failure (never raises).

        Called on init and periodically by the flusher thread. Capture never
        blocks on policy — on failure the last-known policy keeps being used.
        """
        if not self._policy_cache.is_stale():
            return
        try:
            self._policy_cache.fetch()
        except Exception as exc:
            self.emit_diagnostic("policy_refresh_failed", detail=str(exc))

    def emit_diagnostic(
        self, code: str, *, detail: str | None = None, severity: str = "warning"
    ) -> None:
        """Record an SDK health diagnostic and surface it via on_diagnostic.

        Used for failures that aren't run-scoped wire events (policy refresh, auth)
        — kept observable without ever writing to stdout/stderr by default.
        """
        record: Diagnostic = {
            "code": code,
            "severity": severity,
            "detail": detail,
            "at": utc_now_iso(),
        }
        self.diagnostics.append(record)
        if len(self.diagnostics) > _MAX_DIAGNOSTICS:
            del self.diagnostics[0]
        if self._on_diagnostic is not None:
            try:
                self._on_diagnostic(record)
            except Exception:
                pass  # a customer callback must never break capture

    def notify_flusher(self) -> None:
        """Nudge the background flusher to drain promptly (buffer size trigger)."""
        self._flusher.notify()

    def flush(self) -> None:
        """Force a synchronous buffer drain. Blocks until the in-flight batches ship."""
        self._flusher.flush_now()

    def shutdown(self) -> None:
        """Graceful shutdown: stop + final-drain the flusher, then release resources."""
        atexit.unregister(self._atexit_drain)
        self._flusher.stop()
        self.datakeys.zeroize()
        self._http.close()


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def init(
    api_key: str | None = None,
    *,
    environment: str | None = None,
    region: str | None = None,
    base_url: str | None = None,
    disabled: bool | None = None,
    start_flusher: bool = True,
    fetch_policy: bool = True,
    spool_dir: str | os.PathLike[str] | None = None,
    buffer_soft_cap: int = 10_000,
    capture_blocking: bool = True,
    on_diagnostic: Callable[[Diagnostic], None] | None = None,
    check_health: bool | None = None,
) -> RunfileClient:
    """Initialise the SDK once at process start. Idempotent (returns the existing instance).

    Configuration precedence is **explicit arg > environment variable > default**:

    - ``api_key``        ← ``RUNFILE_API_KEY``
    - ``region``         ← ``RUNFILE_REGION``      (default ``eu-west-2``)
    - ``environment``    ← ``RUNFILE_ENVIRONMENT`` (default ``production``)
    - ``disabled``       ← ``RUNFILE_DISABLED``    (``1``/``true``/``yes``/``on``)
    - ``spool_dir``      ← ``RUNFILE_SPOOL_DIR``

    ``base_url`` defaults to ``https://api.<region>.runfile.ai``; pass it only to
    target a non-standard host. ``on_diagnostic`` receives SDK health records
    (policy-refresh failure, auth failure, overflow drops); default is silent.
    When disabled the SDK becomes a no-op (capture is silently dropped).
    """
    global _instance
    if _instance is not None:
        return _instance

    resolved_disabled = disabled if disabled is not None else _env_truthy("RUNFILE_DISABLED")
    resolved_api_key = api_key or os.environ.get("RUNFILE_API_KEY")
    if not resolved_api_key and not resolved_disabled:
        raise ValueError(
            "api_key is required: pass api_key=... or set RUNFILE_API_KEY "
            "(or set RUNFILE_DISABLED=1 to run the SDK as a no-op)"
        )
    resolved_region = region or os.environ.get("RUNFILE_REGION") or DEFAULT_REGION
    resolved_environment = environment or os.environ.get("RUNFILE_ENVIRONMENT") or "production"

    _instance = RunfileClient(
        resolved_api_key or "",
        environment=resolved_environment,
        region=resolved_region,
        base_url=base_url,
        disabled=resolved_disabled,
        start_flusher=start_flusher,
        fetch_policy=fetch_policy,
        spool_dir=spool_dir,
        buffer_soft_cap=buffer_soft_cap,
        capture_blocking=capture_blocking,
        on_diagnostic=on_diagnostic,
        check_health=check_health,
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
