"""Redaction-policy fetch + cache.

Fetched from ``GET /v1/policies/current`` (best-effort on init, then refreshed on
a TTL by the flusher thread). The policy version is stamped on every run/event at
capture time, and the classification rules drive the redactor (real detection
lands with the classifier slice). On fetch failure the SDK keeps using the last
known policy (or the safe default version) — capture never blocks on the policy.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

DEFAULT_TTL_SECONDS = 300
#: Used until a real policy is fetched (valid X.Y.Z so events still validate).
DEFAULT_POLICY_VERSION = "0.0.0"


@dataclass
class RedactionPolicy:
    policy_version: str
    classification_rules: list[dict[str, Any]]
    ttl_seconds: int = DEFAULT_TTL_SECONDS


@dataclass
class PolicyCache:
    """Holds the current redaction policy with a TTL; refresh is best-effort."""

    client: httpx.Client
    base_url: str
    api_key: str
    _policy: RedactionPolicy | None = None
    _expires_monotonic: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def current(self) -> RedactionPolicy | None:
        with self._lock:
            return self._policy

    def version(self) -> str:
        policy = self.current()
        return policy.policy_version if policy is not None else DEFAULT_POLICY_VERSION

    def is_stale(self) -> bool:
        with self._lock:
            return self._policy is None or time.monotonic() >= self._expires_monotonic

    def fetch(self) -> RedactionPolicy:
        """Fetch and cache the current policy. Raises on transport/HTTP error."""
        resp = self.client.get(
            f"{self.base_url}/v1/policies/current",
            headers={"Authorization": f"Bearer {self.api_key}"},
        )
        resp.raise_for_status()
        body = resp.json()
        policy = RedactionPolicy(
            policy_version=body["policy_version"],
            classification_rules=body.get("classification_rules", []),
            ttl_seconds=int(body.get("ttl_seconds", DEFAULT_TTL_SECONDS)),
        )
        with self._lock:
            self._policy = policy
            self._expires_monotonic = time.monotonic() + policy.ttl_seconds
        return policy
