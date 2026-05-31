"""Redaction-policy fetch + cache.

Fetched from ``GET /v1/policies/current`` on startup and refreshed every TTL
(default 300s) in the background. Configures the classifier/redactor. On fetch
failure the SDK keeps using the cached policy and emits an ``sdk_diagnostic``.
Each captured item records the ``redaction_policy_version`` active at capture.

Skeleton: signatures in place; bodies TODO.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

DEFAULT_TTL_SECONDS = 300


@dataclass
class RedactionPolicy:
    policy_version: str
    classification_rules: list[dict[str, Any]]
    ttl_seconds: int = DEFAULT_TTL_SECONDS


class PolicyCache:
    def __init__(self, *, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> None:
        self._ttl = ttl_seconds

    async def get(self) -> RedactionPolicy:
        raise NotImplementedError

    async def refresh(self) -> RedactionPolicy:
        raise NotImplementedError
