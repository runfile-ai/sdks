"""In-process ring buffer + background flusher.

Hot path appends buffered events (metadata + still-cleartext payload, in memory
only). The flusher drains on size (default 100 items), interval (default 2s),
explicit ``flush()``, or process exit; it classifies, redacts, encrypts,
batches, and ships. On sustained failure it spools ciphertext to disk
(:mod:`runfile_ai.spool`).

Overflow: never drop individual events (that tears a run's hash chain). Apply
backpressure; if disabled, drop WHOLE runs atomically and emit a loud
``sdk_diagnostic`` (``code=run_dropped_overflow``).

Skeleton: signatures in place; bodies TODO.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from .run import Run


@dataclass
class BufferedEvent:
    event: Any            # the in-progress event metadata
    raw_payload: Optional[Any]  # cleartext; memory-only until the flusher processes it
    run: Run


class EventBuffer:
    def __init__(self, *, soft_cap: int = 10_000, flush_threshold: int = 100) -> None:
        self._soft_cap = soft_cap
        self._flush_threshold = flush_threshold
        # TODO: deque, flusher task handle, backpressure signal.

    def append(self, item: BufferedEvent) -> None:
        raise NotImplementedError

    async def drain(self) -> None:
        """Process and ship buffered items as one or more mixed batches."""
        raise NotImplementedError
