"""In-process buffer of captured items + (next slice) the background flusher.

The hot path appends buffered items here and returns immediately — no I/O, no
crypto. Items are either:

- ``BufferedEvent`` — a captured event's metadata dict (no ``payload_ref`` /
  ``event_hash`` yet) plus the still-cleartext ``raw_payload``, held in memory
  ONLY (never written to disk in this form).
- ``BufferedRunItem`` — a run-level wire item (``run_create`` / ``run_update`` /
  ``run_end``), which carries no payload.

The background flusher (next slice) drains in capture order, computes the hash
chain in ``local_seq`` order, classifies + redacts + encrypts event payloads,
assembles mixed batches, and ships them. Overflow policy: never drop individual
events (that tears a run's hash chain) — apply backpressure or drop whole runs.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from .run import Run


@dataclass
class BufferedEvent:
    """A captured event awaiting redaction/encryption in the flusher."""

    event: dict[str, Any]  # SDK-authored capture fields; no payload_ref/event_hash yet
    raw_payload: Any | None  # cleartext; memory-only until the flusher processes it
    run: "Run"

    @property
    def run_id(self) -> str:
        return self.run.run_id


@dataclass
class BufferedRunItem:
    """A run-level wire item (run_create / run_update / run_end)."""

    item: dict[str, Any]
    run: "Run"

    @property
    def run_id(self) -> str:
        return self.run.run_id


BufferedItem = Union[BufferedEvent, BufferedRunItem]


class EventBuffer:
    """Thread-safe FIFO of buffered items. Capture appends; the flusher drains."""

    def __init__(self, *, soft_cap: int = 10_000, flush_threshold: int = 100) -> None:
        self.soft_cap = soft_cap
        self.flush_threshold = flush_threshold
        self._items: list[BufferedItem] = []
        self._lock = threading.Lock()

    def append(self, item: BufferedItem) -> None:
        with self._lock:
            self._items.append(item)

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def snapshot(self) -> list[BufferedItem]:
        """A copy of the current items, in capture order (for tests/inspection)."""
        with self._lock:
            return list(self._items)

    def take_all(self) -> list[BufferedItem]:
        """Atomically remove and return all buffered items, in capture order."""
        with self._lock:
            items = self._items
            self._items = []
            return items

    def drop_run(self, run_id: str) -> int:
        """Remove all buffered items for one run (overflow whole-run drop).

        Returns the number of items removed. Used to drop a run *atomically* —
        dropping individual events would tear that run's hash chain.
        """
        with self._lock:
            kept = [i for i in self._items if i.run_id != run_id]
            removed = len(self._items) - len(kept)
            self._items = kept
            return removed
