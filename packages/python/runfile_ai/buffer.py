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

**Turn-atomic flushing (opt-in).** Inferred parallel-grouping is decided in the
flusher and committed inside ``event_hash``, so all events of one model turn must
land in the *same* drain or a turn split across the 2s cadence ships ungrouped.
A turn-defining adapter (e.g. Claude) calls :meth:`EventBuffer.arm_turn_atomic`
and, when a turn closes, :meth:`EventBuffer.advance_flush_watermark`. While armed,
the buffer *holds* the tail of the current open turn (``_held_suffix`` trailing
items) so :meth:`take_flushable` only releases whole, closed turns. The manual /
OTel paths never arm, so they flush everything immediately — unchanged. Durability
always wins: :meth:`take_all` (overflow back-pressure, shutdown, the flusher's
safety valve) ignores the watermark.
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
        # Turn-atomic flushing (see module docstring). When armed, every appended
        # event joins the current open turn's held tail; _held_suffix counts the
        # trailing held items. Everything before them is flushable. Unarmed, nothing
        # is ever held (the manual / OTel path) and take_flushable == take_all.
        self._armed = False
        self._held_suffix = 0

    @property
    def armed(self) -> bool:
        with self._lock:
            return self._armed

    def arm_turn_atomic(self) -> None:
        """Engage turn-atomic flushing (idempotent).

        Called by adapters that define model turns. Already-buffered items stay
        immediately flushable — only events appended *after* arming are held, until
        the next :meth:`advance_flush_watermark`.
        """
        with self._lock:
            self._armed = True

    def advance_flush_watermark(self) -> None:
        """Mark everything currently buffered as flushable: the open turn closed.

        The turn-defining adapter calls this when a turn completes (the next turn's
        ``llm_call`` is about to be authored, or the run is ending), releasing the
        just-closed turn's events while the next turn becomes the new held tail.
        """
        with self._lock:
            self._held_suffix = 0

    def append(self, item: BufferedItem) -> None:
        with self._lock:
            self._items.append(item)
            if self._armed:
                self._held_suffix += 1

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def snapshot(self) -> list[BufferedItem]:
        """A copy of the current items, in capture order (for tests/inspection)."""
        with self._lock:
            return list(self._items)

    def take_flushable(self) -> list[BufferedItem]:
        """Remove and return the flushable prefix (whole, closed turns), in order.

        Holds back the trailing ``_held_suffix`` items — the current open turn — so
        the flusher's inferred parallel-grouping always sees complete turns. When
        unarmed (or no turn is open) ``_held_suffix`` is 0, so this returns
        everything, exactly like :meth:`take_all`.
        """
        with self._lock:
            n = len(self._items) - self._held_suffix
            if n <= 0:
                return []
            taken = self._items[:n]
            self._items = self._items[n:]
            # The held suffix (now the whole of _items) is unchanged.
            return taken

    def take_all(self) -> list[BufferedItem]:
        """Atomically remove and return ALL buffered items, ignoring the watermark.

        Durability path: overflow back-pressure, shutdown, and the flusher's
        safety valve drain everything regardless of any open turn.
        """
        with self._lock:
            items = self._items
            self._items = []
            self._held_suffix = 0
            return items

    def requeue_front(self, items: list[BufferedItem]) -> None:
        """Put items back at the FRONT, preserving capture order.

        Used when a drain can't process items (e.g. the data-key endpoint is
        unreachable, so payloads can't be encrypted): they wait in memory for the
        next drain rather than being dropped or spooled as plaintext.
        """
        with self._lock:
            # Requeued items were flushable (taken from the front), so they sit
            # BEFORE the held suffix; _held_suffix (a trailing count) is unchanged.
            self._items = list(items) + self._items

    def drop_run(self, run_id: str) -> int:
        """Remove all buffered items for one run (overflow whole-run drop).

        Returns the number of items removed. Used to drop a run *atomically* —
        dropping individual events would tear that run's hash chain.
        """
        with self._lock:
            held_start = len(self._items) - self._held_suffix
            kept: list[BufferedItem] = []
            held_removed = 0
            for idx, item in enumerate(self._items):
                if item.run_id == run_id:
                    if idx >= held_start:
                        held_removed += 1
                else:
                    kept.append(item)
            removed = len(self._items) - len(kept)
            self._items = kept
            self._held_suffix -= held_removed
            return removed
