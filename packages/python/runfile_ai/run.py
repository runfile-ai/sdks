"""The in-memory Run model, lifecycle calls, and event capture.

A ``Run`` tracks per-run bookkeeping the SDK owns: ``run_id``, ``agent_identity``,
``lifecycle_state``, the ``parent_event_id`` for the next event, the open
parallel-group stack, ``local_seq`` (monotonic per segment, reset on resume),
``segment_index``, and ``last_event_hash_local`` (the SDK's local computation
used as ``prev_event_hash_intent``).

The hot path (``capture_event`` / ``_construct_event``) only builds metadata and
appends to the buffer with the still-cleartext payload. Classification,
redaction, and encryption happen later in the background flusher.

Skeleton: signatures and the contextmanager shape are in place; bodies TODO.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional


@dataclass
class Run:
    run_id: str
    agent_identity: str
    lifecycle_state: str = "active"
    conversation_id: Optional[str] = None
    segment_index: int = 0
    local_seq: int = 0
    parent_event_id: Optional[str] = None
    last_event_hash_local: Optional[str] = None
    _parallel_groups: list[str] = field(default_factory=list)

    def next_local_seq(self) -> int:
        seq = self.local_seq
        self.local_seq += 1
        return seq

    def begin_segment(self) -> None:
        """On resume: open a new segment — bump ``segment_index``, reset ``local_seq``."""
        self.segment_index += 1
        self.local_seq = 0


@contextmanager
def run(
    *,
    agent_identity: str,
    conversation_id: Optional[str] = None,
    continued_from: Optional[dict[str, str]] = None,
) -> Iterator[Run]:
    """Create a run, set ambient context, and end it on exit.

    The run boundary is dynamic (per invocation), not lexical — hence a context
    manager rather than a decorator.
    """
    r = start_run(
        agent_identity=agent_identity,
        conversation_id=conversation_id,
        continued_from=continued_from,
    )
    try:
        yield r
        end_run(outcome="success")
    except Exception:
        end_run(outcome="failure")
        raise


def start_run(
    *,
    agent_identity: str,
    conversation_id: Optional[str] = None,
    continued_from: Optional[dict[str, str]] = None,
) -> Run:
    """Begin a run: emit a ``run_create`` item and set ambient context."""
    raise NotImplementedError


def end_run(*, outcome: str = "success") -> None:
    """End the current run: emit a ``run_end`` item and clear ambient context."""
    raise NotImplementedError


def suspend_run(
    *,
    reason: str,
    expected_resumer: Optional[str] = None,
    expected_resume_by: Optional[str] = None,
) -> None:
    """Emit a ``run_suspend`` event + a ``run_update`` flipping to ``awaiting_*``."""
    raise NotImplementedError


def resume_run(
    *,
    run_id: str,
    triggered_by: str,
    resumer_principal: Optional[str] = None,
) -> None:
    """Emit a ``run_resume`` event (new segment) + a ``run_update`` back to ``active``."""
    raise NotImplementedError


def abandon_run(*, run_id: str, reason: Optional[str] = None) -> None:
    """Abandon a suspended run: ``run_abandon`` event, run ends with ``outcome=abandoned``."""
    raise NotImplementedError


def capture_event(*, kind: str, name: str, payload: Any = None, **fields: Any) -> None:
    """Manually capture one event within the current run (hot-path, metadata only)."""
    raise NotImplementedError
