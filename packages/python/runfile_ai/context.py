"""Ambient context propagation via :mod:`contextvars`.

Customers never thread ``run_id`` / ``parent_event_id`` manually. These context
vars carry the current run, parent event, and parallel group across ``await``
boundaries, threadpool executors, and ``concurrent.futures`` — but NOT across
``multiprocessing`` (the customer serialises ``run_id`` explicitly there).

The setters return contextvars ``Token``s so callers (lifecycle calls / context
managers) can reset to the prior value, which is what keeps nested runs and
parallel groups well-scoped.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .run import Run

_current_run: ContextVar[Optional["Run"]] = ContextVar("_current_run", default=None)
_current_parent_event: ContextVar[Optional[str]] = ContextVar(
    "_current_parent_event", default=None
)
_current_parallel_group: ContextVar[Optional[str]] = ContextVar(
    "_current_parallel_group", default=None
)


def current_run() -> Optional["Run"]:
    """The current ambient run, or ``None`` outside a run."""
    return _current_run.get()


def current_parent_event() -> Optional[str]:
    return _current_parent_event.get()


def current_parallel_group() -> Optional[str]:
    return _current_parallel_group.get()


def set_current_run(run: Optional["Run"]) -> Token[Optional["Run"]]:
    return _current_run.set(run)


def reset_current_run(token: Token[Optional["Run"]]) -> None:
    _current_run.reset(token)


def set_parent_event(event_id: Optional[str]) -> Token[Optional[str]]:
    return _current_parent_event.set(event_id)


def reset_parent_event(token: Token[Optional[str]]) -> None:
    _current_parent_event.reset(token)


def set_parallel_group(group_id: Optional[str]) -> Token[Optional[str]]:
    return _current_parallel_group.set(group_id)


def reset_parallel_group(token: Token[Optional[str]]) -> None:
    _current_parallel_group.reset(token)
