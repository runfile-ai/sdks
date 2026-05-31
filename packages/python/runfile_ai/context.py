"""Ambient context propagation via :mod:`contextvars`.

Customers never thread ``run_id`` / ``parent_event_id`` manually. These context
vars carry the current run, parent event, and parallel group across ``await``
boundaries, threadpool executors, and ``concurrent.futures`` — but NOT across
``multiprocessing`` (the customer serialises ``run_id`` explicitly there).
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Iterator, Optional

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


@contextmanager
def parallel_group() -> Iterator[str]:
    """Open a ``parallel_group_id`` around concurrent operations; close on exit.

    Events captured inside are tagged concurrent and rendered side-by-side in the
    Workbench; anomaly detectors relax ordering checks within the group.
    """
    raise NotImplementedError
    yield ""  # pragma: no cover  (skeleton)
