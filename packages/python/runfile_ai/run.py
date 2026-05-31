"""The in-memory Run model, lifecycle calls, and event capture (hot path).

This is the synchronous hot path: lifecycle calls and ``capture_event`` build
metadata and append to the in-memory buffer, then return. No I/O, no crypto, no
hashing here — the background flusher (next slice) computes the hash chain in
``local_seq`` order, redacts, encrypts, and ships.

Per ``sdk-design.md`` the Python public surface is synchronous; framework
adapters (which may be async) call these same functions from their callbacks.
Ambient run/parent/parallel-group context flows via :mod:`contextvars`.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

from ._clock import detected_wall_clock_source, utc_now_iso
from ._constants import SCHEMA_VERSION_FULL, SDK_NAME, sdk_version
from ._ids import generate_event_id, generate_parallel_group_id, generate_run_id
from .buffer import BufferedEvent, BufferedRunItem
from .context import (
    current_parallel_group,
    current_parent_event,
    current_run,
    reset_current_run,
    reset_parallel_group,
    reset_parent_event,
    set_current_run,
    set_parallel_group,
    set_parent_event,
)

# Reason → lifecycle_state mapping for run_suspend transitions.
_AWAITING_STATE = {
    "awaiting_human_approval": "awaiting_human",
    "awaiting_human_input": "awaiting_human",
    "awaiting_webhook": "awaiting_webhook",
    "awaiting_schedule": "awaiting_schedule",
    "awaiting_external_system": "awaiting_human",
    "awaiting_subagent": "awaiting_human",
    "other": "awaiting_human",
}


@dataclass
class Run:
    run_id: str
    agent_identity: str
    framework: str = "manual"
    lifecycle_state: str = "active"
    conversation_id: Optional[str] = None
    segment_index: int = 0
    local_seq: int = 0
    last_event_hash_local: Optional[str] = None  # set by the flusher as it chains
    _run_token: Any = field(default=None, repr=False, compare=False)
    _parent_token: Any = field(default=None, repr=False, compare=False)

    def next_local_seq(self) -> int:
        seq = self.local_seq
        self.local_seq += 1
        return seq

    def begin_segment(self) -> None:
        """On resume: open a new segment — bump ``segment_index``, reset ``local_seq``."""
        self.segment_index += 1
        self.local_seq = 0


def _require_instance() -> Any:
    from .client import get_instance

    inst = get_instance()
    if inst is None:
        raise RuntimeError("runfile_ai.init(...) must be called before capturing runs/events")
    return inst


def _sdk_metadata(run: Run) -> dict[str, str]:
    return {"name": SDK_NAME, "version": sdk_version(), "framework": run.framework}


def _default_actor(run: Run) -> dict[str, str]:
    return {"type": "agent", "agent_identity": run.agent_identity}


def _emit_event(
    run: Run,
    *,
    kind: str,
    name: str,
    payload: Any = None,
    actor: Optional[dict[str, Any]] = None,
    **extra: Any,
) -> str:
    """Construct one event's metadata, stash its cleartext payload, buffer it.

    Returns the new event_id. Sets it as the ambient parent for the next event.
    """
    inst = _require_instance()
    event_id = generate_event_id()
    event: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION_FULL,
        "event_id": event_id,
        "run_id": run.run_id,
        "parent_event_id": current_parent_event(),
        "segment_index": run.segment_index,
        "local_seq": run.next_local_seq(),
        "captured_at": utc_now_iso(),
        "wall_clock_source": detected_wall_clock_source(),
        "sdk": _sdk_metadata(run),
        "actor": actor or _default_actor(run),
        "action": {"kind": kind, "name": name},
        "redaction_policy_version": inst.redaction_policy_version,
        "environment": inst.environment,
    }
    parallel_group_id = current_parallel_group()
    if parallel_group_id is not None:
        event["parallel_group_id"] = parallel_group_id
    for key, value in extra.items():
        if value is not None:
            event[key] = value

    inst.buffer.append(BufferedEvent(event=event, raw_payload=payload, run=run))
    set_parent_event(event_id)
    return event_id


# --------------------------------------------------------------------------- #
# Lifecycle
# --------------------------------------------------------------------------- #


def start_run(
    *,
    agent_identity: str,
    conversation_id: Optional[str] = None,
    framework: str = "manual",
    continued_from: Optional[dict[str, str]] = None,
) -> Run:
    """Begin a run: emit a ``run_create`` item and set ambient context."""
    inst = _require_instance()
    run = Run(
        run_id=generate_run_id(),
        agent_identity=agent_identity,
        framework=framework,
        conversation_id=conversation_id,
    )
    run._run_token = set_current_run(run)
    run._parent_token = set_parent_event(None)

    run_body: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION_FULL,
        "run_id": run.run_id,
        "agent_identity": agent_identity,
        "lifecycle_state": "active",
        "started_at": utc_now_iso(),
        "environment": inst.environment,
        "redaction_policy_version": inst.redaction_policy_version,
        "sdk_at_start": _sdk_metadata(run),
    }
    if conversation_id is not None:
        run_body["conversation_id"] = conversation_id
    if continued_from is not None:
        run_body["continued_from"] = continued_from

    inst.buffer.append(BufferedRunItem(item={"type": "run_create", "run": run_body}, run=run))
    return run


def end_run(*, outcome: str = "success", final_event_id: Optional[str] = None) -> None:
    """End the current run: emit a ``run_end`` item and clear ambient context."""
    run = current_run()
    if run is None:
        return
    inst = _require_instance()
    item: dict[str, Any] = {
        "type": "run_end",
        "run_id": run.run_id,
        "ended_at": utc_now_iso(),
        "outcome": outcome,
    }
    if final_event_id is not None:
        item["final_event_id"] = final_event_id
    inst.buffer.append(BufferedRunItem(item=item, run=run))
    run.lifecycle_state = "ended"
    _clear_run_context(run)


def suspend_run(
    *,
    reason: str,
    name: Optional[str] = None,
    expected_resumer: Optional[str] = None,
    expected_resume_by: Optional[str] = None,
) -> str:
    """Emit a ``run_suspend`` event + a ``run_update`` flipping to ``awaiting_*``.

    Returns the run_suspend event_id (the ``triggered_by_event_id`` link).
    """
    run = _require_run()
    inst = _require_instance()
    details: dict[str, Any] = {"reason": reason, "detection_source": "customer_explicit"}
    if expected_resumer is not None:
        details["expected_resumer"] = expected_resumer
    if expected_resume_by is not None:
        details["expected_resume_by"] = expected_resume_by

    event_id = _emit_event(run, kind="run_suspend", name=name or reason, suspension_details=details)
    state = _AWAITING_STATE.get(reason, "awaiting_human")
    inst.buffer.append(
        BufferedRunItem(
            item={
                "type": "run_update",
                "run_id": run.run_id,
                "lifecycle_state": state,
                "triggered_by_event_id": event_id,
            },
            run=run,
        )
    )
    run.lifecycle_state = state
    return event_id


def resume_run(
    *,
    triggered_by: str,
    run_id: Optional[str] = None,
    resumer_principal: Optional[str] = None,
) -> str:
    """Emit a ``run_resume`` event (new segment) + a ``run_update`` back to ``active``."""
    run = _require_run()
    if run_id is not None and run_id != run.run_id:
        raise ValueError(f"resume_run: active run is {run.run_id}, not {run_id}")
    inst = _require_instance()
    run.begin_segment()
    details: dict[str, Any] = {"triggered_by": triggered_by}
    if resumer_principal is not None:
        details["resumer_principal"] = resumer_principal

    event_id = _emit_event(run, kind="run_resume", name="resume", resume_details=details)
    inst.buffer.append(
        BufferedRunItem(
            item={
                "type": "run_update",
                "run_id": run.run_id,
                "lifecycle_state": "active",
                "triggered_by_event_id": event_id,
            },
            run=run,
        )
    )
    run.lifecycle_state = "active"
    return event_id


def abandon_run(*, reason: Optional[str] = None, run_id: Optional[str] = None) -> str:
    """Abandon a suspended run: ``run_abandon`` event; run ends ``outcome=abandoned``."""
    run = _require_run()
    if run_id is not None and run_id != run.run_id:
        raise ValueError(f"abandon_run: active run is {run.run_id}, not {run_id}")
    inst = _require_instance()
    event_id = _emit_event(run, kind="run_abandon", name=reason or "abandon")
    inst.buffer.append(
        BufferedRunItem(
            item={
                "type": "run_update",
                "run_id": run.run_id,
                "lifecycle_state": "ended",
                "triggered_by_event_id": event_id,
            },
            run=run,
        )
    )
    run.lifecycle_state = "ended"
    _clear_run_context(run)
    return event_id


def capture_event(*, kind: str, name: str, payload: Any = None, **fields: Any) -> str:
    """Manually capture one event within the current run. Returns the event_id."""
    run = _require_run()
    actor = fields.pop("actor", None)
    return _emit_event(run, kind=kind, name=name, payload=payload, actor=actor, **fields)


@contextmanager
def run(
    *,
    agent_identity: str,
    conversation_id: Optional[str] = None,
    framework: str = "manual",
    continued_from: Optional[dict[str, str]] = None,
) -> Iterator[Run]:
    """Create a run, set ambient context, and end it on exit.

    The run boundary is dynamic (per invocation), not lexical — hence a context
    manager rather than a decorator.
    """
    r = start_run(
        agent_identity=agent_identity,
        conversation_id=conversation_id,
        framework=framework,
        continued_from=continued_from,
    )
    try:
        yield r
    except Exception:
        if current_run() is r:
            end_run(outcome="failure")
        raise
    else:
        if current_run() is r:
            end_run(outcome="success")


@contextmanager
def parallel_group() -> Iterator[str]:
    """Open a ``parallel_group_id`` around concurrent operations; close on exit.

    Events captured inside are tagged concurrent (rendered side-by-side in the
    Workbench; anomaly detectors relax ordering checks within the group).
    """
    run_obj = _require_run()
    group_id = generate_parallel_group_id()
    token = set_parallel_group(group_id)
    try:
        _emit_event(run_obj, kind="parallel_group_open", name="parallel_group")
        yield group_id
    finally:
        _emit_event(run_obj, kind="parallel_group_close", name="parallel_group")
        reset_parallel_group(token)


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #


def _require_run() -> Run:
    run_obj = current_run()
    if run_obj is None:
        raise RuntimeError("no active run; start one with runfile_ai.run(...) or start_run(...)")
    return run_obj


def _clear_run_context(run_obj: Run) -> None:
    if run_obj._parent_token is not None:
        reset_parent_event(run_obj._parent_token)
        run_obj._parent_token = None
    if run_obj._run_token is not None:
        reset_current_run(run_obj._run_token)
        run_obj._run_token = None
