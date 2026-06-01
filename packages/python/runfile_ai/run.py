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
    dropped: bool = False  # set when the run is abandoned due to buffer overflow
    # Parent-event continuity for adapters whose callbacks fire OUTSIDE the ambient
    # context (e.g. framework hooks dispatched on another task): the adapter binds
    # this as the parent before each capture and writes the new event id back, so
    # chaining survives without relying on contextvars persisting between calls.
    last_parent_event_id: Optional[str] = None
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


#: Sentinel distinguishing "caller did not pass this" from an explicit ``None``.
#: An explicit ``parent_event_id`` (even ``None``) means "this is a real causal
#: edge the adapter resolved" — it is used verbatim AND it does not advance the
#: ambient parent spine, so a branch event (e.g. a ``tool_result`` parented on
#: its ``tool_call``) never leaks into the next ambient-threaded event's parent.
_UNSET: Any = object()


def _build_event(
    run: Run,
    *,
    kind: str,
    name: str,
    actor: Optional[dict[str, Any]] = None,
    parent_event_id: Any = _UNSET,
    parallel_group_id: Any = _UNSET,
    **extra: Any,
) -> tuple[str, dict[str, Any]]:
    inst = _require_instance()
    event_id = generate_event_id()
    # An explicit parent_event_id (incl. None) wins over the ambient parent; the
    # sentinel means "fall back to the ambient linear thread" (manual API, llm_call).
    parent = parent_event_id if parent_event_id is not _UNSET else current_parent_event()
    event: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION_FULL,
        "event_id": event_id,
        "run_id": run.run_id,
        "parent_event_id": parent,
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
    # Explicit group (incl. None to force "ungrouped") wins over the ambient group.
    group = parallel_group_id if parallel_group_id is not _UNSET else current_parallel_group()
    if group is not None:
        event["parallel_group_id"] = group
    # action_extra merges into the action sub-object (outcome, duration_ms) rather
    # than becoming a stray top-level field.
    action_extra = extra.pop("action_extra", None)
    if action_extra:
        event["action"].update(action_extra)
    for key, value in extra.items():
        if value is not None:
            event[key] = value
    return event_id, event


def _emit_event(
    run: Run,
    *,
    kind: str,
    name: str,
    payload: Any = None,
    actor: Optional[dict[str, Any]] = None,
    parent_event_id: Any = _UNSET,
    parallel_group_id: Any = _UNSET,
    **extra: Any,
) -> str:
    """Construct one event's metadata, stash its cleartext payload, buffer it.

    Returns the new event_id (empty if the run was overflow-dropped).

    Parent threading: without an explicit ``parent_event_id``, the event takes the
    ambient parent and then becomes the ambient parent for the next event (the
    linear spine the manual API and ``llm_call`` capture rely on). With an explicit
    ``parent_event_id``, the event is a resolved causal branch — it uses that parent
    and does NOT advance the spine, so e.g. a ``tool_result`` parented on its
    ``tool_call`` never becomes the parent of the following ``llm_call``.
    """
    if run.dropped:
        return ""
    if _require_instance().disabled:
        return ""  # disabled SDK: silent no-op
    event_id, event = _build_event(
        run,
        kind=kind,
        name=name,
        actor=actor,
        parent_event_id=parent_event_id,
        parallel_group_id=parallel_group_id,
        **extra,
    )
    _buffer_append(run, BufferedEvent(event=event, raw_payload=payload, run=run))
    if parent_event_id is _UNSET:
        set_parent_event(event_id)
    return event_id


def _emit_lifecycle_event(
    run: Run, *, kind: str, name: str, parent_event_id: Any = None
) -> str:
    """Author a run-boundary event (``run_create`` / ``run_end``) as a real chain link.

    Per the witness-authored-lifecycle decision the SDK — the real witness — owns
    every link in the chain, *including* the run boundaries, rather than shipping a
    bare metadata item the server then synthesises an event from. The flusher chains
    this exactly like any other ``BufferedEvent``: the ``run_create`` event, being
    the first for a brand-new conversation, takes the zero sentinel as its
    ``prev_event_hash`` (the genesis) and its hash seeds the first real event; there
    is no longer any expected first-event ``chain_break``.

    Unlike :func:`_emit_event` this does NOT touch ambient context: ``parent_event_id``
    is passed explicitly (default ``None``) so it is used verbatim and does not
    advance the ambient parent spine — the chain link is ``prev_event_hash`` (set by
    the flusher), not the causal parent. This keeps the "WITHOUT touching ambient
    context" contract of :func:`create_run` / :func:`emit_run_end` that adapters rely on.
    """
    inst = _require_instance()
    if run.dropped or inst.disabled:
        return ""
    event_id, event = _build_event(
        run,
        kind=kind,
        name=name,
        parent_event_id=parent_event_id,
        parallel_group_id=None,  # a run boundary is never part of a parallel group
    )
    _buffer_append(run, BufferedEvent(event=event, raw_payload=None, run=run))
    return event_id


def _buffer_append(run: Run, item: Any) -> None:
    """Append to the buffer and apply the overflow policy.

    Backpressure (default ``capture_blocking``): on reaching the soft cap, do a
    synchronous flush to drain — the agent's hot path briefly pays the ship cost,
    but no data is lost. With blocking disabled, drop the WHOLE current run
    atomically (never mid-run events) and emit a loud ``sdk_diagnostic``.
    """
    inst = _require_instance()
    if inst.disabled or run.dropped:
        return  # disabled SDK: silent no-op; dropped run: already abandoned
    inst.buffer.append(item)
    buffered = len(inst.buffer)
    if buffered >= inst.buffer.soft_cap:
        if inst.capture_blocking:
            inst.flush(force_all=True)  # overflow: drain everything, watermark be damned
        else:
            _drop_run_overflow(inst, run)
    elif buffered >= inst.buffer.flush_threshold:
        # Size trigger: nudge the background flusher (non-blocking) so a burst
        # drains promptly instead of waiting out the 2s interval.
        inst.notify_flusher()


def _drop_run_overflow(inst: Any, run: Run) -> None:
    removed = inst.buffer.drop_run(run.run_id)
    run.dropped = True
    _, diagnostic = _build_event(
        run,
        kind="sdk_diagnostic",
        name="run_dropped_overflow",
        labels={"dropped_event_count": str(removed)},
    )
    # Append directly (bypass the dropped-run guard) so the failure is visible.
    inst.buffer.append(BufferedEvent(event=diagnostic, raw_payload=None, run=run))
    # Also surface on the diagnostics channel (callback/observability).
    inst.emit_diagnostic(
        "run_dropped_overflow", detail=f"run={run.run_id} dropped_events={removed}"
    )


# --------------------------------------------------------------------------- #
# Lifecycle
# --------------------------------------------------------------------------- #


def create_run(
    *,
    agent_identity: str,
    conversation_id: Optional[str] = None,
    framework: str = "manual",
    run_id: Optional[str] = None,
    continued_from: Optional[dict[str, str]] = None,
    delegated_from: Optional[dict[str, str]] = None,
    handed_off_from: Optional[dict[str, str]] = None,
    labels: Optional[dict[str, str]] = None,
) -> Run:
    """Create a run and emit its ``run_create`` item, WITHOUT touching ambient context.

    This is the explicit-run primitive that framework adapters use: they key runs
    by a framework cursor (session_id, thread_id, agent_id) and bind context per
    callback, rather than relying on a single ambient run. :func:`start_run` layers
    ambient-context management on top of this for the manual API.
    """
    inst = _require_instance()
    run = Run(
        run_id=run_id or generate_run_id(),
        agent_identity=agent_identity,
        framework=framework,
        conversation_id=conversation_id,
    )
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
    if delegated_from is not None:
        run_body["delegated_from"] = delegated_from
    if handed_off_from is not None:
        run_body["handed_off_from"] = handed_off_from
    if labels:
        run_body["labels"] = labels

    # The companion run_create ITEM survives only to materialise the runs row (it
    # carries conversation_id / continued_from / labels the chain event has no slot
    # for). The run_create EVENT below is the actual genesis chain link — authored
    # by the witness, not synthesised by the server.
    _buffer_append(run, BufferedRunItem(item={"type": "run_create", "run": run_body}, run=run))
    _emit_lifecycle_event(run, kind="run_create", name="run_create")
    return run


def start_run(
    *,
    agent_identity: str,
    conversation_id: Optional[str] = None,
    framework: str = "manual",
    continued_from: Optional[dict[str, str]] = None,
    labels: Optional[dict[str, str]] = None,
) -> Run:
    """Begin a run (manual API): emit a ``run_create`` item and set ambient context."""
    run = create_run(
        agent_identity=agent_identity,
        conversation_id=conversation_id,
        framework=framework,
        continued_from=continued_from,
        labels=labels,
    )
    run._run_token = set_current_run(run)
    run._parent_token = set_parent_event(None)
    return run


def emit_run_end(
    run: Run, *, outcome: str = "success", final_event_id: Optional[str] = None
) -> None:
    """Emit a ``run_end`` item for an explicit run, WITHOUT clearing ambient context.

    The explicit-run counterpart to :func:`end_run` (which targets the ambient run
    and tears down its context). Idempotent: a no-op once the run has ended.
    """
    if run.lifecycle_state == "ended":
        return
    item: dict[str, Any] = {
        "type": "run_end",
        "run_id": run.run_id,
        "ended_at": utc_now_iso(),
        "outcome": outcome,
    }
    if final_event_id is not None:
        item["final_event_id"] = final_event_id
    # Companion item (above) closes the runs row; the run_end EVENT (below) is the
    # run's final chain link. Parent it on final_event_id when known — purely
    # causal; the chain link is prev_event_hash, set by the flusher.
    _buffer_append(run, BufferedRunItem(item=item, run=run))
    _emit_lifecycle_event(run, kind="run_end", name="run_end", parent_event_id=final_event_id)
    run.lifecycle_state = "ended"


def end_run(*, outcome: str = "success", final_event_id: Optional[str] = None) -> None:
    """End the current ambient run: emit a ``run_end`` item and clear ambient context."""
    run = current_run()
    if run is None:
        return
    emit_run_end(run, outcome=outcome, final_event_id=final_event_id)
    _clear_run_context(run)


def suspend_run(
    *,
    reason: str,
    name: Optional[str] = None,
    expected_resumer: Optional[str] = None,
    expected_resume_by: Optional[str] = None,
    detection_source: str = "customer_explicit",
    framework_signal: Optional[dict[str, Any]] = None,
) -> str:
    """Emit a ``run_suspend`` event + a ``run_update`` flipping to ``awaiting_*``.

    Returns the run_suspend event_id (the ``triggered_by_event_id`` link).

    The manual API defaults ``detection_source="customer_explicit"``; framework
    adapters pass ``"framework_inferred"`` plus the ``framework_signal`` they
    observed (so auditors can verify the suspension was grounded in a real signal).
    """
    run = _require_run()
    details: dict[str, Any] = {"reason": reason, "detection_source": detection_source}
    if expected_resumer is not None:
        details["expected_resumer"] = expected_resumer
    if expected_resume_by is not None:
        details["expected_resume_by"] = expected_resume_by
    if framework_signal is not None:
        details["framework_signal"] = framework_signal

    event_id = _emit_event(run, kind="run_suspend", name=name or reason, suspension_details=details)
    state = _AWAITING_STATE.get(reason, "awaiting_human")
    _buffer_append(
        run,
        BufferedRunItem(
            item={
                "type": "run_update",
                "run_id": run.run_id,
                "lifecycle_state": state,
                "triggered_by_event_id": event_id,
            },
            run=run,
        ),
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
    run.begin_segment()
    details: dict[str, Any] = {"triggered_by": triggered_by}
    if resumer_principal is not None:
        details["resumer_principal"] = resumer_principal

    event_id = _emit_event(run, kind="run_resume", name="resume", resume_details=details)
    _buffer_append(
        run,
        BufferedRunItem(
            item={
                "type": "run_update",
                "run_id": run.run_id,
                "lifecycle_state": "active",
                "triggered_by_event_id": event_id,
            },
            run=run,
        ),
    )
    run.lifecycle_state = "active"
    return event_id


def abandon_run(*, reason: Optional[str] = None, run_id: Optional[str] = None) -> str:
    """Abandon a suspended run: ``run_abandon`` event; run ends ``outcome=abandoned``."""
    run = _require_run()
    if run_id is not None and run_id != run.run_id:
        raise ValueError(f"abandon_run: active run is {run.run_id}, not {run_id}")
    event_id = _emit_event(run, kind="run_abandon", name=reason or "abandon")
    _buffer_append(
        run,
        BufferedRunItem(
            item={
                "type": "run_update",
                "run_id": run.run_id,
                "lifecycle_state": "ended",
                "triggered_by_event_id": event_id,
            },
            run=run,
        ),
    )
    run.lifecycle_state = "ended"
    _clear_run_context(run)
    return event_id


def capture_event(*, kind: str, name: str, payload: Any = None, **fields: Any) -> str:
    """Manually capture one event within the current run. Returns the event_id.

    ``parent_event_id`` and ``parallel_group_id`` may be passed to set a resolved
    causal edge / concurrency group explicitly (framework adapters that can derive
    true causality from native ids do this); omitting them falls back to the
    ambient linear parent and ambient parallel group.
    """
    run = _require_run()
    actor = fields.pop("actor", None)
    parent_event_id = fields.pop("parent_event_id", _UNSET)
    parallel_group_id = fields.pop("parallel_group_id", _UNSET)
    return _emit_event(
        run,
        kind=kind,
        name=name,
        payload=payload,
        actor=actor,
        parent_event_id=parent_event_id,
        parallel_group_id=parallel_group_id,
        **fields,
    )


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
