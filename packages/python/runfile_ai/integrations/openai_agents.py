"""OpenAI Agents SDK adapter (``openai-agents`` >= 0.1, package ``agents``).

Verified against the **published** ``agents`` source AND an empirical span dump on
``openai-agents==0.17.4`` (see ``private/v3/openai-agents-adapter-design.md``), NOT the
draft in ``sdk-design.md`` — the draft proposed a ``RunHooks`` adapter, which is the
wrong surface: ``on_tool_start`` carries no tool arguments and no call id, so two
parallel tool calls give ``start, start, end, end`` with nothing to pair them. You
cannot build a correct hash chain from the hooks alone.

The right surface is the SDK's first-class **tracing** system — the same integration
point Logfire / LangSmith / Langfuse use, and the OTel-shaped surface our Mastra /
Pydantic adapters will use. We register a :class:`TracingProcessor`; it receives
**spans** in real time carrying ``span_id`` / ``parent_id`` (the causal DAG), and typed
``span_data`` with full tool input/output, per-turn token usage, and handoff from/to.

**Verified span tree** (``openai-agents==0.17.4``), for a tool-using, handing-off agent::

    TaskSpanData (root)                       the whole Runner.run
      AgentSpanData(Triage)                   run #1 (one run per agent identity)
        TurnSpanData(turn, agent_name, usage) the LLM turn         -> llm_call
          FunctionSpanData(name, input, output)  parallel tool A   ┐ siblings sharing
          FunctionSpanData(name, input, output)  parallel tool B   ┘ the turn -> group
        TurnSpanData
          HandoffSpanData(from_agent, to_agent)                    -> handoff (ends run #1)
      AgentSpanData(Specialist)               run #2 (handed_off_from run #1)
        TurnSpanData

Mapping (all empirically confirmed; span_data are ``__slots__``+property, read by attr):

- ``AgentSpanData`` is the **run boundary** — one Runfile run per agent identity (the
  Runfile invariant). The first agent span opens the root run; a handoff opens a new run.
- ``TurnSpanData`` is the **llm_call** (always present, runner-emitted, carries
  ``usage``). ``model_ref.model_id`` comes from the configured model map / ``agent.model``
  (the turn span has no model field); ``Generation``/``Response`` spans — emitted only by
  built-in OpenAI models, nested under the turn — *enrich* the model id when present and
  are never depended on.
- ``FunctionSpanData`` (parent = the turn) is a ``tool_call`` (its ``input`` args) plus a
  ``tool_result`` (its ``output``); a turn with >= 2 function children gets one
  ``parallel_group_id``.
- ``HandoffSpanData`` -> ``handoff`` on the source run (which ends) + a new target run
  with ``handed_off_from``.

**Turn-atomic emission.** Function spans END before their TurnSpan ends, so we stash
each function span's data on its ``on_span_end`` and emit the *whole turn* atomically on
the TurnSpan's ``on_span_end``: ``llm_call`` first, then its tool calls/results in order.
This guarantees correct ``local_seq`` ordering (llm_call before the tools that depend on
it — impossible if we emitted tools as they ended) and lets us assign the parallel group
deterministically, without relying on the flusher's grouping heuristic.

**Causal binding across tasks.** The SDK may dispatch span callbacks on different tasks,
so (like the LangGraph adapter) we never rely on ambient ``contextvars`` persisting
*between* callbacks: runs and per-span scratch live in a process-global registry keyed by
``trace_id`` / ``span_id``, and each capture binds the core's ambient context for its
duration under a per-run lock.

What this module (phase 1) does NOT yet do: HITL ``run_suspend`` / ``run_resume`` and
fork detection (needs a thin ``Runner.run`` wrapper around ``result.interruptions`` —
tracing models execution, not the human pause), ``as_tool`` delegation, and
``Generation``/``Response`` enrichment of the model id. All are documented follow-ups.

Every callback degrades to a no-op when the SDK isn't initialised and swallows its own
exceptions: an observer must never break the customer's run.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

from .._ids import generate_event_id, generate_parallel_group_id, generate_run_id
from ..client import get_instance
from ..context import (
    current_parent_event,
    reset_current_run,
    reset_parent_event,
    set_current_run,
    set_parent_event,
)
from ..run import (
    Run,
    capture_event,
    create_run,
    emit_run_end,
    resume_run,
    suspend_run,
)

_FRAMEWORK = "openai_agents"


def _active() -> bool:
    """True when the SDK is initialised and not disabled (else: transparent no-op)."""
    inst = get_instance()
    return inst is not None and not inst.disabled


# --------------------------------------------------------------------------- #
# Translation helpers
# --------------------------------------------------------------------------- #


def _span_type(span: Any) -> str:
    """The SpanData subclass name (e.g. ``TurnSpanData``) — our dispatch key."""
    data = getattr(span, "span_data", None)
    return type(data).__name__ if data is not None else ""


def _attr(data: Any, name: str) -> Any:
    """Read a span_data property safely (they're ``__slots__``+property, not dataclasses)."""
    try:
        return getattr(data, name, None)
    except Exception:
        return None


def _model_ref(model_id: Optional[str], usage: Any) -> dict[str, Any]:
    """Build ``model_ref`` for an ``llm_call``: provider + model id + token usage.

    ``provider`` is always ``openai`` for this SDK. ``model_id`` comes from the agent's
    configured model (resolved by the caller); ``unknown`` when the model isn't a plain
    string and no built-in Generation/Response span supplied it. ``usage`` is the
    ``TurnSpanData.usage`` dict (``input_tokens`` / ``output_tokens``).
    """
    ref: dict[str, Any] = {"provider": "openai", "model_id": (model_id or "unknown")[:128]}
    if isinstance(usage, dict):
        if isinstance(usage.get("input_tokens"), int):
            ref["input_tokens"] = usage["input_tokens"]
        if isinstance(usage.get("output_tokens"), int):
            ref["output_tokens"] = usage["output_tokens"]
    return ref


def _func_output(raw: Any) -> tuple[Any, bool]:
    """Normalise a function span's ``output`` to ``(value, approval_pending)``.

    A ``needs_approval`` tool that the model wanted to call but which is awaiting human
    approval ends its span with a ``FunctionToolResult``-like wrapper whose ``run_item``
    is a ``ToolApprovalItem`` and whose ``output`` is ``None`` — i.e. the tool did NOT
    execute. We surface that as ``approval_pending=True`` so the processor skips emitting
    a (bogus) ``tool_result`` for it; the ``Runner.run`` wrapper represents the request as
    a ``tool_approval_requested`` + ``run_suspend`` instead. A normal completed tool ends
    with its plain return value (verified: ``'RESULT[x]'``); an approved-then-executed
    tool likewise ends with its plain value on the resumed segment.
    """
    run_item = getattr(raw, "run_item", None)
    if run_item is not None and type(run_item).__name__ == "ToolApprovalItem":
        return None, True
    if run_item is not None and hasattr(raw, "output"):  # FunctionToolResult wrapper
        return getattr(raw, "output"), False
    return raw, False


def _usage_dict(usage: Any) -> Any:
    """Coerce a ``Usage``-like object/dict into a plain ``{input_tokens, output_tokens}``."""
    if usage is None:
        return None
    if isinstance(usage, dict):
        return usage
    out: dict[str, int] = {}
    for k in ("input_tokens", "output_tokens"):
        v = getattr(usage, k, None)
        if isinstance(v, int):
            out[k] = v
    return out or None


def _derive_identity(root: str, agent_name: str) -> str:
    """Derive a handoff-target DID from the root run's DID namespace + the agent name.

    ``did:web:bank.com:agents:triage:v1`` + ``Refund Specialist`` ->
    ``did:web:bank.com:agents:refund-specialist``. Only used when the customer supplies
    no explicit identity for a handoff target; such runs are labelled ``derived_identity``
    so auditors can see the value was inferred, not asserted by the customer.
    """
    slug = "".join(c if c.isalnum() else "-" for c in agent_name.strip().lower())
    slug = "-".join(filter(None, slug.split("-"))) or "agent"
    head = root.rsplit(":", 1)[0] if root.count(":") >= 3 else root
    return f"{head}:{slug}"


# --------------------------------------------------------------------------- #
# Per-turn scratch + per-trace state + registry
# --------------------------------------------------------------------------- #


@dataclass
class _FuncRecord:
    """One function span's captured data, stashed on its ``on_span_end`` and flushed when
    its parent turn ends. Either a tool call (``name``/``input``/``output``) or — when the
    function was an ``agent.as_tool()`` delegation — a ``delegate`` marker carrying the
    pre-allocated delegate event id + the child run it spawned."""

    name: str
    input: Any = None
    output: Any = None
    failed: bool = False
    delegate: Optional[dict[str, str]] = None


@dataclass
class _TraceState:
    """Everything we track for one ``trace`` (one ``Runner.run`` invocation).

    A trace may own several runs (handoffs), so run identity is per ``AgentSpanData``.
    ``span_run`` lets any descendant span resolve its owning run by walking the
    ``parent_id`` it inherited at ``on_span_start``.
    """

    conversation_id: Optional[str] = None
    #: any span_id -> the Run that span belongs to (inherited from parent at start)
    span_run: dict[str, Run] = field(default_factory=dict)
    #: turn span_ids seen (so a function knows if its parent is a turn -> defer, else eager)
    turn_spans: set[str] = field(default_factory=set)
    #: span_id -> SpanData type name (to walk ancestry for delegation detection)
    span_kind: dict[str, str] = field(default_factory=dict)
    #: span_id -> parent_id (to walk ancestry without the live span objects)
    span_parent: dict[str, str] = field(default_factory=dict)
    #: function span_ids that delegated (an agent.as_tool sub-agent ran inside) — their
    #: tool_call/tool_result is suppressed; the delegate event represents them instead
    delegation_funcs: set[str] = field(default_factory=set)
    #: delegated agent span_id -> its child Run (ended when that agent span ends)
    delegated_agent_runs: dict[str, Run] = field(default_factory=dict)
    #: turn span_id -> buffered function records for that turn (emitted at turn end)
    turn_funcs: dict[str, list[_FuncRecord]] = field(default_factory=dict)
    #: turn span_id -> model id from a nested Generation/Response span (built-in models)
    turn_model: dict[str, str] = field(default_factory=dict)
    #: the run currently accepting events (last opened agent run)
    active_run: Optional[Run] = None
    #: a handoff target run created at HandoffSpan end, awaiting its AgentSpan start
    pending_handoff_run: Optional[Run] = None
    #: True when a Runner.run wrapper owns lifecycle end (don't auto-end at trace end)
    lifecycle_managed: bool = False
    #: True while the run is suspended awaiting approval (set by the wrapper)
    suspended: bool = False
    #: the interruptions the wrapper suspended on, to infer granted/denied on resume
    pending_interruptions: list[Any] = field(default_factory=list)
    #: serialises this trace's capture critical section (spans may fire on many tasks)
    lock: "threading.RLock" = field(default_factory=threading.RLock)


class _Registry:
    """Process-global ``trace_id`` -> :class:`_TraceState`."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._traces: dict[str, _TraceState] = {}

    def get_or_create(self, trace_id: str) -> _TraceState:
        with self._lock:
            st = self._traces.get(trace_id)
            if st is None:
                st = _TraceState()
                self._traces[trace_id] = st
            return st

    def get(self, trace_id: str) -> Optional[_TraceState]:
        with self._lock:
            return self._traces.get(trace_id)

    def drop(self, trace_id: str) -> None:
        with self._lock:
            self._traces.pop(trace_id, None)

    def reset(self) -> None:
        with self._lock:
            self._traces.clear()


_registry = _Registry()


@contextmanager
def _bound(run: Run, state: _TraceState) -> Iterator[None]:
    """Bind ``run`` (and its last parent event) as ambient context for one capture,
    holding the trace's lock so concurrent span callbacks can't interleave the core's
    ``local_seq`` assignment + buffer append. Persists the advanced parent back onto the
    run so the linear spine survives across callbacks dispatched on unrelated tasks.
    """
    with state.lock:
        run_token = set_current_run(run)
        parent_token = set_parent_event(run.last_parent_event_id)
        try:
            yield
        finally:
            run.last_parent_event_id = current_parent_event()
            reset_parent_event(parent_token)
            reset_current_run(run_token)


# --------------------------------------------------------------------------- #
# The tracing processor
# --------------------------------------------------------------------------- #


def build_processor(
    agent_identity: str,
    *,
    conversation_id: Optional[str] = None,
    agent_identity_map: Optional[dict[str, str]] = None,
    agent_model_map: Optional[dict[str, str]] = None,
) -> Any:
    """Construct the Runfile :class:`TracingProcessor`.

    ``agent_identity`` owns the first agent. ``agent_identity_map`` (agent name -> DID)
    resolves handoff targets; unmapped targets are derived from the root DID and labelled.
    ``agent_model_map`` (agent name -> model id) supplies ``model_ref.model_id`` per turn
    (the turn span carries usage but no model id). Imported lazily so importing this
    module never requires ``agents`` to be installed.
    """
    from agents.tracing.processor_interface import TracingProcessor

    identity_map = dict(agent_identity_map or {})
    model_map = dict(agent_model_map or {})

    class _RunfileProcessor(TracingProcessor):
        # ---- trace lifecycle -------------------------------------------- #

        def on_trace_start(self, trace: Any) -> None:
            if not _active():
                return
            try:
                st = _registry.get_or_create(trace.trace_id)
                # group_id (a conversation/chat-thread id) lives in trace.export().
                if st.conversation_id is None:
                    exported = trace.export() or {}
                    st.conversation_id = conversation_id or exported.get("group_id")
            except Exception:
                pass

        def on_trace_end(self, trace: Any) -> None:
            if not _active():
                return
            try:
                st = _registry.get(trace.trace_id)
                if st is None:
                    return
                # Standalone (no Runner.run wrapper): end the still-active run as a clean
                # success here. With a wrapper, lifecycle_managed is set and the wrapper
                # renders run_end / run_suspend from result.interruptions after return.
                if not st.lifecycle_managed and st.active_run is not None:
                    emit_run_end(st.active_run, outcome="success")
                if not st.lifecycle_managed:
                    _registry.drop(trace.trace_id)
            except Exception:
                pass

        # ---- span lifecycle --------------------------------------------- #

        def on_span_start(self, span: Any) -> None:
            if not _active():
                return
            try:
                st = _registry.get_or_create(span.trace_id)
                kind = _span_type(span)
                st.span_kind[span.span_id] = kind
                if span.parent_id:
                    st.span_parent[span.span_id] = span.parent_id
                if kind == "AgentSpanData":
                    self._open_agent_run(st, span)
                else:
                    if kind == "TurnSpanData":
                        st.turn_spans.add(span.span_id)
                    # Inherit the owning run from the parent span (Task has no run).
                    parent_run = st.span_run.get(span.parent_id or "")
                    if parent_run is not None:
                        st.span_run[span.span_id] = parent_run
            except Exception:
                pass

        def on_span_end(self, span: Any) -> None:
            if not _active():
                return
            try:
                st = _registry.get(span.trace_id)
                if st is None:
                    return
                kind = _span_type(span)
                if kind == "FunctionSpanData":
                    self._stash_function(st, span)
                elif kind == "TurnSpanData":
                    self._emit_turn(st, span)
                elif kind in ("GenerationSpanData", "ResponseSpanData"):
                    self._record_model(st, span, kind)
                elif kind == "HandoffSpanData":
                    self._emit_handoff(st, span)
                elif kind == "GuardrailSpanData":
                    self._emit_guardrail(st, span)
                elif kind == "AgentSpanData":
                    # A delegated (as-tool) sub-agent's run ends when its agent span ends;
                    # the root / handed-off run is closed by the wrapper or at trace end.
                    child = st.delegated_agent_runs.pop(span.span_id, None)
                    if child is not None:
                        emit_run_end(child, outcome="success")
            except Exception:
                pass

        def shutdown(self) -> None:
            return None

        def force_flush(self) -> None:
            inst = get_instance()
            if inst is not None and not inst.disabled:
                inst.flush(force_all=True)

        # ---- run boundaries --------------------------------------------- #

        def _delegation_ancestor(self, st: _TraceState, span: Any) -> Optional[str]:
            """The nearest FunctionSpanData ancestor of an agent span, if any — i.e. this
            agent is running inside an ``agent.as_tool()`` call (a delegation). Returns the
            function span_id, or None for a root / handoff-target agent (whose nearest
            non-task ancestor is the Task, not a function)."""
            pid: Optional[str] = span.parent_id
            while pid:
                kind = st.span_kind.get(pid)
                if kind == "FunctionSpanData":
                    return pid
                if kind == "AgentSpanData":
                    return None
                pid = st.span_parent.get(pid)
            return None

        def _open_agent_run(self, st: _TraceState, span: Any) -> None:
            name = _attr(span.span_data, "name") or "agent"
            func_anc = self._delegation_ancestor(st, span)
            if func_anc is not None:
                self._open_delegated_run(st, span, func_anc, str(name))
                return
            with st.lock:
                if st.pending_handoff_run is not None:
                    # This agent span is the target of a just-emitted handoff.
                    run = st.pending_handoff_run
                    st.pending_handoff_run = None
                elif st.active_run is None:
                    # First agent in the trace -> the root run.
                    run = create_run(
                        agent_identity=agent_identity,
                        conversation_id=st.conversation_id,
                        framework=_FRAMEWORK,
                        labels={"openai_agent_name": str(name)},
                    )
                else:
                    # A nested/secondary agent span with no handoff (rare) — reuse the
                    # active run rather than inventing an unlinked one.
                    run = st.active_run
                st.span_run[span.span_id] = run
                st.active_run = run

        def _open_delegated_run(
            self, st: _TraceState, span: Any, func_anc: str, name: str
        ) -> None:
            """An ``agent.as_tool()`` sub-agent: create a child run delegated from the run
            that owns the as-tool function, and queue a ``delegate`` event onto that
            function's turn (emitted after the turn's llm_call, in order). The as-tool
            function's own tool_call/tool_result is suppressed — the delegate represents it.
            """
            parent_run = st.span_run.get(func_anc)
            outer_turn = st.span_parent.get(func_anc)
            if parent_run is None or outer_turn is None:
                return
            child_identity = identity_map.get(name)
            derived = child_identity is None
            if child_identity is None:
                child_identity = _derive_identity(parent_run.agent_identity, name)
            delegate_eid = generate_event_id()
            with st.lock:
                child = create_run(
                    agent_identity=child_identity,
                    conversation_id=st.conversation_id,
                    framework=_FRAMEWORK,
                    delegated_from={"run_id": parent_run.run_id, "event_id": delegate_eid},
                    labels=(
                        {"openai_agent_name": name, "derived_identity": "true"}
                        if derived
                        else {"openai_agent_name": name}
                    ),
                )
                st.turn_funcs.setdefault(outer_turn, []).append(
                    _FuncRecord(
                        name=name,
                        delegate={
                            "event_id": delegate_eid,
                            "delegated_run_id": child.run_id,
                            "delegated_agent_identity": child_identity,
                        },
                    )
                )
                st.delegation_funcs.add(func_anc)
                st.span_run[span.span_id] = child
                st.delegated_agent_runs[span.span_id] = child

        # ---- turn (llm_call) + its tools -------------------------------- #

        def _stash_function(self, st: _TraceState, span: Any) -> None:
            if span.span_id in st.delegation_funcs:
                # An agent.as_tool() call — the sub-agent ran in its own delegated run and
                # a delegate event already represents it; don't also emit a tool_call.
                return
            data = span.span_data
            output, approval_pending = _func_output(_attr(data, "output"))
            if approval_pending:
                # The model wanted this tool but it's awaiting human approval (it did NOT
                # run). The Runner.run wrapper emits tool_approval_requested + run_suspend
                # for it; emitting a tool_call/tool_result here would be a phantom record.
                return
            rec = _FuncRecord(
                name=str(_attr(data, "name") or "tool"),
                input=_attr(data, "input"),
                output=output,
                failed=span.error is not None,
            )
            parent_id = span.parent_id or ""
            if parent_id in st.turn_spans:
                # Normal case: the function is a child of its turn span. Defer to the
                # turn's end so the llm_call is emitted first (correct local_seq) and >= 2
                # siblings can be grouped.
                with st.lock:
                    st.turn_funcs.setdefault(parent_id, []).append(rec)
            else:
                # Resume case: the approved tool re-executes as a direct child of the Task
                # span (before any new turn), so there is no turn to defer to. Emit it
                # eagerly, parented on the run's current spine (the run_resume event the
                # wrapper just emitted), which keeps it correctly ordered in the segment.
                run = st.span_run.get(span.span_id) or st.active_run
                if run is not None:
                    self._emit_loose_function(run, st, rec)

        def _emit_loose_function(self, run: Run, st: _TraceState, rec: _FuncRecord) -> None:
            with _bound(run, st):
                call_id = capture_event(kind="tool_call", name=rec.name, payload=rec.input)
                capture_event(
                    kind="tool_result",
                    name=rec.name,
                    payload=rec.output,
                    action_extra={"outcome": "failure" if rec.failed else "success"},
                    parent_event_id=call_id,
                )

        def _record_model(self, st: _TraceState, span: Any, kind: str) -> None:
            # A built-in OpenAI model nests a Generation/Response span (with the real
            # model id) under the turn. Record it so the turn's llm_call reports the
            # precise model rather than the configured/"unknown" fallback.
            data = span.span_data
            model_id: Optional[str] = None
            if kind == "GenerationSpanData":
                model_id = _attr(data, "model")
            else:  # ResponseSpanData: the id lives on the response object
                resp = _attr(data, "response")
                model_id = getattr(resp, "model", None) if resp is not None else None
            parent_id = span.parent_id or ""
            if model_id and parent_id:
                with st.lock:
                    st.turn_model[parent_id] = str(model_id)

        def _emit_turn(self, st: _TraceState, span: Any) -> None:
            run = st.span_run.get(span.span_id)
            if run is None:
                return
            data = span.span_data
            # Prefer the precise model id from a nested Generation/Response span, then the
            # caller-supplied map, then "unknown" (custom models expose no model id).
            model_id = st.turn_model.pop(span.span_id, None) or model_map.get(
                str(_attr(data, "agent_name") or "")
            )
            usage = _usage_dict(_attr(data, "usage"))
            funcs = st.turn_funcs.pop(span.span_id, [])

            with _bound(run, st):
                llm_event_id = capture_event(
                    kind="llm_call",
                    name="chat",
                    model_ref=_model_ref(model_id, usage),
                    payload=None,
                )
                # A delegation (agent.as_tool) becomes a delegate event — emitted with the
                # id its child run already references in delegated_from — parented on this
                # turn's llm_call, in order after it.
                tool_recs = [r for r in funcs if r.delegate is None]
                for rec in funcs:
                    if rec.delegate is not None:
                        capture_event(
                            kind="delegate",
                            name=rec.name,
                            event_id=rec.delegate["event_id"],
                            parent_event_id=llm_event_id,
                            delegation_details={
                                "delegated_run_id": rec.delegate["delegated_run_id"],
                                "delegated_agent_identity": rec.delegate["delegated_agent_identity"],
                                "framework_signal": "openai_agents_as_tool",
                            },
                        )
                # Emit all tool_calls then all tool_results (matches the schema's parallel
                # rendering); a single tool degenerates to call->result. Each tool parents
                # on this turn's llm_call; results parent on their own call. Grouping is
                # over the tool calls only (delegations are not part of the fan-out group).
                tool_group = generate_parallel_group_id() if len(tool_recs) >= 2 else None
                call_events: list[Optional[str]] = []
                for rec in tool_recs:
                    call_events.append(
                        capture_event(
                            kind="tool_call",
                            name=rec.name,
                            payload=rec.input,
                            parent_event_id=llm_event_id,
                            parallel_group_id=tool_group,
                        )
                    )
                for rec, call_id in zip(tool_recs, call_events):
                    capture_event(
                        kind="tool_result",
                        name=rec.name,
                        payload=rec.output,
                        action_extra={"outcome": "failure" if rec.failed else "success"},
                        parent_event_id=call_id,
                        parallel_group_id=tool_group,
                    )

        # ---- handoff ----------------------------------------------------- #

        def _emit_handoff(self, st: _TraceState, span: Any) -> None:
            source = st.active_run
            if source is None:
                return
            data = span.span_data
            to_name = str(_attr(data, "to_agent") or "agent")
            target_identity = identity_map.get(to_name)
            derived = target_identity is None
            if target_identity is None:
                target_identity = _derive_identity(agent_identity, to_name)

            # Pre-allocate the target run_id so the handoff event can reference it, then
            # create the target run referencing the handoff event — so handed_off_from
            # points at the exact event that established the link (bidirectional nav).
            target_run_id = generate_run_id()
            with _bound(source, st):
                handoff_event_id = capture_event(
                    kind="handoff",
                    name=f"{_attr(data, 'from_agent') or 'agent'}_to_{to_name}",
                    handoff_details={
                        "target_run_id": target_run_id,
                        "target_agent_identity": target_identity,
                        "framework_signal": "openai_agents_handoff",
                    },
                )
            emit_run_end(source, outcome="success", final_event_id=handoff_event_id)
            with st.lock:
                target = create_run(
                    run_id=target_run_id,
                    agent_identity=target_identity,
                    conversation_id=st.conversation_id,
                    framework=_FRAMEWORK,
                    handed_off_from={"run_id": source.run_id, "event_id": handoff_event_id},
                    labels=(
                        {"openai_agent_name": to_name, "derived_identity": "true"}
                        if derived
                        else {"openai_agent_name": to_name}
                    ),
                )
                # Bound to the next AgentSpan (the target agent) via pending_handoff_run.
                st.pending_handoff_run = target
                st.active_run = target

        # ---- guardrail (policy_check) ----------------------------------- #

        def _emit_guardrail(self, st: _TraceState, span: Any) -> None:
            run = st.span_run.get(span.span_id) or st.active_run
            if run is None:
                return
            data = span.span_data
            triggered = bool(_attr(data, "triggered"))
            with _bound(run, st):
                capture_event(
                    kind="policy_check",
                    name=str(_attr(data, "name") or "guardrail"),
                    action_extra={"outcome": "failure" if triggered else "success"},
                )

    return _RunfileProcessor()


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #


def instrument(
    *,
    agent_identity: str,
    conversation_id: Optional[str] = None,
    agent_identity_map: Optional[dict[str, str]] = None,
    agent_model_map: Optional[dict[str, str]] = None,
    replace_processors: bool = True,
) -> Any:
    """Register the Runfile tracing processor and return it.

    One call before running agents::

        from runfile_ai.integrations import openai_agents as runfile_openai

        runfile_openai.instrument(agent_identity="did:web:bank.com:agents:triage:v1")
        result = await Runner.run(agent, "…")    # used exactly as before

    ``replace_processors=True`` (the compliance default) uses ``set_trace_processors`` so
    agent traces do **not** also ship to OpenAI's backend; ``False`` uses
    ``add_trace_processor`` to keep OpenAI's dashboard alongside Runfile. Keep the SDK's
    ``trace_include_sensitive_data`` on (the default) — otherwise span input/output are
    empty and Runfile captures metadata only; Runfile's own redaction + encryption is the
    real privacy boundary.

    Safe to call when the SDK isn't initialised: the processor's callbacks are no-ops.
    """
    processor = build_processor(
        agent_identity,
        conversation_id=conversation_id,
        agent_identity_map=agent_identity_map,
        agent_model_map=agent_model_map,
    )
    from agents import add_trace_processor, set_trace_processors

    if replace_processors:
        set_trace_processors([processor])
    else:
        add_trace_processor(processor)
    return processor


# --------------------------------------------------------------------------- #
# HITL wrapper — Runner.run lifecycle (suspend / resume / approvals)
# --------------------------------------------------------------------------- #


def _approval_args(interruption: Any) -> Any:
    """The arguments captured on a ``ToolApprovalItem`` (for the request payload)."""
    return getattr(interruption, "arguments", None)


def _approval_decisions(run_state: Any) -> dict[str, bool]:
    """The customer's approve/reject decisions recorded on a resumed ``RunState``.

    ``RunState._context._approvals`` maps ``tool_name -> _ApprovalRecord(approved=[ids],
    rejected=[ids])``. This is the *observed* decision (set by the customer's
    ``state.approve``/``state.reject`` before they passed the state back) — so the
    witness records what it can see, rather than guessing from a rejection-sentinel
    string in the output. Returns ``{call_id: granted}``.
    """
    out: dict[str, bool] = {}
    ctx = getattr(run_state, "_context", None)
    approvals = getattr(ctx, "_approvals", None) if ctx is not None else None
    if isinstance(approvals, dict):
        for record in approvals.values():
            for cid in getattr(record, "approved", None) or []:
                if isinstance(cid, str):
                    out[cid] = True
            for cid in getattr(record, "rejected", None) or []:
                if isinstance(cid, str):
                    out[cid] = False
    return out


class _WrappedRunner:
    """Wraps a ``Runner`` so each ``run()`` is bracketed by Runfile lifecycle events.

    Tracing captures execution; this wrapper adds the one thing tracing does not model —
    the human pause: ``run_suspend`` / ``tool_approval_requested`` when ``Runner.run``
    returns with ``result.interruptions``, and ``run_resume`` + ``tool_approval_granted``
    / ``tool_approval_denied`` when the caller resumes by passing the ``RunState`` back.

    The wrapper owns the trace (``with trace(trace_id=…)``) so it and the processor share
    a ``trace_id`` key; on resume the incoming ``RunState`` carries that same id, which is
    how a resume is matched to its suspended run.
    """

    def __init__(self, runner: Any, *, conversation_id: Optional[str]) -> None:
        self._runner = runner
        self._conversation_id = conversation_id

    async def run(self, agent: Any, input: Any, **kwargs: Any) -> Any:
        if not _active():
            return await self._runner.run(agent, input, **kwargs)
        from agents import RunState, gen_trace_id, trace

        is_resume = isinstance(input, RunState)
        tid = None
        if is_resume:
            tid = getattr(getattr(input, "_trace_state", None), "trace_id", None)
        tid = tid or gen_trace_id()

        st = _registry.get_or_create(tid)
        st.lifecycle_managed = True
        prior = list(st.pending_interruptions) if is_resume else []

        if is_resume and st.active_run is not None:
            # Resume the suspended run and record each prior approval decision BEFORE the
            # tools re-execute — read from the RunState the customer just resolved, so the
            # grant/deny sits correctly ahead of the (loose) tool execution in the segment.
            decisions = _approval_decisions(input)
            with _bound(st.active_run, st):
                resume_run(triggered_by="human_approval_granted", correlation_token=tid)
                for inter in prior:
                    cid = getattr(inter, "call_id", None)
                    granted = decisions.get(cid, True) if isinstance(cid, str) else True
                    capture_event(
                        kind="tool_approval_granted" if granted else "tool_approval_denied",
                        name=str(getattr(inter, "tool_name", None) or "tool"),
                    )
            st.suspended = False
            st.pending_interruptions = []

        with trace(workflow_name="runfile", trace_id=tid, group_id=self._conversation_id):
            result = await self._runner.run(agent, input, **kwargs)

        after = _registry.get(tid)
        if after is None:
            return result
        run = after.active_run
        interruptions = list(getattr(result, "interruptions", None) or [])
        if run is not None and interruptions:
            self._suspend(run, after, interruptions, tid)
        elif run is not None:
            emit_run_end(run, outcome="success")
            _registry.drop(tid)
        return result

    def run_streamed(self, agent: Any, input: Any, **kwargs: Any) -> Any:
        """Streamed runs delegate to the underlying runner unwrapped.

        The streamed result is consumed by the caller via ``stream_events()``, and the
        SDK opens its own trace for the duration. Because the wrapper does NOT create that
        trace, it is not ``lifecycle_managed`` — so the processor's *auto-lifecycle*
        captures the full run (``run_create`` at the first agent span, ``run_end`` at
        trace end) plus all execution. The only gap vs the non-streamed path is HITL
        bracketing (``run_suspend`` / ``run_resume`` for a streamed tool-approval pause),
        which needs the wrapper to await stream completion before inspecting
        ``interruptions`` — a documented follow-up.
        """
        return self._runner.run_streamed(agent, input, **kwargs)

    def _suspend(
        self, run: Run, st: _TraceState, interruptions: list[Any], correlation_token: Optional[str] = None
    ) -> None:
        with _bound(run, st):
            for it in interruptions:
                capture_event(
                    kind="tool_approval_requested",
                    name=str(getattr(it, "tool_name", None) or "tool"),
                    payload=_approval_args(it),
                )
            suspend_run(
                reason="awaiting_human_approval",
                name="tool_approval_required",
                detection_source="framework_inferred",
                framework_signal={
                    "framework": _FRAMEWORK,
                    "signal_name": "result.interruptions",
                },
                correlation_token=correlation_token,
            )
        st.suspended = True
        st.pending_interruptions = list(interruptions)


def instrument_runner(
    runner: Any,
    *,
    agent_identity: str,
    conversation_id: Optional[str] = None,
    agent_identity_map: Optional[dict[str, str]] = None,
    agent_model_map: Optional[dict[str, str]] = None,
    replace_processors: bool = True,
) -> Any:
    """Register the Runfile processor AND return a ``Runner`` wrapper that adds HITL
    lifecycle (suspend / resume / approvals) around ``run()``.

    Use this (rather than :func:`instrument`) when the agent has human-in-the-loop tool
    approvals::

        from runfile_ai.integrations import openai_agents as runfile_openai
        from agents import Runner

        runner = runfile_openai.instrument_runner(
            Runner, agent_identity="did:web:bank.com:agents:triage:v1"
        )
        result = await runner.run(agent, "…")
        while result.interruptions:          # suspend captured on the line above
            state = result.to_state()
            for it in result.interruptions:
                state.approve(it)            # or state.reject(it)
            result = await runner.run(agent, state)   # resume captured here

    Plain (non-HITL) runs work too — they just never suspend. See :func:`instrument` for
    the ``replace_processors`` / sensitive-data notes.
    """
    build_and_register = instrument(
        agent_identity=agent_identity,
        conversation_id=conversation_id,
        agent_identity_map=agent_identity_map,
        agent_model_map=agent_model_map,
        replace_processors=replace_processors,
    )
    _ = build_and_register  # the processor; registered as a side effect
    return _WrappedRunner(runner, conversation_id=conversation_id)
