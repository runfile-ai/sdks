"""LangGraph adapter (``langgraph`` 1.x, built on ``langchain-core``).

Verified against the **published** ``langgraph`` / ``langchain_core`` types (see
the inline notes), NOT the design draft — the draft was right that LangGraph 1.x
exposes a ``GraphCallbackHandler`` with ``on_interrupt`` / ``on_resume``, but those
hooks live only on the **sync** handler (``langchain_core.AsyncCallbackHandler`` has
no ``on_interrupt``), so this adapter's handler is sync. Our capture is non-blocking
(metadata + buffer append), so a sync handler is the correct, simplest choice.

One handler subclassing ``GraphCallbackHandler`` receives the whole surface:

- ``on_chain_start`` with ``parent_run_id is None`` → the run begins (witness-authored
  ``run_create`` event + companion item). The final ``on_chain_end`` with
  ``parent_run_id is None`` → the run ends (``run_end``).
- ``on_chat_model_start`` / ``on_llm_end`` → one ``llm_call`` per model turn. Unlike
  the Claude CLI (which streams a turn as several messages), LangChain fires these
  once per turn, so no coalescing is needed. ``model_ref`` is filled from the
  ``AIMessage``'s ``response_metadata`` (model id/provider) + ``usage_metadata``.
- ``on_tool_start`` / ``on_tool_end`` → ``tool_call`` / ``tool_result``. The
  originating ``tool_call_id`` arrives in ``on_tool_start``'s kwargs and the tool
  ``run_id`` is stable across start↔end, so we resolve causality from the framework's
  own ids: each ``tool_call`` is parented on the ``llm_call`` that issued it (matched
  by ``tool_call_id`` against the issuing ``AIMessage.tool_calls``), and each
  ``tool_result`` on its ``tool_call`` (matched by tool ``run_id``). When one
  ``AIMessage`` issues ≥2 tool calls, the flusher groups them into one
  ``parallel_group_id`` (they share an ``llm_call`` parent — the API's own statement
  that the calls were dispatched together).
- ``on_interrupt`` (GraphInterruptEvent) → ``run_suspend`` (``framework_inferred``,
  ``awaiting_human_input``) + ``run_update`` → ``awaiting_human``. ``on_resume``
  (GraphResumeEvent) → ``run_resume`` (new segment) + ``run_update`` → ``active``.
- ``on_tool_error`` → ``tool_result`` (outcome ``failure``); ``on_chain_error`` at the
  root → ``run_end`` (outcome ``failure``).

**Same-run resume vs new run (the HITL trap).** On ``interrupt()`` the root chain
*still* fires ``on_chain_end`` (with no ``__interrupt__`` in its outputs), so we track
a per-run suspended flag and skip ``run_end`` while suspended. The resuming
``ainvoke(Command(resume=...))`` spawns a **new** root ``run_id`` but reuses the same
``metadata.thread_id`` — so we key runs by ``thread_id`` to route the resume's events
into the existing suspended run (a new segment via ``run_resume``), never a duplicate
run. Runs with no ``thread_id`` (no checkpointer) are independent per invocation.

**Causal binding across tasks.** LangGraph may dispatch callbacks on different
asyncio tasks, so we never rely on ambient ``contextvars`` persisting *between*
callbacks: runs are keyed in a process-global registry by the LangChain root
``run_id`` (and ``thread_id``), and each callback binds the core's ambient context
for the duration of one capture (see :func:`_bound`), writing parent continuity back
onto the ``Run``. This mirrors the Claude adapter.

Every callback degrades to a no-op if the SDK isn't initialised, and swallows its
own exceptions: an observer must never break the customer's graph.

What v1 does NOT capture (deliberately, to keep the trail high-signal and correct):
``graph_node_enter`` / ``graph_node_exit`` (every nested Runnable fires a chain
start/end — node-boundary capture is a documented follow-up), subgraph ``delegate``
events (reliable subgraph detection from callback metadata is a follow-up), and
explicit fork / ``continued_from`` (LangGraph "time travel").

Known edge — **node re-execution on interrupt resume.** LangGraph re-runs an
interrupted node from the top on resume, so an ``interrupt()`` raised *inside a
tool* makes that tool's pre-interrupt ``tool_call`` appear in both the suspended
and the resumed segment (the first attempt has no ``tool_result``; the resumed one
completes). Both attempts share the issuing ``llm_call``, so the flusher tags them
as a (size-2) parallel group. The chain, ordering, and lifecycle stay correct — it
is the documented "interrupt restarts the node" behaviour; content-hash dedup of
re-executed pre-interrupt events is a follow-up. (Interrupts in a dedicated node,
and blocking-tool HITL that never raises an interrupt, don't hit this.)
"""

from __future__ import annotations

import hashlib
import json
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional
from uuid import UUID

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
    expected_resumer_from,
    resume_run,
    suspend_run,
)

_FRAMEWORK = "langgraph"

# langchain `ls_provider` / AIMessage.response_metadata.model_provider → the schema's
# ModelProviderEnum. Anything unrecognised maps to "other" (model_ref still validates).
_PROVIDER_MAP = {
    "anthropic": "anthropic",
    "openai": "openai",
    "azure_openai": "azure_openai",
    "azure": "azure_openai",
    "google_genai": "google",
    "google_vertexai": "google",
    "google": "google",
    "bedrock": "aws_bedrock",
    "amazon_bedrock": "aws_bedrock",
    "aws": "aws_bedrock",
    "ollama": "ollama",
}


def _active() -> bool:
    """True when the SDK is initialised and not disabled.

    Callbacks must degrade to a transparent no-op otherwise — never crash the
    customer's graph because Runfile wasn't set up.
    """
    inst = get_instance()
    return inst is not None and not inst.disabled


def _buffer() -> Any:
    """The active SDK buffer, or ``None`` when the SDK isn't initialised.

    Used to drive turn-atomic flushing (arm + advance the watermark) so a model
    turn's events stay whole across the flusher's 2s cadence — letting the flusher
    group a turn's concurrent tool fan-out deterministically.
    """
    inst = get_instance()
    return inst.buffer if inst is not None else None


def _sha256_hex(value: Any) -> str:
    raw = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _is_control_flow(error: BaseException) -> bool:
    """True if ``error`` is a LangGraph control-flow signal, not a real failure.

    ``interrupt()`` (and ``Command`` bubbling, parent commands) raise exceptions
    subclassing ``GraphBubbleUp`` that propagate through tool/chain error callbacks —
    e.g. ``interrupt()`` inside a tool surfaces as ``on_tool_error(GraphInterrupt)``.
    Those are suspensions, not failures, so we must not record a failed ``tool_result``
    or end the run; the ``on_interrupt`` hook handles the suspension. Falls back to a
    class-name check if ``langgraph.errors`` can't be imported.
    """
    try:
        from langgraph.errors import GraphBubbleUp

        if isinstance(error, GraphBubbleUp):
            return True
    except Exception:
        pass
    return type(error).__name__ in {
        "GraphInterrupt",
        "GraphBubbleUp",
        "NodeInterrupt",
        "ParentCommand",
        "GraphDelegate",
    }


def _provider_of(raw: Optional[str]) -> str:
    if not raw:
        return "other"
    return _PROVIDER_MAP.get(raw.lower(), "other")


# --------------------------------------------------------------------------- #
# Per-run causal scratch + run registry (keyed by framework cursor, not ambient)
# --------------------------------------------------------------------------- #


@dataclass
class _CausalLinks:
    """Per-run scratch mapping LangChain's native ids onto the event DAG.

    We never infer causality from arrival order — LangChain states it via ids:
    ``tool_call_id`` (on the issuing ``AIMessage.tool_calls`` and in
    ``on_tool_start``'s kwargs) and the stable tool ``run_id`` (same on a tool's
    start and end). We map those onto ``parent_event_id`` so a ``tool_call`` parents
    on its issuing ``llm_call`` and a ``tool_result`` on its ``tool_call``. Concurrency
    grouping is decided downstream by the flusher (tool_calls sharing one issuer).
    """

    #: tool_call_id → the issuing ``llm_call`` event_id (a ``tool_call``'s parent)
    issuer_event: dict[str, str] = field(default_factory=dict)
    #: tool_call_id → the ``tool_call`` event_id (so its ``tool_result`` parents on it)
    call_event: dict[str, str] = field(default_factory=dict)
    #: tool run_id → tool_call_id (correlate on_tool_end back to on_tool_start)
    tool_run_tcid: dict[UUID, str] = field(default_factory=dict)
    #: llm run_id → model identity captured at on_chat_model_start (provider/model_id)
    llm_meta: dict[UUID, dict[str, str]] = field(default_factory=dict)
    #: a turn has been emitted, so the flusher watermark is armed (turn-atomic)
    turn_emitted: bool = False
    #: Serialises this run's capture critical section. LangGraph runs concurrent
    #: tool calls on a threadpool (sync tools under ``ainvoke``), so two ``on_tool_*``
    #: callbacks for the same run fire on different threads at once. The core's
    #: ``local_seq`` assignment + buffer append (in ``capture_event``) are not atomic,
    #: so without this they can interleave → events land out of ``local_seq`` order →
    #: server ``sequence_gap`` / ``chain_break``. Held only for the in-memory capture
    #: (no I/O), so contention is microseconds; reentrant for safety.
    lock: "threading.RLock" = field(default_factory=threading.RLock)


class _Registry:
    """Process-global map of LangChain root ``run_id`` / ``thread_id`` → Runfile Run."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._runs: dict[UUID, Run] = {}  # root run_id → Run
        self._root_of: dict[UUID, UUID] = {}  # any run_id → its root run_id
        self._thread_root: dict[str, UUID] = {}  # thread_id → current root run_id
        self._run_thread: dict[str, str] = {}  # Runfile run_id → thread_id (resume handle)
        self._suspended: set[str] = set()  # Runfile run_ids currently suspended
        self._links: dict[str, _CausalLinks] = {}  # Runfile run_id → scratch

    # ---- root resolution ------------------------------------------------- #

    def resolve_root(self, run_id: UUID, parent_run_id: Optional[UUID]) -> UUID:
        """Memoised root for any callback's run_id (the run with parent_run_id=None).

        Node/llm/tool runs are descendants of the root; their parent chain's root is
        already recorded (chain_start fires outermost-first), so we inherit it.
        """
        with self._lock:
            existing = self._root_of.get(run_id)
            if existing is not None:
                return existing
            if parent_run_id is None:
                root = run_id
            else:
                root = self._root_of.get(parent_run_id, parent_run_id)
            self._root_of[run_id] = root
            return root

    def run_for(self, run_id: UUID, parent_run_id: Optional[UUID]) -> Optional[Run]:
        root = self.resolve_root(run_id, parent_run_id)
        with self._lock:
            return self._runs.get(root)

    def run_for_root(self, root: Optional[UUID]) -> Optional[Run]:
        if root is None:
            return None
        with self._lock:
            return self._runs.get(self._root_of.get(root, root))

    # ---- lifecycle ------------------------------------------------------- #

    def begin_root(
        self,
        root: UUID,
        *,
        thread_id: Optional[str],
        agent_identity: str,
        conversation_id: Optional[str],
    ) -> tuple[Run, bool]:
        """Return ``(run, resumed)`` for a top-level chain start.

        If ``thread_id`` maps to a suspended run, this invocation is a resume
        continuation: bind the new root to that existing run and return
        ``resumed=True`` (no new run). Otherwise create a fresh witness run.
        """
        with self._lock:
            if thread_id is not None:
                prior_root = self._thread_root.get(thread_id)
                if prior_root is not None:
                    run = self._runs.get(prior_root)
                    if run is not None and run.run_id in self._suspended:
                        self._root_of[root] = prior_root  # route this invoke's events
                        return run, True
        run = create_run(
            agent_identity=agent_identity,
            conversation_id=conversation_id or thread_id,
            framework=_FRAMEWORK,
            labels={"langgraph_thread_id": thread_id} if thread_id else None,
        )
        with self._lock:
            self._runs[root] = run
            self._root_of[root] = root
            if thread_id is not None:
                self._thread_root[thread_id] = root
                self._run_thread[run.run_id] = thread_id
        return run, False

    def thread_for(self, run: Run) -> Optional[str]:
        """The LangGraph ``thread_id`` (resume handle) this run was invoked under, if any."""
        with self._lock:
            return self._run_thread.get(run.run_id)

    def mark_suspended(self, run: Run) -> None:
        with self._lock:
            self._suspended.add(run.run_id)

    def mark_resumed(self, run: Run) -> None:
        with self._lock:
            self._suspended.discard(run.run_id)

    def is_suspended(self, run: Run) -> bool:
        with self._lock:
            return run.run_id in self._suspended

    def end_root_for_run(self, run: Run) -> None:
        """Forget a run and its scratch once it has ended (by Runfile run_id)."""
        with self._lock:
            roots = [root for root, r in self._runs.items() if r is run]
            for root in roots:
                self._runs.pop(root, None)
            self._links.pop(run.run_id, None)
            self._suspended.discard(run.run_id)
            self._run_thread.pop(run.run_id, None)
            for tid, r in list(self._thread_root.items()):
                if r in roots:
                    del self._thread_root[tid]
            for rid, root in list(self._root_of.items()):
                if root in roots:
                    del self._root_of[rid]

    def links_for(self, run: Run) -> _CausalLinks:
        with self._lock:
            links = self._links.get(run.run_id)
            if links is None:
                links = _CausalLinks()
                self._links[run.run_id] = links
            return links

    def reset(self) -> None:
        with self._lock:
            self._runs.clear()
            self._root_of.clear()
            self._thread_root.clear()
            self._suspended.clear()
            self._links.clear()


_registry = _Registry()


@contextmanager
def _bound(run: Run) -> Iterator[None]:
    """Bind ``run`` (and its last parent event) as ambient context for one capture,
    holding the run's capture lock for the duration.

    The lock serialises capture per run: LangGraph dispatches concurrent tool calls
    on a threadpool, so two callbacks for the same run can run this block at once.
    Holding the lock makes the enclosed ``capture_event`` (``local_seq`` assignment +
    buffer append) atomic, so events are always buffered in ``local_seq`` order — and
    the shared ``run.last_parent_event_id`` update below is race-free too. The block
    does only in-memory work (no I/O), so the critical section is microseconds.

    Persists the new parent back onto the run on exit, so the linear ``llm_call``
    spine survives even when the next callback fires on an unrelated thread/task.
    """
    with _registry.links_for(run).lock:
        run_token = set_current_run(run)
        parent_token = set_parent_event(run.last_parent_event_id)
        try:
            yield
        finally:
            run.last_parent_event_id = current_parent_event()
            reset_parent_event(parent_token)
            reset_current_run(run_token)


# --------------------------------------------------------------------------- #
# Translation helpers
# --------------------------------------------------------------------------- #


def _model_ref_from_response(message: Any, start_meta: Optional[dict[str, str]]) -> dict[str, Any]:
    """Build ``model_ref`` from an ``AIMessage`` + the start-time model metadata.

    Provider/model id come from the message's ``response_metadata`` (what the
    provider actually returned), falling back to the ``ls_provider`` / ``ls_model_name``
    captured at ``on_chat_model_start``. Tokens come from ``usage_metadata``.
    """
    resp_meta = getattr(message, "response_metadata", None) or {}
    start_meta = start_meta or {}
    model_id = resp_meta.get("model_name") or start_meta.get("model_id") or "unknown"
    provider_raw = resp_meta.get("model_provider") or start_meta.get("provider")
    model_ref: dict[str, Any] = {
        "provider": _provider_of(provider_raw),
        "model_id": str(model_id)[:128],
    }
    usage = getattr(message, "usage_metadata", None)
    if isinstance(usage, dict):
        if isinstance(usage.get("input_tokens"), int):
            model_ref["input_tokens"] = usage["input_tokens"]
        if isinstance(usage.get("output_tokens"), int):
            model_ref["output_tokens"] = usage["output_tokens"]
    return model_ref


def _message_content(message: Any) -> Any:
    """A capturable view of an AIMessage's content (text, or a list of blocks)."""
    content = getattr(message, "content", None)
    return content if content is not None else ""


def _tool_calls_of(message: Any) -> list[dict[str, Any]]:
    calls = getattr(message, "tool_calls", None)
    return calls if isinstance(calls, list) else []


def _first_message(response: Any) -> Any:
    """The ``AIMessage`` from an ``LLMResult`` (``generations[0][0].message``), or None.

    Chat models populate ``generation.message``; bare completion models don't, so we
    fall back to None (the turn is still chained, just without a model message).
    """
    try:
        generations = getattr(response, "generations", None)
        if not generations or not generations[0]:
            return None
        return getattr(generations[0][0], "message", None)
    except Exception:
        return None


def _tool_output_payload(output: Any) -> Any:
    """Render a tool result (often a ``ToolMessage``) to a capturable payload."""
    content = getattr(output, "content", None)
    if content is not None:
        return content
    return output


# --------------------------------------------------------------------------- #
# The callback handler
# --------------------------------------------------------------------------- #


def _resolve_base() -> Any:
    """The handler base class: ``GraphCallbackHandler`` (1.x, gives interrupt/resume)
    if available, else ``BaseCallbackHandler`` (older LangGraph — no graph hooks).

    Imported lazily so importing this module never requires ``langgraph`` /
    ``langchain_core`` to be installed (they're an optional extra).
    """
    try:
        from langgraph.callbacks import GraphCallbackHandler

        return GraphCallbackHandler
    except Exception:
        from langchain_core.callbacks import BaseCallbackHandler

        return BaseCallbackHandler


def build_handler(agent_identity: str, conversation_id: Optional[str] = None) -> Any:
    """Construct and return the Runfile LangGraph callback handler instance.

    Advanced callers who manage ``config["callbacks"]`` themselves can use this
    directly; :func:`instrument` wraps it onto a graph for the one-line path.

    The handler class is built lazily over whichever base is installed so the graph
    interrupt/resume hooks are picked up on LangGraph 1.x and gracefully absent on
    older versions.
    """
    base = _resolve_base()

    class _RunfileGraphHandler(base):  # type: ignore[misc, valid-type]
        """Sync handler: translates LangChain/LangGraph callbacks into Runfile events."""

        #: never abort the customer's graph if a callback raises
        raise_error = False

        def __init__(self) -> None:
            self.agent_identity = agent_identity
            self.conversation_id = conversation_id

        # ---- run lifecycle ---------------------------------------------- #

        def on_chain_start(
            self,
            serialized: dict[str, Any],
            inputs: dict[str, Any],
            *,
            run_id: UUID,
            parent_run_id: Optional[UUID] = None,
            tags: Optional[list[str]] = None,
            metadata: Optional[dict[str, Any]] = None,
            **kwargs: Any,
        ) -> None:
            if not _active():
                return
            if parent_run_id is not None:
                # Only the top-level chain is a run boundary; inner chains (node
                # runnables) are not captured as events in v1 — but we memoise their
                # root so descendant llm / tool runs resolve to the right run.
                self._registry_resolve(run_id, parent_run_id)
                return
            try:
                thread_id = (metadata or {}).get("thread_id")
                _registry.begin_root(
                    run_id,
                    thread_id=str(thread_id) if thread_id is not None else None,
                    agent_identity=self.agent_identity,
                    conversation_id=self.conversation_id,
                )
            except Exception:
                pass

        def on_chain_end(
            self,
            outputs: dict[str, Any],
            *,
            run_id: UUID,
            parent_run_id: Optional[UUID] = None,
            **kwargs: Any,
        ) -> None:
            if parent_run_id is not None or not _active():
                return
            try:
                run = _registry.run_for_root(run_id)
                if run is None:
                    return
                # A graph that interrupted still fires the root chain_end; that's a
                # pause, not an end — leave the run suspended for the resume invoke.
                if _registry.is_suspended(run):
                    return
                self._emit_run_end(run, outcome="success")
            except Exception:
                pass

        def on_chain_error(
            self,
            error: BaseException,
            *,
            run_id: UUID,
            parent_run_id: Optional[UUID] = None,
            **kwargs: Any,
        ) -> None:
            if parent_run_id is not None or not _active():
                return
            if _is_control_flow(error):
                return  # an interrupt bubbling through the root chain — not a failure
            try:
                run = _registry.run_for_root(run_id)
                if run is not None and not _registry.is_suspended(run):
                    self._emit_run_end(run, outcome="failure")
            except Exception:
                pass

        # ---- llm turns --------------------------------------------------- #

        def on_chat_model_start(
            self,
            serialized: dict[str, Any],
            messages: Any,
            *,
            run_id: UUID,
            parent_run_id: Optional[UUID] = None,
            tags: Optional[list[str]] = None,
            metadata: Optional[dict[str, Any]] = None,
            **kwargs: Any,
        ) -> None:
            self._stash_model_meta(run_id, parent_run_id, metadata)

        def on_llm_start(
            self,
            serialized: dict[str, Any],
            prompts: list[str],
            *,
            run_id: UUID,
            parent_run_id: Optional[UUID] = None,
            tags: Optional[list[str]] = None,
            metadata: Optional[dict[str, Any]] = None,
            **kwargs: Any,
        ) -> None:
            # Non-chat (completion) models route here; same model-meta stash.
            self._stash_model_meta(run_id, parent_run_id, metadata)

        def on_llm_end(
            self,
            response: Any,
            *,
            run_id: UUID,
            parent_run_id: Optional[UUID] = None,
            **kwargs: Any,
        ) -> None:
            if not _active():
                return
            try:
                run = _registry.run_for(run_id, parent_run_id)
                if run is None:
                    return
                message = _first_message(response)
                if message is None:
                    return
                links = _registry.links_for(run)
                start_meta = links.llm_meta.pop(run_id, None)
                model_ref = _model_ref_from_response(message, start_meta)

                # Turn-atomic flushing: a new llm turn begins, so the PREVIOUS turn
                # (its llm_call + the tools it issued) is complete — release it; this
                # turn becomes the new held tail kept whole for the flusher's grouping.
                buf = _buffer()
                if buf is not None:
                    if not links.turn_emitted:
                        buf.arm_turn_atomic()
                        links.turn_emitted = True
                    else:
                        buf.advance_flush_watermark()

                with _bound(run):
                    llm_event_id = capture_event(
                        kind="llm_call",
                        name="chat",
                        model_ref=model_ref,
                        payload={"content": _message_content(message)},
                    )
                # Map each tool_call this turn issued onto this llm_call, so the
                # tool runs parent on it (and ≥2 siblings get grouped by the flusher).
                if llm_event_id:
                    for tc in _tool_calls_of(message):
                        tcid = tc.get("id")
                        if tcid:
                            links.issuer_event[tcid] = llm_event_id
            except Exception:
                pass

        # ---- tools ------------------------------------------------------- #

        def on_tool_start(
            self,
            serialized: dict[str, Any],
            input_str: str,
            *,
            run_id: UUID,
            parent_run_id: Optional[UUID] = None,
            tags: Optional[list[str]] = None,
            metadata: Optional[dict[str, Any]] = None,
            inputs: Optional[dict[str, Any]] = None,
            **kwargs: Any,
        ) -> None:
            if not _active():
                return
            try:
                run = _registry.run_for(run_id, parent_run_id)
                if run is None:
                    return
                links = _registry.links_for(run)
                tcid = kwargs.get("tool_call_id")
                name = (serialized or {}).get("name") or kwargs.get("name") or "tool"
                parent_kwargs: dict[str, Any] = {}
                if tcid and tcid in links.issuer_event:
                    parent_kwargs["parent_event_id"] = links.issuer_event[tcid]
                with _bound(run):
                    event_id = capture_event(
                        kind="tool_call",
                        name=name,
                        payload=inputs if inputs is not None else input_str,
                        **parent_kwargs,
                    )
                if event_id and tcid:
                    links.call_event[tcid] = event_id
                    links.tool_run_tcid[run_id] = tcid
            except Exception:
                pass

        def on_tool_end(
            self,
            output: Any,
            *,
            run_id: UUID,
            parent_run_id: Optional[UUID] = None,
            **kwargs: Any,
        ) -> None:
            self._emit_tool_result(output, run_id, parent_run_id, outcome="success")

        def on_tool_error(
            self,
            error: BaseException,
            *,
            run_id: UUID,
            parent_run_id: Optional[UUID] = None,
            **kwargs: Any,
        ) -> None:
            # An ``interrupt()`` raised inside a tool surfaces here as a GraphInterrupt;
            # that's a suspension (handled by on_interrupt), not a tool failure — the
            # tool re-runs and completes on resume. Drop the bookkeeping so the result
            # isn't recorded as a spurious failure, but keep the call→result mapping
            # clean for the resumed execution.
            if _is_control_flow(error):
                self._discard_tool_run(run_id, parent_run_id)
                return
            self._emit_tool_result(
                {"error": str(error)}, run_id, parent_run_id, outcome="failure"
            )

        # ---- graph interrupt / resume (LangGraph 1.x GraphCallbackHandler) -- #

        def on_interrupt(self, event: Any) -> None:
            if not _active():
                return
            try:
                run = _registry.run_for_root(getattr(event, "run_id", None))
                if run is None:
                    return
                interrupts = getattr(event, "interrupts", ()) or ()
                values = [getattr(i, "value", None) for i in interrupts]
                signal = {
                    "framework": _FRAMEWORK,
                    "signal_name": "__interrupt__",
                    "signal_payload_hash": _sha256_hex(values),
                }
                with _bound(run):
                    suspend_run(
                        reason="awaiting_human_input",
                        name="__interrupt__",
                        detection_source="framework_inferred",
                        framework_signal=signal,
                        correlation_token=_registry.thread_for(run),
                        # Passive: if the agent named where it escalated (a queue/role
                        # in the interrupt value), record it as the expected resumer.
                        expected_resumer=expected_resumer_from(values),
                    )
                _registry.mark_suspended(run)
                buf = _buffer()
                if buf is not None:
                    buf.advance_flush_watermark()  # release up to & incl. the suspend
            except Exception:
                pass

        def on_resume(self, event: Any) -> None:
            if not _active():
                return
            try:
                run = _registry.run_for_root(getattr(event, "run_id", None))
                if run is None or not _registry.is_suspended(run):
                    return
                with _bound(run):
                    resume_run(
                        triggered_by="human_input_received",
                        correlation_token=_registry.thread_for(run),
                    )
                _registry.mark_resumed(run)
            except Exception:
                pass

        # ---- internals --------------------------------------------------- #

        def _registry_resolve(self, run_id: UUID, parent_run_id: Optional[UUID]) -> None:
            # Memoise this inner chain's root so descendant llm/tool runs resolve.
            try:
                _registry.resolve_root(run_id, parent_run_id)
            except Exception:
                pass

        def _stash_model_meta(
            self, run_id: UUID, parent_run_id: Optional[UUID], metadata: Optional[dict[str, Any]]
        ) -> None:
            if not _active():
                return
            try:
                run = _registry.run_for(run_id, parent_run_id)
                if run is None:
                    return
                meta = metadata or {}
                stash: dict[str, str] = {}
                if meta.get("ls_provider"):
                    stash["provider"] = str(meta["ls_provider"])
                if meta.get("ls_model_name"):
                    stash["model_id"] = str(meta["ls_model_name"])
                if stash:
                    _registry.links_for(run).llm_meta[run_id] = stash
            except Exception:
                pass

        def _discard_tool_run(self, run_id: UUID, parent_run_id: Optional[UUID]) -> None:
            # Clear scratch for a tool run that suspended instead of returning (its
            # already-buffered tool_call is left as an honest "invoked-then-interrupted"
            # record; the tool re-runs with a fresh run_id on resume and re-maps).
            if not _active():
                return
            try:
                run = _registry.run_for(run_id, parent_run_id)
                if run is None:
                    return
                links = _registry.links_for(run)
                tcid = links.tool_run_tcid.pop(run_id, None)
                if tcid is not None:
                    links.call_event.pop(tcid, None)
            except Exception:
                pass

        def _emit_tool_result(
            self,
            payload: Any,
            run_id: UUID,
            parent_run_id: Optional[UUID],
            *,
            outcome: str,
        ) -> None:
            if not _active():
                return
            try:
                run = _registry.run_for(run_id, parent_run_id)
                if run is None:
                    return
                links = _registry.links_for(run)
                tcid = links.tool_run_tcid.pop(run_id, None)
                parent_kwargs: dict[str, Any] = {}
                name = "tool"
                if tcid is not None:
                    parent = links.call_event.pop(tcid, None)
                    if parent is not None:
                        parent_kwargs["parent_event_id"] = parent
                with _bound(run):
                    capture_event(
                        kind="tool_result",
                        name=name,
                        payload=_tool_output_payload(payload),
                        action_extra={"outcome": outcome},
                        **parent_kwargs,
                    )
            except Exception:
                pass

        def _emit_run_end(self, run: Run, *, outcome: str) -> None:
            # Flush the final turn before the run ends, then close the run (witness
            # run_end event + companion item) and release everything held.
            emit_run_end(run, outcome=outcome)
            _registry.end_root_for_run(run)
            buf = _buffer()
            if buf is not None:
                buf.advance_flush_watermark()

    return _RunfileGraphHandler()


def instrument(
    graph: Any, *, agent_identity: str, conversation_id: Optional[str] = None
) -> Any:
    """Register the Runfile callback handler on ``graph`` and return the wrapped graph.

    One line of customer code — the returned graph is used exactly as before::

        from runfile_ai.integrations import langgraph as runfile_langgraph

        agent = runfile_langgraph.instrument(
            agent, agent_identity="did:web:bank.com:agents:loan-triage:v2"
        )
        await agent.ainvoke({"messages": [...]}, config={"configurable": {"thread_id": "t1"}})

    Implemented via ``graph.with_config({"callbacks": [...]})`` so every later
    ``invoke`` / ``ainvoke`` / ``stream`` carries the handler. ``with_config`` returns
    a new runnable and does not mutate the original. If the SDK isn't initialised the
    handler's callbacks are transparent no-ops, so instrumenting is always safe.
    """
    handler = build_handler(agent_identity, conversation_id)
    return graph.with_config({"callbacks": [handler]})
