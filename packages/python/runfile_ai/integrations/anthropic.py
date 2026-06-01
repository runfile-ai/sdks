"""Claude Agent SDK adapter (``claude-agent-sdk``, Anthropic's agent SDK).

Verified against the published SDK types (``claude_agent_sdk.types``), NOT the
design draft — two facts there diverge from an earlier sketch:

1. **There is no ``SessionStart`` / ``SessionEnd`` hook in the Python SDK.** The
   ``HookEvent`` literal is PreToolUse, PostToolUse, PostToolUseFailure,
   UserPromptSubmit, Stop, SubagentStart, SubagentStop, PreCompact, Notification,
   PermissionRequest. So a run's lifecycle can't be hook-driven. The primary path
   is :func:`observe_query`, which wraps ``query()`` and owns run create/end;
   ``session_id`` (present on every hook input and on ``ResultMessage`` /
   ``AssistantMessage``) is the run key.

2. **Hooks may be dispatched off the wrapper's async context**, so we cannot rely
   on ``contextvars`` carrying run/parent state *between* hook calls. Runs are
   keyed by ``session_id`` (and subagents by ``(session_id, agent_id)``) in a
   process-global registry; each callback binds the core's ambient context for the
   duration of one capture (see :func:`_bound`) and writes parent continuity back
   onto the ``Run``.

Translation:

- ``PreToolUse`` → ``tool_call`` (payload = ``tool_input``)
- ``PostToolUse`` → ``tool_result`` (payload = ``tool_response``, outcome success)
- ``PostToolUseFailure`` → ``tool_result`` (outcome failure, error in payload)
- ``PermissionRequest`` → ``tool_approval_requested`` (observe-only — the hook
  returns ``{}`` and never decides)
- ``can_use_tool`` wrapper → ``tool_approval_granted`` / ``tool_approval_denied``
  around the customer's own decision (installed ONLY when the customer already
  supplies a ``can_use_tool``; absent one we don't fabricate a decision)
- ``Notification`` ``permission_prompt`` → ``run_suspend`` (awaiting_human_approval);
  ``idle_prompt`` / ``elicitation_dialog`` → ``run_suspend`` (awaiting_human_input);
  ``elicitation_response`` → ``run_resume``
- ``SubagentStart`` → ``delegate`` on the parent + a child run with ``delegated_from``;
  ``SubagentStop`` → end the child run
- ``AssistantMessage`` (seen by :func:`observe_query`) → ``llm_call`` with ``model_ref``.
  The CLI streams one model turn as several ``AssistantMessage``s (Thinking / text /
  ToolUse) sharing a ``message_id``; these are coalesced into ONE ``llm_call`` per
  turn (tagged with ``labels.claude_message_id``) so the trail records real model
  calls, not stream fragments.

**Causal parenting (the event DAG, not a linear chain).** ``parent_event_id`` is the
OTel-aligned causal primitive, distinct from ``local_seq`` (the ordering ordinal).
We resolve it from the SDK's *own* ids, never from arrival order: the Anthropic
``tool_use_id`` (identical on a tool's call and result) parents each ``tool_result``
on its ``tool_call``, and each ``tool_call`` on the ``llm_call`` whose
``AssistantMessage`` carried that ``tool_use`` block. When one ``AssistantMessage``
carries more than one ``ToolUseBlock`` — the API's own statement that the calls are
concurrent — those events share a ``parallel_group_id``. (Tool-use blocks the CLI
*streams* as separate AssistantMessages are parented correctly but not yet grouped;
regrouping streamed siblings by ``message_id`` is a future flusher-side step.) These
causal edges are set explicitly, so they do not advance the ambient linear parent —
a ``tool_result`` never becomes the parent of the next ``llm_call``.

Every hook returns ``{}`` (an empty ``HookJSONOutput``): the adapter observes, it
never alters the agent's behaviour.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Iterator, Optional

from .._ids import generate_run_id
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

if TYPE_CHECKING:  # pragma: no cover - import only for typing
    pass

_FRAMEWORK = "claude_agent_sdk"

# Minimum claude-agent-sdk that defines every hook event this adapter registers.
# PostToolUseFailure arrived in 0.1.26; Notification, SubagentStart and
# PermissionRequest in 0.1.29 (#545). On older installs the SDK silently ignores
# unknown hook keys, so suspension / subagent / failure capture would vanish with
# no error — we version-check and refuse to start instead.
_MIN_SDK_VERSION: tuple[int, ...] = (0, 1, 29)
_MIN_SDK_VERSION_STR = "0.1.29"
_DIST_NAME = "claude-agent-sdk"


def _installed_sdk_version() -> tuple[int, ...] | None:
    """The installed ``claude-agent-sdk`` version as an int tuple, or None if it
    can't be determined (e.g. an editable/source install without metadata)."""
    try:
        raw = importlib.metadata.version(_DIST_NAME)
    except importlib.metadata.PackageNotFoundError:
        return None
    parts: list[int] = []
    for segment in raw.split("."):
        digits = ""
        for ch in segment:
            if ch.isdigit():
                digits += ch
            else:
                break  # stop at the first non-numeric (e.g. "29rc1" -> 29)
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts) or None


def _check_sdk_version() -> None:
    """Fail loudly if the installed SDK is too old to fire all registered hooks.

    A no-op when the version can't be determined (source installs) — we don't block
    on missing metadata, only on a version we can confirm is too old.
    """
    version = _installed_sdk_version()
    if version is not None and version < _MIN_SDK_VERSION:
        found = ".".join(str(p) for p in version)
        raise RuntimeError(
            f"runfile_ai Claude Agent SDK adapter requires {_DIST_NAME}>="
            f"{_MIN_SDK_VERSION_STR}, but {found} is installed. Releases before "
            f"{_MIN_SDK_VERSION_STR} lack the PostToolUseFailure / Notification / "
            "SubagentStart / PermissionRequest hook events, which the SDK silently "
            "ignores — failure, suspension, and subagent capture would not fire. "
            f"Upgrade with `pip install -U {_DIST_NAME}`."
        )

# Notification.notification_type → how we translate it. Values per the SDK docs;
# unknown types are ignored (not every notification is a lifecycle transition).
_SUSPEND_NOTIFICATIONS = {
    "permission_prompt": "awaiting_human_approval",
    "idle_prompt": "awaiting_human_input",
    "elicitation_dialog": "awaiting_human_input",
}
_RESUME_NOTIFICATIONS = {"elicitation_response": "human_input_received"}


def _active() -> bool:
    """True when the SDK is initialised and not disabled.

    Hooks/observe must degrade to a transparent no-op otherwise — never crash the
    customer's agent because Runfile wasn't set up.
    """
    inst = get_instance()
    return inst is not None and not inst.disabled


# --------------------------------------------------------------------------- #
# Run registry (keyed by framework cursor, not ambient context)
# --------------------------------------------------------------------------- #


@dataclass
class _Turn:
    """A model turn being accumulated before its single ``llm_call`` is emitted.

    The CLI streams one turn (one ``message_id``) as several ``AssistantMessage``s
    — a ThinkingBlock, a text block, ToolUseBlock(s). We gather them all here and
    emit ONE ``llm_call`` carrying the full content and the turn's cumulative usage,
    rather than one event per fragment (which over-counted calls, lost the thinking
    reasoning, and mis-attributed usage). The emission is deferred until the turn's
    first tool fires (or the next turn / stream end), so the single ``llm_call``
    still precedes — and is the parent of — its tool calls.
    """

    message_id: Optional[str]
    model_ref: dict[str, Any]
    content: list[Any] = field(default_factory=list)
    otel: Optional[dict[str, Any]] = None
    tool_use_ids: list[str] = field(default_factory=list)


@dataclass
class _CausalLinks:
    """Per-run scratch mapping the Claude SDK's native ``tool_use_id`` onto the
    causal edges of the event DAG.

    The SDK already states causality via ids — we do not infer it: ``tool_use_id``
    is the Anthropic Messages API correlation id, identical on a tool's call and
    its result. We record those ids here and map them onto ``parent_event_id`` so
    the chain stops asserting the false "previous event is my parent" edge.
    (Concurrency grouping is decided downstream by the flusher from the structural
    call/result ordering — never guessed here from "same turn".)
    """

    #: tool_use_id → the ``tool_call`` event_id (so its ``tool_result`` parents on it)
    call_event: dict[str, str] = field(default_factory=dict)
    #: tool_use_id → the ``llm_call`` event_id that issued it (the ``tool_call``'s parent)
    issuer_event: dict[str, str] = field(default_factory=dict)
    #: The turn being accumulated (lazy emission), and the last EMITTED turn's
    #: ``llm_call`` event_id (the parent a fallback tool call resolves to).
    pending: Optional[_Turn] = None
    current_llm_event_id: Optional[str] = None


class _Registry:
    """Process-global map of Claude ``session_id`` / subagent ``agent_id`` → Run."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[str, Run] = {}
        self._subagents: dict[tuple[str, str], Run] = {}
        # Causal-link scratch keyed by run_id; cleared when the run is popped.
        self._links: dict[str, _CausalLinks] = {}

    def links_for(self, run: Run) -> _CausalLinks:
        """The causal-link scratch for ``run``, created on first use."""
        with self._lock:
            links = self._links.get(run.run_id)
            if links is None:
                links = _CausalLinks()
                self._links[run.run_id] = links
            return links

    def peek_session(self, session_id: str) -> Optional[Run]:
        """The session's run if it exists, without creating one (for teardown)."""
        with self._lock:
            return self._sessions.get(session_id)

    def get_or_create_session(
        self, session_id: str, agent_identity: str, conversation_id: Optional[str]
    ) -> Run:
        with self._lock:
            run = self._sessions.get(session_id)
            if run is not None:
                return run
            run = create_run(
                agent_identity=agent_identity,
                conversation_id=conversation_id,
                framework=_FRAMEWORK,
                labels={"claude_session_id": session_id},
            )
            self._sessions[session_id] = run
            return run

    def pop_session(self, session_id: str) -> Optional[Run]:
        with self._lock:
            run = self._sessions.pop(session_id, None)
            if run is not None:
                self._links.pop(run.run_id, None)
            return run

    def create_subagent(
        self, session_id: str, agent_id: str, child: Run
    ) -> None:
        with self._lock:
            self._subagents[(session_id, agent_id)] = child

    def peek_subagent(self, session_id: str, agent_id: str) -> Optional[Run]:
        """The subagent's run if it exists, without removing it (for teardown)."""
        with self._lock:
            return self._subagents.get((session_id, agent_id))

    def pop_subagent(self, session_id: str, agent_id: str) -> Optional[Run]:
        with self._lock:
            run = self._subagents.pop((session_id, agent_id), None)
            if run is not None:
                self._links.pop(run.run_id, None)
            return run

    def route(
        self, session_id: str, agent_id: Optional[str], agent_identity: str, conversation_id: Optional[str]
    ) -> Run:
        """The run an event belongs to: a live subagent run if its agent_id matches,
        else the session's top-level run."""
        if agent_id is not None:
            with self._lock:
                child = self._subagents.get((session_id, agent_id))
            if child is not None:
                return child
        return self.get_or_create_session(session_id, agent_identity, conversation_id)

    def reset(self) -> None:
        with self._lock:
            self._sessions.clear()
            self._subagents.clear()
            self._links.clear()


_registry = _Registry()


@contextmanager
def _bound(run: Run) -> Iterator[None]:
    """Bind ``run`` (and its last parent event) as the ambient context for one capture.

    Persists the new parent back onto the run on exit, so chaining survives even
    when the next hook fires on an unrelated async context.
    """
    run_token = set_current_run(run)
    parent_token = set_parent_event(run.last_parent_event_id)
    try:
        yield
    finally:
        run.last_parent_event_id = current_parent_event()
        reset_parent_event(parent_token)
        reset_current_run(run_token)


# --------------------------------------------------------------------------- #
# Hook callbacks (each: async (input_data, tool_use_id, context) -> HookJSONOutput)
# --------------------------------------------------------------------------- #


def _sha256_hex(value: Any) -> str:
    raw = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _call_links(links: _CausalLinks, tool_use_id: Optional[str]) -> dict[str, Any]:
    """``capture_event`` kwargs for a ``tool_call``: parent on the ``llm_call`` that
    issued it, keyed by the SDK's ``tool_use_id``. Returns ``{}`` (→ ambient parent)
    when the issuing turn hasn't been observed yet — a benign race fallback, never a
    fabricated edge. (Parallel grouping is assigned later by the flusher.)"""
    if tool_use_id and tool_use_id in links.issuer_event:
        return {"parent_event_id": links.issuer_event[tool_use_id]}
    return {}


def _result_links(links: _CausalLinks, tool_use_id: Optional[str]) -> dict[str, Any]:
    """``capture_event`` kwargs for a ``tool_result``: parent on its own
    ``tool_call`` (matched by ``tool_use_id``, not arrival order). Pops the call
    mapping — a tool_use_id yields exactly one result. Returns ``{}`` (→ ambient
    parent) when the call wasn't recorded (race)."""
    if not tool_use_id:
        return {}
    parent = links.call_event.pop(tool_use_id, None)
    return {"parent_event_id": parent} if parent is not None else {}


def build_hooks(
    agent_identity: str, conversation_id: Optional[str] = None
) -> dict[str, list[Any]]:
    """Return the Claude Agent SDK ``hooks`` dict wired to Runfile capture.

    Keys are ``HookEvent`` strings; values are ``[HookMatcher(hooks=[callback])]``.
    ``HookMatcher`` is imported lazily so importing this module never requires the
    ``claude-agent-sdk`` package to be installed.

    Raises ``RuntimeError`` if the installed SDK predates the hook events this
    adapter relies on (see :func:`_check_sdk_version`).
    """
    _check_sdk_version()
    from claude_agent_sdk import HookMatcher  # lazy: optional dependency

    def route(input_data: dict[str, Any]) -> Run:
        return _registry.route(
            input_data["session_id"],
            input_data.get("agent_id"),
            agent_identity,
            conversation_id,
        )

    async def on_pre_tool_use(input_data: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if not _active():
            return {}
        try:
            run = route(input_data)
            links = _registry.links_for(run)
            # Emit this turn's accumulated llm_call BEFORE its first tool, so the
            # single llm_call precedes (and is the parent of) the tool call.
            _emit_pending_turn(run, links)
            tuid = tool_use_id or input_data.get("tool_use_id")
            with _bound(run):
                event_id = capture_event(
                    kind="tool_call",
                    name=input_data.get("tool_name", "unknown"),
                    payload=input_data.get("tool_input"),
                    **_call_links(links, tuid),
                )
            # Remember this call's event_id so its tool_result can parent on it
            # (matched by the SDK's own tool_use_id, not by arrival order).
            if tuid and event_id:
                links.call_event[tuid] = event_id
        except Exception:  # an observer must never break the agent loop
            pass
        return {}

    async def on_post_tool_use(input_data: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if not _active():
            return {}
        try:
            run = route(input_data)
            links = _registry.links_for(run)
            tuid = tool_use_id or input_data.get("tool_use_id")
            with _bound(run):
                capture_event(
                    kind="tool_result",
                    name=input_data.get("tool_name", "unknown"),
                    payload=input_data.get("tool_response"),
                    action_extra={"outcome": "success"},
                    **_result_links(links, tuid),
                )
        except Exception:
            pass
        return {}

    async def on_post_tool_use_failure(input_data: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if not _active():
            return {}
        try:
            run = route(input_data)
            links = _registry.links_for(run)
            tuid = tool_use_id or input_data.get("tool_use_id")
            with _bound(run):
                capture_event(
                    kind="tool_result",
                    name=input_data.get("tool_name", "unknown"),
                    payload={"error": input_data.get("error")},
                    action_extra={"outcome": "failure"},
                    **_result_links(links, tuid),
                )
        except Exception:
            pass
        return {}

    async def on_permission_request(input_data: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        # Observe-only: record that approval was requested. We return {} and let the
        # SDK's own permission flow decide — the adapter never makes the decision.
        if not _active():
            return {}
        try:
            run = route(input_data)
            with _bound(run):
                capture_event(
                    kind="tool_approval_requested",
                    name=input_data.get("tool_name", "unknown"),
                    payload=input_data.get("tool_input"),
                )
        except Exception:
            pass
        return {}

    async def on_notification(input_data: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if not _active():
            return {}
        try:
            ntype = input_data.get("notification_type", "")
            run = route(input_data)
            signal = {
                "framework": _FRAMEWORK,
                "signal_name": f"Notification.{ntype}",
                "signal_payload_hash": _sha256_hex(input_data.get("message", "")),
            }
            with _bound(run):
                if ntype in _SUSPEND_NOTIFICATIONS:
                    suspend_run(
                        reason=_SUSPEND_NOTIFICATIONS[ntype],
                        name=ntype,
                        detection_source="framework_inferred",
                        framework_signal=signal,
                    )
                elif ntype in _RESUME_NOTIFICATIONS:
                    resume_run(triggered_by=_RESUME_NOTIFICATIONS[ntype])
        except Exception:
            pass
        return {}

    async def on_subagent_start(input_data: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if not _active():
            return {}
        try:
            session_id = input_data["session_id"]
            agent_id = input_data.get("agent_id", "")
            agent_type = input_data.get("agent_type", "subagent")
            parent = _registry.get_or_create_session(session_id, agent_identity, conversation_id)
            child_run_id = generate_run_id()
            child_identity = f"{parent.agent_identity}:subagent:{agent_type}"
            with _bound(parent):
                delegate_eid = capture_event(
                    kind="delegate",
                    name=agent_type,
                    delegation_details={
                        "delegated_run_id": child_run_id,
                        "delegated_agent_identity": child_identity,
                        "wait_for_completion": True,
                        "framework_signal": "claude_agent_sdk_subagent",
                    },
                )
            child = create_run(
                agent_identity=child_identity,
                conversation_id=parent.conversation_id,
                framework=_FRAMEWORK,
                run_id=child_run_id,
                delegated_from={"run_id": parent.run_id, "event_id": delegate_eid},
                labels={"claude_session_id": session_id, "claude_agent_id": agent_id},
            )
            _registry.create_subagent(session_id, agent_id, child)
        except Exception:
            pass
        return {}

    async def on_subagent_stop(input_data: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if not _active():
            return {}
        try:
            session_id, agent_id = input_data["session_id"], input_data.get("agent_id", "")
            child = _registry.peek_subagent(session_id, agent_id)
            if child is not None:
                _emit_pending_turn(child, _registry.links_for(child))  # flush its last turn
            child = _registry.pop_subagent(session_id, agent_id)
            if child is not None:
                emit_run_end(child, outcome="success")
        except Exception:
            pass
        return {}

    return {
        "PreToolUse": [HookMatcher(hooks=[on_pre_tool_use])],
        "PostToolUse": [HookMatcher(hooks=[on_post_tool_use])],
        "PostToolUseFailure": [HookMatcher(hooks=[on_post_tool_use_failure])],
        "PermissionRequest": [HookMatcher(hooks=[on_permission_request])],
        "Notification": [HookMatcher(hooks=[on_notification])],
        "SubagentStart": [HookMatcher(hooks=[on_subagent_start])],
        "SubagentStop": [HookMatcher(hooks=[on_subagent_stop])],
    }


def build_can_use_tool(
    *,
    agent_identity: str,
    conversation_id: Optional[str] = None,
    inner_callback: Optional[Callable[..., Any]] = None,
) -> Callable[..., Any]:
    """Wrap the customer's ``can_use_tool`` to observe the approval decision.

    Signature matches the SDK's ``CanUseTool``:
    ``async (tool_name, input_data, context) -> PermissionResult``. We emit
    ``tool_approval_granted`` / ``tool_approval_denied`` around the *inner*
    decision and pass that decision through unchanged. When ``inner_callback`` is
    None we default to allow — so prefer installing this only when the customer
    has their own policy (which :func:`instrument` does).
    """

    async def runfile_can_use_tool(tool_name: str, input_data: dict[str, Any], context: Any) -> Any:
        from claude_agent_sdk import PermissionResultAllow  # lazy

        result: Any
        if inner_callback is not None:
            result = await inner_callback(tool_name, input_data, context)
        else:
            result = PermissionResultAllow()

        if _active():
            try:
                session_id = getattr(context, "session_id", None) or getattr(context, "tool_use_id", None)
                # context carries no session_id; fall back to the agent-scoped run
                # via agent_id if present, else a session keyed by agent_identity.
                agent_id = getattr(context, "agent_id", None)
                run = _registry.route(
                    session_id or agent_identity, agent_id, agent_identity, conversation_id
                )
                granted = getattr(result, "behavior", "allow") == "allow"
                with _bound(run):
                    capture_event(
                        kind="tool_approval_granted" if granted else "tool_approval_denied",
                        name=tool_name,
                        action_extra=None if granted else {"outcome": "failure"},
                    )
            except Exception:
                pass
        return result

    return runfile_can_use_tool


def instrument(
    options: Any = None, *, agent_identity: str, conversation_id: Optional[str] = None
) -> Any:
    """Merge Runfile hooks (and a ``can_use_tool`` wrapper, if the customer set one)
    into a ``ClaudeAgentOptions``, returning the options to pass to ``query()``.

    Used directly by advanced ``ClaudeSDKClient`` callers; :func:`observe_query`
    calls it for the common one-shot path.
    """
    from claude_agent_sdk import ClaudeAgentOptions  # lazy

    hooks = build_hooks(agent_identity, conversation_id)
    if options is None:
        options = ClaudeAgentOptions()

    merged = dict(getattr(options, "hooks", None) or {})
    for event, matchers in hooks.items():
        merged[event] = list(merged.get(event, [])) + matchers
    options.hooks = merged

    # Only wrap can_use_tool when the customer already has one — otherwise we'd be
    # *deciding* (default-allow), which violates "observe, don't decide". Approval
    # *requests* are still captured via the PermissionRequest hook regardless.
    inner = getattr(options, "can_use_tool", None)
    if inner is not None:
        options.can_use_tool = build_can_use_tool(
            agent_identity=agent_identity,
            conversation_id=conversation_id,
            inner_callback=inner,
        )
    return options


# --------------------------------------------------------------------------- #
# Primary path: wrap query() and own run lifecycle
# --------------------------------------------------------------------------- #


def _session_id_of(message: Any) -> Optional[str]:
    sid = getattr(message, "session_id", None)
    if isinstance(sid, str):
        return sid
    data = getattr(message, "data", None)  # SystemMessage carries metadata in .data
    if isinstance(data, dict):
        from_data = data.get("session_id")
        if isinstance(from_data, str):
            return from_data
    return None


def _accumulate_turn(run: Run, message: Any) -> None:
    """Fold an ``AssistantMessage`` into the run's pending model turn (lazy emission).

    The CLI streams one turn as several ``AssistantMessage``s sharing a
    ``message_id`` (Thinking / text / ToolUse). We accumulate their content and the
    turn's (cumulative) usage in :class:`_Turn` and emit a single ``llm_call`` only
    when the turn completes — at its first tool, the next turn, or stream end (see
    :func:`_emit_pending_turn`). This records one call per real turn, captures the
    full reasoning (incl. ``ThinkingBlock.thinking``), and attributes the turn's
    true cumulative usage — without losing later blocks or mutating a shipped event.
    """
    if type(message).__name__ != "AssistantMessage":
        return
    model_id = getattr(message, "model", None)
    if not model_id:
        return
    links = _registry.links_for(run)
    message_id = getattr(message, "message_id", None)
    content = getattr(message, "content", None)

    # New turn (different message_id, or none open) → close the previous one first.
    if links.pending is None or (message_id is not None and message_id != links.pending.message_id):
        _emit_pending_turn(run, links)
        links.pending = _Turn(
            message_id=message_id,
            model_ref={"provider": "anthropic", "model_id": model_id},
        )

    turn = links.pending
    fragment = _stringify_content(content)
    if isinstance(fragment, list):
        turn.content.extend(fragment)
    elif fragment is not None:
        turn.content.append(fragment)
    # Streamed usage is cumulative per message, so the latest block carries the
    # turn total — overwrite rather than sum.
    usage_fields, otel_usage = _model_usage(getattr(message, "usage", None))
    if usage_fields:
        turn.model_ref.update(usage_fields)
    if otel_usage is not None:
        turn.otel = otel_usage
    if isinstance(content, list):
        for block in content:
            if type(block).__name__ == "ToolUseBlock" and getattr(block, "id", None):
                turn.tool_use_ids.append(block.id)


def _emit_pending_turn(run: Run, links: _CausalLinks) -> None:
    """Emit the pending turn's single ``llm_call`` and register its tool issuers.

    Idempotent: a no-op when no turn is open. Called when a turn completes — its
    first tool fires, the next turn begins, or the stream ends — so the one
    ``llm_call`` is captured (and assigned its ``local_seq``) before any of its tool
    calls, making it their true parent. Concurrency grouping is left to the flusher.
    """
    turn = links.pending
    if turn is None:
        return
    links.pending = None
    extra: dict[str, Any] = {}
    if turn.otel is not None:
        extra["otel_attributes"] = turn.otel
    if turn.message_id is not None:
        extra["labels"] = {"claude_message_id": turn.message_id}
    with _bound(run):
        llm_event_id = capture_event(
            kind="llm_call",
            name="messages.create",
            model_ref=turn.model_ref,
            payload={"content": turn.content},
            **extra,
        )
    links.current_llm_event_id = llm_event_id
    if llm_event_id:
        for tuid in turn.tool_use_ids:
            links.issuer_event[tuid] = llm_event_id


def _model_usage(usage: Any) -> tuple[dict[str, int], Optional[dict[str, Any]]]:
    """Map an Anthropic ``usage`` dict to ``(model_ref token fields, otel_attributes)``.

    With prompt caching the bare ``input_tokens`` is only the *non-cached* delta —
    typically a handful of tokens — which badly understates the prompt the model
    actually processed (the bulk arrives as ``cache_read_input_tokens`` /
    ``cache_creation_input_tokens``). So ``model_ref.input_tokens`` is the TRUE
    input — uncached + cache-read + cache-creation — and the cached/uncached
    breakdown is preserved in ``otel_attributes.extra`` for cost analysis.
    """
    if not isinstance(usage, dict):
        return {}, None

    def _int(key: str) -> int:
        value = usage.get(key)
        return value if isinstance(value, int) else 0

    uncached = _int("input_tokens")
    cache_read = _int("cache_read_input_tokens")
    cache_creation = _int("cache_creation_input_tokens")
    output = _int("output_tokens")
    total_input = uncached + cache_read + cache_creation

    fields: dict[str, int] = {}
    if total_input:
        fields["input_tokens"] = total_input
    if output or "output_tokens" in usage:
        fields["output_tokens"] = output
    if not fields:
        return {}, None

    otel: dict[str, Any] = {
        "gen_ai_usage_input_tokens": total_input,
        "gen_ai_usage_output_tokens": output,
    }
    breakdown: dict[str, Any] = {}
    if cache_read or cache_creation:
        # Only worth recording the split when caching actually occurred.
        breakdown["uncached_input_tokens"] = uncached
        if cache_read:
            breakdown["cache_read_input_tokens"] = cache_read
        if cache_creation:
            breakdown["cache_creation_input_tokens"] = cache_creation
    if breakdown:
        otel["extra"] = breakdown
    return fields, otel


def _stringify_content(content: Any) -> Any:
    """Render an AssistantMessage's content blocks to capturable strings.

    Each block exposes its payload on a DIFFERENT attribute: ``TextBlock.text``,
    ``ThinkingBlock.thinking`` (the model's extended-thinking reasoning — the
    "why"), ``ToolUseBlock.name``/``input``. Reading only ``.text`` (the old bug)
    silently discarded every thinking block as the bare token "ThinkingBlock",
    losing the reasoning behind a decision. We pull each block's real content.
    """
    if content is None:
        return None
    if isinstance(content, str):
        return content
    out = []
    for block in content if isinstance(content, list) else [content]:
        text = getattr(block, "text", None)
        if text is not None:
            out.append(text)
            continue
        thinking = getattr(block, "thinking", None)  # ThinkingBlock — the reasoning
        if thinking is not None:
            # Opus omits thinking text by default (signature only) → empty string;
            # record that the model thought rather than emitting blank noise. Pass
            # thinking={"display": "summarized"} in options to capture the text.
            out.append(thinking if thinking else "[thinking omitted by model]")
            continue
        # ToolUseBlock etc.: the full input is captured on the tool_call event, so
        # here record a useful descriptor (name) rather than the bare type name.
        name = getattr(block, "name", None)
        out.append(f"{type(block).__name__}:{name}" if name is not None else type(block).__name__)
    return out


async def observe_query(
    *,
    prompt: Any,
    options: Any = None,
    agent_identity: str,
    conversation_id: Optional[str] = None,
    **query_kwargs: Any,
) -> AsyncIterator[Any]:
    """Wrap ``claude_agent_sdk.query`` with Runfile capture, yielding messages through.

    Owns the run lifecycle (the SDK has no SessionStart/SessionEnd hook): a run is
    created lazily per ``session_id`` and ended when the stream completes (outcome
    ``success``) or raises (``failure``). Tool/approval/suspend events arrive via
    the merged hooks; ``llm_call`` events are derived from AssistantMessages here.

    Usage::

        async for message in observe_query(
            prompt="Summarize the latest market data.",
            options=ClaudeAgentOptions(allowed_tools=["WebSearch", "Read"]),
            agent_identity="did:web:bank.com:agents:research-assistant:v1",
        ):
            print(message)
    """
    from claude_agent_sdk import query  # lazy: optional dependency

    if not _active():
        # SDK not initialised → transparent pass-through, zero capture.
        async for message in query(prompt=prompt, options=options, **query_kwargs):
            yield message
        return

    opts = instrument(options, agent_identity=agent_identity, conversation_id=conversation_id)
    seen: set[str] = set()
    outcome = "success"
    try:
        async for message in query(prompt=prompt, options=opts, **query_kwargs):
            sid = _session_id_of(message)
            if sid is not None:
                seen.add(sid)
                run = _registry.get_or_create_session(sid, agent_identity, conversation_id)
                _accumulate_turn(run, message)
            yield message
    except BaseException:
        outcome = "failure"
        raise
    finally:
        for sid in seen:
            open_run = _registry.peek_session(sid)
            if open_run is not None:
                # Flush the final turn (e.g. a closing summary with no tool call)
                # before the run ends, so its llm_call precedes run_end.
                _emit_pending_turn(open_run, _registry.links_for(open_run))
            ended = _registry.pop_session(sid)
            if ended is not None:
                emit_run_end(ended, outcome=outcome)
