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
- ``AssistantMessage`` (seen by :func:`observe_query`) → ``llm_call`` with ``model_ref``

Every hook returns ``{}`` (an empty ``HookJSONOutput``): the adapter observes, it
never alters the agent's behaviour.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import threading
from contextlib import contextmanager
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


class _Registry:
    """Process-global map of Claude ``session_id`` / subagent ``agent_id`` → Run."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[str, Run] = {}
        self._subagents: dict[tuple[str, str], Run] = {}

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
            return self._sessions.pop(session_id, None)

    def create_subagent(
        self, session_id: str, agent_id: str, child: Run
    ) -> None:
        with self._lock:
            self._subagents[(session_id, agent_id)] = child

    def pop_subagent(self, session_id: str, agent_id: str) -> Optional[Run]:
        with self._lock:
            return self._subagents.pop((session_id, agent_id), None)

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
            with _bound(run):
                capture_event(
                    kind="tool_call",
                    name=input_data.get("tool_name", "unknown"),
                    payload=input_data.get("tool_input"),
                )
        except Exception:  # an observer must never break the agent loop
            pass
        return {}

    async def on_post_tool_use(input_data: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if not _active():
            return {}
        try:
            run = route(input_data)
            with _bound(run):
                capture_event(
                    kind="tool_result",
                    name=input_data.get("tool_name", "unknown"),
                    payload=input_data.get("tool_response"),
                    action_extra={"outcome": "success"},
                )
        except Exception:
            pass
        return {}

    async def on_post_tool_use_failure(input_data: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if not _active():
            return {}
        try:
            run = route(input_data)
            with _bound(run):
                capture_event(
                    kind="tool_result",
                    name=input_data.get("tool_name", "unknown"),
                    payload={"error": input_data.get("error")},
                    action_extra={"outcome": "failure"},
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
            child = _registry.pop_subagent(input_data["session_id"], input_data.get("agent_id", ""))
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


def _maybe_capture_llm(run: Run, message: Any) -> None:
    """Emit an ``llm_call`` for an AssistantMessage (the model-call evidence hooks
    don't provide)."""
    if type(message).__name__ != "AssistantMessage":
        return
    model_id = getattr(message, "model", None)
    if not model_id:
        return
    usage = getattr(message, "usage", None) or {}
    model_ref: dict[str, Any] = {"provider": "anthropic", "model_id": model_id}
    if isinstance(usage, dict):
        if "input_tokens" in usage:
            model_ref["input_tokens"] = usage["input_tokens"]
        if "output_tokens" in usage:
            model_ref["output_tokens"] = usage["output_tokens"]
    with _bound(run):
        capture_event(
            kind="llm_call",
            name="messages.create",
            model_ref=model_ref,
            payload={"content": _stringify_content(getattr(message, "content", None))},
        )


def _stringify_content(content: Any) -> Any:
    if content is None:
        return None
    if isinstance(content, str):
        return content
    out = []
    for block in content if isinstance(content, list) else [content]:
        text = getattr(block, "text", None)
        out.append(text if text is not None else type(block).__name__)
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
                _maybe_capture_llm(run, message)
            yield message
    except BaseException:
        outcome = "failure"
        raise
    finally:
        for sid in seen:
            ended = _registry.pop_session(sid)
            if ended is not None:
                emit_run_end(ended, outcome=outcome)
