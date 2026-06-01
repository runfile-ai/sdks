"""Claude Agent SDK adapter tests.

The ``claude-agent-sdk`` package isn't a hard dependency, so we install a faithful
fake module mirroring the verified public types (HookMatcher, ClaudeAgentOptions,
PermissionResult*, query) and drive the adapter against it. Every emitted item is
validated against the real ingest schema models, so invalid field placement (e.g.
an action outcome leaking to a top-level field) fails the test.
"""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import sys
from dataclasses import dataclass, field
from types import ModuleType, SimpleNamespace
from typing import Any, Iterator

import pytest
from runfile_schemas.ingest import EventItem, RunCreateItem, RunEndItem, RunUpdateItem

from runfile_ai._hashing import ZERO_SENTINEL
from runfile_ai.buffer import BufferedEvent, BufferedRunItem, EventBuffer
from runfile_ai.integrations import anthropic as rf_anthropic

AGENT = "did:web:bank.com:agents:research-assistant:v1"


# --------------------------------------------------------------------------- #
# Fake claude_agent_sdk module (mirrors the verified types)
# --------------------------------------------------------------------------- #


@dataclass
class _HookMatcher:
    hooks: list[Any] = field(default_factory=list)
    matcher: str | None = None
    timeout: float | None = None


@dataclass
class _ClaudeAgentOptions:
    hooks: dict | None = None
    can_use_tool: Any = None
    allowed_tools: list = field(default_factory=list)


@dataclass
class _PermissionResultAllow:
    behavior: str = "allow"
    updated_input: dict | None = None


@dataclass
class _PermissionResultDeny:
    behavior: str = "deny"
    message: str = ""
    interrupt: bool = False


class _TextBlock:
    def __init__(self, text: str) -> None:
        self.text = text


class ToolUseBlock:  # name matters: adapter checks type(...).__name__
    def __init__(self, id: str, name: str = "tool", input: dict | None = None) -> None:
        self.id = id
        self.name = name
        self.input = input or {}


class AssistantMessage:  # name matters: adapter checks type(...).__name__
    def __init__(self, content, model, session_id, usage=None, message_id=None) -> None:
        self.content = content
        self.model = model
        self.session_id = session_id
        self.usage = usage or {}
        self.message_id = message_id
        self.parent_tool_use_id = None


class ResultMessage:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id


def _base_input(session_id: str, **extra: Any) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "transcript_path": "/tmp/x.jsonl",
        "cwd": "/tmp",
        **extra,
    }


@pytest.fixture
def fake_claude(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    """Install a fake ``claude_agent_sdk`` and reset the adapter registry."""
    mod = ModuleType("claude_agent_sdk")
    mod.HookMatcher = _HookMatcher
    mod.ClaudeAgentOptions = _ClaudeAgentOptions
    mod.PermissionResultAllow = _PermissionResultAllow
    mod.PermissionResultDeny = _PermissionResultDeny

    async def _query(*, prompt, options=None, **kw):  # default fake; tests override
        yield ResultMessage(session_id="sess-default")

    mod.query = _query
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", mod)
    rf_anthropic._registry.reset()
    try:
        yield mod
    finally:
        rf_anthropic._registry.reset()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _events(buf: EventBuffer) -> list[dict]:
    return [b.event for b in buf.snapshot() if isinstance(b, BufferedEvent)]


def _run_items(buf: EventBuffer) -> list[dict]:
    return [b.item for b in buf.snapshot() if isinstance(b, BufferedRunItem)]


def _kinds(buf: EventBuffer) -> list[str]:
    return [e["action"]["kind"] for e in _events(buf)]


_MODELS = {
    "event": EventItem,
    "run_create": RunCreateItem,
    "run_update": RunUpdateItem,
    "run_end": RunEndItem,
}


def _assert_all_wire_valid(buf: EventBuffer) -> None:
    """Every buffered item must validate against its real ingest schema model.

    Events get the flusher-assigned ``prev_event_hash_intent`` stubbed in (the
    flusher adds it at drain time); everything else is exactly what the adapter
    produced on the hot path.
    """
    for b in buf.snapshot():
        if isinstance(b, BufferedEvent):
            event = {**b.event, "prev_event_hash_intent": ZERO_SENTINEL}
            EventItem.model_validate({"type": "event", "event": event})
        else:
            _MODELS[b.item["type"]].model_validate(b.item)


def _hook(agent_identity: str, event_name: str):
    hooks = rf_anthropic.build_hooks(agent_identity)
    return hooks[event_name][0].hooks[0]


# --------------------------------------------------------------------------- #
# Hook-level tests
# --------------------------------------------------------------------------- #


async def test_pre_and_post_tool_use_chain(sdk, fake_claude) -> None:
    pre = _hook(AGENT, "PreToolUse")
    post = _hook(AGENT, "PostToolUse")
    inp = _base_input("s1", hook_event_name="PreToolUse", tool_name="WebSearch", tool_input={"q": "x"}, tool_use_id="t1")
    assert await pre(inp, "t1", None) == {}  # observer returns empty output
    await post(_base_input("s1", hook_event_name="PostToolUse", tool_name="WebSearch", tool_response={"ok": True}, tool_use_id="t1"), "t1", None)

    # lazily created the run, then a tool_call → tool_result chain
    assert _run_items(sdk.buffer)[0]["type"] == "run_create"
    assert _run_items(sdk.buffer)[0]["run"]["sdk_at_start"]["framework"] == "claude_agent_sdk"
    events = _events(sdk.buffer)
    assert [e["action"]["kind"] for e in events] == ["tool_call", "tool_result"]
    assert events[1]["action"]["outcome"] == "success"
    assert events[0]["parent_event_id"] is None
    assert events[1]["parent_event_id"] == events[0]["event_id"]  # parent continuity across hooks
    assert events[1]["local_seq"] == 1
    _assert_all_wire_valid(sdk.buffer)


async def test_post_tool_use_failure_marks_outcome(sdk, fake_claude) -> None:
    fail = _hook(AGENT, "PostToolUseFailure")
    await fail(_base_input("s1", hook_event_name="PostToolUseFailure", tool_name="Bash", tool_input={"command": "x"}, tool_use_id="t1", error="boom"), "t1", None)
    e = _events(sdk.buffer)[0]
    assert e["action"]["kind"] == "tool_result"
    assert e["action"]["outcome"] == "failure"
    _assert_all_wire_valid(sdk.buffer)


async def test_permission_request_is_observe_only(sdk, fake_claude) -> None:
    req = _hook(AGENT, "PermissionRequest")
    out = await req(_base_input("s1", hook_event_name="PermissionRequest", tool_name="Bash", tool_input={"command": "rm"}), None, None)
    assert out == {}  # never decides
    assert _kinds(sdk.buffer) == ["tool_approval_requested"]
    _assert_all_wire_valid(sdk.buffer)


async def test_notification_permission_prompt_suspends(sdk, fake_claude) -> None:
    note = _hook(AGENT, "Notification")
    await note(_base_input("s1", hook_event_name="Notification", notification_type="permission_prompt", message="need approval"), None, None)
    susp = [e for e in _events(sdk.buffer) if e["action"]["kind"] == "run_suspend"]
    assert len(susp) == 1
    assert susp[0]["suspension_details"]["reason"] == "awaiting_human_approval"
    assert susp[0]["suspension_details"]["detection_source"] == "framework_inferred"
    assert susp[0]["suspension_details"]["framework_signal"]["framework"] == "claude_agent_sdk"
    upd = [i for i in _run_items(sdk.buffer) if i["type"] == "run_update"]
    assert upd[0]["lifecycle_state"] == "awaiting_human"
    assert upd[0]["triggered_by_event_id"] == susp[0]["event_id"]
    _assert_all_wire_valid(sdk.buffer)


async def test_notification_idle_and_resume(sdk, fake_claude) -> None:
    note = _hook(AGENT, "Notification")
    await note(_base_input("s1", hook_event_name="Notification", notification_type="idle_prompt", message="waiting"), None, None)
    await note(_base_input("s1", hook_event_name="Notification", notification_type="elicitation_response", message="answered"), None, None)
    kinds = _kinds(sdk.buffer)
    assert "run_suspend" in kinds and "run_resume" in kinds
    resume = [e for e in _events(sdk.buffer) if e["action"]["kind"] == "run_resume"][0]
    assert resume["segment_index"] == 1 and resume["local_seq"] == 0  # resume opens a new segment
    _assert_all_wire_valid(sdk.buffer)


async def test_subagent_delegate_and_child_run(sdk, fake_claude) -> None:
    start = _hook(AGENT, "SubagentStart")
    stop = _hook(AGENT, "SubagentStop")
    await start(_base_input("s1", hook_event_name="SubagentStart", agent_id="a1", agent_type="researcher"), None, None)

    # a delegate event on the parent + a child run_create linked via delegated_from
    delegate = [e for e in _events(sdk.buffer) if e["action"]["kind"] == "delegate"][0]
    creates = [i for i in _run_items(sdk.buffer) if i["type"] == "run_create"]
    parent_create, child_create = creates[0], creates[1]
    assert child_create["run"]["delegated_from"]["run_id"] == parent_create["run"]["run_id"]
    assert child_create["run"]["delegated_from"]["event_id"] == delegate["event_id"]
    assert delegate["delegation_details"]["delegated_run_id"] == child_create["run"]["run_id"]
    assert child_create["run"]["agent_identity"].endswith(":subagent:researcher")

    # an event carrying the subagent's agent_id routes to the CHILD run
    pre = _hook(AGENT, "PreToolUse")
    await pre(_base_input("s1", hook_event_name="PreToolUse", tool_name="Read", tool_input={}, tool_use_id="t9", agent_id="a1"), "t9", None)
    tool_call = [e for e in _events(sdk.buffer) if e["action"]["kind"] == "tool_call"][0]
    assert tool_call["run_id"] == child_create["run"]["run_id"]

    await stop(_base_input("s1", hook_event_name="SubagentStop", agent_id="a1", agent_type="researcher", stop_hook_active=False, agent_transcript_path="/x"), None, None)
    ends = [i for i in _run_items(sdk.buffer) if i["type"] == "run_end"]
    assert ends[-1]["run_id"] == child_create["run"]["run_id"]
    _assert_all_wire_valid(sdk.buffer)


# --------------------------------------------------------------------------- #
# can_use_tool wrapper
# --------------------------------------------------------------------------- #


async def test_can_use_tool_passes_through_and_records(sdk, fake_claude) -> None:
    decisions = []

    async def inner(tool_name, input_data, context):
        decisions.append(tool_name)
        return _PermissionResultDeny(message="nope") if tool_name == "Bash" else _PermissionResultAllow()

    wrapped = rf_anthropic.build_can_use_tool(agent_identity=AGENT, inner_callback=inner)
    ctx = SimpleNamespace(session_id="s1", agent_id=None)
    allow = await wrapped("Read", {"path": "/x"}, ctx)
    deny = await wrapped("Bash", {"command": "rm"}, ctx)

    assert allow.behavior == "allow" and deny.behavior == "deny"  # decision passed through unchanged
    assert decisions == ["Read", "Bash"]  # inner policy was actually consulted
    kinds = _kinds(sdk.buffer)
    assert "tool_approval_granted" in kinds and "tool_approval_denied" in kinds
    _assert_all_wire_valid(sdk.buffer)


# --------------------------------------------------------------------------- #
# instrument() merge behaviour
# --------------------------------------------------------------------------- #


def test_instrument_merges_hooks_and_wraps_only_existing_can_use_tool(sdk, fake_claude) -> None:
    async def existing_cut(tool_name, input_data, context):
        return _PermissionResultAllow()

    async def existing_pre(input_data, tool_use_id, context):
        return {}

    opts = _ClaudeAgentOptions(hooks={"PreToolUse": [_HookMatcher(hooks=[existing_pre])]}, can_use_tool=existing_cut)
    out = rf_anthropic.instrument(opts, agent_identity=AGENT)
    # the customer's PreToolUse handler is preserved alongside ours
    assert len(out.hooks["PreToolUse"]) == 2
    assert "SubagentStart" in out.hooks
    # a customer can_use_tool gets wrapped (now a different object)
    assert out.can_use_tool is not existing_cut

    # with NO existing can_use_tool, we don't fabricate one (observe, don't decide)
    out2 = rf_anthropic.instrument(_ClaudeAgentOptions(), agent_identity=AGENT)
    assert out2.can_use_tool is None


def test_instrument_is_noop_when_sdk_disabled(fake_claude) -> None:
    # No init(): hooks dict still builds, but callbacks degrade to no-ops.
    hooks = rf_anthropic.build_hooks(AGENT)
    assert set(hooks) >= {"PreToolUse", "PostToolUse", "Notification", "SubagentStart"}


# --------------------------------------------------------------------------- #
# SDK version gate
# --------------------------------------------------------------------------- #


def test_build_hooks_rejects_too_old_sdk(fake_claude, monkeypatch) -> None:
    # 0.1.3 predates Notification/SubagentStart/PermissionRequest (added in 0.1.29).
    monkeypatch.setattr(rf_anthropic, "_installed_sdk_version", lambda: (0, 1, 3))
    with pytest.raises(RuntimeError, match="0.1.29"):
        rf_anthropic.build_hooks(AGENT)


@pytest.mark.parametrize("version", [(0, 1, 29), (0, 2, 87), (1, 0, 0), None])
def test_build_hooks_accepts_supported_or_unknown_sdk(fake_claude, monkeypatch, version) -> None:
    # >= minimum passes; None (source install, no metadata) is not blocked.
    monkeypatch.setattr(rf_anthropic, "_installed_sdk_version", lambda: version)
    hooks = rf_anthropic.build_hooks(AGENT)
    assert "PermissionRequest" in hooks


@pytest.mark.parametrize(
    "raw,expected",
    [("0.1.29", (0, 1, 29)), ("0.2.87", (0, 2, 87)), ("0.0.23", (0, 0, 23)), ("0.1.29rc1", (0, 1, 29))],
)
def test_version_parser(monkeypatch, raw, expected) -> None:
    monkeypatch.setattr(importlib_metadata, "version", lambda _name: raw)
    assert rf_anthropic._installed_sdk_version() == expected


def test_version_parser_missing_package(monkeypatch) -> None:
    def _raise(_name):
        raise importlib_metadata.PackageNotFoundError

    monkeypatch.setattr(importlib_metadata, "version", _raise)
    assert rf_anthropic._installed_sdk_version() is None


# --------------------------------------------------------------------------- #
# observe_query end-to-end
# --------------------------------------------------------------------------- #


async def test_observe_query_full_lifecycle(sdk, fake_claude) -> None:
    async def fake_query(*, prompt, options=None, **kw):
        # fire the merged PreToolUse hook mid-stream, then stream messages
        for matcher in (options.hooks or {}).get("PreToolUse", []):
            for cb in matcher.hooks:
                await cb(_base_input("sess-1", hook_event_name="PreToolUse", tool_name="WebSearch", tool_input={"q": "x"}, tool_use_id="t1"), "t1", None)
        yield AssistantMessage(content=[_TextBlock("here you go")], model="claude-opus-4-8", session_id="sess-1", usage={"input_tokens": 11, "output_tokens": 7})
        yield ResultMessage(session_id="sess-1")

    fake_claude.query = fake_query

    seen = [m async for m in rf_anthropic.observe_query(prompt="hi", agent_identity=AGENT)]
    assert len(seen) == 2  # messages pass through unchanged

    items = _run_items(sdk.buffer)
    assert items[0]["type"] == "run_create"
    assert items[-1]["type"] == "run_end" and items[-1]["outcome"] == "success"
    kinds = _kinds(sdk.buffer)
    assert kinds == ["tool_call", "llm_call"]
    llm = [e for e in _events(sdk.buffer) if e["action"]["kind"] == "llm_call"][0]
    assert llm["model_ref"] == {"provider": "anthropic", "model_id": "claude-opus-4-8", "input_tokens": 11, "output_tokens": 7}
    _assert_all_wire_valid(sdk.buffer)


async def test_observe_query_failure_outcome(sdk, fake_claude) -> None:
    async def boom_query(*, prompt, options=None, **kw):
        yield AssistantMessage(content=[_TextBlock("x")], model="claude-opus-4-8", session_id="sess-2")
        raise RuntimeError("stream broke")

    fake_claude.query = boom_query
    with pytest.raises(RuntimeError):
        async for _ in rf_anthropic.observe_query(prompt="hi", agent_identity=AGENT):
            pass
    ends = [i for i in _run_items(sdk.buffer) if i["type"] == "run_end"]
    assert ends and ends[-1]["outcome"] == "failure"
    _assert_all_wire_valid(sdk.buffer)


async def test_tool_result_parents_on_its_own_tool_call_under_interleave(sdk, fake_claude) -> None:
    """Causal parenting via tool_use_id, not arrival order.

    Two tools are dispatched (t1 then t2) but complete in inverted order (t2 then
    t1) — the real concurrency signature. Each tool_result must parent on its OWN
    tool_call (matched by tool_use_id), and the last result (t1) must NOT parent on
    the previous-by-seq event (t2's result) the way the old linear chain did.
    """

    async def fake_query(*, prompt, options=None, **kw):
        pre = (options.hooks or {})["PreToolUse"][0].hooks[0]
        post = (options.hooks or {})["PostToolUse"][0].hooks[0]
        # turn 1 issues tool t1 (its own assistant message)
        yield AssistantMessage(content=[ToolUseBlock(id="t1", name="policy")], model="claude-opus-4-8", session_id="s1", message_id="m1")
        await pre(_base_input("s1", hook_event_name="PreToolUse", tool_name="policy", tool_input={}, tool_use_id="t1"), "t1", None)
        # turn 2 issues tool t2
        yield AssistantMessage(content=[ToolUseBlock(id="t2", name="bureau")], model="claude-opus-4-8", session_id="s1", message_id="m2")
        await pre(_base_input("s1", hook_event_name="PreToolUse", tool_name="bureau", tool_input={}, tool_use_id="t2"), "t2", None)
        # results return inverted: t2 first, then t1
        await post(_base_input("s1", hook_event_name="PostToolUse", tool_name="bureau", tool_response={"ok": 2}, tool_use_id="t2"), "t2", None)
        await post(_base_input("s1", hook_event_name="PostToolUse", tool_name="policy", tool_response={"ok": 1}, tool_use_id="t1"), "t1", None)
        yield ResultMessage(session_id="s1")

    fake_claude.query = fake_query
    [m async for m in rf_anthropic.observe_query(prompt="hi", agent_identity=AGENT)]

    events = _events(sdk.buffer)

    def by(kind, name):
        return next(e for e in events if e["action"]["kind"] == kind and e["action"]["name"] == name)

    llm1, llm2 = [e for e in events if e["action"]["kind"] == "llm_call"][:2]
    call_t1, call_t2 = by("tool_call", "policy"), by("tool_call", "bureau")
    res_t1, res_t2 = by("tool_result", "policy"), by("tool_result", "bureau")

    # each tool_call parents on the llm_call that issued it
    assert call_t1["parent_event_id"] == llm1["event_id"]
    assert call_t2["parent_event_id"] == llm2["event_id"]
    # each tool_result parents on its OWN tool_call (matched by tool_use_id)
    assert res_t2["parent_event_id"] == call_t2["event_id"]
    assert res_t1["parent_event_id"] == call_t1["event_id"]
    # the regression: t1's result was emitted last but does NOT chain to t2's result
    assert res_t1["local_seq"] > res_t2["local_seq"]
    assert res_t1["parent_event_id"] != res_t2["event_id"]
    # next llm spine wasn't polluted by the branch events (tool events don't advance it)
    assert llm2["parent_event_id"] == llm1["event_id"]
    _assert_all_wire_valid(sdk.buffer)


async def test_parallel_tool_calls_in_one_message_share_a_group(sdk, fake_claude) -> None:
    """One AssistantMessage with >1 ToolUseBlock = the API's statement that the
    calls are concurrent → all four events (2 calls + 2 results) share one
    parallel_group_id and both calls parent on the single issuing llm_call."""

    async def fake_query(*, prompt, options=None, **kw):
        pre = (options.hooks or {})["PreToolUse"][0].hooks[0]
        post = (options.hooks or {})["PostToolUse"][0].hooks[0]
        yield AssistantMessage(
            content=[ToolUseBlock(id="t1", name="a"), ToolUseBlock(id="t2", name="b")],
            model="claude-opus-4-8", session_id="s1", message_id="m1",
        )
        await pre(_base_input("s1", hook_event_name="PreToolUse", tool_name="a", tool_input={}, tool_use_id="t1"), "t1", None)
        await pre(_base_input("s1", hook_event_name="PreToolUse", tool_name="b", tool_input={}, tool_use_id="t2"), "t2", None)
        await post(_base_input("s1", hook_event_name="PostToolUse", tool_name="a", tool_response={}, tool_use_id="t1"), "t1", None)
        await post(_base_input("s1", hook_event_name="PostToolUse", tool_name="b", tool_response={}, tool_use_id="t2"), "t2", None)
        yield ResultMessage(session_id="s1")

    fake_claude.query = fake_query
    [m async for m in rf_anthropic.observe_query(prompt="hi", agent_identity=AGENT)]

    events = _events(sdk.buffer)
    llm = [e for e in events if e["action"]["kind"] == "llm_call"][0]
    tool_events = [e for e in events if e["action"]["kind"] in ("tool_call", "tool_result")]
    groups = {e.get("parallel_group_id") for e in tool_events}
    assert len(groups) == 1 and None not in groups  # one shared, real group id
    assert (next(iter(groups))).startswith("pg_")
    for e in [e for e in tool_events if e["action"]["kind"] == "tool_call"]:
        assert e["parent_event_id"] == llm["event_id"]  # both calls parent on the one turn
    _assert_all_wire_valid(sdk.buffer)


async def test_single_tool_call_has_no_parallel_group(sdk, fake_claude) -> None:
    async def fake_query(*, prompt, options=None, **kw):
        pre = (options.hooks or {})["PreToolUse"][0].hooks[0]
        post = (options.hooks or {})["PostToolUse"][0].hooks[0]
        yield AssistantMessage(content=[ToolUseBlock(id="t1", name="a")], model="claude-opus-4-8", session_id="s1", message_id="m1")
        await pre(_base_input("s1", hook_event_name="PreToolUse", tool_name="a", tool_input={}, tool_use_id="t1"), "t1", None)
        await post(_base_input("s1", hook_event_name="PostToolUse", tool_name="a", tool_response={}, tool_use_id="t1"), "t1", None)
        yield ResultMessage(session_id="s1")

    fake_claude.query = fake_query
    [m async for m in rf_anthropic.observe_query(prompt="hi", agent_identity=AGENT)]
    tool_events = [e for e in _events(sdk.buffer) if e["action"]["kind"] in ("tool_call", "tool_result")]
    assert all("parallel_group_id" not in e for e in tool_events)
    _assert_all_wire_valid(sdk.buffer)


async def test_streamed_turn_coalesces_to_one_llm_call(sdk, fake_claude) -> None:
    """The CLI streams one model turn as several AssistantMessages sharing a
    message_id (thinking, text, tool-use). They must collapse into ONE llm_call —
    not one per block — usage counted once, and a tool call in a later block still
    parents on that single llm_call."""

    def msg(content, mid, usage):
        return AssistantMessage(content=content, model="claude-opus-4-8", session_id="s1", message_id=mid, usage=usage)

    async def fake_query(*, prompt, options=None, **kw):
        pre = (options.hooks or {})["PreToolUse"][0].hooks[0]
        u1 = {"input_tokens": 143, "output_tokens": 54}
        # one model turn (m1) streamed as three blocks, same usage repeated
        yield msg([_TextBlock("<thinking>")], "m1", u1)
        yield msg([_TextBlock("I'll call a tool")], "m1", u1)
        yield msg([ToolUseBlock(id="t1", name="a")], "m1", u1)
        await pre(_base_input("s1", hook_event_name="PreToolUse", tool_name="a", tool_input={}, tool_use_id="t1"), "t1", None)
        # a second, distinct turn (m2)
        yield msg([_TextBlock("done")], "m2", {"input_tokens": 5, "output_tokens": 2})
        yield ResultMessage(session_id="s1")

    fake_claude.query = fake_query
    [m async for m in rf_anthropic.observe_query(prompt="hi", agent_identity=AGENT)]

    events = _events(sdk.buffer)
    llm = [e for e in events if e["action"]["kind"] == "llm_call"]
    assert len(llm) == 2  # one per turn (m1, m2), NOT one per block (would be 4)
    # usage counted once per turn, not repeated across the turn's blocks
    assert sum(e["model_ref"].get("input_tokens", 0) for e in llm) == 148  # 143 + 5
    assert llm[0]["labels"]["claude_message_id"] == "m1"
    # the tool call (a later block of m1) parents on the single m1 llm_call
    tc = next(e for e in events if e["action"]["kind"] == "tool_call")
    assert tc["parent_event_id"] == llm[0]["event_id"]
    _assert_all_wire_valid(sdk.buffer)


async def test_llm_usage_counts_cached_input_tokens(sdk, fake_claude) -> None:
    """With prompt caching, raw input_tokens is only the non-cached delta (often a
    handful). model_ref.input_tokens must reflect the TRUE input the model
    processed (uncached + cache-read + cache-creation), with the split preserved in
    otel_attributes — otherwise the audit reports an absurd prompt size like 2."""

    async def fake_query(*, prompt, options=None, **kw):
        yield AssistantMessage(
            content=[_TextBlock("ok")], model="claude-opus-4-8", session_id="s1", message_id="m1",
            usage={"input_tokens": 2, "output_tokens": 38,
                   "cache_read_input_tokens": 9000, "cache_creation_input_tokens": 1200},
        )
        yield ResultMessage(session_id="s1")

    fake_claude.query = fake_query
    [m async for m in rf_anthropic.observe_query(prompt="hi", agent_identity=AGENT)]

    llm = next(e for e in _events(sdk.buffer) if e["action"]["kind"] == "llm_call")
    assert llm["model_ref"]["input_tokens"] == 2 + 9000 + 1200  # true context, not 2
    assert llm["model_ref"]["output_tokens"] == 38
    extra = llm["otel_attributes"]["extra"]
    assert extra["uncached_input_tokens"] == 2
    assert extra["cache_read_input_tokens"] == 9000
    assert extra["cache_creation_input_tokens"] == 1200
    assert llm["otel_attributes"]["gen_ai_usage_input_tokens"] == 10202
    _assert_all_wire_valid(sdk.buffer)


async def test_observe_query_passthrough_without_init(fake_claude) -> None:
    # SDK not initialised → transparent pass-through, nothing captured, no crash.
    async def fake_query(*, prompt, options=None, **kw):
        yield ResultMessage(session_id="sess-x")

    fake_claude.query = fake_query
    seen = [m async for m in rf_anthropic.observe_query(prompt="hi", agent_identity=AGENT)]
    assert len(seen) == 1
