"""LangGraph adapter tests.

Driven against **real** ``langgraph`` (gated on import — it's an optional extra):
we build an actual ``create_react_agent`` graph with a fake tool-calling chat model
(no network, no API key), instrument it, and ``ainvoke`` it, then assert on the
captured buffer. This exercises the real callback dispatch — run-id threading,
interrupt/resume, the tool ``run_id`` / ``tool_call_id`` correlation — which is the
part a hand-rolled fake can't faithfully reproduce. Every buffered item is validated
against the real ingest schema models.
"""

from __future__ import annotations

from typing import Any, Iterator

import pytest
from runfile_schemas.ingest import EventItem, RunCreateItem, RunEndItem, RunUpdateItem

from runfile_ai._hashing import ZERO_SENTINEL
from runfile_ai.buffer import BufferedEvent, BufferedRunItem, EventBuffer
from runfile_ai.flusher import _assign_parallel_groups
from runfile_ai.integrations import langgraph as rf_lg

pytest.importorskip("langgraph", reason="langgraph extra not installed")

from langchain_core.language_models.chat_models import BaseChatModel  # noqa: E402
from langchain_core.messages import AIMessage  # noqa: E402
from langchain_core.outputs import ChatGeneration, ChatResult  # noqa: E402
from langchain_core.tools import tool  # noqa: E402
from langgraph.checkpoint.memory import InMemorySaver  # noqa: E402
from langgraph.prebuilt import create_react_agent  # noqa: E402
from langgraph.types import Command, interrupt  # noqa: E402

AGENT = "did:web:bank.com:agents:loan-triage:v2"

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


# --------------------------------------------------------------------------- #
# Fakes: a tool-calling chat model + tools, wired into a real ReAct graph
# --------------------------------------------------------------------------- #


@tool
def lookup(q: str) -> str:
    """Look up a value."""
    return f"result:{q}"


@tool
def ask_human(q: str) -> str:
    """Ask a human (interrupts the graph)."""
    return f"human:{interrupt({'question': q})}"


@tool
def boom(q: str) -> str:
    """A tool that fails for real (not a control-flow signal)."""
    raise ValueError("tool exploded")


@tool
def slow_lookup(q: str) -> str:
    """A sync tool with a tiny delay — widens the concurrency window so parallel
    tool calls (which LangGraph runs on a threadpool) reliably overlap."""
    import time

    time.sleep(0.01)
    return f"result:{q}"


class _FakeChat(BaseChatModel):
    """A scripted chat model. ``script`` maps the count of prior AIMessages → the
    AIMessage to return, so it drives a real ReAct loop deterministically."""

    script: list[dict[str, Any]] = []

    @property
    def _llm_type(self) -> str:
        return "fake-chat"

    def bind_tools(self, *a: Any, **k: Any) -> "BaseChatModel":  # ReAct calls this
        return self

    def _generate(self, messages: Any, stop: Any = None, run_manager: Any = None, **kw: Any) -> ChatResult:
        turn = sum(1 for m in messages if isinstance(m, AIMessage))
        spec = self.script[min(turn, len(self.script) - 1)]
        msg = AIMessage(
            content=spec.get("content", ""),
            tool_calls=spec.get("tool_calls", []),
            usage_metadata=spec.get("usage", {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12}),
        )
        msg.response_metadata = {"model_name": "claude-test-1", "model_provider": "anthropic"}
        return ChatResult(generations=[ChatGeneration(message=msg)])


def _model(script: list[dict[str, Any]]) -> _FakeChat:
    m = _FakeChat()
    m.script = script
    return m


@pytest.fixture(autouse=True)
def _reset_registry() -> Iterator[None]:
    rf_lg._registry.reset()
    yield
    rf_lg._registry.reset()


# --------------------------------------------------------------------------- #
# Buffer inspection helpers (mirror the Claude adapter tests)
# --------------------------------------------------------------------------- #

_LIFECYCLE_BOUNDARY = {"run_create", "run_end"}
_MODELS = {"event": EventItem, "run_create": RunCreateItem, "run_update": RunUpdateItem, "run_end": RunEndItem}


def _events(buf: EventBuffer, *, include_boundary: bool = False) -> list[dict]:
    return [
        b.event
        for b in buf.snapshot()
        if isinstance(b, BufferedEvent)
        and (include_boundary or b.event["action"]["kind"] not in _LIFECYCLE_BOUNDARY)
    ]


def _run_items(buf: EventBuffer) -> list[dict]:
    return [b.item for b in buf.snapshot() if isinstance(b, BufferedRunItem)]


def _kinds(buf: EventBuffer) -> list[str]:
    return [e["action"]["kind"] for e in _events(buf)]


def _assert_all_wire_valid(buf: EventBuffer) -> None:
    """Every buffered item validates against its real ingest schema model.

    Events get the flusher-assigned ``prev_event_hash_intent`` stubbed in (the
    flusher adds it at drain time); everything else is exactly what the adapter
    produced on the hot path. A misplaced field (e.g. an outcome leaking to the top
    level, or framework="langgraph" not in the enum) fails here.
    """
    for b in buf.snapshot():
        if isinstance(b, BufferedEvent):
            event = {**b.event, "prev_event_hash_intent": ZERO_SENTINEL}
            EventItem.model_validate({"type": "event", "event": event})
        else:
            _MODELS[b.item["type"]].model_validate(b.item)


# --------------------------------------------------------------------------- #
# Core ReAct flow
# --------------------------------------------------------------------------- #


async def test_react_flow_lifecycle_llm_and_tools(sdk) -> None:
    model = _model(
        [
            {"content": "go", "tool_calls": [{"name": "lookup", "args": {"q": "x"}, "id": "c1"}],
             "usage": {"input_tokens": 100, "output_tokens": 20, "total_tokens": 120}},
            {"content": "final answer"},
        ]
    )
    agent = rf_lg.instrument(create_react_agent(model, [lookup]), agent_identity=AGENT)
    await agent.ainvoke({"messages": [{"role": "user", "content": "hi"}]})

    # Witness-authored lifecycle: a run_create item AND a run_create chain event.
    items = _run_items(sdk.buffer)
    assert items[0]["type"] == "run_create"
    assert items[0]["run"]["sdk_at_start"]["framework"] == "langgraph"
    assert items[-1]["type"] == "run_end" and items[-1]["outcome"] == "success"
    boundary = [e["action"]["kind"] for e in _events(sdk.buffer, include_boundary=True)]
    assert boundary[0] == "run_create" and boundary[-1] == "run_end"

    # The substantive activity: two model turns + one tool round-trip.
    assert _kinds(sdk.buffer) == ["llm_call", "tool_call", "tool_result", "llm_call"]
    llm = _events(sdk.buffer)[0]
    assert llm["model_ref"] == {
        "provider": "anthropic", "model_id": "claude-test-1", "input_tokens": 100, "output_tokens": 20,
    }
    _assert_all_wire_valid(sdk.buffer)


async def test_tool_parenting_and_parallel_grouping(sdk) -> None:
    # One model turn issues TWO tool calls → the API's statement of concurrency.
    model = _model(
        [
            {"content": "go", "tool_calls": [
                {"name": "lookup", "args": {"q": "a"}, "id": "c1"},
                {"name": "lookup", "args": {"q": "b"}, "id": "c2"},
            ]},
            {"content": "done"},
        ]
    )
    agent = rf_lg.instrument(create_react_agent(model, [lookup]), agent_identity=AGENT)
    await agent.ainvoke({"messages": [{"role": "user", "content": "hi"}]})

    snap = sdk.buffer.snapshot()
    _assign_parallel_groups(snap)  # the flusher's structural concurrency pass
    by_id = {b.event["event_id"]: b.event for b in snap if isinstance(b, BufferedEvent)}
    first_llm = next(e for e in by_id.values() if e["action"]["kind"] == "llm_call")
    calls = [e for e in by_id.values() if e["action"]["kind"] == "tool_call"]
    results = [e for e in by_id.values() if e["action"]["kind"] == "tool_result"]

    # Each tool_call is parented on the issuing llm_call (resolved from tool_call_id).
    assert len(calls) == 2
    assert all(c["parent_event_id"] == first_llm["event_id"] for c in calls)
    # Each tool_result is parented on its own tool_call (matched by the tool run_id).
    assert {r["parent_event_id"] for r in results} == {c["event_id"] for c in calls}
    # Co-issued calls + their results share ONE parallel group; the llm_call doesn't.
    groups = {c["parallel_group_id"] for c in calls}
    assert len(groups) == 1 and next(iter(groups)).startswith("pg_")
    assert "parallel_group_id" not in first_llm
    assert all(r["parallel_group_id"] in groups for r in results)
    _assert_all_wire_valid(sdk.buffer)


async def test_local_seq_contiguous_and_linear_spine(sdk) -> None:
    model = _model([
        {"content": "go", "tool_calls": [{"name": "lookup", "args": {"q": "x"}, "id": "c1"}]},
        {"content": "final"},
    ])
    agent = rf_lg.instrument(create_react_agent(model, [lookup]), agent_identity=AGENT)
    await agent.ainvoke({"messages": [{"role": "user", "content": "hi"}]})

    events = _events(sdk.buffer, include_boundary=True)
    seqs = [e["local_seq"] for e in events if e["segment_index"] == 0]
    assert seqs == list(range(len(seqs)))  # no gaps → no lost events
    # The linear spine threads llm_call → llm_call (tool branches don't advance it).
    llms = [e for e in events if e["action"]["kind"] == "llm_call"]
    assert llms[1]["parent_event_id"] == llms[0]["event_id"]


async def test_parallel_tool_capture_stays_ordered_under_thread_concurrency(sdk) -> None:
    """LangGraph runs sync tools on a threadpool, so a turn's parallel tool callbacks
    fire on different threads concurrently. The adapter's per-run capture lock must
    keep events buffered in ``local_seq`` order (and seqs unique) every time — else
    they ship out of order and the server flags ``sequence_gap`` / ``chain_break``.

    Repeated to beat the race: without the lock this fails intermittently (the
    ``slow_lookup`` delay widens the overlap window so an inversion is near-certain
    across the iterations); with it, ordering is guaranteed.
    """
    for i in range(20):
        rf_lg._registry.reset()
        sdk.buffer.take_all()  # isolate this iteration's run
        model = _model([
            {"content": "go", "tool_calls": [
                {"name": "slow_lookup", "args": {"q": "a"}, "id": "p1"},
                {"name": "slow_lookup", "args": {"q": "b"}, "id": "p2"},
            ]},
            {"content": "done"},
        ])
        agent = rf_lg.instrument(create_react_agent(model, [slow_lookup]), agent_identity=AGENT)
        await agent.ainvoke({"messages": [{"role": "user", "content": "hi"}]})

        events = [b.event for b in sdk.buffer.snapshot() if isinstance(b, BufferedEvent)]
        keys = [(e.get("segment_index", 0), e["local_seq"]) for e in events]
        assert keys == sorted(keys), f"iter {i}: buffer out of local_seq order: {keys}"
        seqs = [k[1] for k in keys]
        assert len(seqs) == len(set(seqs)), f"iter {i}: duplicate local_seq: {seqs}"


# --------------------------------------------------------------------------- #
# HITL interrupt / resume
# --------------------------------------------------------------------------- #


async def test_interrupt_then_resume_is_one_run_two_segments(sdk) -> None:
    model = _model([
        {"content": "ask", "tool_calls": [{"name": "ask_human", "args": {"q": "approve?"}, "id": "c1"}]},
        {"content": "done"},
    ])
    agent = rf_lg.instrument(
        create_react_agent(model, [ask_human], checkpointer=InMemorySaver()), agent_identity=AGENT
    )
    cfg = {"configurable": {"thread_id": "T1"}}
    await agent.ainvoke({"messages": [{"role": "user", "content": "hi"}]}, config=cfg)

    # After the interrupt: suspended, awaiting_human, NOT ended.
    items = _run_items(sdk.buffer)
    assert [i["type"] for i in items] == ["run_create", "run_update"]
    assert items[1]["lifecycle_state"] == "awaiting_human"
    susp = [e for e in _events(sdk.buffer) if e["action"]["kind"] == "run_suspend"][0]
    assert susp["suspension_details"]["reason"] == "awaiting_human_input"
    assert susp["suspension_details"]["detection_source"] == "framework_inferred"
    assert susp["suspension_details"]["framework_signal"]["framework"] == "langgraph"
    # The LangGraph thread_id is captured as the durable resume handle so an
    # out-of-process resume/approval can be joined back to this suspension.
    assert susp["suspension_details"]["correlation_token"] == "T1"
    assert items[1]["triggered_by_event_id"] == susp["event_id"]

    # Resume on the SAME thread_id → same run, new segment, no duplicate run.
    await agent.ainvoke(Command(resume="YES"), config=cfg)
    run_ids = {b.event["run_id"] for b in sdk.buffer.snapshot() if isinstance(b, BufferedEvent)}
    assert len(run_ids) == 1
    item_types = [i["type"] for i in _run_items(sdk.buffer)]
    assert item_types == ["run_create", "run_update", "run_update", "run_end"]
    states = [i["lifecycle_state"] for i in _run_items(sdk.buffer) if i["type"] == "run_update"]
    assert states == ["awaiting_human", "active"]
    resume = [e for e in _events(sdk.buffer) if e["action"]["kind"] == "run_resume"][0]
    assert resume["segment_index"] == 1 and resume["local_seq"] == 0  # new segment
    # The resume mirrors the suspend's correlation_token (same thread_id) so the
    # suspend/resume pair stitches together.
    assert resume["resume_details"]["correlation_token"] == "T1"

    # The interrupt (a suspension) is NOT recorded as a tool failure.
    assert not any(
        e["action"]["kind"] == "tool_result" and e["action"].get("outcome") == "failure"
        for e in _events(sdk.buffer)
    )
    _assert_all_wire_valid(sdk.buffer)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


async def test_real_tool_error_records_failure_and_ends_run(sdk) -> None:
    model = _model([{"content": "go", "tool_calls": [{"name": "boom", "args": {"q": "x"}, "id": "c1"}]}])
    agent = rf_lg.instrument(create_react_agent(model, [boom]), agent_identity=AGENT)

    # A real (non-control-flow) tool error propagates through the graph and raises.
    with pytest.raises(ValueError, match="tool exploded"):
        await agent.ainvoke({"messages": [{"role": "user", "content": "hi"}]})

    results = [e for e in _events(sdk.buffer) if e["action"]["kind"] == "tool_result"]
    assert results and results[0]["action"]["outcome"] == "failure"
    # The error bubbles to the root chain → the run ends with outcome=failure.
    assert _run_items(sdk.buffer)[-1]["type"] == "run_end"
    assert _run_items(sdk.buffer)[-1]["outcome"] == "failure"
    _assert_all_wire_valid(sdk.buffer)


# --------------------------------------------------------------------------- #
# instrument() wiring and disabled no-op
# --------------------------------------------------------------------------- #


async def test_instrument_returns_configured_graph_without_mutating_original(sdk) -> None:
    model = _model([{"content": "hi"}])
    base = create_react_agent(model, [])
    wrapped = rf_lg.instrument(base, agent_identity=AGENT)
    assert wrapped is not base  # with_config returns a new runnable
    await wrapped.ainvoke({"messages": [{"role": "user", "content": "hi"}]})
    assert _run_items(sdk.buffer)[0]["type"] == "run_create"


async def test_no_capture_when_sdk_not_initialised() -> None:
    # No runfile_ai.init(): callbacks degrade to transparent no-ops, graph still runs.
    model = _model([{"content": "hi"}])
    agent = rf_lg.instrument(create_react_agent(model, []), agent_identity=AGENT)
    result = await agent.ainvoke({"messages": [{"role": "user", "content": "hi"}]})
    assert result["messages"][-1].content == "hi"  # graph unaffected
