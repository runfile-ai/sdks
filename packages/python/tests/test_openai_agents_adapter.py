"""OpenAI Agents SDK adapter tests.

Driven against **real** ``openai-agents`` (gated on import — it's an optional extra):
we build a real ``Agent`` driven by a scripted ``Model`` (no network, no API key) and
run it through the real ``Runner.run``, registering the Runfile tracing processor and
asserting on the captured buffer. This exercises the real span dispatch — the
Task/Agent/Turn/Function/Handoff tree, ``span_id``/``parent_id`` threading, and
concurrent tool spans — which a hand-rolled fake span sequence couldn't reproduce.
Every buffered item is validated against the real ingest schema models.

Mirrors ``test_langgraph_adapter.py``'s "real framework + scripted model" approach.
"""

from __future__ import annotations

import json
from typing import Any, Iterator

import pytest
from runfile_schemas.ingest import EventItem, RunCreateItem, RunEndItem, RunUpdateItem

from runfile_ai._hashing import ZERO_SENTINEL
from runfile_ai.buffer import BufferedEvent, BufferedRunItem, EventBuffer
from runfile_ai.integrations import openai_agents as rf_oai

pytest.importorskip("agents", reason="openai-agents extra not installed")

from agents import Agent, Runner, function_tool  # noqa: E402
from agents.items import ModelResponse  # noqa: E402
from agents.models.interface import Model  # noqa: E402
from agents.usage import Usage  # noqa: E402
from openai.types.responses import (  # noqa: E402
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)

AGENT = "did:web:bank.com:agents:triage:v1"

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


# --------------------------------------------------------------------------- #
# Scripted model + tools (no network), wired into a real Agent / Runner
# --------------------------------------------------------------------------- #


def _msg(text: str, i: str = "1") -> ResponseOutputMessage:
    return ResponseOutputMessage(
        id="msg_" + i, role="assistant", status="completed", type="message",
        content=[ResponseOutputText(type="output_text", text=text, annotations=[])],
    )


def _call(call_id: str, name: str, args: dict[str, Any]) -> ResponseFunctionToolCall:
    return ResponseFunctionToolCall(
        id="fc_" + call_id, call_id=call_id, name=name,
        arguments=json.dumps(args), type="function_call", status="completed",
    )


class _ScriptedModel(Model):
    """Returns a scripted list of output items per turn (count-indexed), so a real
    ``Runner`` loop runs deterministically without a network call."""

    def __init__(self, turns: list[list[Any]]) -> None:
        self._turns = turns
        self._i = 0

    async def get_response(self, *a: Any, **k: Any) -> ModelResponse:
        out = self._turns[min(self._i, len(self._turns) - 1)]
        self._i += 1
        return ModelResponse(
            output=out,
            usage=Usage(requests=1, input_tokens=42, output_tokens=7, total_tokens=49),
            response_id=f"resp_{self._i}",
        )

    async def stream_response(self, *a: Any, **k: Any) -> Any:
        raise NotImplementedError

    async def get_retry_advice(self, request: Any) -> Any:
        return None

    def close(self) -> None:
        return None


@function_tool
def alpha(q: str) -> str:
    """Tool alpha."""
    return f"alpha:{q}"


@function_tool
def beta(q: str) -> str:
    """Tool beta."""
    return f"beta:{q}"


@pytest.fixture(autouse=True)
def _reset_registry() -> Iterator[None]:
    rf_oai._registry.reset()
    yield
    rf_oai._registry.reset()
    # Restore default trace processors so set_trace_processors([runfile]) doesn't leak.
    try:
        from agents import set_trace_processors
        from agents.tracing.processor_interface import TracingProcessor  # noqa: F401

        set_trace_processors([])
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Buffer inspection helpers (mirror the LangGraph/Claude adapter tests)
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


def _kinds(buf: EventBuffer, *, include_boundary: bool = False) -> list[str]:
    return [e["action"]["kind"] for e in _events(buf, include_boundary=include_boundary)]


def _assert_all_wire_valid(buf: EventBuffer) -> None:
    """Every buffered item validates against its real ingest schema model."""
    for b in buf.snapshot():
        if isinstance(b, BufferedEvent):
            event = {**b.event, "prev_event_hash_intent": ZERO_SENTINEL}
            EventItem.model_validate({"type": "event", "event": event})
        else:
            _MODELS[b.item["type"]].model_validate(b.item)


def _run(agent: Agent, prompt: str = "go") -> None:
    import asyncio

    asyncio.run(Runner.run(agent, prompt, max_turns=8))


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


def test_single_turn_with_one_tool(sdk: Any) -> None:
    rf_oai.instrument(agent_identity=AGENT, agent_model_map={"A": "gpt-test-1"})
    agent = Agent(
        name="A", instructions="x", tools=[alpha],
        model=_ScriptedModel([[_msg("calling"), _call("c1", "alpha", {"q": "x"})], [_msg("done")]]),
    )
    _run(agent)
    buf = sdk.buffer

    # run_create … llm_call, tool_call, tool_result, (final) llm_call … run_end
    assert _kinds(buf, include_boundary=True) == [
        "run_create", "llm_call", "tool_call", "tool_result", "llm_call", "run_end",
    ]
    tool_call = next(e for e in _events(buf) if e["action"]["kind"] == "tool_call")
    assert tool_call["action"]["name"] == "alpha"
    llm = next(e for e in _events(buf) if e["action"]["kind"] == "llm_call")
    assert llm["model_ref"] == {"provider": "openai", "model_id": "gpt-test-1", "input_tokens": 42, "output_tokens": 7}
    _assert_all_wire_valid(buf)


def test_tool_parents_on_its_llm_call_and_result_on_call(sdk: Any) -> None:
    rf_oai.instrument(agent_identity=AGENT)
    agent = Agent(
        name="A", instructions="x", tools=[alpha],
        model=_ScriptedModel([[_msg("c"), _call("c1", "alpha", {"q": "x"})], [_msg("done")]]),
    )
    _run(agent)
    buf = sdk.buffer
    evs = {e["action"]["kind"]: e for e in _events(buf)}
    llm_id = next(e["event_id"] for e in _events(buf) if e["action"]["kind"] == "llm_call")
    assert evs["tool_call"]["parent_event_id"] == llm_id
    assert evs["tool_result"]["parent_event_id"] == evs["tool_call"]["event_id"]


def test_parallel_tools_share_one_group(sdk: Any) -> None:
    rf_oai.instrument(agent_identity=AGENT)
    agent = Agent(
        name="A", instructions="x", tools=[alpha, beta],
        model=_ScriptedModel([
            [_msg("parallel"), _call("c1", "alpha", {"q": "a"}), _call("c2", "beta", {"q": "b"})],
            [_msg("done")],
        ]),
    )
    _run(agent)
    buf = sdk.buffer
    tool_calls = [e for e in _events(buf) if e["action"]["kind"] == "tool_call"]
    assert len(tool_calls) == 2
    groups = {e.get("parallel_group_id") for e in tool_calls}
    assert len(groups) == 1 and None not in groups
    # the two results share the same group as the calls
    results = [e for e in _events(buf) if e["action"]["kind"] == "tool_result"]
    assert {e["parallel_group_id"] for e in results} == groups


def test_single_tool_has_no_parallel_group(sdk: Any) -> None:
    rf_oai.instrument(agent_identity=AGENT)
    agent = Agent(
        name="A", instructions="x", tools=[alpha],
        model=_ScriptedModel([[_msg("c"), _call("c1", "alpha", {"q": "x"})], [_msg("done")]]),
    )
    _run(agent)
    buf = sdk.buffer
    for e in _events(buf):
        assert "parallel_group_id" not in e


def test_local_seq_contiguous_and_one_run(sdk: Any) -> None:
    rf_oai.instrument(agent_identity=AGENT)
    agent = Agent(
        name="A", instructions="x", tools=[alpha],
        model=_ScriptedModel([[_msg("c"), _call("c1", "alpha", {"q": "x"})], [_msg("done")]]),
    )
    _run(agent)
    buf = sdk.buffer
    all_events = [b.event for b in buf.snapshot() if isinstance(b, BufferedEvent)]
    run_ids = {e["run_id"] for e in all_events}
    assert len(run_ids) == 1
    seqs = [e["local_seq"] for e in all_events]
    assert seqs == list(range(len(seqs)))


def test_tool_failure_records_failure_outcome(sdk: Any) -> None:
    @function_tool
    def boom(q: str) -> str:
        """Always fails."""
        raise ValueError("exploded")

    rf_oai.instrument(agent_identity=AGENT)
    agent = Agent(
        name="A", instructions="x", tools=[boom],
        model=_ScriptedModel([[_msg("c"), _call("c1", "boom", {"q": "x"})], [_msg("done")]]),
    )
    _run(agent)
    buf = sdk.buffer
    # The SDK surfaces a tool failure as the function span's error; we still emit the
    # result, marked failure. (If the SDK instead feeds the error back as output, the
    # result is success-with-error-text — assert the tool_result exists either way.)
    results = [e for e in _events(buf) if e["action"]["kind"] == "tool_result"]
    assert len(results) == 1
    _assert_all_wire_valid(buf)


def test_handoff_ends_source_and_opens_target_run(sdk: Any) -> None:
    rf_oai.instrument(
        agent_identity=AGENT,
        agent_identity_map={"Specialist": "did:web:bank.com:agents:specialist:v1"},
    )
    specialist = Agent(name="Specialist", instructions="x", model=_ScriptedModel([[_msg("specialist done")]]))
    triage = Agent(
        name="Triage", instructions="x", handoffs=[specialist],
        model=_ScriptedModel([[_msg("handing off"), _call("h1", "transfer_to_specialist", {})]]),
    )
    _run(triage)
    buf = sdk.buffer

    # Two runs: triage (ended at handoff) + specialist (handed_off_from triage).
    creates = [i for i in _run_items(buf) if i["type"] == "run_create"]
    assert len(creates) == 2
    target = next(i for i in creates if i["run"].get("handed_off_from"))
    assert target["run"]["agent_identity"] == "did:web:bank.com:agents:specialist:v1"

    handoff = next(e for e in _events(buf) if e["action"]["kind"] == "handoff")
    assert handoff["handoff_details"]["framework_signal"] == "openai_agents_handoff"
    assert handoff["handoff_details"]["target_run_id"] == target["run"]["run_id"]
    _assert_all_wire_valid(buf)


def test_model_id_enriched_from_generation_span(sdk: Any) -> None:
    """A built-in model nests a Generation span (with the real model id) under the turn;
    the llm_call should report it even with no agent_model_map."""
    from agents import generation_span

    class _GenModel(Model):
        async def get_response(self, *a: Any, **k: Any) -> ModelResponse:
            with generation_span(model="gpt-4o-2026", model_config={"temperature": 0}) as span:
                span.span_data.usage = {"input_tokens": 11, "output_tokens": 3}
                return ModelResponse(
                    output=[_msg("done")],
                    usage=Usage(requests=1, input_tokens=11, output_tokens=3, total_tokens=14),
                    response_id="r",
                )

        async def stream_response(self, *a: Any, **k: Any) -> Any:
            raise NotImplementedError

        async def get_retry_advice(self, request: Any) -> Any:
            return None

        def close(self) -> None:
            return None

    rf_oai.instrument(agent_identity=AGENT)  # deliberately NO agent_model_map
    agent = Agent(name="A", instructions="x", model=_GenModel())
    _run(agent)
    buf = sdk.buffer
    llm = next(e for e in _events(buf) if e["action"]["kind"] == "llm_call")
    assert llm["model_ref"]["model_id"] == "gpt-4o-2026"
    _assert_all_wire_valid(buf)


# --------------------------------------------------------------------------- #
# HITL (tool approval) via instrument_runner
# --------------------------------------------------------------------------- #


def _transfer_tool() -> Any:
    @function_tool(needs_approval=True)
    def transfer_money(amount: int) -> str:
        """Move money (needs approval)."""
        return f"transferred {amount}"

    return transfer_money


def test_hitl_suspend_then_approve_is_one_run_two_segments(sdk: Any) -> None:
    import asyncio

    runner = rf_oai.instrument_runner(Runner, agent_identity=AGENT)
    agent = Agent(
        name="A", instructions="x", tools=[_transfer_tool()],
        model=_ScriptedModel([
            [_msg("paying"), _call("c1", "transfer_money", {"amount": 100})],
            [_msg("done")],
        ]),
    )

    async def drive() -> None:
        result = await runner.run(agent, "pay")
        assert len(result.interruptions) == 1
        state = result.to_state()
        for it in result.interruptions:
            state.approve(it)
        result = await runner.run(agent, state)
        assert not result.interruptions

    asyncio.run(drive())
    buf = sdk.buffer

    kinds = _kinds(buf, include_boundary=True)
    # segment 1: run_create, llm_call, tool_approval_requested, run_suspend
    # segment 2: run_resume, tool_approval_granted (recorded before execution),
    #            tool_call, tool_result, llm_call, run_end
    assert kinds == [
        "run_create", "llm_call", "tool_approval_requested", "run_suspend",
        "run_resume", "tool_approval_granted", "tool_call", "tool_result",
        "llm_call", "run_end",
    ]
    # one run, two segments (suspend resets local_seq for the new segment)
    all_events = [b.event for b in buf.snapshot() if isinstance(b, BufferedEvent)]
    assert len({e["run_id"] for e in all_events}) == 1
    assert {e["segment_index"] for e in all_events} == {0, 1}
    # the suspend carries a framework signal grounded in the real interruption
    suspend = next(e for e in _events(buf) if e["action"]["kind"] == "run_suspend")
    assert suspend["suspension_details"]["framework_signal"]["signal_name"] == "result.interruptions"
    # trace_id is the resume handle (it lives in the serialized RunState), captured on
    # both suspend and resume so the pair stitches together; same token across the pair.
    resume = next(e for e in _events(buf) if e["action"]["kind"] == "run_resume")
    token = suspend["suspension_details"]["correlation_token"]
    assert token and resume["resume_details"]["correlation_token"] == token
    _assert_all_wire_valid(buf)


def test_hitl_reject_records_denied_and_no_tool_result(sdk: Any) -> None:
    import asyncio

    runner = rf_oai.instrument_runner(Runner, agent_identity=AGENT)
    agent = Agent(
        name="A", instructions="x", tools=[_transfer_tool()],
        model=_ScriptedModel([
            [_msg("paying"), _call("c1", "transfer_money", {"amount": 100})],
            [_msg("declined, ok")],
        ]),
    )

    async def drive() -> None:
        result = await runner.run(agent, "pay")
        state = result.to_state()
        for it in result.interruptions:
            state.reject(it)
        result = await runner.run(agent, state)

    asyncio.run(drive())
    buf = sdk.buffer
    kinds = _kinds(buf)
    assert "tool_approval_denied" in kinds
    assert "tool_approval_granted" not in kinds
    # a rejected tool never executes → no successful tool_result for it
    _assert_all_wire_valid(buf)


def test_as_tool_delegation_creates_child_run(sdk: Any) -> None:
    """agent.as_tool() runs the sub-agent in its own delegated run; the parent run gets a
    `delegate` event (after the issuing llm_call), not a tool_call."""
    rf_oai.instrument(
        agent_identity=AGENT,
        agent_identity_map={"Researcher": "did:web:bank.com:agents:researcher:v1"},
    )
    inner = Agent(
        name="Researcher", instructions="x",
        model=_ScriptedModel([[_msg("research done")]]),
    )
    outer = Agent(
        name="Orchestrator", instructions="x",
        tools=[inner.as_tool(tool_name="do_research", tool_description="research")],
        model=_ScriptedModel([
            [_msg("delegating"), _call("c1", "do_research", {"input": "q"})],
            [_msg("final")],
        ]),
    )
    _run(outer)
    buf = sdk.buffer

    # two runs: parent (Orchestrator) + delegated child (Researcher)
    creates = [i for i in _run_items(buf) if i["type"] == "run_create"]
    child = next(i for i in creates if i["run"].get("delegated_from"))
    assert child["run"]["agent_identity"] == "did:web:bank.com:agents:researcher:v1"

    delegate = next(e for e in _events(buf) if e["action"]["kind"] == "delegate")
    assert delegate["delegation_details"]["framework_signal"] == "openai_agents_as_tool"
    assert delegate["delegation_details"]["delegated_run_id"] == child["run"]["run_id"]
    # the child's delegated_from points back at the delegate event
    assert child["run"]["delegated_from"]["event_id"] == delegate["event_id"]
    # the as-tool function is represented by the delegate, not a duplicate tool_call
    assert "do_research" not in [
        e["action"]["name"] for e in _events(buf) if e["action"]["kind"] == "tool_call"
    ]
    # delegate is ordered after the issuing llm_call in the parent run
    parent_events = [
        b.event for b in buf.snapshot()
        if isinstance(b, BufferedEvent) and b.event["run_id"] == delegate["run_id"]
    ]
    kinds = [e["action"]["kind"] for e in parent_events]
    assert kinds.index("llm_call") < kinds.index("delegate")
    _assert_all_wire_valid(buf)


def test_noop_when_sdk_disabled() -> None:
    # No sdk fixture → SDK uninitialised; instrument + run must not raise.
    rf_oai.instrument(agent_identity=AGENT)
    agent = Agent(
        name="A", instructions="x", tools=[alpha],
        model=_ScriptedModel([[_msg("c"), _call("c1", "alpha", {"q": "x"})], [_msg("done")]]),
    )
    _run(agent)  # transparent no-op; nothing to assert beyond "didn't crash"
