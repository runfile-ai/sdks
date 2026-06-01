"""Tests for run lifecycle, event construction, ordering, segments, parallel groups."""

from __future__ import annotations

import pytest

import runfile_ai
from runfile_ai.buffer import BufferedEvent, BufferedRunItem, EventBuffer

AGENT = "did:web:bank.com:agents:loan-triage:v2"


# Witness-authored lifecycle: run_create / run_end are now real chain EVENTS, not
# just items. These activity-focused helpers exclude the two boundary kinds so the
# assertions about captured *activity* stay meaningful; the genesis behaviour has
# its own dedicated test (test_run_create_is_witness_authored_genesis).
_LIFECYCLE_BOUNDARY = {"run_create", "run_end"}


def _all_events(buffer: EventBuffer) -> list[dict]:
    return [b.event for b in buffer.snapshot() if isinstance(b, BufferedEvent)]


def _events(buffer: EventBuffer) -> list[dict]:
    return [e for e in _all_events(buffer) if e["action"]["kind"] not in _LIFECYCLE_BOUNDARY]


def _run_items(buffer: EventBuffer) -> list[dict]:
    return [b.item for b in buffer.snapshot() if isinstance(b, BufferedRunItem)]


def test_capture_without_init_raises() -> None:
    with pytest.raises(RuntimeError):
        runfile_ai.start_run(agent_identity=AGENT)


def test_capture_without_run_raises(sdk) -> None:
    with pytest.raises(RuntimeError):
        runfile_ai.capture_event(kind="llm_call", name="x")


def test_run_context_manager_emits_create_and_end(sdk) -> None:
    with runfile_ai.run(agent_identity=AGENT) as r:
        runfile_ai.capture_event(kind="llm_call", name="messages.create")
        assert runfile_ai.current_run() is r

    assert runfile_ai.current_run() is None  # context cleared on exit
    items = _run_items(sdk.buffer)
    types = [i["type"] for i in items]
    assert types == ["run_create", "run_end"]
    assert items[0]["run"]["agent_identity"] == AGENT
    assert items[0]["run"]["lifecycle_state"] == "active"
    assert items[-1]["outcome"] == "success"


def test_failure_outcome_on_exception(sdk) -> None:
    with pytest.raises(ValueError):
        with runfile_ai.run(agent_identity=AGENT):
            raise ValueError("boom")
    assert _run_items(sdk.buffer)[-1]["outcome"] == "failure"


def test_local_seq_and_parent_chaining(sdk) -> None:
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="llm_call", name="a")
        runfile_ai.capture_event(kind="tool_call", name="b")
        runfile_ai.capture_event(kind="tool_result", name="c")

    events = _events(sdk.buffer)
    # local_seq 0 belongs to the witness-authored run_create genesis, so the first
    # real event is seq 1; activity events stay contiguous from there.
    assert [e["local_seq"] for e in events] == [1, 2, 3]
    assert [e["segment_index"] for e in events] == [0, 0, 0]
    # parent threads to the previous event; first real event's parent is None
    assert events[0]["parent_event_id"] is None
    assert events[1]["parent_event_id"] == events[0]["event_id"]
    assert events[2]["parent_event_id"] == events[1]["event_id"]


def test_run_create_is_witness_authored_genesis(sdk) -> None:
    """The SDK authors run_create/run_end as real chain events (not bare items)."""
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="llm_call", name="a")

    events = _all_events(sdk.buffer)
    # the event chain opens with a run_create EVENT at seq 0 and closes with run_end
    assert events[0]["action"]["kind"] == "run_create"
    assert events[0]["local_seq"] == 0
    assert events[0]["segment_index"] == 0
    assert events[0]["parent_event_id"] is None
    assert events[-1]["action"]["kind"] == "run_end"
    # the companion run_create / run_end ITEMS still ride along for runs-row materialisation
    assert [i["type"] for i in _run_items(sdk.buffer)] == ["run_create", "run_end"]


def test_default_actor_is_the_run_agent(sdk) -> None:
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="llm_call", name="a")
    e = _events(sdk.buffer)[0]
    assert e["actor"] == {"type": "agent", "agent_identity": AGENT}
    assert e["sdk"]["name"] == "runfile-ai"


def test_optional_fields_pass_through(sdk) -> None:
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(
            kind="llm_call",
            name="messages.create",
            model_ref={"provider": "anthropic", "model_id": "claude-opus-4-8"},
            subject={"data_classification": "pii"},
        )
    e = _events(sdk.buffer)[0]
    assert e["model_ref"]["model_id"] == "claude-opus-4-8"
    assert e["subject"]["data_classification"] == "pii"


def test_suspend_emits_event_and_run_update_linked(sdk) -> None:
    with runfile_ai.run(agent_identity=AGENT):
        eid = runfile_ai.suspend_run(reason="awaiting_human_approval", expected_resumer="tok_alice")

    suspend_events = [e for e in _events(sdk.buffer) if e["action"]["kind"] == "run_suspend"]
    assert len(suspend_events) == 1
    assert suspend_events[0]["event_id"] == eid
    assert suspend_events[0]["suspension_details"]["reason"] == "awaiting_human_approval"
    assert suspend_events[0]["suspension_details"]["detection_source"] == "customer_explicit"

    updates = [i for i in _run_items(sdk.buffer) if i["type"] == "run_update"]
    assert updates[0]["lifecycle_state"] == "awaiting_human"
    assert updates[0]["triggered_by_event_id"] == eid


def test_resume_opens_new_segment(sdk) -> None:
    with runfile_ai.run(agent_identity=AGENT) as r:
        runfile_ai.capture_event(kind="llm_call", name="a")  # seg 0, seq 0
        runfile_ai.suspend_run(reason="awaiting_human_approval")  # seg 0, seq 1
        runfile_ai.resume_run(triggered_by="human_approval_granted")  # seg 1, seq 0
        runfile_ai.capture_event(kind="llm_call", name="b")  # seg 1, seq 1
        assert r.segment_index == 1

    events = _events(sdk.buffer)
    by_kind = {e["action"]["kind"]: e for e in events}
    assert (by_kind["run_resume"]["segment_index"], by_kind["run_resume"]["local_seq"]) == (1, 0)
    last = events[-1]
    assert (last["segment_index"], last["local_seq"]) == (1, 1)


def test_parallel_group_tags_inner_events(sdk) -> None:
    with runfile_ai.run(agent_identity=AGENT):
        with runfile_ai.parallel_group() as gid:
            runfile_ai.capture_event(kind="tool_call", name="transunion")
            runfile_ai.capture_event(kind="tool_call", name="equifax")

    events = _events(sdk.buffer)
    opens = [e for e in events if e["action"]["kind"] == "parallel_group_open"]
    closes = [e for e in events if e["action"]["kind"] == "parallel_group_close"]
    tool_calls = [e for e in events if e["action"]["kind"] == "tool_call"]
    assert len(opens) == 1 and len(closes) == 1
    assert all(e["parallel_group_id"] == gid for e in tool_calls)
    # context is closed after the block — a later event carries no group id
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="llm_call", name="after")
    assert "parallel_group_id" not in _events(sdk.buffer)[-1]


def test_abandon_ends_run(sdk) -> None:
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.suspend_run(reason="awaiting_human_approval")
        runfile_ai.abandon_run(reason="approver_timed_out")
    # abandon cleared context, so the context manager won't double-emit run_end
    kinds = [e["action"]["kind"] for e in _events(sdk.buffer)]
    assert "run_abandon" in kinds
    last_update = [i for i in _run_items(sdk.buffer) if i["type"] == "run_update"][-1]
    assert last_update["lifecycle_state"] == "ended"
