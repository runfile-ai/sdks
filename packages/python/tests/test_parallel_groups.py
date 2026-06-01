"""Flusher-side parallel-group assignment (structural concurrency detection)."""

from __future__ import annotations

from types import SimpleNamespace

from runfile_ai.buffer import BufferedEvent
from runfile_ai.flusher import _assign_parallel_groups


def _ev(event_id: str, kind: str, parent: str | None, run: str = "r1") -> BufferedEvent:
    return BufferedEvent(
        event={
            "event_id": event_id,
            "run_id": run,
            "parent_event_id": parent,
            "action": {"kind": kind, "name": kind},
        },
        raw_payload=None,
        run=SimpleNamespace(run_id=run),  # only .run_id is read off the run
    )


def _g(items):
    return [i.event.get("parallel_group_id") for i in items]


def test_concurrent_tool_calls_share_a_group() -> None:
    # llm L issues A and B back-to-back (no result between) → concurrent.
    items = [
        _ev("L", "llm_call", None),
        _ev("A", "tool_call", "L"),
        _ev("B", "tool_call", "L"),
        _ev("RA", "tool_result", "A"),
        _ev("RB", "tool_result", "B"),
    ]
    _assign_parallel_groups(items)
    g = _g(items)
    assert g[0] is None  # the llm_call isn't in the group
    assert g[1] == g[2] == g[3] == g[4] is not None  # calls + their results share it
    assert g[1].startswith("pg_")


def test_sequential_tool_calls_not_grouped() -> None:
    # call/result/call/result (same issuer) → sequential, never grouped.
    items = [
        _ev("L", "llm_call", None),
        _ev("A", "tool_call", "L"),
        _ev("RA", "tool_result", "A"),
        _ev("B", "tool_call", "L"),
        _ev("RB", "tool_result", "B"),
    ]
    _assign_parallel_groups(items)
    assert all(v is None for v in _g(items))


def test_three_way_fanout_grouped() -> None:
    items = [_ev("L", "llm_call", None)]
    items += [_ev(c, "tool_call", "L") for c in "ABC"]
    items += [_ev("R" + c, "tool_result", c) for c in "ABC"]
    _assign_parallel_groups(items)
    groups = {i.event.get("parallel_group_id") for i in items if i.event["action"]["kind"] != "llm_call"}
    assert len(groups) == 1 and None not in groups


def test_new_turn_breaks_the_batch() -> None:
    # A under turn L1, then a new turn L2 issues B — different issuers, not concurrent.
    items = [
        _ev("L1", "llm_call", None),
        _ev("A", "tool_call", "L1"),
        _ev("L2", "llm_call", None),
        _ev("B", "tool_call", "L2"),
    ]
    _assign_parallel_groups(items)
    assert all(v is None for v in _g(items))


def test_already_grouped_events_left_untouched() -> None:
    items = [
        _ev("L", "llm_call", None),
        _ev("A", "tool_call", "L"),
        _ev("B", "tool_call", "L"),
    ]
    items[1].event["parallel_group_id"] = "pg_PRESET0000000000000000000"
    _assign_parallel_groups(items)
    assert items[1].event["parallel_group_id"] == "pg_PRESET0000000000000000000"  # preserved
    assert items[2].event.get("parallel_group_id") is None  # left alone (single → no group)
