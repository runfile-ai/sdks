"""Turn-atomic flushing: the buffer's held-suffix watermark + the flusher's
drain-selection (flushable prefix, safety valve, force-all override).

The end-to-end "a turn split across a flush still groups" guarantee is exercised
against the real Claude stream shape in ``test_claude_adapter.py``; this file
unit-tests the mechanism underneath it.
"""

from __future__ import annotations

from types import SimpleNamespace

from runfile_ai.buffer import BufferedRunItem, EventBuffer


def _item(run_id: str = "run_x", n: int = 0) -> BufferedRunItem:
    # BufferedRunItem only needs .run for .run_id; a namespace stands in for a Run.
    return BufferedRunItem(item={"type": "run_update", "n": n}, run=SimpleNamespace(run_id=run_id))


# --------------------------------------------------------------------------- #
# Buffer: held-suffix watermark
# --------------------------------------------------------------------------- #


def test_unarmed_buffer_flushes_everything() -> None:
    buf = EventBuffer()
    for i in range(3):
        buf.append(_item(n=i))
    # never armed → take_flushable == take_all (the manual / OTel path)
    assert [b.item["n"] for b in buf.take_flushable()] == [0, 1, 2]
    assert len(buf) == 0


def test_armed_holds_open_turn_until_watermark_advances() -> None:
    buf = EventBuffer()
    buf.append(_item(n=0))  # captured before arming → flushable
    buf.arm_turn_atomic()
    buf.append(_item(n=1))  # open turn (held)
    buf.append(_item(n=2))  # open turn (held)

    # only the pre-arm item is flushable; the open turn is held back
    assert [b.item["n"] for b in buf.take_flushable()] == [0]
    assert len(buf) == 2  # turn still held

    # turn closes → its events become flushable
    buf.advance_flush_watermark()
    assert [b.item["n"] for b in buf.take_flushable()] == [1, 2]
    assert len(buf) == 0


def test_advance_releases_prior_turn_but_holds_the_next() -> None:
    buf = EventBuffer()
    buf.arm_turn_atomic()
    buf.append(_item(n=0))  # turn A
    buf.append(_item(n=1))  # turn A
    buf.advance_flush_watermark()  # turn A closed
    buf.append(_item(n=2))  # turn B (held)

    # take_flushable releases all of A, holds B
    assert [b.item["n"] for b in buf.take_flushable()] == [0, 1]
    assert [b.item["n"] for b in buf.snapshot()] == [2]


def test_take_all_ignores_watermark() -> None:
    buf = EventBuffer()
    buf.arm_turn_atomic()
    buf.append(_item(n=0))
    buf.append(_item(n=1))  # open, held — but force-drain takes it anyway
    assert [b.item["n"] for b in buf.take_all()] == [0, 1]
    assert len(buf) == 0
    # held_suffix reset: a fresh append is held again only after re-open
    buf.append(_item(n=2))
    assert [b.item["n"] for b in buf.take_flushable()] == []  # still held (armed)


def test_requeue_front_keeps_held_suffix_trailing() -> None:
    buf = EventBuffer()
    buf.arm_turn_atomic()
    buf.append(_item(n=0))
    buf.append(_item(n=1))  # held
    flushed = buf.take_flushable()  # nothing flushable yet
    assert flushed == []
    buf.advance_flush_watermark()
    taken = buf.take_flushable()  # now [0, 1]
    # the data-key-unreachable path puts them back at the front; still flushable
    buf.requeue_front(taken)
    buf.append(_item(n=2))  # a new open turn arrives behind them (held)
    assert [b.item["n"] for b in buf.take_flushable()] == [0, 1]
    assert [b.item["n"] for b in buf.snapshot()] == [2]


def test_drop_run_adjusts_held_suffix() -> None:
    buf = EventBuffer()
    buf.arm_turn_atomic()
    buf.append(_item("run_a", 0))  # flushable-to-be
    buf.advance_flush_watermark()
    buf.append(_item("run_a", 1))  # held
    buf.append(_item("run_b", 2))  # held (different run)
    # drop run_a removes its flushable AND held item; held_suffix tracks the change
    removed = buf.drop_run("run_a")
    assert removed == 2
    # run_b's held item remains, still held
    assert buf.take_flushable() == []
    buf.advance_flush_watermark()
    assert [b.item["n"] for b in buf.take_flushable()] == [2]


# --------------------------------------------------------------------------- #
# Flusher: drain selection (uses the buffer above through a stub client)
# --------------------------------------------------------------------------- #


class _StubClient:
    def __init__(self, buffer: EventBuffer) -> None:
        self.buffer = buffer


def _flusher(buf: EventBuffer, clock: list[float]):
    from runfile_ai.flusher import Flusher

    f = Flusher(client=_StubClient(buf), interval_seconds=2.0, turn_hold_max_seconds=5.0)
    f.monotonic = lambda: clock[0]
    return f


def test_take_for_drain_holds_open_turn() -> None:
    buf = EventBuffer()
    buf.arm_turn_atomic()
    buf.append(_item(n=0))
    buf.advance_flush_watermark()
    buf.append(_item(n=1))  # open turn
    clock = [100.0]
    f = _flusher(buf, clock)
    # closed turn drains; open turn held
    assert [b.item["n"] for b in f._take_for_drain(force_all=False)] == [0]
    assert len(buf) == 1


def test_force_all_drains_open_turn() -> None:
    buf = EventBuffer()
    buf.arm_turn_atomic()
    buf.append(_item(n=0))  # held
    f = _flusher(buf, [100.0])
    assert [b.item["n"] for b in f._take_for_drain(force_all=True)] == [0]


def test_safety_valve_releases_stalled_turn() -> None:
    buf = EventBuffer()
    buf.arm_turn_atomic()
    buf.append(_item(n=0))  # open turn that never closes (stalled stream)
    clock = [100.0]
    f = _flusher(buf, clock)

    # drain 1: nothing flushable; the held turn starts its stall clock
    assert f._take_for_drain(force_all=False) == []
    assert len(buf) == 1
    # drain 2, still within the cap (max(2,5)=5s): held, not yet released
    clock[0] = 104.0
    assert f._take_for_drain(force_all=False) == []
    assert len(buf) == 1
    # drain 3, past the cap: safety valve releases the stalled turn
    clock[0] = 106.0
    assert [b.item["n"] for b in f._take_for_drain(force_all=False)] == [0]
    assert len(buf) == 0


def test_safety_valve_resets_when_turn_grows() -> None:
    buf = EventBuffer()
    buf.arm_turn_atomic()
    buf.append(_item(n=0))
    clock = [100.0]
    f = _flusher(buf, clock)

    f._take_for_drain(force_all=False)  # stall clock starts at 100
    clock[0] = 104.0
    buf.append(_item(n=1))  # turn made progress → clock should reset
    f._take_for_drain(force_all=False)
    clock[0] = 107.0  # 3s since the reset — under the 5s cap, still held
    assert f._take_for_drain(force_all=False) == []
    assert len(buf) == 2
