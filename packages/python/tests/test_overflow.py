"""Buffer overflow: backpressure (flush) vs whole-run drop."""

from __future__ import annotations

import runfile_ai
from runfile_ai.buffer import BufferedEvent
from tests.conftest import VALID_TEST_KEY
from tests.fake_ingest import FakeIngest

AGENT = "did:web:bank.com:agents:loan-triage:v2"


def test_backpressure_flushes_on_soft_cap(fake_ingest: FakeIngest) -> None:
    # blocking (default): hitting the cap triggers a synchronous flush — no loss.
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY,
        base_url=fake_ingest.base_url,
        start_flusher=False,
        buffer_soft_cap=2,
    )
    with runfile_ai.run(agent_identity=AGENT):  # run_create append already hits cap=2
        runfile_ai.capture_event(kind="tool_call", name="a")
        runfile_ai.capture_event(kind="tool_call", name="b")
    runfile_ai.flush()

    # everything shipped, nothing dropped; buffer drained by backpressure flushes
    assert fake_ingest.batch_count >= 1
    assert len(inst.buffer) == 0


def test_nonblocking_drops_whole_run_with_diagnostic() -> None:
    # non-blocking + unreachable endpoint: on overflow, drop the WHOLE run.
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY,
        base_url="http://localhost:9",
        start_flusher=False,
        fetch_policy=False,
        buffer_soft_cap=3,
        capture_blocking=False,
    )
    run_obj = runfile_ai.start_run(agent_identity=AGENT)  # item 1: run_create
    runfile_ai.capture_event(kind="tool_call", name="a")  # item 2
    runfile_ai.capture_event(kind="tool_call", name="b")  # item 3 -> hits cap -> drop run
    assert run_obj.dropped is True

    snap = inst.buffer.snapshot()
    # the run's normal items were removed; only the diagnostic remains
    kinds = [b.event["action"]["kind"] for b in snap if isinstance(b, BufferedEvent)]
    assert kinds == ["sdk_diagnostic"]
    diag = next(b.event for b in snap if isinstance(b, BufferedEvent))
    assert diag["action"]["name"] == "run_dropped_overflow"
    assert int(diag["labels"]["dropped_event_count"]) >= 1

    # further captures on the dropped run are silently no-ops (don't re-fill)
    assert runfile_ai.capture_event(kind="tool_call", name="c") == ""
    kinds_after = [b.event["action"]["kind"] for b in inst.buffer.snapshot() if isinstance(b, BufferedEvent)]
    assert kinds_after == ["sdk_diagnostic"]
