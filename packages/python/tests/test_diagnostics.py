"""sdk_diagnostic emission: policy-refresh failure and auth failure."""

from __future__ import annotations

import runfile_ai
from runfile_ai.flusher import RetryConfig
from tests.conftest import VALID_TEST_KEY
from tests.fake_ingest import FakeIngest

AGENT = "did:web:bank.com:agents:loan-triage:v2"


def test_policy_refresh_failure_emits_diagnostic() -> None:
    records: list[dict] = []
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY,
        base_url="http://localhost:9",  # unreachable
        start_flusher=False,
        fetch_policy=False,
        on_diagnostic=records.append,
    )
    inst.refresh_policy_if_stale()  # endpoint down → must not raise, must emit

    assert any(r["code"] == "policy_refresh_failed" for r in records)
    assert inst.diagnostics  # also recorded on the client


def test_policy_failure_on_init_is_recorded() -> None:
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY, base_url="http://localhost:9", start_flusher=False
    )  # fetch_policy defaults True → init attempts + fails + records
    assert any(d["code"] == "policy_refresh_failed" for d in inst.diagnostics)


def test_auth_failure_emits_stops_shipping_and_spools(fake_ingest: FakeIngest) -> None:
    records: list[dict] = []
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY,
        base_url=fake_ingest.base_url,
        start_flusher=False,
        on_diagnostic=records.append,
    )
    inst._flusher.retry = RetryConfig(base_ms=1, max_attempts=1, sleep=lambda _s: None)
    fake_ingest.batch_fail_statuses = [401]  # revoked/invalid key (terminal)

    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="tool_call", name="a")
    r1 = inst._flusher.flush_now()

    assert any(rec["code"] == "auth_failure" and rec["severity"] == "error" for rec in records)
    assert inst._flusher._auth_failed is True
    assert r1.items_spooled >= 1  # encrypted batch kept, not dropped
    assert r1.items_dropped == 0

    # subsequent drains do NOT attempt to POST (auth is broken) — only one POST ever
    posts_before = len([r for r in fake_ingest.requests if r.path == "/v1/batches"])
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="tool_call", name="b")
    inst._flusher.flush_now()
    posts_after = len([r for r in fake_ingest.requests if r.path == "/v1/batches"])
    assert posts_after == posts_before == 1


def test_diagnostics_silent_by_default(fake_ingest: FakeIngest) -> None:
    # No on_diagnostic callback: failures are recorded but nothing is raised/printed.
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY, base_url="http://localhost:9", start_flusher=False
    )
    inst.refresh_policy_if_stale()
    assert inst._on_diagnostic is None
    assert any(d["code"] == "policy_refresh_failed" for d in inst.diagnostics)
