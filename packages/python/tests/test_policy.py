"""Tests for redaction-policy fetch + cache and version stamping."""

from __future__ import annotations

import runfile_ai
from runfile_ai.buffer import BufferedEvent, BufferedRunItem
from runfile_ai.policy import DEFAULT_POLICY_VERSION
from tests.conftest import VALID_TEST_KEY
from tests.fake_ingest import FakeIngest

AGENT = "did:web:bank.com:agents:loan-triage:v2"


def test_policy_fetched_on_init_and_stamped(fake_ingest: FakeIngest) -> None:
    fake_ingest.policy_version = "3.2.1"
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY, base_url=fake_ingest.base_url, start_flusher=False
    )
    assert inst.redaction_policy_version == "3.2.1"

    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="tool_call", name="a")

    snap = inst.buffer.snapshot()
    run_create = next(b.item for b in snap if isinstance(b, BufferedRunItem))
    event = next(b.event for b in snap if isinstance(b, BufferedEvent))
    assert run_create["run"]["redaction_policy_version"] == "3.2.1"
    assert event["redaction_policy_version"] == "3.2.1"

    # the fetch went to the right place with the bearer token
    policy_req = next(r for r in fake_ingest.requests if r.path == "/v1/policies/current")
    assert policy_req.method == "GET"
    assert policy_req.headers["authorization"].startswith("Bearer rf_test_")


def test_policy_fetch_failure_falls_back_to_default() -> None:
    # unreachable endpoint: refresh is best-effort and must not raise.
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY, base_url="http://localhost:9", start_flusher=False
    )
    assert inst.redaction_policy_version == DEFAULT_POLICY_VERSION
