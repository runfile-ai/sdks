"""Startup schema-version negotiation via GET /v1/health."""

from __future__ import annotations

import runfile_ai
from tests.conftest import VALID_TEST_KEY
from tests.fake_ingest import FakeIngest


def test_supported_version_no_warning(fake_ingest: FakeIngest) -> None:
    fake_ingest.schema_versions_supported = ["1.0"]
    records: list[dict] = []
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY,
        base_url=fake_ingest.base_url,
        start_flusher=False,
        on_diagnostic=records.append,
    )  # fetch_policy default True → health negotiation runs
    assert inst.server_schema_versions == ["1.0"]
    assert not any(r["code"] == "schema_version_unsupported" for r in records)
    # the SDK actually called /v1/health
    assert any(r.path == "/v1/health" for r in fake_ingest.requests)


def test_unsupported_version_warns_but_continues(fake_ingest: FakeIngest) -> None:
    fake_ingest.schema_versions_supported = ["2.0"]  # server ahead/behind
    records: list[dict] = []
    runfile_ai.init(
        api_key=VALID_TEST_KEY,
        base_url=fake_ingest.base_url,
        start_flusher=False,
        on_diagnostic=records.append,
    )
    warning = next(r for r in records if r["code"] == "schema_version_unsupported")
    assert warning["severity"] == "warning"
    assert "2.0" in warning["detail"]
    # keeps running: capture still works (visible failure beats silent break)
    with runfile_ai.run(agent_identity="did:web:acme.com:agents:x"):
        assert runfile_ai.capture_event(kind="tool_call", name="a")  # non-empty id


def test_health_unreachable_is_best_effort() -> None:
    # check_health explicitly on, but endpoint down → no raise, no false warning
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY,
        base_url="http://localhost:9",
        start_flusher=False,
        fetch_policy=False,
        check_health=True,
    )
    assert inst.server_schema_versions is None


def test_check_health_defaults_to_fetch_policy() -> None:
    # fetch_policy=False (offline) → health negotiation is skipped (no network)
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY,
        base_url="http://localhost:9",
        start_flusher=False,
        fetch_policy=False,
    )
    assert inst.server_schema_versions is None
