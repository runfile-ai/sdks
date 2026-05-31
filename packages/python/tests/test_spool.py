"""Spool durability: failed ships persist (ciphertext only) and re-deliver."""

from __future__ import annotations

import runfile_ai
from runfile_ai.flusher import RetryConfig
from runfile_ai.spool import Spool
from tests.conftest import VALID_TEST_KEY
from tests.fake_ingest import FakeIngest

AGENT = "did:web:bank.com:agents:loan-triage:v2"


def test_failed_ship_is_spooled_then_redelivered(fake_ingest: FakeIngest) -> None:
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY, base_url=fake_ingest.base_url, start_flusher=False
    )
    # exhaust retries quickly: more forced failures than attempts.
    inst._flusher.retry = RetryConfig(base_ms=1, max_attempts=2, sleep=lambda _s: None)
    fake_ingest.batch_fail_statuses = [503, 503, 503, 503, 503, 503]

    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="tool_call", name="a")
    r1 = inst._flusher.flush_now()  # all attempts fail → batch goes to spool

    assert r1.items_spooled >= 1
    assert fake_ingest.batch_count == 0
    assert inst.spool.entries(), "batch should be on disk"

    # endpoint recovers; next drain re-sends from the spool with the same key.
    fake_ingest.batch_fail_statuses = []
    spooled_key = inst.spool.entries()[0].idempotency_key
    inst._flusher.flush_now()

    assert fake_ingest.batch_count == 1
    assert inst.spool.entries() == []  # cleared after delivery
    delivered = [r for r in fake_ingest.requests if r.path == "/v1/batches"][-1]
    assert delivered.headers["idempotency-key"] == spooled_key  # dedupe preserved


def test_spool_only_holds_ciphertext(fake_ingest: FakeIngest) -> None:
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY, base_url=fake_ingest.base_url, start_flusher=False
    )
    inst._flusher.retry = RetryConfig(base_ms=1, max_attempts=1, sleep=lambda _s: None)
    fake_ingest.batch_fail_statuses = [503]
    secret = "social-security-123-45-6789"

    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(
            kind="llm_call",
            name="messages.create",
            payload={"prompt": secret},
            model_ref={"provider": "anthropic", "model_id": "claude-opus-4-8"},
        )
    inst._flusher.flush_now()

    raw = inst.spool.entries()[0].path.read_bytes()
    assert secret.encode() not in raw  # plaintext never hits disk
    body = inst.spool.entries()[0].body
    pr = next(it["event"]["payload_ref"] for it in body["items"] if it["type"] == "event")
    assert "ciphertext_base64" in pr  # only ciphertext is persisted


def test_spool_full_drops_with_flag(fake_ingest: FakeIngest) -> None:
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY, base_url=fake_ingest.base_url, start_flusher=False
    )
    inst.spool = Spool(directory=inst.spool.directory, max_bytes=1)  # full immediately
    inst._flusher.retry = RetryConfig(base_ms=1, max_attempts=1, sleep=lambda _s: None)
    fake_ingest.batch_fail_statuses = [503]

    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="tool_call", name="a")
    result = inst._flusher.flush_now()

    assert result.items_dropped >= 1
    assert result.items_spooled == 0
    assert inst.spool.entries() == []
