"""End-to-end: capture -> flusher -> fake /v1/batches.

Exercises hash chaining, encryption, Pydantic validation, batch assembly, the
Idempotency-Key + retry path, and 207 handling against the in-process fake.
"""

from __future__ import annotations

import base64
import hashlib
import json

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

import runfile_ai
from runfile_ai._hashing import ZERO_SENTINEL, compute_event_hash
from runfile_ai.flusher import RetryConfig
from tests.conftest import VALID_TEST_KEY
from tests.fake_ingest import FakeIngest

AGENT = "did:web:bank.com:agents:loan-triage:v2"


def _init(server: FakeIngest) -> runfile_ai.RunfileClient:
    return runfile_ai.init(
        api_key=VALID_TEST_KEY, base_url=server.base_url, start_flusher=False
    )


def _batch_posts(server: FakeIngest) -> list[dict]:
    return [r.body for r in server.requests if r.path == "/v1/batches"]


def test_end_to_end_batch_shipped(fake_ingest: FakeIngest) -> None:
    inst = _init(fake_ingest)
    with runfile_ai.run(agent_identity=AGENT, conversation_id="conv1"):
        runfile_ai.capture_event(
            kind="llm_call",
            name="messages.create",
            payload={"prompt": "summarise the loan application"},
            model_ref={"provider": "anthropic", "model_id": "claude-opus-4-8"},
        )
        runfile_ai.capture_event(kind="tool_call", name="credit_check")
    runfile_ai.flush()

    assert fake_ingest.batch_count == 1
    posts = _batch_posts(fake_ingest)
    assert len(posts) == 1
    body = posts[0]

    # item order preserved, run_create synthesised from the lifecycle item
    assert [it["type"] for it in body["items"]] == ["run_create", "event", "event", "run_end"]

    # headers
    headers = next(r.headers for r in fake_ingest.requests if r.path == "/v1/batches")
    assert headers["runfile-sdk-name"] == "runfile-ai"
    assert headers["runfile-schema-version"] == "1.0"
    assert headers["authorization"].startswith("Bearer rf_test_")
    assert headers["idempotency-key"]

    events = [it["event"] for it in body["items"] if it["type"] == "event"]
    # required-nullable field survived the dump (not dropped as None)
    assert "parent_event_id" in events[0]
    assert events[0]["parent_event_id"] is None

    # the hash chain links: event0 starts at the zero sentinel; event1's intent
    # equals the SDK's hash of event0 (over the exact dumped bytes).
    assert events[0]["prev_event_hash_intent"] == ZERO_SENTINEL
    h0 = compute_event_hash({**events[0], "prev_event_hash": ZERO_SENTINEL})
    assert events[1]["prev_event_hash_intent"] == h0

    # the ciphertext is real: decrypts with the cached data key to the original.
    payload_ref = events[0]["payload_ref"]
    ciphertext = base64.b64decode(payload_ref["ciphertext_base64"])
    nonce = base64.b64decode(payload_ref["encryption"]["nonce"])
    key = inst.datakeys._entries[("self", AGENT)].key.plaintext
    cleartext = AESGCM(bytes(key)).decrypt(nonce, ciphertext, None)
    assert json.loads(cleartext) == {"prompt": "summarise the loan application"}
    assert payload_ref["sha256"] == "sha256:" + hashlib.sha256(ciphertext).hexdigest()
    # the non-payload event carries no payload_ref
    assert "payload_ref" not in events[1]


def test_chain_continues_across_flushes(fake_ingest: FakeIngest) -> None:
    _init(fake_ingest)
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="tool_call", name="a")
        runfile_ai.flush()  # batch 1: run_create + event0
        runfile_ai.capture_event(kind="tool_call", name="b")
        runfile_ai.flush()  # batch 2: event1
    runfile_ai.flush()  # batch 3: run_end

    posts = _batch_posts(fake_ingest)
    assert len(posts) == 3
    event0 = next(it["event"] for it in posts[0]["items"] if it["type"] == "event")
    event1 = next(it["event"] for it in posts[1]["items"] if it["type"] == "event")
    # chain state persisted across drains
    h0 = compute_event_hash({**event0, "prev_event_hash": ZERO_SENTINEL})
    assert event1["prev_event_hash_intent"] == h0


def test_retry_reuses_idempotency_key(fake_ingest: FakeIngest) -> None:
    inst = _init(fake_ingest)
    inst._flusher.retry = RetryConfig(base_ms=1, max_attempts=5, sleep=lambda _s: None)
    fake_ingest.batch_fail_statuses = [503, 503]  # two transient failures, then 200

    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="llm_call", name="a")
    runfile_ai.flush()

    posts = [r for r in fake_ingest.requests if r.path == "/v1/batches"]
    assert len(posts) == 3  # 2 failed + 1 success
    assert fake_ingest.batch_count == 1  # only the success counted
    keys = {r.headers["idempotency-key"] for r in posts}
    assert len(keys) == 1  # same Idempotency-Key across retries


def test_207_partial_is_not_retried(fake_ingest: FakeIngest) -> None:
    inst = _init(fake_ingest)
    fake_ingest.batch_return_207 = True

    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="llm_call", name="a")
    result = inst._flusher.flush_now()

    posts = [r for r in fake_ingest.requests if r.path == "/v1/batches"]
    assert len(posts) == 1  # 207 is terminal for the batch — no retry
    assert result.items_rejected == 1


def test_invalid_event_is_shipped_not_dropped(fake_ingest: FakeIngest) -> None:
    # llm_call without model_ref fails the schema's conditional. The flusher must
    # NOT drop it (that would tear the chain) — it ships raw and flags it.
    inst = _init(fake_ingest)
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="llm_call", name="messages.create")  # no model_ref
    result = inst._flusher.flush_now()

    assert result.items_invalid >= 1
    body = _batch_posts(fake_ingest)[0]
    kinds = [it["event"]["action"]["kind"] for it in body["items"] if it["type"] == "event"]
    assert "llm_call" in kinds  # present despite being locally invalid


def test_batch_size_cap_splits_batches(fake_ingest: FakeIngest) -> None:
    # With a tiny byte cap, sizeable events must split across multiple batches,
    # each under the cap, with no items lost.
    inst = _init(fake_ingest)
    inst._flusher.max_batch_bytes = 4096
    big = {"prompt": "x" * 1500}
    with runfile_ai.run(agent_identity=AGENT):
        for i in range(4):
            runfile_ai.capture_event(
                kind="llm_call",
                name=f"call{i}",
                payload=big,
                model_ref={"provider": "anthropic", "model_id": "claude-opus-4-8"},
            )
    runfile_ai.flush()

    posts = _batch_posts(fake_ingest)
    assert len(posts) >= 2  # split by the byte cap, not the 100-item cap
    for body in posts:
        assert len(json.dumps(body).encode("utf-8")) <= inst._flusher.max_batch_bytes
    # all 6 items delivered (run_create + 4 events + run_end), none dropped
    assert sum(len(b["items"]) for b in posts) == 6


def test_item_count_cap_splits_batches(fake_ingest: FakeIngest) -> None:
    inst = _init(fake_ingest)
    inst._flusher.max_items_per_batch = 3
    with runfile_ai.run(agent_identity=AGENT):
        for i in range(7):
            runfile_ai.capture_event(kind="tool_call", name=f"t{i}")
    runfile_ai.flush()

    posts = _batch_posts(fake_ingest)
    assert all(len(b["items"]) <= 3 for b in posts)
    assert sum(len(b["items"]) for b in posts) == 9  # run_create + 7 events + run_end


def test_datakey_unreachable_defers_not_drops(fake_ingest: FakeIngest) -> None:
    # Data-key endpoint down → payloads can't be encrypted. Items must wait in the
    # buffer (NOT dropped, NOT spooled as plaintext) and ship once it recovers.
    inst = _init(fake_ingest)
    fake_ingest.datakey_fail_statuses = [503]  # first mint fails

    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(
            kind="llm_call",
            name="messages.create",
            payload={"prompt": "secret"},
            model_ref={"provider": "anthropic", "model_id": "claude-opus-4-8"},
        )
    r1 = inst._flusher.flush_now()

    assert r1.items_deferred >= 1
    assert len(inst.buffer) >= 1  # held in memory for retry
    assert inst.spool.entries() == []  # never spooled (no plaintext to disk)
    assert r1.items_dropped == 0  # nothing lost

    # endpoint recovers → the deferred payload event ships and decrypts
    fake_ingest.datakey_fail_statuses = []
    inst._flusher.flush_now()
    assert len(inst.buffer) == 0

    events = [
        it["event"]
        for body in _batch_posts(fake_ingest)
        for it in body["items"]
        if it["type"] == "event"
    ]
    payload_events = [e for e in events if "payload_ref" in e]
    assert payload_events  # the previously-undeliverable event made it
    pr = payload_events[0]["payload_ref"]
    ciphertext = base64.b64decode(pr["ciphertext_base64"])
    nonce = base64.b64decode(pr["encryption"]["nonce"])
    key = inst.datakeys._entries[("self", AGENT)].key.plaintext
    assert AESGCM(bytes(key)).decrypt(nonce, ciphertext, None) == b'{"prompt":"secret"}'


def test_background_thread_drains(fake_ingest: FakeIngest) -> None:
    import time

    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY, base_url=fake_ingest.base_url, start_flusher=False
    )
    inst._flusher.interval_seconds = 0.05
    inst._flusher.start()
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(kind="llm_call", name="a", payload={"x": 1})

    deadline = time.monotonic() + 3.0
    while fake_ingest.batch_count == 0 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert fake_ingest.batch_count >= 1  # the daemon flusher shipped without an explicit flush()
