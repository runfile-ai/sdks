"""Tests for the event hash-chain wrapper (delegates to runfile_schemas.canonical)."""

from __future__ import annotations

import re

from runfile_ai._hashing import ZERO_SENTINEL, compute_event_hash

SHA256_RE = re.compile(r"^sha256:[a-f0-9]{64}$")

_BASE = {
    "schema_version": "1.0.0",
    "event_id": "01H8X9N3K2P4Q5RTVWX6YZ5678",
    "run_id": "run_01H8X9N3K2P4Q5RTVWX6YZ1234",
    "action": {"kind": "llm_call", "name": "messages.create"},
}


def test_zero_sentinel_shape() -> None:
    assert ZERO_SENTINEL == "sha256:" + "0" * 64
    assert SHA256_RE.match(ZERO_SENTINEL)


def test_hash_is_deterministic_and_well_formed() -> None:
    event = {**_BASE, "prev_event_hash": ZERO_SENTINEL}
    h = compute_event_hash(event)
    assert SHA256_RE.match(h)
    assert compute_event_hash(event) == h


def test_hash_commits_to_prev_event_hash() -> None:
    h0 = compute_event_hash({**_BASE, "prev_event_hash": ZERO_SENTINEL})
    h1 = compute_event_hash({**_BASE, "prev_event_hash": "sha256:" + "1" * 64})
    assert h0 != h1  # changing the link changes the hash → real chain


def test_hash_ignores_server_set_fields() -> None:
    base = {**_BASE, "prev_event_hash": ZERO_SENTINEL}
    h = compute_event_hash(base)
    # tenant_id/received_at/event_hash/anomaly_flags/merkle_inclusion are server-set
    polluted = {
        **base,
        "tenant_id": "tnt_xxxxxxxxxxxx",
        "received_at": "2026-05-31T00:00:00Z",
        "event_hash": "sha256:" + "f" * 64,
        "anomaly_flags": [{"code": "chain_break", "severity": "warning"}],
    }
    assert compute_event_hash(polluted) == h


def test_hash_commits_to_payload_ref() -> None:
    # payload_ref IS hashed — so the chain can only be computed after encryption.
    base = {**_BASE, "prev_event_hash": ZERO_SENTINEL}
    with_payload = {**base, "payload_ref": {"sha256": "sha256:" + "a" * 64}}
    assert compute_event_hash(with_payload) != compute_event_hash(base)
