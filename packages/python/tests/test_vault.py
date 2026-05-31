"""Vault tokenization: client mapping + end-to-end tokenize redaction."""

from __future__ import annotations

import base64

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

import runfile_ai
from runfile_ai.vault import VaultClient
from tests.conftest import VALID_TEST_KEY
from tests.fake_ingest import FakeIngest

AGENT = "did:web:bank.com:agents:loan-triage:v2"


def test_vault_client_maps_classification_and_returns_token(
    fake_ingest: FakeIngest, http_client: object
) -> None:
    import httpx

    assert isinstance(http_client, httpx.Client)
    vault = VaultClient(client=http_client, base_url=fake_ingest.base_url, api_key=VALID_TEST_KEY)
    token = vault.tokenize("alice@example.com", "email_address")

    assert token is not None and token.startswith("tok_")
    req = next(r for r in fake_ingest.requests if r.path == "/v1/tokenize")
    assert req.body["classification"] == "email"  # email_address -> Vault 'email'
    assert req.body["value"] == "alice@example.com"
    assert req.headers["authorization"].startswith("Bearer rf_test_")


def test_vault_client_returns_none_on_error(fake_ingest: FakeIngest, http_client: object) -> None:
    import httpx

    assert isinstance(http_client, httpx.Client)
    # unreachable vault → None (redactor will drop, never leak)
    vault = VaultClient(client=http_client, base_url="http://localhost:9", api_key=VALID_TEST_KEY)
    assert vault.tokenize("x", "email_address") is None


def test_end_to_end_tokenize_replaces_pii_with_token(fake_ingest: FakeIngest) -> None:
    fake_ingest.policy_rules = [{"classification": "email_address", "treatment": "tokenize"}]
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY,
        base_url=fake_ingest.base_url,
        vault_base_url=fake_ingest.base_url,  # fake serves /v1/tokenize too
        start_flusher=False,
    )
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(
            kind="llm_call",
            name="messages.create",
            payload={"prompt": "email alice@example.com"},
            model_ref={"provider": "anthropic", "model_id": "claude-opus-4-8"},
        )
    runfile_ai.flush()

    assert fake_ingest.tokenize_count >= 1
    body = next(r.body for r in fake_ingest.requests if r.path == "/v1/batches")
    event = next(it["event"] for it in body["items"] if it["type"] == "event")
    payload_ref = event["payload_ref"]
    assert payload_ref["redaction_applied"]["tokenized_classes"] == ["email_address"]

    ciphertext = base64.b64decode(payload_ref["ciphertext_base64"])
    nonce = base64.b64decode(payload_ref["encryption"]["nonce"])
    key = inst.datakeys._entries[("self", AGENT)].key.plaintext
    cleartext = AESGCM(bytes(key)).decrypt(nonce, ciphertext, None)
    assert b"alice@example.com" not in cleartext  # replaced with a token
    assert b"tok_" in cleartext
