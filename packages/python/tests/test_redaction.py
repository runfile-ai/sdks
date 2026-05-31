"""PII classifier + redactor: detection and drop/hash/pass_through treatments."""

from __future__ import annotations

import base64
import hashlib

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

import runfile_ai
from runfile_ai.classifier import Redactor
from runfile_ai.policy import RedactionPolicy
from tests.conftest import VALID_TEST_KEY
from tests.fake_ingest import FakeIngest

AGENT = "did:web:bank.com:agents:loan-triage:v2"


def _policy(rules: list[dict]) -> RedactionPolicy:
    return RedactionPolicy(policy_version="1.0.0", classification_rules=rules)


def test_no_policy_is_passthrough() -> None:
    result = Redactor().apply({"prompt": "alice@example.com"}, None)
    assert result.value == {"prompt": "alice@example.com"}
    assert result.redacted_classes == []


def test_drop_email() -> None:
    result = Redactor().apply(
        {"to": "alice@example.com", "ok": "no pii here"},
        _policy([{"classification": "email_address", "treatment": "drop"}]),
    )
    assert result.value["to"] == "[REDACTED:email_address]"
    assert result.value["ok"] == "no pii here"
    assert result.redacted_classes == ["email_address"]


def test_hash_ssn_is_deterministic() -> None:
    result = Redactor().apply(
        "ssn 123-45-6789",
        _policy([{"classification": "us_ssn", "treatment": "hash"}]),
    )
    expected = "sha256:" + hashlib.sha256(b"123-45-6789").hexdigest()
    assert expected in result.value
    assert "123-45-6789" not in result.value


def test_credit_card_luhn_only_redacts_valid() -> None:
    rules = _policy([{"classification": "credit_card", "treatment": "drop"}])
    valid = Redactor().apply("card 4242 4242 4242 4242", rules)  # valid Luhn
    invalid = Redactor().apply("card 1234 5678 9012 3456", rules)  # fails Luhn
    assert "4242" not in valid.value
    assert "1234 5678 9012 3456" in invalid.value


def test_pass_through_keeps_value() -> None:
    result = Redactor().apply(
        "alice@example.com",
        _policy([{"classification": "email_address", "treatment": "pass_through"}]),
    )
    assert result.value == "alice@example.com"
    assert result.redacted_classes == []


def test_nested_structures_are_walked() -> None:
    result = Redactor().apply(
        {"msgs": [{"body": "reach me at a@b.co"}], "n": 5},
        _policy([{"classification": "email_address", "treatment": "drop"}]),
    )
    assert result.value["msgs"][0]["body"] == "reach me at [REDACTED:email_address]"
    assert result.value["n"] == 5


def test_tokenize_falls_back_to_drop_without_vault() -> None:
    result = Redactor().apply(
        "alice@example.com",
        _policy([{"classification": "email_address", "treatment": "tokenize"}]),
    )
    assert result.value == "[REDACTED:email_address]"  # dropped, not leaked
    assert result.redacted_classes == ["email_address"]


def test_custom_detector_pattern_from_policy() -> None:
    result = Redactor().apply(
        "acct ACME-99",
        _policy(
            [
                {
                    "classification": "internal_id",
                    "treatment": "drop",
                    "detector": {"pattern": r"ACME-\d+"},
                }
            ]
        ),
    )
    assert result.value == "acct [REDACTED:internal_id]"


def test_end_to_end_redaction_before_encryption(fake_ingest: FakeIngest) -> None:
    fake_ingest.policy_rules = [{"classification": "email_address", "treatment": "drop"}]
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY, base_url=fake_ingest.base_url, start_flusher=False
    )
    with runfile_ai.run(agent_identity=AGENT):
        runfile_ai.capture_event(
            kind="llm_call",
            name="messages.create",
            payload={"prompt": "email alice@example.com about the loan"},
            model_ref={"provider": "anthropic", "model_id": "claude-opus-4-8"},
        )
    runfile_ai.flush()

    body = next(r.body for r in fake_ingest.requests if r.path == "/v1/batches")
    event = next(it["event"] for it in body["items"] if it["type"] == "event")
    payload_ref = event["payload_ref"]
    assert payload_ref["redaction_applied"]["redacted_classes"] == ["email_address"]

    # decrypt and confirm the email never reached the ciphertext
    ciphertext = base64.b64decode(payload_ref["ciphertext_base64"])
    nonce = base64.b64decode(payload_ref["encryption"]["nonce"])
    key = inst.datakeys._entries[("self", AGENT)].key.plaintext
    cleartext = AESGCM(bytes(key)).decrypt(nonce, ciphertext, None)
    assert b"alice@example.com" not in cleartext
    assert b"[REDACTED:email_address]" in cleartext
