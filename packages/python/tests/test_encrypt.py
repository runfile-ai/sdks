"""Tests for AES-256-GCM payload encryption."""

from __future__ import annotations

import os

import pytest
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from runfile_ai.encrypt import KEY_BYTES, NONCE_BYTES, aes_gcm_encrypt


def test_roundtrip() -> None:
    key = os.urandom(KEY_BYTES)
    plaintext = b'{"prompt":"summarise the loan application"}'
    enc = aes_gcm_encrypt(plaintext, key)

    assert len(enc.nonce) == NONCE_BYTES
    decrypted = AESGCM(key).decrypt(enc.nonce, enc.ciphertext, None)
    assert decrypted == plaintext


def test_fresh_nonce_per_call() -> None:
    key = os.urandom(KEY_BYTES)
    a = aes_gcm_encrypt(b"same", key)
    b = aes_gcm_encrypt(b"same", key)
    assert a.nonce != b.nonce
    assert a.ciphertext != b.ciphertext  # distinct ciphertext under the shared key


def test_rejects_non_256_bit_key() -> None:
    with pytest.raises(ValueError):
        aes_gcm_encrypt(b"data", os.urandom(16))


def test_tamper_is_detected() -> None:
    key = os.urandom(KEY_BYTES)
    enc = aes_gcm_encrypt(b"important", key)
    tampered = bytearray(enc.ciphertext)
    tampered[0] ^= 0x01  # flip one bit
    with pytest.raises(InvalidTag):
        AESGCM(key).decrypt(enc.nonce, bytes(tampered), None)
