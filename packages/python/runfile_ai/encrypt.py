"""Client-side AES-256-GCM envelope encryption of redacted payloads.

Runs in the background flusher, after redaction, before shipping. Encrypts under
the cached per-``(tenant, agent)`` plaintext data key with a fresh per-event
nonce, producing the ciphertext the Ingest API stores. The Ingest API cannot
decrypt — only Vault / Selective Decrypt can unwrap the data key.

The auth tag is appended to the ciphertext (the AES-GCM convention the
``cryptography`` library uses), so ``ciphertext`` is ``ct || tag`` (tag = 16 B).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

#: AES-GCM standard nonce length in bytes (96 bits). One fresh nonce per event.
NONCE_BYTES = 12

#: AES-256 key length in bytes.
KEY_BYTES = 32


@dataclass(frozen=True)
class Encrypted:
    ciphertext: bytes  # ct || 16-byte auth tag
    nonce: bytes  # 12-byte per-event nonce


def aes_gcm_encrypt(plaintext: bytes, data_key: bytes) -> Encrypted:
    """AES-256-GCM encrypt ``plaintext`` under ``data_key`` with a fresh nonce.

    Raises ``ValueError`` if the key is not 32 bytes.
    """
    if len(data_key) != KEY_BYTES:
        raise ValueError(f"data key must be {KEY_BYTES} bytes (AES-256), got {len(data_key)}")
    nonce = os.urandom(NONCE_BYTES)
    ciphertext = AESGCM(bytes(data_key)).encrypt(nonce, plaintext, None)
    return Encrypted(ciphertext=ciphertext, nonce=nonce)
