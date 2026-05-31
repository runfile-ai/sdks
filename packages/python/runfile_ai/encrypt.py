"""Client-side AES-256-GCM envelope encryption of redacted payloads.

Runs in the background flusher, after redaction, before shipping. Encrypts under
the cached per-``(tenant, agent)`` plaintext data key with a fresh per-event
nonce, producing the ``payload_ref`` the Ingest API stores as ciphertext. The
Ingest API cannot decrypt — only Vault / Selective Decrypt can unwrap.

Skeleton: signature + result shape in place; body TODO.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Encrypted:
    ciphertext: bytes
    nonce: bytes


def aes_gcm_encrypt(plaintext: bytes, data_key: bytes) -> Encrypted:
    """AES-256-GCM encrypt with a fresh 12-byte nonce (auth tag appended to ciphertext)."""
    raise NotImplementedError
