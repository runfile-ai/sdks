"""Event hash-chain helpers — thin wrapper over the shipped canonical module.

The canonical RFC 8785 (JCS) serialisation and the ``event_hash`` projection are
owned by ``runfile-ai-schemas`` (``runfile_schemas.canonical``) and shared
byte-for-byte across the TS SDK, TS backend, Go Event Processor, and Verifier
CLI. The SDK MUST use that exact implementation so its ``prev_event_hash_intent``
is a genuine integrity claim the server can confirm — not noise. We re-export it
here so the rest of the SDK has one import site.

The flusher computes the chain in ``local_seq`` order: each event's
``prev_event_hash`` is the previous event's ``event_hash`` (or the zero sentinel
for the first event of a brand-new conversation), and ``event_hash`` commits to
the SDK-authored fields (including ``payload_ref``) per
``runfile_schemas.canonical.NON_HASHED_EVENT_FIELDS``.
"""

from __future__ import annotations

from runfile_schemas.canonical import (
    NON_HASHED_EVENT_FIELDS,
    canonical_event_for_hash,
    canonical_sha256,
    compute_event_hash,
    stringify,
)

#: prev_event_hash for the first event of a brand-new conversation.
ZERO_SENTINEL = "sha256:" + "0" * 64

__all__ = [
    "ZERO_SENTINEL",
    "NON_HASHED_EVENT_FIELDS",
    "canonical_event_for_hash",
    "canonical_sha256",
    "compute_event_hash",
    "stringify",
]
