"""ULID generation (Crockford base32, 26 chars).

Event/run ids are ULIDs: a 48-bit millisecond timestamp + 80 bits of randomness,
encoded as 26 Crockford-base32 chars. This matches the schema patterns:
``event_id`` = ``[0-9A-HJKMNP-TV-Z]{26}``, ``run_id`` = ``run_<26>``,
``parallel_group_id`` = ``pg_<26>``. Crockford base32 excludes I, L, O, U.
"""

from __future__ import annotations

import os
import time

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode(value: int, length: int) -> str:
    chars = [""] * length
    for i in range(length - 1, -1, -1):
        chars[i] = _CROCKFORD[value & 0x1F]
        value >>= 5
    return "".join(chars)


def generate_ulid() -> str:
    """A 26-char Crockford-base32 ULID (48-bit ms time + 80-bit randomness)."""
    timestamp_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    randomness = int.from_bytes(os.urandom(10), "big")  # 80 bits
    return _encode((timestamp_ms << 80) | randomness, 26)


def generate_run_id() -> str:
    return "run_" + generate_ulid()


def generate_event_id() -> str:
    return generate_ulid()


def generate_parallel_group_id() -> str:
    return "pg_" + generate_ulid()


def generate_batch_id() -> str:
    return "b_" + generate_ulid()
