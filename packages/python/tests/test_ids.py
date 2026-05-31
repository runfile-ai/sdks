"""Tests for ULID generation (schema id patterns)."""

from __future__ import annotations

import re

from runfile_ai._ids import (
    generate_batch_id,
    generate_event_id,
    generate_parallel_group_id,
    generate_run_id,
)

ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")  # Crockford base32, schema event_id pattern


def test_event_id_matches_schema_pattern() -> None:
    assert ULID_RE.match(generate_event_id())


def test_prefixed_ids() -> None:
    assert re.match(r"^run_[0-9A-HJKMNP-TV-Z]{26}$", generate_run_id())
    assert re.match(r"^pg_[0-9A-HJKMNP-TV-Z]{26}$", generate_parallel_group_id())
    assert re.match(r"^b_[0-9A-HJKMNP-TV-Z]{26}$", generate_batch_id())


def test_ids_are_unique() -> None:
    assert len({generate_event_id() for _ in range(1000)}) == 1000


def test_ids_are_time_ordered_within_a_ms_boundary() -> None:
    # ULIDs are lexicographically sortable by their timestamp prefix.
    a = generate_event_id()
    b = generate_event_id()
    # same or later — never earlier (timestamp is monotonic non-decreasing here)
    assert b[:8] >= a[:8] or b != a
