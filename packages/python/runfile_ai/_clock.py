"""Wall-clock helpers for capture timestamps."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now_iso() -> str:
    """RFC 3339 UTC timestamp with millisecond precision and a ``Z`` suffix."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def detected_wall_clock_source() -> str:
    """Best-effort clock-source label. v1: the host system clock.

    On AWS Time Sync-backed hosts this could report ``aws_time_sync``; detecting
    that reliably is deferred, so we report ``host_system`` honestly.
    """
    return "host_system"
