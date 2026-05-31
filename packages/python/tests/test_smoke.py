"""Scaffold smoke tests: the package imports and the public surface exists.

Real behaviour tests (event construction, redaction, encryption roundtrip,
chain bookkeeping, adapter translation) are added as each module is implemented.
SDK tests mock the network — they never hit a live Ingest API.
"""

from __future__ import annotations

import pytest

import runfile_ai


def test_public_surface_exported() -> None:
    for name in (
        "init",
        "flush",
        "shutdown",
        "run",
        "start_run",
        "end_run",
        "suspend_run",
        "resume_run",
        "abandon_run",
        "capture_event",
        "current_run",
        "parallel_group",
    ):
        assert hasattr(runfile_ai, name), f"missing public symbol: {name}"


def test_wire_sdk_name_is_runfile_ai() -> None:
    assert runfile_ai.SDK_NAME == "runfile-ai"


def test_init_rejects_malformed_api_key() -> None:
    with pytest.raises(ValueError):
        runfile_ai.init("not-a-real-key")


def test_current_run_is_none_outside_a_run() -> None:
    assert runfile_ai.current_run() is None
