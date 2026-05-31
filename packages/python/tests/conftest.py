"""Shared pytest fixtures."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

import runfile_ai
from runfile_ai import context as _ctx
from tests.fake_ingest import FakeIngest

VALID_TEST_KEY = "rf_test_" + "a" * 32


@pytest.fixture(autouse=True)
def _reset_sdk() -> Iterator[None]:
    """Tear down any global SDK instance and ambient context after each test."""
    yield
    runfile_ai.shutdown()
    _ctx._current_run.set(None)
    _ctx._current_parent_event.set(None)
    _ctx._current_parallel_group.set(None)


@pytest.fixture
def sdk() -> Iterator[runfile_ai.RunfileClient]:
    """An initialised SDK instance (no flusher running; base_url unused for hot-path)."""
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY,
        environment="production",
        base_url="http://localhost:9",
        start_flusher=False,  # deterministic: hot-path tests inspect the buffer
    )
    yield inst


@pytest.fixture
def fake_ingest() -> Iterator[FakeIngest]:
    """A running in-process fake Ingest API; torn down after the test."""
    server = FakeIngest().start()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
def http_client() -> Iterator[httpx.Client]:
    """A sync httpx client (as the background flusher would use)."""
    with httpx.Client(timeout=5.0) as client:
        yield client
