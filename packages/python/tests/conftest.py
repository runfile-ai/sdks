"""Shared pytest fixtures."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from tests.fake_ingest import FakeIngest


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
