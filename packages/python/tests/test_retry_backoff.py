"""Retry backoff applies ±25% jitter by default and respects the cap."""

from __future__ import annotations

import runfile_ai
from runfile_ai.flusher import RetryConfig, _default_jitter
from tests.conftest import VALID_TEST_KEY


def test_default_jitter_is_in_range_and_varies() -> None:
    values = [_default_jitter() for _ in range(200)]
    assert all(0.75 <= v <= 1.25 for v in values)  # ±25%
    assert len(set(values)) > 1  # actually random, not a constant


def test_retry_config_default_jitter_is_enabled() -> None:
    # regression: the default must NOT be the no-op 1.0 (thundering-herd risk)
    assert RetryConfig().jitter is _default_jitter


def test_backoff_applies_jitter_and_respects_cap() -> None:
    inst = runfile_ai.init(api_key=VALID_TEST_KEY, start_flusher=False, fetch_policy=False)
    flusher = inst._flusher

    # attempt 0: base 200ms ± 25% → 150..250ms
    samples = [flusher._backoff_seconds(0) for _ in range(200)]
    assert all(0.150 <= s <= 0.250 for s in samples)
    assert len(set(samples)) > 1  # jitter actually applied

    # large attempt: never exceeds the 30s cap even with +25% jitter
    assert all(flusher._backoff_seconds(50) <= flusher.retry.cap_ms / 1000.0 for _ in range(50))
