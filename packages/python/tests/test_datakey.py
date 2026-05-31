"""Tests for the per-(tenant, agent) data-key cache against the fake Ingest API."""

from __future__ import annotations

import httpx
import pytest

from runfile_ai.datakey import DataKeyCache, DataKeyError
from runfile_ai.encrypt import KEY_BYTES, aes_gcm_encrypt
from tests.fake_ingest import FakeIngest

TENANT = "tnt_h7q3n2bz5kp8"
AGENT = "did:web:bank.com:agents:loan-triage:v2"


def _cache(server: FakeIngest, client: httpx.Client, ttl: float = 3600) -> DataKeyCache:
    return DataKeyCache(
        client=client, base_url=server.base_url, api_key="rf_test_" + "a" * 32, ttl_seconds=ttl
    )


def test_mint_returns_usable_key_and_sends_bearer(
    fake_ingest: FakeIngest, http_client: httpx.Client
) -> None:
    cache = _cache(fake_ingest, http_client)
    key = cache.get_or_fetch(TENANT, AGENT)

    assert key.key_id.startswith("dk_")
    assert len(key.plaintext) == KEY_BYTES
    assert key.wrapped
    # the minted key actually encrypts
    enc = aes_gcm_encrypt(b"payload", bytes(key.plaintext))
    assert enc.ciphertext

    req = fake_ingest.requests[-1]
    assert req.path == "/v1/data-keys"
    assert req.headers["authorization"].startswith("Bearer rf_test_")
    assert req.body == {"agent_identity": AGENT}


def test_caches_per_tenant_agent(fake_ingest: FakeIngest, http_client: httpx.Client) -> None:
    cache = _cache(fake_ingest, http_client)
    k1 = cache.get_or_fetch(TENANT, AGENT)
    k2 = cache.get_or_fetch(TENANT, AGENT)
    assert k1 is k2
    assert fake_ingest.mint_count == 1  # second call served from cache


def test_distinct_agents_mint_separately(
    fake_ingest: FakeIngest, http_client: httpx.Client
) -> None:
    cache = _cache(fake_ingest, http_client)
    cache.get_or_fetch(TENANT, AGENT)
    cache.get_or_fetch(TENANT, "did:web:bank.com:agents:other:v1")
    assert fake_ingest.mint_count == 2


def test_expiry_refetches_and_zeroizes_old_key(
    fake_ingest: FakeIngest, http_client: httpx.Client
) -> None:
    cache = _cache(fake_ingest, http_client, ttl=0)  # every entry is immediately stale
    k1 = cache.get_or_fetch(TENANT, AGENT)
    k2 = cache.get_or_fetch(TENANT, AGENT)
    assert fake_ingest.mint_count == 2
    assert k1 is not k2
    assert bytes(k1.plaintext) == b"\x00" * KEY_BYTES  # old plaintext wiped


def test_terminal_status_is_not_retryable(
    fake_ingest: FakeIngest, http_client: httpx.Client
) -> None:
    fake_ingest.datakey_fail_statuses = [403]
    cache = _cache(fake_ingest, http_client)
    with pytest.raises(DataKeyError) as exc:
        cache.get_or_fetch(TENANT, AGENT)
    assert exc.value.retryable is False
    assert exc.value.status == 403


def test_transient_status_is_retryable(
    fake_ingest: FakeIngest, http_client: httpx.Client
) -> None:
    fake_ingest.datakey_fail_statuses = [503]
    cache = _cache(fake_ingest, http_client)
    with pytest.raises(DataKeyError) as exc:
        cache.get_or_fetch(TENANT, AGENT)
    assert exc.value.retryable is True


def test_zeroize_clears_cache(fake_ingest: FakeIngest, http_client: httpx.Client) -> None:
    cache = _cache(fake_ingest, http_client)
    key = cache.get_or_fetch(TENANT, AGENT)
    cache.zeroize()
    assert bytes(key.plaintext) == b"\x00" * KEY_BYTES
    cache.get_or_fetch(TENANT, AGENT)  # re-mints after zeroize
    assert fake_ingest.mint_count == 2
