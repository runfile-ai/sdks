"""Environment-variable initialisation (sdk-design §Initialization).

Precedence: explicit arg > env var > default.
"""

from __future__ import annotations

import pytest

import runfile_ai
from tests.conftest import VALID_TEST_KEY

OTHER_KEY = "rf_live_" + "b" * 32

# These keep every init() in this module off the network.
_OFFLINE = {"start_flusher": False, "fetch_policy": False}


def test_api_key_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNFILE_API_KEY", VALID_TEST_KEY)
    inst = runfile_ai.init(**_OFFLINE)
    assert inst.api_key == VALID_TEST_KEY


def test_explicit_api_key_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNFILE_API_KEY", OTHER_KEY)
    inst = runfile_ai.init(api_key=VALID_TEST_KEY, **_OFFLINE)
    assert inst.api_key == VALID_TEST_KEY


def test_missing_api_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUNFILE_API_KEY", raising=False)
    with pytest.raises(ValueError):
        runfile_ai.init(**_OFFLINE)


def test_region_from_env_drives_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNFILE_REGION", "us-east-1")
    inst = runfile_ai.init(api_key=VALID_TEST_KEY, **_OFFLINE)
    assert inst.region == "us-east-1"
    assert inst.base_url == "https://api.us-east-1.runfile.ai"


def test_explicit_region_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNFILE_REGION", "us-east-1")
    inst = runfile_ai.init(api_key=VALID_TEST_KEY, region="eu-west-2", **_OFFLINE)
    assert inst.region == "eu-west-2"


def test_environment_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNFILE_ENVIRONMENT", "staging")
    inst = runfile_ai.init(api_key=VALID_TEST_KEY, **_OFFLINE)
    assert inst.environment == "staging"


def test_disabled_from_env_is_a_silent_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNFILE_DISABLED", "1")
    monkeypatch.delenv("RUNFILE_API_KEY", raising=False)
    # disabled tolerates a missing api key (local-dev use)
    inst = runfile_ai.init(**_OFFLINE)
    assert inst.disabled is True

    with runfile_ai.run(agent_identity="did:web:acme.com:agents:x"):
        runfile_ai.capture_event(kind="tool_call", name="a")
    assert len(inst.buffer) == 0  # nothing captured — true no-op


def test_disabled_arg_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNFILE_DISABLED", "1")
    inst = runfile_ai.init(api_key=VALID_TEST_KEY, disabled=False, **_OFFLINE)
    assert inst.disabled is False
