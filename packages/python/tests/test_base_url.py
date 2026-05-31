"""Base URL is derived from the region unless explicitly overridden."""

from __future__ import annotations

import runfile_ai
from runfile_ai._constants import default_base_url
from tests.conftest import VALID_TEST_KEY


def test_default_base_url_helper() -> None:
    assert default_base_url("eu-west-2") == "https://api.eu-west-2.runfile.ai"
    assert default_base_url("us-east-1") == "https://api.us-east-1.runfile.ai"


def test_base_url_derived_from_region() -> None:
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY, region="us-east-1", start_flusher=False, fetch_policy=False
    )
    assert inst.base_url == "https://api.us-east-1.runfile.ai"


def test_default_region_base_url() -> None:
    inst = runfile_ai.init(api_key=VALID_TEST_KEY, start_flusher=False, fetch_policy=False)
    assert inst.base_url == "https://api.eu-west-2.runfile.ai"


def test_explicit_base_url_overrides_region() -> None:
    inst = runfile_ai.init(
        api_key=VALID_TEST_KEY,
        region="us-east-1",
        base_url="https://custom.example.com/",
        start_flusher=False,
        fetch_policy=False,
    )
    assert inst.base_url == "https://custom.example.com"  # trailing slash trimmed
