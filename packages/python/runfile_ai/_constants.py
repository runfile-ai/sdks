"""Shared constants for the Runfile Python SDK.

The wire-level identity the SDK reports on every event and on the
``Runfile-SDK-Name`` ingest header. This MUST be a value the schema's
``SdkNameEnum`` (and the Ingest API header enum) accepts, or the Ingest API
rejects the batch at validation.

Follow-up (see CONTRIBUTING.md): the deployed schema's ``SdkNameEnum`` is
currently ``['runfile-py', '@runfile-ai/sdk']``-era; it must be updated to
include ``runfile-ai`` and the Ingest/Event-Processor validators redeployed
before the first release of this package.
"""

from __future__ import annotations

import importlib.metadata

# Wire identity (sdk.name / Runfile-SDK-Name). Decoupled from the import name
# (runfile_ai) and the PyPI distribution name (runfile-ai).
SDK_NAME = "runfile-ai"

# Schema major.minor this SDK produces (sent as the Runfile-Schema-Version
# header). Patch is omitted — the Ingest API routes on major.minor only.
SCHEMA_VERSION = "1.0"

# Full semver written into each run/event's `schema_version` field (the schema
# requires X.Y.Z there, distinct from the major.minor wire header above).
SCHEMA_VERSION_FULL = "1.0.0"

# Default region. The base URL is derived from the region (one regional host
# fronts every SDK-facing endpoint: /v1/batches, /v1/data-keys, /v1/policies,
# /v1/tokenize) unless the caller passes an explicit base_url — the same pattern
# the OpenAI / Anthropic SDKs use.
DEFAULT_REGION = "eu-west-2"


def default_base_url(region: str = DEFAULT_REGION) -> str:
    """The canonical regional host: ``https://api.<region>.runfile.ai``."""
    return f"https://api.{region}.runfile.ai"


def sdk_version() -> str:
    """Installed version of this package, or ``0.0.0`` when run from source."""
    try:
        return importlib.metadata.version("runfile-ai")
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"
