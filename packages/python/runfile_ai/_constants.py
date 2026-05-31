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

# Default regional Ingest API base URL.
DEFAULT_REGION = "eu-west-2"
DEFAULT_BASE_URL = "https://api.eu-west-2.runfile.ai"


def sdk_version() -> str:
    """Installed version of this package, or ``0.0.0`` when run from source."""
    try:
        return importlib.metadata.version("runfile-ai")
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"
