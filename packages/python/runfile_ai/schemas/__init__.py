"""Re-export of the published schema package (``runfile-ai-schemas``).

The SDK constructs runs and events against the versioned schema contract. Import
the generated Pydantic models from here so call sites don't depend on the
external module path directly::

    from runfile_ai.schemas import RunfileEvent, EventSubmission, BatchSubmission

The dependency is a published PyPI package (``runfile-ai-schemas``, imported as
``runfile_schemas``), pinned as a version range in ``pyproject.toml`` — never a
path link. A schema change is an explicit dependency bump.
"""

from __future__ import annotations

# Persisted entities.
from runfile_schemas.event import RunfileEvent, RunfileRun

# Wire (ingest) submission shapes — what the SDK actually builds and ships.
from runfile_schemas.ingest import (
    Actor,
    Action,
    BatchSubmission,
    EventItem,
    EventSubmission,
    ModelRef,
    PayloadSubmission,
    RunCreateItem,
    RunEndItem,
    RunSubmission,
    RunUpdateItem,
    SdkAtStartModel,
)

__all__ = [
    "RunfileEvent",
    "RunfileRun",
    "BatchSubmission",
    "RunCreateItem",
    "RunUpdateItem",
    "RunEndItem",
    "EventItem",
    "EventSubmission",
    "RunSubmission",
    "PayloadSubmission",
    "Actor",
    "Action",
    "ModelRef",
    "SdkAtStartModel",
]
