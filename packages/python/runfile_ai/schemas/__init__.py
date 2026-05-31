"""Re-export of the published schema package (``runfile-ai-schemas``).

The SDK constructs runs and events against the versioned schema contract. Import
the generated Pydantic models from here so call sites don't depend on the
external module path directly::

    from runfile_ai.schemas import RunfileEvent, RunfileRun

The dependency is a published PyPI package (``runfile-ai-schemas``, imported as
``runfile_schemas``), pinned as a version range in ``pyproject.toml`` — never a
path link. A schema change is an explicit dependency bump.
"""

from __future__ import annotations

# Re-export lazily/defensively: the scaffold may not have the schemas package
# installed yet. Once `runfile-ai-schemas` is a resolved dependency, these names
# resolve to the generated Pydantic models.
try:  # pragma: no cover - thin re-export
    from runfile_schemas.event import (  # type: ignore  # noqa: F401
        RunfileEvent,
        RunfileRun,
    )
except ImportError:  # pragma: no cover
    RunfileEvent = object  # type: ignore[assignment,misc]
    RunfileRun = object  # type: ignore[assignment,misc]

__all__ = ["RunfileEvent", "RunfileRun"]
