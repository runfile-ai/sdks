"""Runfile SDK for Python.

Observe an agent and translate framework-native signals into Runfile runs and
events. The customer's framework remains the source of truth; this SDK is a
passive observer.

Primary path — framework adapters::

    import runfile_ai
    from runfile_ai.integrations import langgraph as runfile_langgraph

    runfile_ai.init(api_key=os.environ["RUNFILE_API_KEY"])
    graph = runfile_langgraph.instrument(graph, agent_identity="did:web:bank.com:agents:loan-triage:v2")

Fallback — manual API::

    with runfile_ai.run(agent_identity="did:web:acme.com:agents:custom") as r:
        runfile_ai.capture_event(kind="llm_call", name="messages.create", model_ref={...})

See ``sdk-design.md`` for the full design. This module is the public surface; it
is intentionally small.
"""

from __future__ import annotations

from ._constants import SDK_NAME, sdk_version
from .client import RunfileClient, flush, init, shutdown
from .context import current_run
from .run import (
    abandon_run,
    capture_event,
    end_run,
    parallel_group,
    resume_run,
    run,
    start_run,
    suspend_run,
)

__version__ = sdk_version()

__all__ = [
    "SDK_NAME",
    "RunfileClient",
    "init",
    "flush",
    "shutdown",
    "run",
    "start_run",
    "end_run",
    "suspend_run",
    "resume_run",
    "abandon_run",
    "capture_event",
    "current_run",
    "parallel_group",
    "__version__",
]
