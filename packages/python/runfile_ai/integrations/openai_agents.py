"""OpenAI Agents SDK adapter.

Provides a ``RunHooks`` subclass and wraps ``Runner.run``:

- first ``on_agent_start`` → create run; ``on_tool_start/end`` → tool events
- ``on_handoff`` → ``handoff`` on the source run (which ends) + target run with
  ``handed_off_from``
- ``result.interruptions`` (post-run) → ``tool_approval_requested`` per
  interruption + ``run_suspend`` (awaiting_human_approval); persist run state
- next ``Runner.run(agent, state)`` → ``tool_approval_granted/denied`` + ``run_resume``
- ``agent.as_tool`` nesting → ``delegate`` + child run with ``delegated_from``

Same-run resume vs fork detected by what the caller passes after a pause
(``RunState`` = resume; fresh input = abandon/fork).

Skeleton: signature in place; body TODO.
"""

from __future__ import annotations

from typing import Any


def instrument_runner(
    runner: Any, *, agent_identity: str, conversation_id: str | None = None
) -> Any:
    """Return a wrapped ``Runner`` that emits Runfile events around ``run()``."""
    raise NotImplementedError
