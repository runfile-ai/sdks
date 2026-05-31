"""Claude Agent SDK adapter.

Builds a hooks dict + a wrapped ``canUseTool`` callback:

- ``SessionStart`` → create run (captures ``session_id``)
- ``PreToolUse`` / ``PostToolUse`` → ``tool_call`` / ``tool_result``
- ``canUseTool`` → ``tool_approval_requested`` then granted/denied (observe only)
- ``Notification`` ``permission_prompt`` → ``run_suspend`` (awaiting_human_approval);
  ``idle_prompt`` / ``elicitation_dialog`` → ``run_suspend`` (awaiting_human_input)
- ``SubagentStart`` / ``SubagentStop`` → ``delegate`` + child run
- ``Stop`` → ``run_end``

Skeleton: signatures in place; bodies TODO.
"""

from __future__ import annotations

from typing import Any, Callable, Optional


def build_hooks(*, agent_identity: str) -> dict[str, Any]:
    """Return the Claude Agent SDK ``hooks`` dict wired to Runfile capture."""
    raise NotImplementedError


def build_can_use_tool(
    *,
    agent_identity: str,
    inner_callback: Optional[Callable[..., Any]] = None,
) -> Callable[..., Any]:
    """Wrap the customer's ``can_use_tool`` to observe approval decisions."""
    raise NotImplementedError
