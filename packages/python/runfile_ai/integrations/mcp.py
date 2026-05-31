"""MCP client-side interception.

When the customer's agent acts as an MCP client, intercept JSON-RPC traffic:

- outgoing ``tools/call`` → ``tool_call``; responses → ``tool_result``
- server ``elicitation/create`` → ``run_suspend`` (awaiting_human_input)
- sampling ``createMessage`` requiring approval → ``tool_approval_requested``

Skeleton: signature in place; body TODO.
"""

from __future__ import annotations

from typing import Any


def instrument_client(client: Any, *, agent_identity: str) -> Any:
    """Wrap an MCP client to emit Runfile events for intercepted traffic."""
    raise NotImplementedError
