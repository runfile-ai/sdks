"""LangGraph adapter.

Registers a handler inheriting from both LangChain's ``AsyncCallbackHandler``
and LangGraph's ``GraphCallbackHandler``:

- ``on_chain_start`` (top-level) → create run; node boundaries → ``graph_node_enter/exit``
- ``on_llm_start/end`` → ``llm_call`` (+ ``model_ref``)
- ``on_tool_start/end`` → ``tool_call`` / ``tool_result`` (linked via ``parent_event_id``)
- ``__interrupt__`` → ``run_suspend`` (``framework_inferred``) + lifecycle → ``awaiting_*``
- resume → ``run_resume`` (new segment, stitched via ``prev_event_hash``)
- subgraph entry → ``delegate`` + a new child run with ``delegated_from``

Same-run resume vs explicit fork is detected via the ``thread_id``→``run_id`` map
(reused ``thread_id`` = resume; new ``thread_id`` / non-current ``checkpoint_id``
= fork → new run with ``continued_from``).

Skeleton: ``instrument()`` signature in place; handler body TODO.
"""

from __future__ import annotations

from typing import Any


def instrument(graph: Any, *, agent_identity: str, conversation_id: str | None = None) -> Any:
    """Register the Runfile callback handler on ``graph`` and return it.

    One line of customer code::

        graph = instrument(graph, agent_identity="did:web:bank.com:agents:loan-triage:v2")
    """
    raise NotImplementedError
