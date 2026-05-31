"""Framework adapters.

Each adapter is a self-contained module that knows one framework's signal
surface, translates native callbacks/hooks/spans into Runfile events via the
shared core, maintains ambient context, and detects suspension / resume /
delegation / handoff. Adapters don't communicate with each other.

v1 Python coverage: LangGraph, OpenAI Agents, Claude Agent SDK, MCP (plus the
manual API in :mod:`runfile_ai.run`). OTel-based adapters (Mastra, Pydantic AI,
Vercel) follow.
"""
