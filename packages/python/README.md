# runfile-ai

The Runfile SDK for Python. Tamper-evident audit capture for AI agents.

```bash
pip install runfile-ai
# framework extras:
pip install 'runfile-ai[langgraph]'   # or [openai-agents], [anthropic], [mcp]
```

```python
import os
import runfile_ai
from runfile_ai.integrations import langgraph as runfile_langgraph

runfile_ai.init(api_key=os.environ["RUNFILE_API_KEY"], environment="production")

graph = runfile_langgraph.instrument(
    graph, agent_identity="did:web:bank.com:agents:loan-triage:v2"
)
```

- **Import name:** `runfile_ai` (distribution: `runfile-ai`).
- **Wire `sdk.name`:** `runfile-ai`.
- **Python:** 3.11+.
- **Schema dependency:** `runfile-ai-schemas` (PyPI), pinned `>=0.5,<1.0`.

> 🚧 Scaffold — module skeletons are in place; logic is being filled in. See the
> repo root `README.md` and `docs/STATUS.md`.

## Local development

```bash
pip install -e '.[dev]'
ruff check .
mypy runfile_ai
pytest
```
