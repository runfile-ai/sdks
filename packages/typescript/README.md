# @runfile-ai/sdk

The Runfile SDK for TypeScript / Node. Tamper-evident audit capture for AI agents.

```bash
pnpm add @runfile-ai/sdk
```

```ts
import { init } from '@runfile-ai/sdk';
import { instrument } from '@runfile-ai/sdk/integrations/langgraph';

await init({ apiKey: process.env.RUNFILE_API_KEY!, environment: 'production' });

const graph = instrument(originalGraph, {
  agentIdentity: 'did:web:bank.com:agents:loan-triage:v2',
});
```

- **Package:** `@runfile-ai/sdk` (wire `sdk.name`: `@runfile-ai/sdk`).
- **Node:** 20+.
- **Schema dependency:** `@runfile-ai/schemas` (`^0.5.0`).
- **v1 adapters:** LangGraph.js, Claude Agent SDK. OpenAI Agents, Mastra, and
  Vercel AI SDK are deferred to v1.5.

> 🚧 Scaffold — module skeletons are in place; logic is being filled in.

## Local development

```bash
pnpm install
pnpm --filter @runfile-ai/sdk build
pnpm --filter @runfile-ai/sdk test
```
