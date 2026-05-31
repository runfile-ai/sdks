/**
 * MCP client-side interception (TypeScript).
 *
 * Intercepts JSON-RPC traffic when the customer's agent acts as an MCP client:
 * outgoing `tools/call` → tool_call; responses → tool_result; server
 * `elicitation/create` → run_suspend (awaiting_human_input); sampling
 * `createMessage` requiring approval → tool_approval_requested.
 *
 * Skeleton: signature in place; body TODO.
 */

export interface McpInstrumentOptions {
  agentIdentity: string;
}

export function instrumentClient<C>(_client: C, _opts: McpInstrumentOptions): C {
  throw new Error('not implemented');
}
