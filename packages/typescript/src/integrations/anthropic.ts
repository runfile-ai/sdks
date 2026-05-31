/**
 * Claude Agent SDK (TypeScript) adapter.
 *
 * Builds a hooks object + a wrapped `canUseTool` callback (SessionStart → run;
 * PreToolUse/PostToolUse → tool events; canUseTool → approval events;
 * Notification permission_prompt/idle_prompt → run_suspend; Subagent* →
 * delegate; Stop → run_end).
 *
 * v1 TypeScript coverage. Skeleton: signatures in place; bodies TODO.
 */

export interface BuildHooksOptions {
  agentIdentity: string;
}

export function buildHooks(_opts: BuildHooksOptions): Record<string, unknown> {
  throw new Error('not implemented');
}

export function buildCanUseTool(
  _opts: BuildHooksOptions & { innerCallback?: (...args: unknown[]) => unknown },
): (...args: unknown[]) => unknown {
  throw new Error('not implemented');
}
