/**
 * Scaffold smoke tests: the public surface exists and key invariants hold.
 * Real behaviour tests are added as each module is implemented. SDK tests mock
 * the network — they never hit a live Ingest API.
 */

import { describe, expect, it } from 'vitest';
import { SDK_NAME, RunfileClient } from '../src/index.js';

describe('scaffold', () => {
  it('reports the runfile-ai wire sdk.name', () => {
    expect(SDK_NAME).toBe('@runfile-ai/sdk');
  });

  it('rejects a malformed API key', () => {
    expect(() => new RunfileClient({ apiKey: 'not-a-real-key' })).toThrow();
  });

  it('accepts a well-shaped test key (32-char base32 body)', () => {
    expect(
      () => new RunfileClient({ apiKey: 'rf_test_abcdefghijklmnopqrstuvwxyz012345' }),
    ).not.toThrow();
  });
});
