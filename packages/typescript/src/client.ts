/**
 * SDK lifecycle and the HTTP client to the Ingest API.
 *
 * `init()` is called once at process start: validates the API key shape,
 * constructs the bearer-auth HTTP client (no AWS credentials), loads the
 * redaction policy, and starts the background flusher. The flusher — not the hot
 * path — does classification, redaction, encryption, batching, and shipping.
 *
 * Skeleton: signatures and wiring in place; bodies are TODO.
 */

import { DEFAULT_BASE_URL, DEFAULT_REGION } from './constants.js';

const API_KEY_RE = /^rf_(live|test)_[a-z0-9]{32}$/;

export interface InitOptions {
  apiKey: string;
  environment?: 'production' | 'staging' | 'development';
  region?: string;
  baseUrl?: string;
  disabled?: boolean;
}

let instance: RunfileClient | undefined;

/** Process-wide SDK instance: buffer, flusher, policy, data-key cache, HTTP. */
export class RunfileClient {
  readonly apiKey: string;
  readonly environment: string;
  readonly region: string;
  readonly baseUrl: string;
  readonly disabled: boolean;

  constructor(opts: InitOptions) {
    if (!opts.disabled && !API_KEY_RE.test(opts.apiKey)) {
      throw new Error('invalid API key shape; expected rf_<live|test>_<32 base32 chars>');
    }
    this.apiKey = opts.apiKey;
    this.environment = opts.environment ?? 'production';
    this.region = opts.region ?? DEFAULT_REGION;
    this.baseUrl = opts.baseUrl ?? DEFAULT_BASE_URL;
    this.disabled = opts.disabled ?? false;
    // TODO: construct undici client, EventBuffer, Spool, DataKeyCache,
    // PolicyCache; start the flusher; register a beforeExit drain.
  }

  /** Force a buffer drain; resolves once in-flight batches complete. */
  async flush(): Promise<void> {
    throw new Error('not implemented');
  }

  /** Graceful shutdown: final drain, then release resources. */
  async shutdown(): Promise<void> {
    throw new Error('not implemented');
  }
}

/** Initialise the SDK once at process start. Idempotent. */
export async function init(opts: InitOptions): Promise<RunfileClient> {
  if (instance) return instance;
  instance = new RunfileClient(opts);
  return instance;
}

export function getInstance(): RunfileClient | undefined {
  return instance;
}

export async function flush(): Promise<void> {
  await instance?.flush();
}

export async function shutdown(): Promise<void> {
  if (instance) {
    await instance.shutdown();
    instance = undefined;
  }
}
