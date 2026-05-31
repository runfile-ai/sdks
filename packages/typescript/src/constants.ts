/**
 * Shared constants for the Runfile TypeScript SDK.
 *
 * `SDK_NAME` is the wire-level identity reported on every event (`sdk.name`) and
 * on the `Runfile-SDK-Name` ingest header. It MUST be a value the schema's
 * `SdkNameEnum` (and the Ingest API header enum) accepts, or the Ingest API
 * rejects the batch at validation. `@runfile-ai/sdk` is in the deployed schema
 * (`@runfile-ai/schemas` >= 0.6.0) and accepted by the live Ingest validator.
 */

/** Wire identity (`sdk.name` / `Runfile-SDK-Name`). Equals the npm package name. */
export const SDK_NAME = '@runfile-ai/sdk' as const;

/** Schema major.minor this SDK produces (`Runfile-Schema-Version` header). */
export const SCHEMA_VERSION = '1.0' as const;

export const DEFAULT_REGION = 'eu-west-2' as const;
export const DEFAULT_BASE_URL = 'https://api.eu-west-2.runfile.ai' as const;

/** Populated from package.json at build time; placeholder in the scaffold. */
export const SDK_VERSION = '0.0.1' as const;
