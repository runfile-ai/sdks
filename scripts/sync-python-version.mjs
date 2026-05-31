/**
 * Keep the Python package (`runfile-ai`) version in lockstep with the TypeScript
 * SDK (`@runfile-ai/sdk`). Changesets only versions JS workspace packages, so this
 * runs right after `changeset version` (see the root `changeset:version` script)
 * and copies the freshly-bumped TS version into packages/python/pyproject.toml.
 *
 * The two SDKs ship as one product at one version — the same model the schemas
 * repo uses (JS and generated Python share a version). The release workflow then
 * publishes the Python package whenever changesets publishes a release.
 *
 * Idempotent: a no-op when the versions already match.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const tsPkgPath = resolve(repoRoot, 'packages/typescript/package.json');
const pyprojectPath = resolve(repoRoot, 'packages/python/pyproject.toml');

const tsVersion = JSON.parse(readFileSync(tsPkgPath, 'utf8')).version;
if (typeof tsVersion !== 'string' || tsVersion.length === 0) {
  console.error('sync-python-version: could not read @runfile-ai/sdk version');
  process.exit(1);
}

const pyproject = readFileSync(pyprojectPath, 'utf8');
// Replace only the first `version = "..."` (the [project] version; the
// [build-system] block has no such key, so the first match is the right one).
const next = pyproject.replace(/^version = "[^"]*"$/m, `version = "${tsVersion}"`);
if (next === pyproject && !pyproject.includes(`version = "${tsVersion}"`)) {
  console.error('sync-python-version: no `version = "..."` line found in pyproject.toml');
  process.exit(1);
}

if (next !== pyproject) {
  writeFileSync(pyprojectPath, next);
  console.log(`sync-python-version: pyproject.toml -> ${tsVersion}`);
} else {
  console.log(`sync-python-version: already at ${tsVersion}`);
}
