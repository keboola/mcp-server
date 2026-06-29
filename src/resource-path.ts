import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Locates the on-disk `resources/` directory across both run modes:
 * - source (vitest / tsx): this file is `src/resource-path.ts`, so `./resources`
 *   resolves to `src/resources`.
 * - bundled (`dist/index.js`): `./resources` resolves to `dist/resources`, where
 *   the tsup build copies the tree (and which npm publishes, since `files: [dist]`).
 *
 * Everything collapses into a single bundled module, so every caller shares this
 * file's `import.meta.url`; resolving once here avoids per-module path drift.
 */
const here = dirname(fileURLToPath(import.meta.url));
const candidates = [join(here, 'resources'), join(here, '..', 'resources')];

export const resourcesDir = candidates.find((c) => existsSync(c)) ?? candidates[0]!;

/** Builds an absolute path to a file under the resources directory. */
export const resourcePath = (...segments: string[]): string => join(resourcesDir, ...segments);
