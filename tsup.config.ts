import { defineConfig } from 'tsup';

// Bundles the server into `dist/`. `index.ts` is the npx/bin entry. Third-party
// deps stay external (this ships as an npm package), but the `@/*` source alias
// is resolved at build time.
export default defineConfig({
  // `index` is the npx/bin server entry; `docs-build` is the docs-index migrate+seed
  // CLI, emitted so it runs in the production image (which has no tsx/scripts/) as
  // `node dist/docs-build.js` — used by the docker-compose docs-seed service.
  entry: { index: 'src/index.ts', 'docs-build': 'scripts/docs-build.ts' },
  format: ['esm'],
  target: 'node22',
  platform: 'node',
  splitting: false,
  dts: true,
  sourcemap: true,
  clean: true,
  outDir: 'dist',
  // Shebang so `npx @keboola/mcp-server` runs directly.
  banner: { js: '#!/usr/bin/env node' },
  external: [/^[^.@]/, /^@(?!\/)/],
  // Force-bundle @keboola/api-client (+ its dayjs dependency). Its published ESM uses
  // extensionless subpath imports (e.g. `import 'dayjs/plugin/utc'`) that Node 22's strict
  // ESM resolver rejects at runtime — so if it stayed external the built `dist/index.js`
  // would crash on boot (`ERR_MODULE_NOT_FOUND: dayjs/plugin/utc`). Bundling lets esbuild
  // resolve those imports at build time. noExternal takes precedence over `external`.
  noExternal: [/^@keboola\/api-client/, 'dayjs'],
  // Resource files (flow schema/examples, system prompt, data-app code templates)
  // are read from disk at runtime via `@/resource-path` (resolves to dist/resources
  // in the bundle). Copy the tree into dist so it ships in the image and on npm.
  // Idempotent copy: rm first so a re-run can't nest into dist/resources/resources.
  onSuccess: 'rm -rf dist/resources && cp -R src/resources dist/resources',
});
