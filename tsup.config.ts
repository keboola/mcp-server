import { defineConfig } from 'tsup';

// Bundles the server into `dist/`. `index.ts` is the npx/bin entry. Third-party
// deps stay external (this ships as an npm package), but the `@/*` source alias
// is resolved at build time.
export default defineConfig({
  entry: { index: 'src/index.ts' },
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
  // Resource files (flow schema/examples, system prompt, data-app code templates)
  // are read from disk at runtime via `@/resource-path` (resolves to dist/resources
  // in the bundle). Copy the tree into dist so it ships in the image and on npm.
  onSuccess: 'cp -R src/resources dist/resources',
});
