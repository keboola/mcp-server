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
});
