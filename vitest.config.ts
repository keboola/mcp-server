import path from 'node:path';
import { defineConfig } from 'vitest/config';

const r = (p: string) => path.resolve(import.meta.dirname, p);

export default defineConfig({
  test: {
    globals: true,
    include: ['__tests__/**/*.test.ts'],
    testTimeout: 15000,
    // Inline-process @keboola/api-client so vitest's transform pipeline resolves the
    // extensionless `dayjs/plugin/utc` imports in its chunks (Node's strict ESM resolver
    // otherwise can't; esbuild/tsup resolve them fine at build time).
    server: { deps: { inline: [/@keboola\/api-client/] } },
  },
  resolve: {
    alias: [{ find: /^@\/(.*)/, replacement: `${r('src')}/$1` }],
  },
});
