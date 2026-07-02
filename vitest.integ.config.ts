import path from 'node:path';
import { defineConfig } from 'vitest/config';

const r = (p: string) => path.resolve(import.meta.dirname, p);

// Integration tests run against real Keboola projects leased from the redis-backed pool
// (see integtests/testproject + feature_spec/integration-tests/RFC.md). They are slow and
// require TEST_KBC_PROJECTS_FILE (+ redis lock env on CI), so they live behind a separate
// config and the `test:integ` script — never part of the default `vitest` unit run.
export default defineConfig({
  test: {
    globals: true,
    include: ['integtests/**/*.test.ts'],
    // A leased project can wait for the pool + run real API calls; allow generous time.
    testTimeout: 120_000,
    hookTimeout: 120_000,
    // Files run in parallel workers; each worker leases its own project.
    fileParallelism: true,
    // See vitest.config.ts: inline @keboola/api-client so its extensionless dayjs imports
    // resolve under vitest.
    server: { deps: { inline: [/@keboola\/api-client/] } },
  },
  resolve: {
    alias: [{ find: /^@\/(.*)/, replacement: `${r('src')}/$1` }],
  },
});
